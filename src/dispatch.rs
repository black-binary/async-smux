use std::{
    collections::HashMap,
    io::Error,
    io::ErrorKind,
    io::Result,
    pin::Pin,
    sync::Arc,
    task::Context,
    task::Poll::{self, Ready},
};

use crate::frame::Command;

use super::frame::Frame;
use bytes::{Buf, Bytes};
use futures::prelude::*;
use futures::ready;
use futures::{
    channel::mpsc::{channel, Receiver, Sender},
    lock::Mutex,
};
use smol::{future::FutureExt, Task};

pub struct MuxDispatcher {
    local_tx_map: Arc<Mutex<HashMap<u32, Sender<Bytes>>>>,
    global_tx: Sender<Frame>,
    stream_rx: Arc<Mutex<Receiver<MuxStream>>>,
    _task: Task<()>,
}

impl MuxDispatcher {
    pub async fn get_streams_count(&self) -> usize {
        self.local_tx_map.lock().await.len()
    }

    pub fn new<T: AsyncRead + AsyncWrite + Unpin + Send + Sync + 'static>(inner: T) -> Self {
        let local_tx_map: Arc<Mutex<HashMap<u32, Sender<Bytes>>>> =
            Arc::new(Mutex::new(HashMap::new()));
        let (mut reader, mut writer) = inner.split();
        let (mut stream_tx, stream_rx) = channel(128);
        let (global_tx, mut global_rx) = channel(1024);

        let read_worker = {
            let local_tx_map = local_tx_map.clone();
            let global_tx = global_tx.clone();
            async move {
                let local_tx_map = local_tx_map.clone();
                loop {
                    let frame = Frame::read_from(&mut reader).await;
                    if frame.is_err() {
                        // TODO wrong syntax, shutdown
                        log::error!("invalid protocol");
                        return;
                    }
                    let frame = frame.unwrap();
                    match frame.command {
                        Command::Sync => {
                            let stream_id = frame.stream_id;
                            let (local_tx, local_rx) = channel(128);
                            let stream = MuxStream {
                                stream_id: stream_id,
                                tx: global_tx.clone(),
                                rx: local_rx,
                                last_read: None,
                                closed: false,
                            };
                            if stream_tx.send(stream).await.is_err() {
                                log::debug!("dispatcher closed");
                                return;
                            }
                            local_tx_map.lock().await.insert(stream_id, local_tx);
                            log::trace!("read worker insert: {:08X}", stream_id);
                        }
                        Command::Push => {
                            let mut map = local_tx_map.lock().await;
                            // TODO lock
                            if let Some(tx) = map.get_mut(&frame.stream_id) {
                                if tx.send(frame.payload).await.is_err() {
                                    map.remove(&frame.stream_id);
                                    log::trace!(
                                        "read worker: stream {:08X} closed, id removed",
                                        frame.stream_id
                                    );
                                }
                            } else {
                                log::error!("stream id {:08X} not found", frame.stream_id);
                            }
                        }
                        Command::Nop => {
                            // TODO keepalive
                        }
                        Command::Finish => {
                            local_tx_map.lock().await.remove(&frame.stream_id);
                            log::trace!("read worker: fin command {:08X}", frame.stream_id);
                        }
                    };
                }
            }
        };

        let write_worker = {
            let local_tx_map = local_tx_map.clone();
            async move {
                loop {
                    let frame = global_rx.next().await;
                    if frame.is_none() {
                        log::trace!("write worker: all streams closed");
                        return;
                    }
                    let frame = frame.unwrap();
                    if let Command::Finish = frame.command {
                        log::trace!("write worker: {:08X}", frame.stream_id);
                        local_tx_map.lock().await.remove(&frame.stream_id);
                    }
                    let result = frame.write_to(&mut writer).await;
                    if result.is_err() {
                        return;
                    }
                }
            }
        };

        let read_task = smol::spawn(read_worker);
        let write_task = smol::spawn(write_worker);

        let race_task = smol::spawn(async move {
            read_task.race(write_task).await;
        });

        Self {
            global_tx: global_tx,
            stream_rx: Arc::new(Mutex::new(stream_rx)),
            local_tx_map: local_tx_map,
            _task: race_task,
        }
    }

    pub async fn accept(&mut self) -> Result<MuxStream> {
        if let Some(stream) = self.stream_rx.lock().await.next().await {
            Ok(stream)
        } else {
            Err(Error::new(ErrorKind::BrokenPipe, "Dispacher closed"))
        }
    }

    pub async fn connect(&mut self) -> Result<MuxStream> {
        let mut stream_id: u32;
        loop {
            stream_id = rand::random();
            if !self.local_tx_map.lock().await.contains_key(&stream_id) {
                break;
            }
        }
        let (local_tx, local_rx) = channel(128);
        let global_tx = self.global_tx.clone();
        let stream = MuxStream {
            stream_id: stream_id,
            tx: global_tx,
            rx: local_rx,
            last_read: None,
            closed: false,
        };
        let frame = Frame {
            version: 1,
            stream_id,
            command: Command::Sync,
            length: 0,
            payload: Bytes::new(),
        };
        if self.global_tx.send(frame).await.is_err() {
            return Err(Error::new(
                ErrorKind::ConnectionRefused,
                "dispatcher closed",
            ));
        }
        self.local_tx_map.lock().await.insert(stream_id, local_tx);
        log::trace!("connect syn insert: {:08X}", stream_id);
        Ok(stream)
    }
}

pub struct MuxStream {
    stream_id: u32,
    rx: Receiver<Bytes>,
    tx: Sender<Frame>,
    last_read: Option<Bytes>,
    closed: bool,
}

impl MuxStream {
    pub fn get_stream_id(&self) -> u32 {
        self.stream_id
    }
}

impl Unpin for MuxStream {}

impl Drop for MuxStream {
    fn drop(&mut self) {
        if !self.closed {
            let frame = Frame {
                version: 1,
                stream_id: self.stream_id,
                command: Command::Finish,
                length: 0,
                payload: Bytes::new(),
            };
            if !self.tx.is_closed() {
                log::trace!("stream {:08X} droped", self.stream_id);
                if let Err(e) = self.tx.try_send(frame) {
                    if e.is_full() {
                        let mut tx = self.tx.clone();
                        let stream_id = self.stream_id;
                        log::trace!("stream {:08X} droped, tx full", stream_id);
                        smol::spawn(async move {
                            let frame = Frame {
                                version: 1,
                                stream_id: stream_id,
                                command: Command::Finish,
                                length: 0,
                                payload: Bytes::new(),
                            };
                            let _ = tx.send(frame).await;
                        })
                        .detach();
                    }
                }
            }
        }
        drop(self)
    }
}

impl AsyncRead for MuxStream {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut [u8],
    ) -> Poll<Result<usize>> {
        if let Some(mut recv_buf) = self.last_read.take() {
            if buf.len() >= recv_buf.len() {
                buf[..recv_buf.len()].copy_from_slice(&recv_buf[..]);
                return Ready(Ok(recv_buf.len()));
            } else {
                let buf_len = buf.len();
                buf[..].copy_from_slice(&recv_buf[..buf_len]);
                recv_buf.advance(buf_len);
                self.last_read = Some(recv_buf);
                return Ready(Ok(buf_len));
            }
        }

        if let Some(mut recv_buf) = ready!(self.rx.poll_next_unpin(cx)) {
            if buf.len() >= recv_buf.len() {
                buf[..recv_buf.len()].copy_from_slice(&recv_buf[..]);
                Ready(Ok(recv_buf.len()))
            } else {
                let buf_len = buf.len();
                buf[..].copy_from_slice(&recv_buf[..buf_len]);
                recv_buf.advance(buf_len);
                self.last_read = Some(recv_buf);
                Ready(Ok(buf_len))
            }
        } else {
            Ready(Err(Error::new(ErrorKind::ConnectionReset, "Stream closed")))
        }
    }
}

impl AsyncWrite for MuxStream {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<Result<usize>> {
        assert!(buf.len() <= 0xffff);
        ready!(self.tx.poll_ready(cx))
            .map_err(|_| Error::new(ErrorKind::ConnectionReset, "stream closed"))?;
        let frame = Frame {
            version: 1,
            stream_id: self.stream_id,
            command: Command::Push,
            length: buf.len() as u16,
            payload: buf.clone().to_bytes(),
        };
        self.tx.try_send(frame).unwrap();
        Ready(Ok(buf.len()))
    }

    fn poll_flush(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<Result<()>> {
        Ready(Ok(()))
    }

    fn poll_close(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<()>> {
        ready!(self.tx.poll_ready(cx))
            .map_err(|_| Error::new(ErrorKind::NotConnected, "Stream closed"))?;
        let frame = Frame {
            version: 1,
            stream_id: self.stream_id,
            command: Command::Finish,
            length: 0,
            payload: Bytes::new(),
        };
        // TODO loop
        self.tx.try_send(frame).unwrap();
        self.rx.close();
        self.closed = true;
        Ready(Ok(()))
    }
}

#[cfg(test)]
mod test {
    use std::time::Duration;

    use super::{MuxDispatcher, MuxStream};
    use rand::prelude::*;
    use smol::net::{TcpListener, TcpStream};
    use smol::prelude::*;

    #[test]
    fn drop() {
        std::env::set_var("SMOL_THREADS", "4");
        smol::block_on(async {
            // init
            let listener = TcpListener::bind("0.0.0.0:0").await.unwrap();
            let local_addr = listener.local_addr().unwrap();

            // server
            let t = smol::spawn(async move {
                let (stream, _) = listener.accept().await.unwrap();
                let mut dispatcher = MuxDispatcher::new(stream);

                let _stream1 = dispatcher.accept().await.unwrap();
                let _stream2 = dispatcher.accept().await.unwrap();
                let _stream3 = dispatcher.accept().await.unwrap();

                let mut invalid_streams = Vec::new();

                for _ in 0..100 {
                    let stream = dispatcher.accept().await.unwrap();
                    invalid_streams.push(stream);
                }

                smol::Timer::after(Duration::from_secs(5)).await;
                assert_eq!(dispatcher.get_streams_count().await, 3);
            });

            // client
            let stream = TcpStream::connect(local_addr).await.unwrap();
            let mut dispatcher = MuxDispatcher::new(stream);

            let _stream1 = dispatcher.connect().await.unwrap();
            assert_eq!(dispatcher.get_streams_count().await, 1);
            let _stream2 = dispatcher.connect().await.unwrap();
            assert_eq!(dispatcher.get_streams_count().await, 2);
            let _stream3 = dispatcher.connect().await.unwrap();
            assert_eq!(dispatcher.get_streams_count().await, 3);

            // connect and drop
            for _ in 0..100 {
                let _ = dispatcher.connect().await.unwrap();
            }

            smol::Timer::after(Duration::from_secs(3)).await;

            assert_eq!(dispatcher.get_streams_count().await, 3);
            t.await
        });
    }

    #[test]
    fn concurrent() {
        smol::block_on(async {
            let listener = TcpListener::bind("0.0.0.0:0").await.unwrap();
            let local_addr = listener.local_addr().unwrap();

            let mut random_payload = [0u8; 0x1000];
            for i in 0..random_payload.len() {
                random_payload[i] = random();
            }

            smol::spawn(async move {
                let (stream, _) = listener.accept().await.unwrap();
                let mut mux = MuxDispatcher::new(stream);
                let mut tasks = Vec::new();
                for i in 0..100 {
                    let mut stream = mux.accept().await.unwrap();
                    let id = i;
                    let task = smol::spawn(async move {
                        let send_buf = [id as u8; 1024];
                        stream.write_all(&send_buf).await.unwrap();
                        stream.close().await.unwrap();
                        println!("thread {} closed", id);
                    });
                    tasks.push(task);
                }
                for t in tasks {
                    t.await;
                }
                let mut streams = Vec::new();
                for _ in 0i32..3 {
                    streams.push(mux.connect().await.unwrap());
                }
                streams[0].write_all(&random_payload).await.unwrap();
                for s in streams.iter_mut() {
                    let mut send_buf = [0u8; 1024];
                    assert!(s.read(&mut send_buf).await.is_err());
                }
                smol::Timer::after(Duration::from_secs(5)).await;
            })
            .detach();

            let stream = TcpStream::connect(local_addr).await.unwrap();
            let mut mux = MuxDispatcher::new(stream);

            let mut tasks = Vec::new();

            for i in 0i32..100 {
                let mut stream: MuxStream = mux.connect().await.unwrap();
                let id = i;
                let task = smol::spawn(async move {
                    let mut recv_buf = [0u8; 1024];
                    stream.read_exact(&mut recv_buf).await.unwrap();
                    println!(
                        "id={}, buf={}, stream_id = {:08X}",
                        id,
                        recv_buf[0],
                        stream.get_stream_id()
                    );
                });
                tasks.push(task);
            }

            for i in tasks {
                i.await;
            }

            let mut streams = Vec::new();
            for _ in 0i32..3 {
                streams.push(mux.accept().await.unwrap());
            }

            let mut payload = Vec::new();

            let mut my_stream = streams.remove(0);
            while payload.len() != 0x1000 {
                let mut small_buf = [0u8; 0x100];
                my_stream.read_exact(&mut small_buf).await.unwrap();
                payload.extend_from_slice(&small_buf[..]);
            }

            assert_eq!(&payload, &random_payload);

            for s in streams.iter_mut() {
                s.close().await.unwrap();
            }
        });
    }

    #[test]
    fn simple_parsing() {
        smol::block_on(async {
            let listener = TcpListener::bind("0.0.0.0:0").await.unwrap();
            let local_addr = listener.local_addr().unwrap();
            smol::spawn(async move {
                let (server_stream, _) = listener.accept().await.unwrap();
                let mut server_dispatcher = MuxDispatcher::new(server_stream);
                let mut server_mux_stream = server_dispatcher.accept().await.unwrap();
                server_mux_stream
                    .write_all(b"testtest12345678")
                    .await
                    .unwrap();
            })
            .detach();
            let client_stream = TcpStream::connect(local_addr).await.unwrap();
            let mut client_dispatcher = MuxDispatcher::new(client_stream);
            let mut client_mux_stream = client_dispatcher.connect().await.unwrap();
            let mut buf = [0u8; 1024];
            let size = client_mux_stream.read(&mut buf).await.unwrap();
            let payload = String::from_utf8(buf[..size].to_vec()).unwrap();
            assert_eq!(payload, "testtest12345678");
        });
    }
}
