use std::{
    collections::HashMap,
    collections::VecDeque,
    io::Error,
    io::ErrorKind,
    io::Result,
    pin::Pin,
    sync::Arc,
    task::Context,
    task::Poll::{self, Ready},
};

use bytes::{Buf, Bytes};
use frame::MAX_PAYLOAD_LENGTH;
use frame::{Command, Frame};
use futures::prelude::*;
use futures::ready;
use futures::{
    channel::mpsc::{channel, Receiver, Sender},
    lock::Mutex,
};
use smol::{future::FutureExt, Task};

mod frame;

/// A smux dispatcher.
///
/// A dispatcher can establish multiple `MuxStream` over the underlying stream.
///
#[derive(Clone)]
pub struct MuxDispatcher {
    local_frame_tx_map: Arc<Mutex<HashMap<u32, Sender<Frame>>>>,
    global_frame_rx: Sender<Frame>,
    stream_rx: Arc<Mutex<Receiver<MuxStream>>>,
    _task: Arc<Task<()>>,
}

impl MuxDispatcher {
    /// Gets the number of connections handled by this dispatcher
    pub async fn get_streams_count(&self) -> usize {
        self.local_frame_tx_map.lock().await.len()
    }

    /// Consumes an async stream to crate a new dispatcher
    pub fn new<T: AsyncRead + AsyncWrite + Send + 'static>(inner: T) -> Self {
        let local_tx_map = Arc::new(Mutex::new(HashMap::new()));
        let (mut reader, mut writer) = inner.split();
        let (mut stream_tx, stream_rx) = channel(0x10);
        let (global_frame_tx, mut global_frame_rx) = channel(0x500);

        let read_worker = {
            let local_frame_tx_map = local_tx_map.clone();
            let global_frame_tx = global_frame_tx.clone();
            async move {
                let local_frame_tx_map = local_frame_tx_map.clone();
                loop {
                    let frame = Frame::read_from(&mut reader).await;
                    if let Err(e) = frame {
                        log::error!("closing underlying stream: {}", e);
                        return;
                    }
                    let frame = frame.unwrap();
                    match frame.header.command {
                        Command::Sync => {
                            let stream_id = frame.header.stream_id;
                            let (local_frame_tx, local_frame_rx) = channel(0x100);
                            let stream = MuxStream {
                                stream_id: stream_id,
                                tx: global_frame_tx.clone(),
                                rx: local_frame_rx,
                                read_buf: None,
                                write_buf: VecDeque::new(),
                                closed: false,
                            };
                            if stream_tx.send(stream).await.is_err() {
                                log::debug!("dispatcher closed");
                                return;
                            }
                            local_frame_tx_map
                                .lock()
                                .await
                                .insert(stream_id, local_frame_tx);
                            log::trace!("read worker insert: {:08X}", stream_id);
                        }
                        Command::Push => {
                            if let Some(tx) = local_frame_tx_map
                                .lock()
                                .await
                                .get_mut(&frame.header.stream_id)
                            {
                                let stream_id = frame.header.stream_id;
                                if tx.send(frame).await.is_err() {
                                    local_frame_tx_map.lock().await.remove(&stream_id);
                                    log::trace!(
                                        "read worker: stream {:08X} closed, id removed",
                                        stream_id
                                    );
                                }
                            } else {
                                log::error!("stream id {:08X} not found", frame.header.stream_id);
                            }
                        }
                        Command::Nop => {}
                        Command::Finish => {
                            local_frame_tx_map
                                .lock()
                                .await
                                .remove(&frame.header.stream_id);
                            log::trace!("read worker: fin command {:08X}", frame.header.stream_id);
                        }
                    };
                }
            }
        };

        let write_worker = {
            let local_tx_map = local_tx_map.clone();
            async move {
                loop {
                    let frame = global_frame_rx.next().await;
                    if frame.is_none() {
                        log::trace!("write worker: all streams closed");
                        return;
                    }
                    let frame = frame.unwrap();
                    if let Command::Finish = frame.header.command {
                        log::trace!("write worker: {:08X}", frame.header.stream_id);
                        local_tx_map.lock().await.remove(&frame.header.stream_id);
                    }
                    let result = frame.write_to(&mut writer).await;
                    if let Err(e) = result {
                        log::error!("failed to write frame to stream: {}", e.to_string());
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
            global_frame_rx: global_frame_tx,
            stream_rx: Arc::new(Mutex::new(stream_rx)),
            local_frame_tx_map: local_tx_map,
            _task: Arc::new(race_task),
        }
    }

    /// Accepts a new smux stream from the peer
    pub async fn accept(&mut self) -> Result<MuxStream> {
        if let Some(stream) = self.stream_rx.lock().await.next().await {
            Ok(stream)
        } else {
            Err(Error::new(ErrorKind::BrokenPipe, "dispacher closed"))
        }
    }

    /// Spawns a new smux stream to the peer
    pub async fn connect(&mut self) -> Result<MuxStream> {
        let mut stream_id: u32;
        loop {
            stream_id = rand::random();
            if !self
                .local_frame_tx_map
                .lock()
                .await
                .contains_key(&stream_id)
            {
                break;
            }
        }
        let (local_frame_tx, local_frame_rx) = channel(0x100);
        let global_frame_tx = self.global_frame_rx.clone();
        let stream = MuxStream {
            stream_id: stream_id,
            tx: global_frame_tx,
            rx: local_frame_rx,
            read_buf: None,
            write_buf: VecDeque::new(),
            closed: false,
        };
        let frame = Frame::new_sync_frame(stream_id);
        if self.global_frame_rx.send(frame).await.is_err() {
            return Err(Error::new(
                ErrorKind::ConnectionRefused,
                "dispatcher closed",
            ));
        }
        self.local_frame_tx_map
            .lock()
            .await
            .insert(stream_id, local_frame_tx);
        log::trace!("connect syn insert: {:08X}", stream_id);
        Ok(stream)
    }
}

/// A smux data stream.
///
/// A `MuxStream` is created by `MuxDispatcher`. It implements the AsyncRead and AsyncWrite traits.
pub struct MuxStream {
    stream_id: u32,
    rx: Receiver<Frame>,
    tx: Sender<Frame>,
    read_buf: Option<Bytes>,
    write_buf: VecDeque<Frame>,
    closed: bool,
}

impl MuxStream {
    pub fn get_stream_id(&self) -> u32 {
        self.stream_id
    }
}

impl Drop for MuxStream {
    fn drop(&mut self) {
        if !self.closed {
            let stream_id = self.stream_id;
            let frame = Frame::new_finish_frame(stream_id);
            if !self.tx.is_closed() {
                log::trace!("stream {:08X} droped", self.stream_id);
                if let Err(e) = self.tx.try_send(frame) {
                    if e.is_full() {
                        let mut tx = self.tx.clone();
                        let stream_id = self.stream_id;
                        log::trace!("stream {:08X} dropped, tx full", stream_id);
                        smol::spawn(async move {
                            let frame = Frame::new_finish_frame(stream_id);
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
        if let Some(mut recv_buf) = self.read_buf.take() {
            if buf.len() >= recv_buf.len() {
                buf[..recv_buf.len()].copy_from_slice(&recv_buf[..]);
                return Ready(Ok(recv_buf.len()));
            } else {
                let buf_len = buf.len();
                buf[..].copy_from_slice(&recv_buf[..buf_len]);
                recv_buf.advance(buf_len);
                self.read_buf = Some(recv_buf);
                return Ready(Ok(buf_len));
            }
        }

        if let Some(frame) = ready!(self.rx.poll_next_unpin(cx)) {
            let payload = frame.get_payload();
            if buf.len() >= payload.len() {
                buf[..payload.len()].copy_from_slice(&payload[..]);
                Ready(Ok(payload.len()))
            } else {
                let buf_len = buf.len();
                buf[..].copy_from_slice(&payload[..buf_len]);
                self.read_buf = Some(Bytes::copy_from_slice(&payload[buf_len..]));
                Ready(Ok(buf_len))
            }
        } else {
            Ready(Err(Error::new(ErrorKind::ConnectionReset, "stream closed")))
        }
    }
}

impl MuxStream {
    fn flush_write_buf(&mut self, cx: &mut Context) -> Poll<Result<()>> {
        loop {
            if self.write_buf.is_empty() {
                break;
            }
            ready!(self.tx.poll_ready(cx))
                .map_err(|e| Error::new(ErrorKind::ConnectionReset, e))?;
            let frame = self.write_buf.front().unwrap().clone();
            self.tx
                .try_send(frame)
                .map_err(|e| Error::new(ErrorKind::ConnectionReset, e))?;
            self.write_buf.pop_front();
        }
        Ready(Ok(()))
    }
}

impl AsyncWrite for MuxStream {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<Result<usize>> {
        ready!(self.flush_write_buf(cx))?;
        ready!(self.tx.poll_ready(cx)).map_err(|e| Error::new(ErrorKind::ConnectionReset, e))?;
        if buf.len() <= MAX_PAYLOAD_LENGTH {
            let frame = Frame::new_push_frame(self.stream_id, buf);
            self.tx
                .try_send(frame)
                .map_err(|e| Error::new(ErrorKind::ConnectionReset, e))?;
        } else {
            let frame = Frame::new_push_frame(self.stream_id, &buf[..MAX_PAYLOAD_LENGTH]);
            self.tx
                .try_send(frame)
                .map_err(|e| Error::new(ErrorKind::ConnectionReset, e))?;
            let leftover = &buf[MAX_PAYLOAD_LENGTH..];
            let frame_count = leftover.len() / MAX_PAYLOAD_LENGTH + 1;
            for i in 0..frame_count - 1 {
                let frame = Frame::new_push_frame(
                    self.stream_id,
                    &leftover[i * MAX_PAYLOAD_LENGTH..(i + 1) * MAX_PAYLOAD_LENGTH],
                );
                self.write_buf.push_back(frame);
            }
            let frame = Frame::new_push_frame(
                self.stream_id,
                &leftover[(frame_count - 1) * MAX_PAYLOAD_LENGTH..],
            );
            self.write_buf.push_back(frame);
        }
        Ready(Ok(buf.len()))
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<()>> {
        ready!(self.flush_write_buf(cx))?;
        ready!(self.tx.poll_ready(cx)).map_err(|e| Error::new(ErrorKind::ConnectionReset, e))?;
        Ready(Ok(()))
    }

    fn poll_close(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<()>> {
        ready!(self.flush_write_buf(cx))?;
        let frame = Frame::new_finish_frame(self.stream_id);
        ready!(self.tx.poll_ready(cx)).map_err(|e| Error::new(ErrorKind::NotConnected, e))?;
        self.tx
            .try_send(frame)
            .map_err(|e| Error::new(ErrorKind::ConnectionReset, e))?;
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
                        stream.write(&send_buf).await.unwrap();
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

    #[test]
    fn huge_payload() {
        smol::block_on(async {
            let listener = TcpListener::bind("0.0.0.0:0").await.unwrap();
            let local_addr = listener.local_addr().unwrap();
            let mut buf = Vec::new();
            buf.resize(0x20000, 0);
            rand::thread_rng().fill_bytes(&mut buf);

            let buf1 = buf.clone();
            smol::spawn(async move {
                let (server_stream, _) = listener.accept().await.unwrap();
                let mut server_dispatcher = MuxDispatcher::new(server_stream);
                let mut server_mux_stream = server_dispatcher.accept().await.unwrap();
                server_mux_stream.write_all(&buf).await.unwrap();
                server_mux_stream.close().await.unwrap();
                println!("done");
            })
            .detach();
            let client_stream = TcpStream::connect(local_addr).await.unwrap();
            let mut client_dispatcher = MuxDispatcher::new(client_stream);
            let mut client_mux_stream = client_dispatcher.connect().await.unwrap();
            let mut buf2 = Vec::new();
            buf2.resize(0x20000, 0);
            client_mux_stream.read_exact(&mut buf2).await.unwrap();
            assert_eq!(buf2, buf1);
        });
    }
}
