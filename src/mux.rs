use std::{
    cmp::min,
    collections::HashMap,
    io,
    pin::Pin,
    sync::atomic::Ordering,
    sync::{atomic::AtomicU64, Arc},
    task::{Context, Poll},
    time::SystemTime,
};

use async_channel::{bounded, Receiver, Sender};
use async_lock::Mutex;
use bytes::{Buf, Bytes};
use futures::{io::ReadHalf, io::WriteHalf, ready, AsyncRead, AsyncReadExt, AsyncWrite, Future};
use futures_lite::FutureExt as LiteFutureExt;

use crate::{
    frame::{Command, Frame, FrameIo},
    Error, MuxConfig,
};

type ReadFrameFuture = Pin<Box<dyn Future<Output = crate::Result<Frame>> + Send>>;
type WriteFrameFuture = Pin<Box<dyn Future<Output = crate::Result<()>> + Send>>;
type StreamFuture<T> = Pin<Box<dyn Future<Output = crate::Result<MuxStream<T>>> + Send>>;

fn now_sec() -> u64 {
    SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .unwrap()
        .as_secs()
}

/// A multiplexer/demultiplexer for AsyncRead / AsyncWrite streams.
///
/// `Mux` can accept new `MuxStream` from the remote peer, or connect new `MuxStream` to the remote peer.
/// Note that if the remote peer is trying to connect to the local `Mux`, while there is no task accepting the new `MuxStream` on the local side, the `Mux` and all related `MuxStream`s might get pended.
///
#[derive(Clone)]
pub struct Mux<T> {
    mux_future_generator: Arc<MuxFutureGenerator<T>>,
}

impl<T: AsyncRead + AsyncWrite + Send + Unpin + 'static> Mux<T> {
    /// Gets the number of currently established `MuxStream`s.
    pub async fn stream_count(&self) -> usize {
        self.mux_future_generator.stream_meta.lock().await.len()
    }

    /// Cleans all dropped `MuxStream`s immediately.
    pub async fn clean(&self) -> crate::Result<()> {
        self.mux_future_generator.force_clean().await
    }

    /// Accepts a new `MuxStream` from the remote peer.
    pub async fn accept(&self) -> crate::Result<MuxStream<T>> {
        self.mux_future_generator.clone().accept().await
    }

    /// Connects a new `MuxStream` to the remote peer.
    pub async fn connect(&self) -> crate::Result<MuxStream<T>> {
        self.mux_future_generator.clone().connect().await
    }

    /// Creates a new `Mux`
    ///
    /// # Panic
    /// Panics if the config is not valid. Call `MuxConfig::check()` to make sure the config is valid.
    pub fn new(inner: T, config: MuxConfig) -> Self {
        let (accept_stream_tx, accept_stream_rx) = bounded(config.stream_buffer_size);
        let (reader, writer) = inner.split();
        let config = Arc::new(config);

        let frame_reader = Arc::new(Mutex::new(FrameIo::new(reader)));
        let frame_writer = Arc::new(Mutex::new(FrameIo::new(writer)));
        let stream_meta = Arc::new(Mutex::new(HashMap::new()));

        let frame_future_generator = Arc::new(FrameFutureGenerator {
            stream_meta: stream_meta.clone(),
            frame_writer: frame_writer.clone(),
            frame_reader: frame_reader.clone(),
            accept_stream_tx,
            config: config.clone(),
        });

        let mux_future_generator = Arc::new(MuxFutureGenerator {
            stream_meta: stream_meta.clone(),
            accept_stream_rx,
            frame_writer: frame_writer.clone(),
            frame_reader: frame_reader.clone(),
            frame_future_generator,
            last_clean: Arc::new(AtomicU64::new(now_sec())),
            config,
        });

        Self {
            mux_future_generator,
        }
    }
}

struct Metadata {
    frame_tx: Sender<Frame>,
    frame_rx: Receiver<Frame>,
    counter: Arc<()>,
}

impl Drop for Metadata {
    fn drop(&mut self) {
        self.frame_tx.close();
        self.frame_rx.close();
    }
}

struct MuxFutureGenerator<T> {
    stream_meta: Arc<Mutex<HashMap<u32, Metadata>>>,
    accept_stream_rx: Receiver<u32>,
    frame_reader: Arc<Mutex<FrameIo<ReadHalf<T>>>>,
    frame_writer: Arc<Mutex<FrameIo<WriteHalf<T>>>>,
    frame_future_generator: Arc<FrameFutureGenerator<T>>,
    last_clean: Arc<AtomicU64>,
    config: Arc<MuxConfig>,
}

impl<T: AsyncRead + AsyncWrite + Unpin + Send + 'static> MuxFutureGenerator<T> {
    async fn obtain_accept_stream(&self, stream_id: u32) -> MuxStream<T> {
        let counter = {
            let stream_meta = self.stream_meta.lock().await;
            stream_meta.get(&stream_id).unwrap().counter.clone()
        };
        let stream = MuxStream {
            stream_id: stream_id,
            read_state: ReadState::Idle,
            write_state: WriteState::Idle,
            close_state: CloseState::Idle,
            future_generator: self.frame_future_generator.clone(),
            max_payload_size: self.config.max_payload_size,
            _counter: counter,
        };
        stream
    }

    async fn insert_connect_stream(&self, stream_id: u32) -> MuxStream<T> {
        let (frame_tx, frame_rx) = bounded(self.config.frame_buffer_size);
        let counter = Arc::new(());
        {
            let mut stream_meta = self.stream_meta.lock().await;
            stream_meta.insert(
                stream_id,
                Metadata {
                    frame_tx,
                    frame_rx,
                    counter: counter.clone(),
                },
            );
        }
        let stream = MuxStream {
            stream_id: stream_id,
            read_state: ReadState::Idle,
            write_state: WriteState::Idle,
            close_state: CloseState::Idle,
            future_generator: self.frame_future_generator.clone(),
            max_payload_size: self.config.max_payload_size,
            _counter: counter,
        };
        stream
    }

    async fn remove_stream(&self, stream_id: u32) {
        let mut stream_meta = self.stream_meta.lock().await;
        stream_meta.remove(&stream_id);
    }

    async fn force_clean(&self) -> crate::Result<()> {
        let mut dead = Vec::new();
        {
            let stream_meta = self.stream_meta.lock().await;
            for (id, m) in stream_meta.iter() {
                if std::sync::Arc::<()>::strong_count(&m.counter) == 1 {
                    dead.push(*id)
                }
            }
        }
        for id in dead.iter() {
            let frame = Frame::new_finish_frame(*id);
            self.frame_future_generator.write_frame(&frame).await?;
        }
        self.last_clean.store(now_sec(), Ordering::Relaxed);
        Ok(())
    }

    async fn clean(&self) -> crate::Result<()> {
        let now = now_sec();
        let duration = self.config.clean_duration;
        if now - self.last_clean.load(Ordering::Relaxed) > duration {
            self.force_clean().await?;
        }
        Ok(())
    }

    fn connect(self: Arc<Self>) -> StreamFuture<T> {
        let frame_writer = self.frame_writer.clone();
        Box::pin(async move {
            self.clean().await?;
            let mut stream_id = rand::random();
            {
                let stream_meta = self.stream_meta.lock().await;
                while stream_meta.contains_key(&stream_id) {
                    stream_id = rand::random();
                }
            }
            let frame = Frame::new_sync_frame(stream_id);
            let stream = self.insert_connect_stream(stream_id).await;
            let mut frame_writer = frame_writer.lock().await;
            frame_writer.write_frame(&frame).await?;
            Ok(stream)
        })
    }

    fn accept(self: Arc<Self>) -> StreamFuture<T> {
        let frame_reader = self.frame_reader.clone();
        let stream_meta = self.stream_meta.clone();
        let accept_stream_rx = self.accept_stream_rx.clone();
        Box::pin(async move {
            let channel_fut = {
                let this = self.clone();
                async move {
                    let stream_id = accept_stream_rx
                        .recv()
                        .await
                        .map_err(|_| Error::MuxClosed)?;
                    Ok(this.obtain_accept_stream(stream_id).await)
                }
            };

            let loop_fut = async move {
                self.clean().await?;
                let mut frame_reader_guard = frame_reader.lock().await;
                loop {
                    let frame = frame_reader_guard.read_frame().await?;

                    match frame.header.command {
                        Command::Sync => {
                            return Ok(self.insert_connect_stream(frame.header.stream_id).await);
                        }
                        Command::Finish => {
                            self.remove_stream(frame.header.stream_id).await;
                            log::trace!("read finish packet {:X}", frame.header.stream_id);
                            continue;
                        }
                        Command::Push => {
                            let stream_id = frame.header.stream_id;
                            let frame_tx = {
                                let mut stream_meta = stream_meta.lock().await;
                                if let Some(m) = stream_meta.get_mut(&frame.header.stream_id) {
                                    m.frame_tx.clone()
                                } else {
                                    return Err(Error::MuxStreamClosed(stream_id));
                                }
                            };
                            let stream_id = frame.header.stream_id;
                            if frame_tx.send(frame).await.is_err() {
                                let mut stream_meta = stream_meta.lock().await;
                                stream_meta.remove(&stream_id);
                                log::error!("push frame to closed stream {:X}", stream_id);
                            }
                        }
                        Command::Nop => {
                            // TODO update timestamp
                        }
                    }
                }
            };
            loop_fut.race(channel_fut).await
        })
    }
}

struct FrameFutureGenerator<T> {
    stream_meta: Arc<Mutex<HashMap<u32, Metadata>>>,
    accept_stream_tx: Sender<u32>,
    frame_reader: Arc<Mutex<FrameIo<ReadHalf<T>>>>,
    frame_writer: Arc<Mutex<FrameIo<WriteHalf<T>>>>,
    config: Arc<MuxConfig>,
}

impl<T: AsyncRead + AsyncWrite + Unpin + Send + 'static> FrameFutureGenerator<T> {
    fn read_frame(&self, stream_id: u32) -> ReadFrameFuture {
        let stream_meta = self.stream_meta.clone();
        let accept_stream_tx = self.accept_stream_tx.clone();
        let frame_reader = self.frame_reader.clone();
        let frame_buffer_size = self.config.frame_buffer_size;
        Box::pin(async move {
            let channel_fut = {
                let stream_meta = stream_meta.clone();
                async move {
                    {
                        let frame_rx = {
                            let stream_meta = stream_meta.lock().await;
                            if let Some(m) = stream_meta.get(&stream_id) {
                                m.frame_rx.clone()
                            } else {
                                return Err(Error::MuxStreamClosed(stream_id));
                            }
                        };
                        let frame = frame_rx
                            .recv()
                            .await
                            .map_err(|_| Error::MuxStreamClosed(stream_id))?;
                        assert_eq!(frame.header.stream_id, stream_id);
                        assert_eq!(frame.header.command, Command::Push);
                        Ok(frame)
                    }
                }
            };

            let loop_fut = async move {
                // Keep locking the reader
                let mut frame_reader_guard = frame_reader.lock().await;
                loop {
                    let frame = frame_reader_guard.read_frame().await?;
                    log::trace!("read {:?}", frame);
                    match frame.header.command {
                        Command::Sync => {
                            let (frame_tx, frame_rx) = bounded(frame_buffer_size);
                            {
                                let mut stream_meta = stream_meta.lock().await;
                                stream_meta.insert(
                                    frame.header.stream_id,
                                    Metadata {
                                        frame_tx,
                                        frame_rx,
                                        counter: Arc::new(()),
                                    },
                                );
                            }
                            accept_stream_tx
                                .send(frame.header.stream_id)
                                .await
                                .map_err(|_| Error::MuxClosed)?;
                            log::trace!("read sync packet {:X}", frame.header.stream_id);
                            continue;
                        }
                        Command::Finish => {
                            let mut stream_meta = stream_meta.lock().await;
                            stream_meta.remove(&frame.header.stream_id);
                            log::trace!("read finish packet {:X}", frame.header.stream_id);
                            continue;
                        }
                        Command::Push => {
                            if frame.header.stream_id == stream_id {
                                return Ok(frame);
                            }
                            let frame_tx = {
                                let mut stream_meta = stream_meta.lock().await;
                                if let Some(m) = stream_meta.get_mut(&frame.header.stream_id) {
                                    m.frame_tx.clone()
                                } else {
                                    debug_assert!(false);
                                    log::error!("stream closed: {}", stream_id);
                                    continue;
                                }
                            };
                            let stream_id = frame.header.stream_id;
                            if frame_tx.send(frame).await.is_err() {
                                let mut stream_meta = stream_meta.lock().await;
                                stream_meta.remove(&stream_id);
                                log::error!("push frame to closed stream {:X}", stream_id);
                            }
                        }
                        Command::Nop => {
                            // TODO update timestamp
                        }
                    }
                }
            };
            loop_fut.race(channel_fut).await
        })
    }

    fn write_frame(&self, frame: &Frame) -> WriteFrameFuture {
        let stream_meta = self.stream_meta.clone();
        let frame_writer = self.frame_writer.clone();
        let frame = frame.clone();
        Box::pin(async move {
            log::trace!("write {:?}", frame);
            match frame.header.command {
                Command::Sync => {
                    log::trace!("spawning new connect stream");
                }
                Command::Finish => {
                    let mut stream_meta = stream_meta.lock().await;
                    stream_meta.remove(&frame.header.stream_id);
                }
                Command::Push => {}
                Command::Nop => {}
            }
            let mut frame_writer = frame_writer.lock().await;
            frame_writer.write_frame(&frame).await?;
            Ok(())
        })
    }
}

enum ReadState {
    Idle,
    Reading(Bytes),
    Polling(ReadFrameFuture),
}

enum WriteState {
    Idle,
    Polling(WriteFrameFuture, usize),
}

enum CloseState {
    Idle,
    Polling(WriteFrameFuture),
    Closed,
}

/// A virtual AsyncRead + AsyncWrite stream spawned by `Mux`.
///
/// Async `read()` `write()` method can read or write at most 65535 bytes for each call. Use `read_exact()` and `write_exact()` to read or write huge payload instead.
///
/// It's ok to drop `MuxStream` directly without calling `close()`. `Mux` will try to notify the remote side to close the virtual stream in the next `connect()` or `accept()` call.
/// But it's still recommended to close the stream explicitly after using.
pub struct MuxStream<T> {
    stream_id: u32,
    future_generator: Arc<FrameFutureGenerator<T>>,
    read_state: ReadState,
    write_state: WriteState,
    close_state: CloseState,
    max_payload_size: usize,
    _counter: Arc<()>,
}

impl<T> MuxStream<T> {
    /// Gets the unique stream ID.
    pub fn stream_id(&self) -> u32 {
        self.stream_id
    }
}

impl<T: AsyncRead + AsyncWrite + Unpin + Send + 'static> AsyncRead for MuxStream<T> {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut [u8],
    ) -> Poll<io::Result<usize>> {
        if let CloseState::Closed = self.close_state {
            return Poll::Ready(Err(Error::MuxStreamClosed(self.stream_id).into()));
        }
        loop {
            match self.read_state {
                ReadState::Idle => {
                    let fut = self.future_generator.read_frame(self.stream_id);
                    self.read_state = ReadState::Polling(fut);
                    continue;
                }
                ReadState::Polling(ref mut fut) => {
                    let frame = ready!(fut.as_mut().poll(cx)).map_err(|e| {
                        self.close_state = CloseState::Closed;
                        e
                    })?;
                    self.read_state = ReadState::Reading(frame.get_payload());
                    continue;
                }
                ReadState::Reading(ref mut payload) => {
                    if payload.len() > buf.len() {
                        payload.copy_to_slice(&mut buf[..]);
                        return Poll::Ready(Ok(buf.len()));
                    } else {
                        let len = payload.len();
                        buf[..len].copy_from_slice(&payload);
                        self.read_state = ReadState::Idle;
                        return Poll::Ready(Ok(len));
                    }
                }
            }
        }
    }
}

impl<T: AsyncRead + AsyncWrite + Unpin + Send + 'static> AsyncWrite for MuxStream<T> {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        if let CloseState::Closed = self.close_state {
            return Poll::Ready(Err(Error::MuxStreamClosed(self.stream_id).into()));
        }
        loop {
            match self.write_state {
                WriteState::Idle => {
                    let len = min(buf.len(), self.max_payload_size);
                    let frame = Frame::new_push_frame(self.stream_id, &buf[..len]);
                    let fut = self.future_generator.write_frame(&frame);
                    self.write_state = WriteState::Polling(fut, len);
                    continue;
                }
                WriteState::Polling(ref mut fut, len) => {
                    ready!(fut.as_mut().poll(cx)).map_err(|e| {
                        self.close_state = CloseState::Closed;
                        e
                    })?;
                    self.write_state = WriteState::Idle;
                    return Poll::Ready(Ok(len));
                }
            }
        }
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        if let WriteState::Polling(ref mut fut, _) = self.write_state {
            ready!(fut.as_mut().poll(cx))?;
        }
        Poll::Ready(Ok(()))
    }

    fn poll_close(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        loop {
            match self.close_state {
                CloseState::Idle => {
                    let frame = Frame::new_finish_frame(self.stream_id);
                    let fut = self.future_generator.write_frame(&frame);
                    self.close_state = CloseState::Polling(fut);
                    continue;
                }
                CloseState::Polling(ref mut fut) => {
                    ready!(fut.as_mut().poll(cx))?;
                    self.close_state = CloseState::Closed;
                    return Poll::Ready(Ok(()));
                }
                CloseState::Closed => return Poll::Ready(Ok(())),
            }
        }
    }
}

#[cfg(test)]
mod test {
    use async_channel::bounded;
    use futures::{AsyncWriteExt, StreamExt};
    use log::LevelFilter;
    use rand::prelude::*;
    use smol::net::{TcpListener, TcpStream};

    use super::*;

    fn init() {
        let _ = env_logger::builder()
            .filter_level(LevelFilter::Info)
            .try_init();
        std::env::set_var("SMOL_THREADS", "16");
    }

    async fn get_tcp_stream_pair() -> (TcpStream, TcpStream) {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let local_addr = listener.local_addr().unwrap();
        let (tx, rx) = bounded(1);
        smol::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            tx.send(stream).await.unwrap();
        })
        .detach();
        let client_stream = TcpStream::connect(local_addr).await.unwrap();
        let server_stream = rx.recv().await.unwrap();
        (client_stream, server_stream)
    }

    #[test]
    fn connect_accept() {
        init();
        smol::block_on(async move {
            let (a, b) = get_tcp_stream_pair().await;
            let t = smol::spawn(async move {
                let mux_a = Mux::new(a, MuxConfig::default());
                let mut a = mux_a.connect().await.unwrap();
                a.write_all(b"hello1").await.unwrap();
                let mut buf = [0u8; 0x100];
                a.read(&mut buf).await.unwrap();
            });
            let mux_b = Mux::new(b, MuxConfig::default());
            let mut b = mux_b.accept().await.unwrap();
            let mut buf = [0u8; 0x100];
            let size = b.read(&mut buf).await.unwrap();
            log::debug!("{:?}", &buf[..size]);
            b.write_all(b"hello2").await.unwrap();
            t.await;
        });
    }

    #[test]
    fn concurrent_connect_accept() {
        init();
        smol::block_on(async move {
            let (a, b) = get_tcp_stream_pair().await;
            const STREAM_COUNT: i32 = 100;
            let t = smol::spawn(async move {
                let mux_a = Mux::new(a, MuxConfig::default());
                let (tx, mut rx) = bounded(0x100);
                for i in 0..STREAM_COUNT {
                    let mut stream = mux_a.connect().await.unwrap();
                    log::debug!("connected: {}", i);
                    let payload = [i as u8; 1];
                    let tx = tx.clone();
                    smol::spawn(async move {
                        stream.write_all(&payload).await.unwrap();
                        let mut buf = [0u8; 0x100];
                        let size = stream.read(&mut buf).await.unwrap();
                        assert_eq!(size, payload.len());
                        assert_eq!(payload[0] + 1, buf[0]);
                        tx.send(()).await.unwrap();
                    })
                    .detach();
                }
                for i in 0..STREAM_COUNT {
                    log::debug!("a done: {}", i);
                    rx.next().await.unwrap();
                }
            });
            let (tx, rx) = bounded(0x100);
            let mux_b = Mux::new(b, MuxConfig::default());
            for i in 0..STREAM_COUNT {
                let mut stream = mux_b.accept().await.unwrap();
                log::debug!("accepted: {}", i);
                let tx = tx.clone();
                smol::spawn(async move {
                    let mut buf = [0u8; 0x100];
                    let size = stream.read(&mut buf).await.unwrap();
                    for i in buf[..size].iter_mut() {
                        *i = *i + 1;
                    }
                    stream.write_all(&buf[..size]).await.unwrap();
                    log::debug!("{:?}", &buf[..size]);
                    tx.send(()).await.unwrap();
                })
                .detach();
            }
            for i in 0..STREAM_COUNT {
                log::debug!("b done: {}", i);
                rx.recv().await.unwrap();
            }
            t.await
        });
    }

    #[test]
    fn close() {
        init();
        smol::block_on(async move {
            let (a, b) = get_tcp_stream_pair().await;
            let mux_a = Mux::new(a, MuxConfig::default());
            let mux_b = Mux::new(b, MuxConfig::default());
            let mut stream_a = mux_a.connect().await.unwrap();
            let mut stream_b = mux_b.accept().await.unwrap();
            stream_a.write(b"hello").await.unwrap();
            let mut buf = [0u8; 0x100];
            let size = stream_b.read(&mut buf).await.unwrap();
            assert_eq!(&buf[..size], b"hello");
            stream_a.close().await.unwrap();
            assert!(stream_a.write(b"hello").await.is_err());
            assert!(stream_b.read(&mut buf).await.is_err());
            assert!(stream_b.write(b"hello").await.is_err());
        })
    }

    #[test]
    fn clean() {
        init();
        smol::block_on(async move {
            let (a, b) = get_tcp_stream_pair().await;
            let mux_a = Mux::new(a, MuxConfig::default());
            let mux_b = Mux::new(b, MuxConfig::default());
            let stream1_a = mux_a.connect().await.unwrap();
            let _stream2_a = mux_a.connect().await.unwrap();
            let mut _stream1_b = mux_b.accept().await.unwrap();
            let mut _stream2_b = mux_b.accept().await.unwrap();
            drop(stream1_a);
            mux_a.clean().await.unwrap();
            assert_eq!(mux_a.mux_future_generator.stream_meta.lock().await.len(), 1);
        })
    }

    #[test]
    fn huge_payload() {
        init();
        smol::block_on(async move {
            let (a, b) = get_tcp_stream_pair().await;
            let mux_a = Mux::new(a, MuxConfig::default());
            let mux_b = Mux::new(b, MuxConfig::default());
            let mut stream_a = mux_a.connect().await.unwrap();
            let mut stream_b = mux_b.accept().await.unwrap();
            let mut payload = Vec::new();
            payload.resize(0x100000, 0);
            rand::thread_rng().fill_bytes(&mut payload);
            let payload = Arc::new(payload);
            {
                let payload = payload.clone();
                smol::spawn(async move {
                    stream_a.write_all(&payload).await.unwrap();
                    stream_a.close().await.unwrap();
                })
                .detach();
            }
            let mut buf = Vec::new();
            buf.resize(0x100000, 0);
            stream_b.read_exact(&mut buf).await.unwrap();
            assert_eq!(&buf[..], &payload[..]);
        })
    }
}
