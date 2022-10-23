use std::{
    collections::{HashMap, VecDeque},
    io::ErrorKind,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll, Waker},
};

use bytes::{Buf, Bytes};
use futures::{
    future::{select, Select},
    ready, Future, FutureExt, SinkExt, Stream, StreamExt,
};
use futures_sink::Sink;
use parking_lot::Mutex;
use std::io as StdIo;
use tokio::io::{self, AsyncRead, AsyncWrite};
use tokio_util::codec::Framed;

use crate::{
    error::{MuxError, MuxResult},
    frame::{MuxCodec, MuxCommand, MuxFrame, MAX_PAYLOAD_SIZE},
};

pub trait TokioConn: AsyncRead + AsyncWrite + Unpin {}

impl<T> TokioConn for T where T: AsyncRead + AsyncWrite + Unpin {}

pub fn mux_connection<T: TokioConn>(
    connection: T,
) -> (MuxConnector<T>, MuxAcceptor<T>, MuxWorker<T>) {
    let inner = Framed::new(connection, MuxCodec {});
    let state = Arc::new(Mutex::new(MuxState {
        inner,
        handles: HashMap::new(),
        accept_queue: VecDeque::new(),
        accept_waker: None,
        accpet_closed: false,
        tx_queue: VecDeque::new(),
        tx_waker: None,
        tx_closed: false,
        rx_closed: false,
    }));
    (
        MuxConnector {
            state: state.clone(),
        },
        MuxAcceptor {
            state: state.clone(),
        },
        MuxWorker {
            select_fut: select(
                MuxDispatcher {
                    state: state.clone(),
                },
                MuxSender { state },
            ),
        },
    )
}

pub struct MuxConnector<T: TokioConn> {
    state: Arc<Mutex<MuxState<T>>>,
}

impl<T: TokioConn> MuxConnector<T> {
    pub fn connect(&self) -> MuxResult<MuxStream<T>> {
        let mut state = self.state.lock();
        state.check_tx_closed()?;

        let stream_id = rand::random();
        state.process_sync(stream_id)?;
        let frame = MuxFrame::new(MuxCommand::Sync, stream_id, Bytes::new());
        state.enqueue_frame_global(frame);
        state.notify_tx();

        let stream = MuxStream {
            stream_id,
            state: self.state.clone(),
            read_buffer: None,
        };
        Ok(stream)
    }
}

pub struct MuxAcceptor<T: TokioConn> {
    state: Arc<Mutex<MuxState<T>>>,
}

impl<T: TokioConn> Drop for MuxAcceptor<T> {
    fn drop(&mut self) {
        self.state.lock().accpet_closed = true;
    }
}

impl<T: TokioConn> Stream for MuxAcceptor<T> {
    type Item = MuxStream<T>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let mut state = self.state.lock();
        if state.check_rx_closed().is_err() {
            return Poll::Ready(None);
        }

        if let Some(stream) = state.accept_queue.pop_front() {
            Poll::Ready(Some(stream))
        } else {
            state.accept_waker = Some(cx.waker().clone());
            Poll::Pending
        }
    }
}

impl<T: TokioConn> MuxAcceptor<T> {
    pub async fn accept(&mut self) -> Option<MuxStream<T>> {
        self.next().await
    }
}

struct MuxSender<T: TokioConn> {
    state: Arc<Mutex<MuxState<T>>>,
}

impl<T: TokioConn> Future for MuxSender<T> {
    type Output = MuxResult<()>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        loop {
            let mut state = self.state.lock();
            ready!(state.poll_ready_tx(cx))?;
            ready!(state.poll_flush_frames(cx))?;
            ready!(state.poll_flush_inner(cx))?;
        }
    }
}

impl<T: TokioConn> Drop for MuxSender<T> {
    fn drop(&mut self) {
        self.state.lock().close_tx();
    }
}

struct MuxDispatcher<T: TokioConn> {
    state: Arc<Mutex<MuxState<T>>>,
}

impl<T: TokioConn> Drop for MuxDispatcher<T> {
    fn drop(&mut self) {
        self.state.lock().close_rx();
    }
}

impl<T: TokioConn> Future for MuxDispatcher<T> {
    type Output = MuxResult<()>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        loop {
            let mut state = self.state.lock();
            if state.rx_closed {
                return Poll::Ready(Ok(()));
            }

            let frame = ready!(state.poll_next_frame(cx))?;
            match frame.header.command {
                MuxCommand::Sync => {
                    if state.accpet_closed {
                        state.send_finish(frame.header.stream_id);
                        continue;
                    }

                    if state.process_sync(frame.header.stream_id).is_err() {
                        // Duplicated
                        state.send_finish(frame.header.stream_id);
                        continue;
                    }

                    let stream = MuxStream {
                        stream_id: frame.header.stream_id,
                        state: self.state.clone(),
                        read_buffer: None,
                    };
                    state.accept_queue.push_back(stream);
                    state.wake_accept();
                }
                MuxCommand::Finish => {
                    state.process_finish(frame.header.stream_id);
                }
                MuxCommand::Push => {
                    let stream_id = frame.header.stream_id;
                    if !state.process_push(frame) {
                        state.send_finish(stream_id);
                    }
                }
                MuxCommand::Nop => {
                    // Do nothing
                }
            }
        }
    }
}

pub struct MuxWorker<T: TokioConn> {
    select_fut: Select<MuxDispatcher<T>, MuxSender<T>>,
}

impl<T: TokioConn> Future for MuxWorker<T> {
    type Output = MuxResult<()>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let (res, _) = ready!(self.select_fut.poll_unpin(cx)).factor_first();
        Poll::Ready(res)
    }
}

pub struct MuxStream<T: TokioConn> {
    stream_id: u32,
    state: Arc<Mutex<MuxState<T>>>,

    read_buffer: Option<Bytes>,
}

impl<T: TokioConn> Drop for MuxStream<T> {
    fn drop(&mut self) {
        let mut state = self.state.lock();
        if state.is_open(self.stream_id) {
            // The user did not call `shutdown()`
            state.enqueue_frame_global(MuxFrame::new(
                MuxCommand::Finish,
                self.stream_id,
                Bytes::new(),
            ));
            state.notify_tx();
        }
        state.remove_stream(self.stream_id);
    }
}

impl<T: TokioConn> AsyncRead for MuxStream<T> {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut io::ReadBuf<'_>,
    ) -> Poll<StdIo::Result<()>> {
        loop {
            if let Some(read_buffer) = &mut self.read_buffer {
                if read_buffer.len() <= buf.remaining() {
                    buf.put_slice(read_buffer);
                    self.read_buffer = None;
                } else {
                    let len = buf.remaining();
                    buf.put_slice(&read_buffer[..len]);
                    read_buffer.advance(len);
                }
                return Poll::Ready(Ok(()));
            }

            let frame = ready!(self.state.lock().poll_read_stream_data(cx, self.stream_id))
                .map_err(mux_to_io_err)?;
            debug_assert_eq!(frame.header.command, MuxCommand::Push);
            self.read_buffer = Some(frame.payload);
        }
    }
}

fn mux_to_io_err(e: MuxError) -> StdIo::Error {
    StdIo::Error::new(ErrorKind::ConnectionReset, e)
}

impl<T: TokioConn> AsyncWrite for MuxStream<T> {
    fn poll_write(
        self: Pin<&mut Self>,
        _: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<Result<usize, StdIo::Error>> {
        let state = self.state.clone();
        let mut state = state.lock();
        if !state.is_open(self.stream_id) {
            return Poll::Ready(Err(StdIo::ErrorKind::ConnectionReset.into()));
        }

        let mut write_buffer = Bytes::copy_from_slice(buf);
        while !write_buffer.is_empty() {
            let len = write_buffer.len().min(MAX_PAYLOAD_SIZE);
            let payload = write_buffer.split_to(len);
            let frame = MuxFrame::new(MuxCommand::Push, self.stream_id, payload);
            state.enqueue_frame_stream(self.stream_id, frame);
        }
        state.notify_tx();
        Poll::Ready(Ok(buf.len()))
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), StdIo::Error>> {
        let mut state = self.state.lock();
        if !state.is_open(self.stream_id) {
            return Poll::Ready(Err(StdIo::Error::new(
                ErrorKind::ConnectionReset,
                "stream is already closed",
            )));
        }

        state
            .poll_flush_stream_frames(cx, self.stream_id)
            .map_err(mux_to_io_err)
    }

    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), StdIo::Error>> {
        loop {
            let mut state = self.state.lock();
            state.check_tx_closed().map_err(mux_to_io_err)?;
            ready!(state
                .poll_flush_stream_frames(cx, self.stream_id)
                .map_err(mux_to_io_err))?;
            if !state.is_open(self.stream_id) {
                return Poll::Ready(Ok(()));
            }

            state.process_finish(self.stream_id);
            let frame = MuxFrame::new(MuxCommand::Finish, self.stream_id, Bytes::new());
            state.enqueue_frame_stream(self.stream_id, frame);
            state.notify_tx();
        }
    }
}

struct StreamHandle {
    closed: bool,

    tx_queue: VecDeque<MuxFrame>,
    tx_waker: Option<Waker>,

    rx_queue: VecDeque<MuxFrame>,
    rx_waker: Option<Waker>,
}

impl StreamHandle {
    #[inline]
    fn wake_rx(&mut self) {
        if let Some(waker) = self.rx_waker.take() {
            waker.wake();
        }
    }

    #[inline]
    fn wake_tx(&mut self) {
        if let Some(waker) = self.tx_waker.take() {
            waker.wake();
        }
    }
}

struct MuxState<T: TokioConn> {
    inner: Framed<T, MuxCodec>,
    handles: HashMap<u32, StreamHandle>,

    accept_queue: VecDeque<MuxStream<T>>,
    accept_waker: Option<Waker>,
    accpet_closed: bool,

    tx_queue: VecDeque<MuxFrame>,
    tx_waker: Option<Waker>,

    tx_closed: bool,
    rx_closed: bool,
}

impl<T: TokioConn> MuxState<T> {
    fn remove_stream(&mut self, stream_id: u32) {
        self.handles
            .remove(&stream_id)
            .expect("Failed to remove stream");
    }

    fn send_finish(&mut self, stream_id: u32) {
        self.enqueue_frame_global(MuxFrame::new(MuxCommand::Finish, stream_id, Bytes::new()));
        self.notify_tx();
    }

    fn process_sync(&mut self, stream_id: u32) -> MuxResult<()> {
        if self.handles.contains_key(&stream_id) {
            return Err(MuxError::DuplicatedStreamId(stream_id));
        }

        let handle = StreamHandle {
            closed: false,

            tx_queue: VecDeque::with_capacity(128),
            tx_waker: None,

            rx_queue: VecDeque::with_capacity(128),
            rx_waker: None,
        };
        self.handles.insert(stream_id, handle);
        Ok(())
    }

    fn process_finish(&mut self, stream_id: u32) {
        if let Some(handle) = self.handles.get_mut(&stream_id) {
            handle.closed = true;
            handle.wake_tx();
            handle.wake_rx();
        }
    }

    fn process_push(&mut self, frame: MuxFrame) -> bool {
        if let Some(handle) = self.handles.get_mut(&frame.header.stream_id) {
            handle.rx_queue.push_back(frame);
            handle.wake_rx();
            true
        } else {
            false
        }
    }

    fn is_open(&self, stream_id: u32) -> bool {
        if let Some(handle) = self.handles.get(&stream_id) {
            !handle.closed
        } else {
            false
        }
    }

    fn poll_next_frame(&mut self, cx: &mut Context<'_>) -> Poll<MuxResult<MuxFrame>> {
        if self.rx_closed {
            return Poll::Ready(Err(MuxError::ConnectionClosed));
        }

        if let Some(r) = ready!(self.inner.poll_next_unpin(cx)) {
            let frame = r.map_err(|e| {
                self.rx_closed = true;
                e
            })?;
            Poll::Ready(Ok(frame))
        } else {
            self.rx_closed = true;
            Poll::Ready(Err(MuxError::ConnectionClosed))
        }
    }

    #[inline]
    fn pin_inner(&mut self) -> Pin<&mut Framed<T, MuxCodec>> {
        Pin::new(&mut self.inner)
    }

    fn poll_write_ready(&mut self, cx: &mut Context<'_>) -> Poll<MuxResult<()>> {
        ready!(self.pin_inner().poll_ready(cx)).map_err(|e| {
            self.tx_closed = true;
            e
        })?;
        Poll::Ready(Ok(()))
    }

    fn write_frame(&mut self, frame: MuxFrame) -> MuxResult<()> {
        self.pin_inner().start_send(frame)?;
        Ok(())
    }

    fn poll_read_stream_data(
        &mut self,
        cx: &mut Context<'_>,
        stream_id: u32,
    ) -> Poll<MuxResult<MuxFrame>> {
        let handle = self.handles.get_mut(&stream_id).unwrap();
        if let Some(f) = handle.rx_queue.pop_front() {
            Poll::Ready(Ok(f))
        } else if handle.closed {
            Poll::Ready(Err(MuxError::StreamClosed(stream_id)))
        } else {
            handle.rx_waker = Some(cx.waker().clone());
            Poll::Pending
        }
    }

    fn enqueue_frame_stream(&mut self, stream_id: u32, frame: MuxFrame) {
        self.handles
            .get_mut(&stream_id)
            .unwrap()
            .tx_queue
            .push_back(frame)
    }

    fn enqueue_frame_global(&mut self, frame: MuxFrame) {
        self.tx_queue.push_back(frame);
    }

    fn notify_tx(&mut self) {
        if let Some(waker) = self.tx_waker.take() {
            waker.wake();
        }
    }

    fn poll_flush_stream_frames(
        &mut self,
        cx: &mut Context<'_>,
        stream_id: u32,
    ) -> Poll<MuxResult<()>> {
        let handle = self.handles.get_mut(&stream_id).unwrap();
        if handle.tx_queue.is_empty() {
            Poll::Ready(Ok(()))
        } else {
            handle.tx_waker = Some(cx.waker().clone());
            self.notify_tx();
            Poll::Pending
        }
    }

    fn wake_accept(&mut self) {
        if let Some(waker) = self.accept_waker.take() {
            waker.wake();
        }
    }

    fn close_tx(&mut self) {
        self.tx_closed = true;
        for (_, handle) in self.handles.iter_mut() {
            handle.closed = true;
            handle.wake_tx();
        }
    }

    fn close_rx(&mut self) {
        self.rx_closed = true;
        self.wake_accept();
        for (_, handle) in self.handles.iter_mut() {
            handle.closed = true;
            handle.wake_rx();
        }
    }

    fn check_rx_closed(&self) -> MuxResult<()> {
        if self.tx_closed {
            Err(MuxError::ConnectionClosed)
        } else {
            Ok(())
        }
    }

    fn check_tx_closed(&self) -> MuxResult<()> {
        if self.tx_closed {
            Err(MuxError::ConnectionClosed)
        } else {
            Ok(())
        }
    }

    fn poll_flush_frames(&mut self, cx: &mut Context<'_>) -> Poll<MuxResult<()>> {
        // Global
        while !self.tx_queue.is_empty() {
            ready!(self.poll_write_ready(cx))?;
            let frame = self.tx_queue.pop_front().unwrap();
            self.write_frame(frame)?;
        }

        // Streams
        for (_, h) in self
            .handles
            .iter_mut()
            .filter(|(_, h)| !h.tx_queue.is_empty())
        {
            while !h.tx_queue.is_empty() {
                ready!(Pin::new(&mut self.inner).poll_ready(cx))?;
                Pin::new(&mut self.inner).start_send(h.tx_queue.pop_front().unwrap())?;
            }
            h.wake_tx();
        }

        Poll::Ready(Ok(()))
    }

    fn poll_flush_inner(&mut self, cx: &mut Context<'_>) -> Poll<MuxResult<()>> {
        self.inner.poll_flush_unpin(cx)
    }

    fn poll_ready_tx(&mut self, cx: &mut Context<'_>) -> Poll<MuxResult<()>> {
        if self.tx_closed {
            return Poll::Ready(Err(MuxError::ConnectionClosed));
        }

        if self.tx_queue.is_empty() && self.handles.iter().all(|(_, h)| h.tx_queue.is_empty()) {
            self.tx_waker = Some(cx.waker().clone());
            Poll::Pending
        } else {
            Poll::Ready(Ok(()))
        }
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use rand::RngCore;
    use tokio::{
        io::{AsyncReadExt, AsyncWriteExt},
        net::{TcpListener, TcpStream},
    };

    use crate::{frame::MAX_PAYLOAD_SIZE, mux::TokioConn, mux_connection, MuxStream};

    async fn get_tcp_pair() -> (TcpStream, TcpStream) {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let h = tokio::spawn(async move {
            let (a, _) = listener.accept().await.unwrap();
            a
        });

        let b = TcpStream::connect(addr).await.unwrap();
        let a = h.await.unwrap();
        a.set_nodelay(true).unwrap();
        b.set_nodelay(true).unwrap();
        (a, b)
    }

    async fn test_stream<T: TokioConn>(mut a: MuxStream<T>, mut b: MuxStream<T>) {
        const LEN: usize = MAX_PAYLOAD_SIZE + 0x200;
        let mut data1 = vec![0; LEN];
        let mut data2 = vec![0; LEN];
        rand::thread_rng().fill_bytes(&mut data1);
        rand::thread_rng().fill_bytes(&mut data2);

        let mut buf = vec![0; LEN];

        a.write_all(&data1).await.unwrap();
        a.flush().await.unwrap();
        b.write_all(&data2).await.unwrap();
        b.flush().await.unwrap();

        a.read_exact(&mut buf).await.unwrap();
        assert_eq!(buf, data2);
        b.read_exact(&mut buf).await.unwrap();
        assert_eq!(buf, data1);

        a.write_all(&data1).await.unwrap();
        a.flush().await.unwrap();
        b.read_exact(&mut buf[..LEN / 2]).await.unwrap();
        b.read_exact(&mut buf[LEN / 2..]).await.unwrap();
        assert_eq!(buf, data1);

        a.write_all(&data1[..LEN / 2]).await.unwrap();
        a.flush().await.unwrap();
        b.read_exact(&mut buf[..LEN / 2]).await.unwrap();
        assert_eq!(buf[..LEN / 2], data1[..LEN / 2]);

        a.shutdown().await.unwrap();
        b.shutdown().await.unwrap();
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_tcp() {
        let (a, b) = get_tcp_pair().await;
        let (connector_a, mut acceptor_a, worker_a) = mux_connection(a);
        let (connector_b, mut acceptor_b, worker_b) = mux_connection(b);
        tokio::spawn(async move {
            worker_a.await.unwrap();
        });
        tokio::spawn(async move {
            worker_b.await.unwrap();
        });

        let stream1 = connector_a.connect().unwrap();
        let stream2 = acceptor_b.accept().await.unwrap();
        test_stream(stream1, stream2).await;

        let stream1 = connector_b.connect().unwrap();
        let stream2 = acceptor_a.accept().await.unwrap();
        test_stream(stream1, stream2).await;

        assert_eq!(connector_a.state.lock().handles.len(), 0);
        assert_eq!(connector_b.state.lock().handles.len(), 0);

        let mut streams1 = vec![];
        let mut streams2 = vec![];
        const STREAM_NUM: usize = 0x1000;
        for _ in 0..STREAM_NUM {
            let stream = connector_a.connect().unwrap();
            streams1.push(stream);
        }
        for _ in 0..STREAM_NUM {
            let stream = acceptor_b.accept().await.unwrap();
            streams2.push(stream);
        }

        let handles = streams1
            .into_iter()
            .zip(streams2.into_iter())
            .map(|(a, b)| {
                tokio::spawn(async move {
                    test_stream(a, b).await;
                })
            })
            .collect::<Vec<_>>();

        for h in handles {
            h.await.unwrap();
        }

        assert_eq!(connector_a.state.lock().handles.len(), 0);
        assert_eq!(connector_b.state.lock().handles.len(), 0);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_worker_drop() {
        let (a, b) = get_tcp_pair().await;
        let (connector_a, mut acceptor_a, worker_a) = mux_connection(a);
        let (connector_b, mut acceptor_b, worker_b) = mux_connection(b);
        let mut stream1 = connector_a.connect().unwrap();
        let h1 = tokio::spawn(async move {
            let mut buf = vec![0; 0x100];
            stream1.read_exact(&mut buf).await.unwrap_err();
        });

        drop(worker_a);
        drop(worker_b);

        assert!(connector_a.connect().is_err());
        assert!(connector_b.connect().is_err());
        assert!(acceptor_a.accept().await.is_none());
        assert!(acceptor_b.accept().await.is_none());
        h1.await.unwrap();
    }

    #[tokio::test]
    async fn test_shutdown() {
        let (a, b) = get_tcp_pair().await;
        let (connector_a, acceptor_a, worker_a) = mux_connection(a);
        let (connector_b, mut acceptor_b, worker_b) = mux_connection(b);
        tokio::spawn(async move {
            worker_a.await.unwrap();
        });
        tokio::spawn(async move {
            worker_b.await.unwrap();
        });

        let mut stream1 = connector_a.connect().unwrap();
        let mut stream2 = acceptor_b.accept().await.unwrap();

        let data = [1, 2, 3, 4];
        stream2.write_all(&data).await.unwrap();
        stream2.shutdown().await.unwrap();

        tokio::time::sleep(Duration::from_secs(1)).await;

        stream1.write_all(&[0, 1, 2, 3]).await.unwrap_err();
        stream1.flush().await.unwrap_err();
        let mut buf = vec![0; 4];
        stream1.read_exact(&mut buf).await.unwrap();
        assert!(buf == data);
        stream1.read(&mut buf).await.unwrap_err();

        drop(acceptor_a);
        let mut stream = connector_b.connect().unwrap();
        stream.read(&mut buf).await.unwrap_err();
        stream.flush().await.unwrap_err();
        stream.shutdown().await.unwrap();

        let mut stream1 = connector_a.connect().unwrap();
        let mut stream2 = acceptor_b.accept().await.unwrap();
        stream1.write_all(&data).await.unwrap();
        stream1.flush().await.unwrap();
        drop(stream1);
        tokio::time::sleep(Duration::from_secs(1)).await;

        let mut buf = vec![0; 4];
        stream2.read_exact(&mut buf).await.unwrap();
        assert!(buf == data);
        stream2.read_exact(&mut buf).await.unwrap_err();
        stream2.write_all(&data).await.unwrap_err();
    }
}
