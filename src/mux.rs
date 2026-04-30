use std::{
    collections::{HashMap, VecDeque},
    io::ErrorKind,
    num::Wrapping,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll, Waker},
    time::Duration,
};

use bytes::{Buf, Bytes};
use futures::{future::poll_fn, ready, Future, FutureExt, SinkExt, Stream, StreamExt};
use futures_sink::Sink;
use log::debug;
use parking_lot::Mutex;
use std::io as StdIo;
use tokio::{
    io::{self, AsyncRead, AsyncWrite},
    time::{interval, Instant, Interval},
};
use tokio_util::codec::Framed;

use crate::{
    config::{MuxConfig, StreamIdType},
    error::{MuxError, MuxResult},
    frame::{MuxCodec, MuxCommand, MuxFrame, MAX_PAYLOAD_SIZE},
};

pub trait TokioConn: AsyncRead + AsyncWrite + Unpin {}

impl<T> TokioConn for T where T: AsyncRead + AsyncWrite + Unpin {}

pub fn mux_connection<T: TokioConn>(
    connection: T,
    config: MuxConfig,
) -> (MuxConnector<T>, MuxAcceptor<T>, MuxWorker<T>) {
    let inner = Framed::new(connection, MuxCodec {});
    // When keep_alive is enabled, default the dead-peer timeout to
    // 3 * keep_alive_interval, matching the convention used by other
    // smux implementations.
    let keep_alive_timeout = config.keep_alive_interval.map(|i| {
        let secs = config
            .keep_alive_timeout
            .map(|t| t.get())
            .unwrap_or_else(|| i.get().saturating_mul(3));
        Duration::from_secs(secs)
    });
    let state = Arc::new(Mutex::new(MuxState {
        inner,
        handles: HashMap::new(),
        accept_queue: VecDeque::new(),
        accept_waker: None,
        tx_queue: VecDeque::with_capacity(config.max_tx_queue.get()),
        should_tx_waker: None,
        rx_consumed_waker: None,
        close_waker: None,
        closed: false,
        shutdown_requested: false,
        closing_inline: false,
        accept_closed: false,
        public_handles: 0,
        stream_id_hint: Wrapping(config.stream_id_type as u32),
        stream_id_type: config.stream_id_type,
        idle_timeout: config.idle_timeout.map(|n| Duration::from_secs(n.get())),
        keep_alive_timeout,
        last_rx: Instant::now(),
        max_tx_queue: config.max_tx_queue.get(),
        max_rx_queue: config.max_rx_queue.get(),
    }));
    {
        // Expose 2 public handles: connector + acceptor.
        state.lock().public_handles = 2;
    }
    (
        MuxConnector {
            state: state.clone(),
        },
        MuxAcceptor {
            state: state.clone(),
        },
        MuxWorker {
            dispatcher: MuxDispatcher {
                state: state.clone(),
            },
            sender: MuxSender {
                state: state.clone(),
            },
            timer: MuxTimer {
                state,
                // Only spin the periodic interval if something needs it.
                // Otherwise the timer future stays pending forever and
                // burns no CPU/wakeups.
                interval: if config.idle_timeout.is_some() || config.keep_alive_interval.is_some() {
                    Some(interval(Duration::from_millis(500)))
                } else {
                    None
                },
                keep_alive_interval: config
                    .keep_alive_interval
                    .map(|a| interval(Duration::from_secs(a.get()))),
            },
        },
    )
}

pub struct MuxConnector<T: TokioConn> {
    state: Arc<Mutex<MuxState<T>>>,
}

impl<T: TokioConn> MuxConnector<T> {
    pub fn connect(&self) -> MuxResult<MuxStream<T>> {
        let mut state = self.state.lock();
        state.check_closed()?;

        let stream_id = state.alloc_stream_id()?;
        state.process_sync(stream_id, Direction::Tx)?;
        let frame = MuxFrame::new(MuxCommand::Sync, stream_id, Bytes::new());
        state.enqueue_frame_global(frame);
        state.notify_should_tx();
        state.public_handles += 1;

        let stream = MuxStream {
            stream_id,
            state: self.state.clone(),
            read_buffer: None,
        };
        Ok(stream)
    }

    pub async fn close(&mut self) -> MuxResult<()> {
        {
            let mut state = self.state.lock();
            state.close();
            // Take ownership of Framed for the rest of close(). The
            // sender will see this flag at the top of its loop and bow
            // out, leaving Framed's single waker slot to us. The mutex
            // already serializes physical access; this flag avoids
            // logical wakeup loss between the two actors.
            state.closing_inline = true;
            // Wake the sender now so it observes the flag promptly
            // (otherwise it could still be parked on Framed's waker
            // slot, which we are about to take over).
            state.notify_should_tx();
        }
        poll_fn(|cx| {
            let mut state = self.state.lock();
            // Save waker so an externally-driven hard_close (dispatcher
            // error, transport failure) can wake us if it happens to
            // race ahead of our own progress here.
            state.close_waker = Some(cx.waker().clone());
            if state.closed {
                return Poll::Ready(Ok(()));
            }
            // Drain → flush → close, ourselves. This makes close() not
            // depend on the worker still being polled.
            match state.poll_flush_frames(cx) {
                Poll::Ready(Ok(())) => {}
                Poll::Ready(Err(e)) => {
                    state.hard_close();
                    return Poll::Ready(Err(e));
                }
                Poll::Pending => return Poll::Pending,
            }
            match state.poll_flush_inner(cx) {
                Poll::Ready(Ok(())) => {}
                Poll::Ready(Err(e)) => {
                    state.hard_close();
                    return Poll::Ready(Err(e));
                }
                Poll::Pending => return Poll::Pending,
            }
            match state.inner.poll_close_unpin(cx) {
                Poll::Ready(Ok(())) => {
                    state.hard_close();
                    Poll::Ready(Ok(()))
                }
                Poll::Ready(Err(e)) => {
                    state.hard_close();
                    Poll::Ready(Err(MuxError::from(e)))
                }
                Poll::Pending => Poll::Pending,
            }
        })
        .await
    }

    pub fn get_num_streams(&self) -> usize {
        self.state.lock().handles.len()
    }
}

impl<T: TokioConn> Clone for MuxConnector<T> {
    fn clone(&self) -> Self {
        self.state.lock().public_handles += 1;
        Self {
            state: self.state.clone(),
        }
    }
}

impl<T: TokioConn> Drop for MuxConnector<T> {
    fn drop(&mut self) {
        self.state.lock().dec_public_handles();
    }
}

pub struct MuxAcceptor<T: TokioConn> {
    state: Arc<Mutex<MuxState<T>>>,
}

impl<T: TokioConn> Drop for MuxAcceptor<T> {
    fn drop(&mut self) {
        let mut state = self.state.lock();
        state.accept_closed = true;
        state.close_unaccepted_streams();
        state.dec_public_handles();
    }
}

impl<T: TokioConn> MuxAcceptor<T> {
    pub async fn accept(&mut self) -> Option<MuxStream<T>> {
        self.next().await
    }
}

impl<T: TokioConn> Stream for MuxAcceptor<T> {
    type Item = MuxStream<T>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let mut state = self.state.lock();
        if state.check_closed().is_err() {
            return Poll::Ready(None);
        }

        if let Some(stream_id) = state.accept_queue.pop_front() {
            state.public_handles += 1;
            let stream = MuxStream {
                stream_id,
                state: self.state.clone(),
                read_buffer: None,
            };
            Poll::Ready(Some(stream))
        } else {
            state.register_accept_waker(cx);
            Poll::Pending
        }
    }
}

struct MuxTimer<T: TokioConn> {
    state: Arc<Mutex<MuxState<T>>>,
    interval: Option<Interval>,

    keep_alive_interval: Option<Interval>,
}

impl<T: TokioConn> Future for MuxTimer<T> {
    type Output = MuxResult<()>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        // All fields are Unpin (Arc<Mutex>, Option<Interval>), so we can
        // freely take a &mut Self and use field-splitting to mutably
        // borrow `interval` and `keep_alive_interval` independently.
        let this = self.as_mut().get_mut();
        let Some(interval) = this.interval.as_mut() else {
            // No idle / keep-alive features enabled, so the timer never
            // needs to fire. Stay pending forever - the worker is driven
            // by dispatcher and sender alone.
            return Poll::Pending;
        };
        loop {
            ready!(interval.poll_tick(cx));
            interval.reset();

            // Ping check
            let mut is_ping_send_needs = false;
            if let Some(keep_alive_interval) = this.keep_alive_interval.as_mut() {
                if keep_alive_interval.poll_tick(cx).is_ready() {
                    keep_alive_interval.reset();

                    is_ping_send_needs = true;
                }
            }

            let mut state = this.state.lock();

            // Ping send
            if is_ping_send_needs {
                state.enqueue_frame_global(MuxFrame::new(MuxCommand::Nop, 0, Bytes::new()));
                state.notify_should_tx();
            }

            // Dead-peer detection. If keep-alive is enabled and we have not
            // received any frame from the peer for keep_alive_timeout, treat
            // the connection as dead and tear everything down so callers
            // unblock instead of hanging on a half-open socket.
            if let Some(timeout) = state.keep_alive_timeout {
                if Instant::now().duration_since(state.last_rx) >= timeout {
                    state.hard_close();
                    return Poll::Ready(Err(MuxError::ConnectionClosed));
                }
            }

            // Clean timeout streams
            if let Some(timeout) = state.idle_timeout {
                let now = Instant::now();
                let dead_ids = state
                    .handles
                    .iter()
                    .filter_map(|(id, h)| {
                        if now.duration_since(h.last_active) >= timeout {
                            Some(*id)
                        } else {
                            None
                        }
                    })
                    .collect::<Vec<_>>();

                for stream_id in dead_ids {
                    state.try_mark_finish(stream_id);
                    state.send_finish(stream_id);
                    state.notify_rx_consumed();
                }
            }
        }
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
            // Always register should_tx_waker first so any state-change
            // notification (close start, hard_close, new tx work) can
            // reach us even if we'd otherwise be parked on Framed's
            // single waker slot deeper in this loop.
            state.should_tx_waker = Some(cx.waker().clone());
            state.check_closed()?;
            // MuxConnector::close drives Framed inline; stay out of the
            // way. We'll be re-woken by hard_close via should_tx_waker.
            if state.closing_inline {
                return Poll::Pending;
            }
            ready!(state.poll_flush_frames(cx)).inspect_err(|_| state.hard_close())?;
            ready!(state.poll_flush_inner(cx)).inspect_err(|_| state.hard_close())?;
            // After draining, finalize an orderly shutdown if one was
            // requested. Closing the inner sink lets the dispatcher's
            // poll_next observe EOF and exit cleanly.
            if state.shutdown_requested && !state.closed {
                let res = state.inner.poll_close_unpin(cx);
                if let Poll::Ready(r) = res {
                    state.hard_close();
                    return Poll::Ready(r);
                }
                return Poll::Pending;
            }
            ready!(state.poll_should_tx(cx));
        }
    }
}

impl<T: TokioConn> Drop for MuxSender<T> {
    fn drop(&mut self) {
        self.state.lock().hard_close();
    }
}

struct MuxDispatcher<T: TokioConn> {
    state: Arc<Mutex<MuxState<T>>>,
}

impl<T: TokioConn> Drop for MuxDispatcher<T> {
    fn drop(&mut self) {
        self.state.lock().hard_close();
    }
}

impl<T: TokioConn> Future for MuxDispatcher<T> {
    type Output = MuxResult<()>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        loop {
            let mut state = self.state.lock();
            state.check_closed()?;

            ready!(state.poll_ready_rx_consumed(cx)); // Can stuck here forever, be careful

            let frame = ready!(state.poll_next_frame(cx)).inspect_err(|_| state.hard_close())?;
            // Refresh peer-liveness clock on every successfully-decoded
            // frame - any frame, including NOPs, proves the peer is alive.
            state.last_rx = Instant::now();
            match frame.header.command {
                MuxCommand::Sync => {
                    if state.accept_closed {
                        state.send_finish(frame.header.stream_id);
                        continue;
                    }

                    state.process_sync(frame.header.stream_id, Direction::Rx)?;
                    state.accept_queue.push_back(frame.header.stream_id);
                    state.notify_accept_stream();
                }
                MuxCommand::Finish => {
                    state.try_mark_finish(frame.header.stream_id);
                }
                MuxCommand::Push => {
                    let stream_id = frame.header.stream_id;
                    if !state.recv_push(frame) {
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
    dispatcher: MuxDispatcher<T>,
    sender: MuxSender<T>,
    timer: MuxTimer<T>,
}

impl<T: TokioConn> Future for MuxWorker<T> {
    type Output = MuxResult<()>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        // Timer used to be a never-resolving infinite future; with dead-peer
        // detection it can now resolve to Err(ConnectionClosed). Surface
        // that result so the worker terminates instead of spinning.
        if let Poll::Ready(res) = self.timer.poll_unpin(cx) {
            return Poll::Ready(res);
        }

        if self.dispatcher.poll_unpin(cx)?.is_ready() {
            return Poll::Ready(Ok(()));
        }

        if self.sender.poll_unpin(cx)?.is_ready() {
            return Poll::Ready(Ok(()));
        }

        Poll::Pending
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
        if !state.is_closed(self.stream_id) {
            // The user did not call `shutdown()`. Anything still queued in
            // the per-stream tx_queue would otherwise be dropped together
            // with the StreamHandle below, so move it onto the global
            // tx_queue first, then enqueue FIN after, so the wire sees
            // PSH... PSH FIN in order.
            if let Some(h) = state.handles.get_mut(&self.stream_id) {
                let drained: VecDeque<MuxFrame> = std::mem::take(&mut h.tx_queue);
                state.tx_queue.extend(drained);
            }
            state.enqueue_frame_global(MuxFrame::new(
                MuxCommand::Finish,
                self.stream_id,
                Bytes::new(),
            ));
            state.notify_should_tx();
        }
        state.remove_stream(self.stream_id);
        state.dec_public_handles();
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

            if let Some(frame) = frame {
                debug_assert_eq!(frame.header.command, MuxCommand::Push);
                self.read_buffer = Some(frame.payload);
            } else {
                // EOF
                return Poll::Ready(Ok(()));
            }
        }
    }
}

#[inline]
fn mux_to_io_err(e: MuxError) -> StdIo::Error {
    StdIo::Error::new(ErrorKind::Other, e)
}

#[inline]
fn new_io_err(kind: ErrorKind, reason: &str) -> StdIo::Error {
    StdIo::Error::new(kind, reason)
}

impl<T: TokioConn> AsyncWrite for MuxStream<T> {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<Result<usize, StdIo::Error>> {
        let mut state = self.state.lock();
        if state.is_closed(self.stream_id) {
            return Poll::Ready(Err(new_io_err(
                StdIo::ErrorKind::ConnectionReset,
                "stream tx is already closed",
            )));
        }

        ready!(state.poll_stream_write_ready(cx, self.stream_id)).map_err(mux_to_io_err)?;

        // Enqueue at most one frame per poll_write so the per-stream
        // back-pressure check above is consulted between frames. Otherwise
        // a single large write_all call would push the tx_queue arbitrarily
        // far past max_tx_queue. AsyncWriteExt::write_all loops until all
        // bytes are accepted, so caller semantics are preserved.
        let len = buf.len().min(MAX_PAYLOAD_SIZE);
        let payload = Bytes::copy_from_slice(&buf[..len]);
        let frame = MuxFrame::new(MuxCommand::Push, self.stream_id, payload);
        state.enqueue_frame_stream(self.stream_id, frame);
        state.notify_should_tx();
        Poll::Ready(Ok(len))
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), StdIo::Error>> {
        let mut state = self.state.lock();
        // Always try to drain the stream's tx_queue first, even if the
        // stream has been remotely closed. Bytes that the user already
        // accepted via poll_write should reach the wire on a best-effort
        // basis; only after the queue is empty do we surface the close as
        // an error.
        match state.poll_flush_stream_frames(cx, self.stream_id) {
            Poll::Ready(Ok(())) => {}
            Poll::Ready(Err(e)) => return Poll::Ready(Err(mux_to_io_err(e))),
            Poll::Pending => return Poll::Pending,
        }
        if state.is_closed(self.stream_id) {
            return Poll::Ready(Err(new_io_err(
                StdIo::ErrorKind::ConnectionReset,
                "stream tx is already closed",
            )));
        }
        Poll::Ready(Ok(()))
    }

    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), StdIo::Error>> {
        loop {
            let mut state = self.state.lock();
            ready!(state
                .poll_flush_stream_frames(cx, self.stream_id)
                .map_err(mux_to_io_err))?;

            if state.is_closed(self.stream_id) {
                return Poll::Ready(Ok(()));
            }

            state.try_mark_finish(self.stream_id);
            state.send_finish(self.stream_id);
        }
    }
}

impl<T: TokioConn> MuxStream<T> {
    pub fn is_closed(&mut self) -> bool {
        self.state.lock().is_closed(self.stream_id)
    }

    pub fn get_stream_id(&self) -> u32 {
        self.stream_id
    }
}

struct StreamHandle {
    closed: bool,

    tx_queue: VecDeque<MuxFrame>,
    tx_done_waker: Option<Waker>,

    rx_queue: VecDeque<MuxFrame>,
    rx_ready_waker: Option<Waker>,

    last_active: Instant,
}

impl StreamHandle {
    fn new() -> Self {
        Self {
            closed: false,
            tx_queue: VecDeque::with_capacity(128),
            tx_done_waker: None,
            rx_queue: VecDeque::with_capacity(128),
            rx_ready_waker: None,
            last_active: Instant::now(),
        }
    }

    #[inline]
    fn register_tx_done_waker(&mut self, cx: &Context<'_>) {
        self.tx_done_waker = Some(cx.waker().clone());
    }

    #[inline]
    fn register_rx_ready_waker(&mut self, cx: &Context<'_>) {
        self.rx_ready_waker = Some(cx.waker().clone());
    }

    #[inline]
    fn notify_rx_ready(&mut self) {
        if let Some(waker) = self.rx_ready_waker.take() {
            waker.wake();
        }
    }

    #[inline]
    fn notify_tx_done(&mut self) {
        if let Some(waker) = self.tx_done_waker.take() {
            waker.wake();
        }
    }
}

#[derive(Debug, Clone, Copy)]
enum Direction {
    Tx,
    Rx,
}

struct MuxState<T: TokioConn> {
    inner: Framed<T, MuxCodec>,
    handles: HashMap<u32, StreamHandle>,

    accept_queue: VecDeque<u32>,
    accept_waker: Option<Waker>,

    tx_queue: VecDeque<MuxFrame>,
    should_tx_waker: Option<Waker>,
    rx_consumed_waker: Option<Waker>,
    /// Wakes whoever is awaiting on hard_close completion (currently
    /// MuxConnector::close).
    close_waker: Option<Waker>,

    closed: bool,
    /// Soft-close requested: drain everything that's already queued, then
    /// hard-close. Distinct from `closed` (which is the immediate, no-drain
    /// teardown) so that user-initiated shutdown does not lose pending FIN
    /// / PSH frames already enqueued by Drop impls.
    shutdown_requested: bool,
    /// Set while `MuxConnector::close` is driving Framed itself. The
    /// sender steps out of the way on this flag so that close() can
    /// finish even when the worker isn't being polled (e.g. the user
    /// forgot to spawn it, or it has already exited). The mutex
    /// serializes access to Framed; the flag prevents close()/sender
    /// from clobbering each other's wakers on Framed's single waker
    /// slot.
    closing_inline: bool,
    accept_closed: bool,
    public_handles: usize,

    stream_id_hint: Wrapping<u32>,
    stream_id_type: StreamIdType,

    idle_timeout: Option<Duration>,

    /// Maximum gap between received frames before declaring the peer dead.
    /// Only Some when keep_alive_interval is also set.
    keep_alive_timeout: Option<Duration>,
    /// Last time the dispatcher successfully decoded a frame from the peer.
    last_rx: Instant,

    max_tx_queue: usize,
    max_rx_queue: usize,
}

impl<T: TokioConn> Drop for MuxState<T> {
    fn drop(&mut self) {
        debug!("mux state dropped");
    }
}

impl<T: TokioConn> MuxState<T> {
    #[inline]
    fn dec_public_handles(&mut self) {
        if self.public_handles == 0 {
            return;
        }
        self.public_handles -= 1;
        if self.public_handles == 0 {
            self.close();
        }
    }

    fn alloc_stream_id(&mut self) -> MuxResult<u32> {
        if self.handles.len() >= (u32::MAX / 2) as usize {
            return Err(MuxError::TooManyStreams);
        }

        loop {
            self.stream_id_hint += 2;

            // 0 is reserved for NOP keep-alive frames - never hand it out as
            // a real stream id. After u32 wraparound the even side would
            // otherwise step onto 0.
            if self.stream_id_hint.0 == 0 {
                continue;
            }

            if !self.handles.contains_key(&self.stream_id_hint.0) {
                break;
            }
        }

        Ok(self.stream_id_hint.0)
    }

    #[inline]
    fn remove_stream(&mut self, stream_id: u32) {
        // Idempotent: a stream may be removed via close_unaccepted_streams
        // and again via MuxStream::Drop in pathological orderings, so
        // tolerate a missing entry instead of panicking.
        self.handles.remove(&stream_id);
        // Rx queue may change
        self.notify_rx_consumed();
    }

    fn send_finish(&mut self, stream_id: u32) {
        self.enqueue_frame_global(MuxFrame::new(MuxCommand::Finish, stream_id, Bytes::new()));
        self.notify_should_tx();
    }

    fn process_sync(&mut self, stream_id: u32, dir: Direction) -> MuxResult<()> {
        // Stream id 0 is reserved (NOP keep-alive). Reject SYN(0) from either
        // side so peer-misbehavior cannot create a regular stream colliding
        // with NOP semantics.
        if stream_id == 0 {
            return Err(MuxError::ReservedStreamId(stream_id));
        }

        if self.handles.contains_key(&stream_id) {
            return Err(MuxError::DuplicatedStreamId(stream_id));
        }

        let from_peer = matches!(dir, Direction::Rx);
        if (stream_id % 2 != self.stream_id_type as u32) ^ from_peer {
            return Err(MuxError::InvalidPeerStreamIdType(
                stream_id,
                self.stream_id_type,
            ));
        }

        let handle = StreamHandle::new();
        self.handles.insert(stream_id, handle);
        Ok(())
    }

    fn close_unaccepted_streams(&mut self) {
        while let Some(stream_id) = self.accept_queue.pop_front() {
            self.try_mark_finish(stream_id);
            self.send_finish(stream_id);
            self.remove_stream(stream_id);
        }
    }

    #[inline]
    fn try_mark_finish(&mut self, stream_id: u32) {
        if let Some(h) = self.handles.get_mut(&stream_id) {
            h.closed = true;
            h.notify_rx_ready();
            h.notify_tx_done();
        }
    }

    fn recv_push(&mut self, frame: MuxFrame) -> bool {
        if let Some(handle) = self.handles.get_mut(&frame.header.stream_id) {
            // If we've already locally closed the stream (shutdown / FIN
            // received), drop incoming PSH instead of silently appending it
            // to rx_queue. Otherwise EOF would not be monotonic: a reader
            // who already saw 0 bytes could then surface bytes that arrived
            // afterwards.
            if handle.closed {
                return false;
            }
            handle.rx_queue.push_back(frame);
            handle.notify_rx_ready();
            handle.last_active = Instant::now();
            true
        } else {
            false
        }
    }

    #[inline]
    fn get_rx_pending(&mut self) -> usize {
        self.handles
            .values()
            .filter(|h| !h.closed)
            .map(|h| h.rx_queue.len())
            .sum()
    }

    fn poll_ready_rx_consumed(&mut self, cx: &Context<'_>) -> Poll<()> {
        let pending = self.get_rx_pending();
        if pending > self.max_rx_queue {
            self.register_rx_consumed_waker(cx);
            Poll::Pending
        } else {
            Poll::Ready(())
        }
    }

    fn is_closed(&self, stream_id: u32) -> bool {
        // Invariant: when a MuxStream holds `stream_id`, its handle must
        // be present. Catch invariant breaks in debug builds; in release
        // builds, treat a missing handle as closed so we don't panic in
        // the user's hot path.
        debug_assert!(
            self.handles.contains_key(&stream_id),
            "is_closed called with unknown stream id {stream_id}"
        );
        self.handles.get(&stream_id).is_none_or(|h| h.closed)
    }

    fn poll_next_frame(&mut self, cx: &mut Context<'_>) -> Poll<MuxResult<MuxFrame>> {
        if let Some(r) = ready!(self.inner.poll_next_unpin(cx)) {
            let frame = r?;
            Poll::Ready(Ok(frame))
        } else {
            Poll::Ready(Err(MuxError::ConnectionClosed))
        }
    }

    #[inline]
    fn pin_inner(&mut self) -> Pin<&mut Framed<T, MuxCodec>> {
        Pin::new(&mut self.inner)
    }

    fn poll_write_ready(&mut self, cx: &mut Context<'_>) -> Poll<MuxResult<()>> {
        ready!(self.pin_inner().poll_ready(cx))?;
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
    ) -> Poll<MuxResult<Option<MuxFrame>>> {
        debug_assert!(
            self.handles.contains_key(&stream_id),
            "poll_read_stream_data called with unknown stream id {stream_id}"
        );
        let Some(handle) = self.handles.get_mut(&stream_id) else {
            // Handle missing -> stream is gone. Surface as EOF.
            return Poll::Ready(Ok(None));
        };
        if let Some(f) = handle.rx_queue.pop_front() {
            self.notify_rx_consumed(); // Rx queue packet consumed
            Poll::Ready(Ok(Some(f)))
        } else if self.closed {
            Poll::Ready(Err(MuxError::ConnectionClosed))
        } else if handle.closed {
            // EOF
            Poll::Ready(Ok(None))
        } else {
            // No further packets, just wait
            handle.register_rx_ready_waker(cx);
            Poll::Pending
        }
    }

    fn poll_stream_write_ready(&mut self, cx: &Context<'_>, stream_id: u32) -> Poll<MuxResult<()>> {
        self.check_closed()?;
        debug_assert!(
            self.handles.contains_key(&stream_id),
            "poll_stream_write_ready called with unknown stream id {stream_id}"
        );
        let Some(handle) = self.handles.get_mut(&stream_id) else {
            return Poll::Ready(Err(MuxError::StreamClosed(stream_id)));
        };
        if handle.tx_queue.len() > self.max_tx_queue {
            // A stream's tx queue is full
            handle.register_tx_done_waker(cx);
            // Notify the worker to transfer data now
            self.notify_should_tx();
            Poll::Pending
        } else {
            Poll::Ready(Ok(()))
        }
    }

    fn enqueue_frame_stream(&mut self, stream_id: u32, frame: MuxFrame) {
        debug_assert!(
            self.handles.contains_key(&stream_id),
            "enqueue_frame_stream called with unknown stream id {stream_id}"
        );
        if let Some(handle) = self.handles.get_mut(&stream_id) {
            handle.tx_queue.push_back(frame);
            handle.last_active = Instant::now();
        }
        // No-op if the handle is gone; the caller's prior is_closed check
        // should have surfaced the closed state already.
    }

    #[inline]
    fn enqueue_frame_global(&mut self, frame: MuxFrame) {
        self.tx_queue.push_back(frame);
    }

    #[inline]
    fn register_should_tx_waker(&mut self, cx: &Context<'_>) {
        self.should_tx_waker = Some(cx.waker().clone());
    }

    #[inline]
    fn register_rx_consumed_waker(&mut self, cx: &Context<'_>) {
        self.rx_consumed_waker = Some(cx.waker().clone());
    }

    #[inline]
    fn notify_should_tx(&mut self) {
        if let Some(waker) = self.should_tx_waker.take() {
            waker.wake();
        }
    }

    #[inline]
    fn notify_rx_consumed(&mut self) {
        if let Some(waker) = self.rx_consumed_waker.take() {
            waker.wake();
        }
    }

    fn poll_flush_stream_frames(
        &mut self,
        cx: &mut Context<'_>,
        stream_id: u32,
    ) -> Poll<MuxResult<()>> {
        self.check_closed()?;
        debug_assert!(
            self.handles.contains_key(&stream_id),
            "poll_flush_stream_frames called with unknown stream id {stream_id}"
        );
        let Some(handle) = self.handles.get_mut(&stream_id) else {
            return Poll::Ready(Ok(()));
        };
        if handle.tx_queue.is_empty() {
            Poll::Ready(Ok(()))
        } else {
            handle.register_tx_done_waker(cx);
            self.notify_should_tx();
            Poll::Pending
        }
    }

    #[inline]
    fn register_accept_waker(&mut self, cx: &Context<'_>) {
        self.accept_waker = Some(cx.waker().clone());
    }

    #[inline]
    fn notify_accept_stream(&mut self) {
        if let Some(waker) = self.accept_waker.take() {
            waker.wake();
        }
    }

    /// Initiate an orderly shutdown: signal the sender to drain anything
    /// still queued, then hard-close. Idempotent.
    fn close(&mut self) {
        if self.closed || self.shutdown_requested {
            return;
        }
        self.shutdown_requested = true;
        // Reject any new SYN from the peer once shutdown has been
        // requested; the dispatcher would otherwise queue late streams
        // that nobody can ever accept.
        self.accept_closed = true;
        self.notify_accept_stream();
        // Tell the sender to wake up: it will drain remaining frames and
        // then call hard_close to actually tear things down.
        self.notify_should_tx();
        // If there's truly nothing left to flush - including Framed's own
        // BytesMut write buffer, which holds bytes that have been
        // start_send'd but not yet written to the underlying transport -
        // jump straight to hard close. Skipping the Framed check here is
        // unsafe: hard_close drops Framed without flushing it, so any
        // bytes still in the BytesMut are lost.
        let nothing_pending = self.tx_queue.is_empty()
            && self.handles.values().all(|h| h.tx_queue.is_empty())
            && self.inner.write_buffer().is_empty();
        if nothing_pending {
            self.hard_close();
        }
    }

    /// Immediate teardown: wake all wakers, mark every handle closed, and
    /// drop any pending data. Used for connection errors and as the
    /// final stage of orderly shutdown.
    fn hard_close(&mut self) {
        if self.closed {
            return;
        }
        self.closed = true;
        // Wake up everyone
        self.notify_accept_stream();
        self.notify_rx_consumed();
        self.notify_should_tx();
        if let Some(w) = self.close_waker.take() {
            w.wake();
        }
        for (_, h) in self.handles.iter_mut() {
            h.closed = true;
            h.notify_rx_ready();
            h.notify_tx_done();
        }
    }

    fn check_closed(&self) -> MuxResult<()> {
        if self.closed {
            Err(MuxError::ConnectionClosed)
        } else {
            Ok(())
        }
    }

    fn poll_flush_frames(&mut self, cx: &mut Context<'_>) -> Poll<MuxResult<()>> {
        // Global queue first - control frames (SYN/FIN/NOP) take priority.
        while !self.tx_queue.is_empty() {
            ready!(self.poll_write_ready(cx))?;
            let frame = self.tx_queue.pop_front().unwrap();
            self.write_frame(frame)?;
        }

        // Round-robin across per-stream tx_queues. Each outer pass pops at
        // most one frame from each non-empty stream, so a single noisy
        // stream can no longer starve smaller ones (head-of-line blocking
        // inside the worker). We also defer notify_tx_done until a
        // stream's queue actually drains, instead of waking the writer
        // after every single frame.
        loop {
            let ids: Vec<u32> = self
                .handles
                .iter()
                .filter(|(_, h)| !h.tx_queue.is_empty())
                .map(|(id, _)| *id)
                .collect();
            if ids.is_empty() {
                break;
            }
            let mut sent_any = false;
            for sid in ids {
                // poll_ready must come *before* the pop; otherwise a Pending
                // return would silently drop the frame.
                ready!(Pin::new(&mut self.inner).poll_ready(cx))?;
                let (frame, drained) = if let Some(h) = self.handles.get_mut(&sid) {
                    let f = h.tx_queue.pop_front();
                    (f, h.tx_queue.is_empty())
                } else {
                    (None, false)
                };
                let Some(frame) = frame else { continue };
                Pin::new(&mut self.inner).start_send(frame)?;
                sent_any = true;
                if drained {
                    if let Some(h) = self.handles.get_mut(&sid) {
                        h.notify_tx_done();
                    }
                }
            }
            if !sent_any {
                break;
            }
        }

        Poll::Ready(Ok(()))
    }

    fn poll_flush_inner(&mut self, cx: &mut Context<'_>) -> Poll<MuxResult<()>> {
        self.inner.poll_flush_unpin(cx)
    }

    fn poll_should_tx(&mut self, cx: &mut Context<'_>) -> Poll<()> {
        if self.tx_queue.is_empty() && self.handles.iter().all(|(_, h)| h.tx_queue.is_empty()) {
            self.register_should_tx_waker(cx);
            Poll::Pending
        } else {
            Poll::Ready(())
        }
    }
}

#[cfg(test)]
mod alloc_tests {
    use super::*;
    use crate::config::StreamIdType;
    use std::num::Wrapping;

    fn fresh_state(stream_id_type: StreamIdType) -> MuxState<tokio::io::DuplexStream> {
        // Construct a state without going through mux_connection so we can
        // poke at alloc_stream_id directly. The TokioConn we feed in is
        // unused by alloc_stream_id.
        let (a, _b) = tokio::io::duplex(64);
        MuxState {
            inner: Framed::new(a, MuxCodec {}),
            handles: HashMap::new(),
            accept_queue: VecDeque::new(),
            accept_waker: None,
            tx_queue: VecDeque::new(),
            should_tx_waker: None,
            rx_consumed_waker: None,
            closed: false,
            accept_closed: false,
            public_handles: 0,
            stream_id_hint: Wrapping(stream_id_type as u32),
            stream_id_type,
            idle_timeout: None,
            keep_alive_timeout: None,
            last_rx: Instant::now(),
            max_tx_queue: 1024,
            max_rx_queue: 1024,
            shutdown_requested: false,
            close_waker: None,
            closing_inline: false,
        }
    }

    // BUG: with stream_id_hint = Wrapping(0) (Even side), repeated alloc
    // wraps from u32::MAX-1 to 0. Stream id 0 is reserved for NOP keep-alive,
    // so alloc must skip it.
    #[test]
    fn alloc_stream_id_never_returns_zero_after_wrap() {
        let mut s = fresh_state(StreamIdType::Even);
        // Simulate already having consumed almost the full id space.
        s.stream_id_hint = Wrapping(u32::MAX - 1);
        let id = s.alloc_stream_id().unwrap();
        assert_ne!(id, 0, "stream id 0 is reserved and must not be allocated");
    }

    // BUG: peer should also not be allowed to use stream id 0 in SYN.
    #[test]
    fn process_sync_rejects_zero_stream_id() {
        // local Odd, so peer-Even (0) is otherwise parity-valid.
        let mut s = fresh_state(StreamIdType::Odd);
        let res = s.process_sync(0, Direction::Rx);
        assert!(res.is_err(), "SYN with stream id 0 must be rejected");
    }
}
