//! Asynchronous [smux](https://github.com/xtaci/smux) (Simple MUltipleXing)
//! for Tokio. Wraps any `AsyncRead + AsyncWrite + Unpin` transport and
//! exposes many bi-directional [`MuxStream`]s over it — each one
//! implements `AsyncRead + AsyncWrite` itself, so you can use them
//! anywhere a TCP stream would go.
//!
//! # Quickstart
//!
//! ```ignore
//! use async_smux::MuxBuilder;
//! use tokio::io::{AsyncReadExt, AsyncWriteExt};
//! use tokio::net::TcpStream;
//!
//! let tcp = TcpStream::connect("127.0.0.1:12345").await?;
//!
//! // build() returns three pieces:
//! //   connector — open outgoing streams
//! //   acceptor  — receive peer-initiated streams
//! //   worker    — the future that drives I/O; spawn it
//! let (connector, mut acceptor, worker) =
//!     MuxBuilder::client().with_connection(tcp).build();
//! tokio::spawn(worker);
//!
//! let mut s = connector.connect()?;
//! s.write_all(b"hello").await?;
//!
//! while let Some(mut peer) = acceptor.accept().await {
//!     // handle peer-initiated stream
//! }
//! ```
//!
//! Use [`MuxBuilder::server`] instead of `client()` on the listening
//! side; the only difference is stream-id parity (odd vs. even) so
//! locally-allocated ids never collide.
//!
//! # Lifecycle
//!
//! The worker future exits when all public handles
//! ([`MuxConnector`] + [`MuxAcceptor`] + [`MuxStream`]s) are dropped,
//! when [`MuxConnector::close`] is awaited, when the peer closes the
//! transport, or when a configured keep-alive timeout fires.
//!
//! `close()` performs an orderly shutdown — frames already accepted
//! via `AsyncWrite::poll_write` are drained to the wire before the
//! transport is closed. It does not require the worker to be polled,
//! and it is cancellation-safe: dropping the future mid-flight hands
//! control back to the worker without wedging it.
//!
//! # Configuration
//!
//! See [`MuxBuilder`] for the available knobs: `with_keep_alive_interval`,
//! `with_keep_alive_timeout`, `with_idle_timeout`, `with_max_tx_queue`,
//! `with_max_rx_queue`. Keep-alive and idle timeout are off unless
//! explicitly enabled.

pub mod builder;
pub mod config;
pub mod error;
pub(crate) mod frame;
pub(crate) mod mux;

pub use builder::MuxBuilder;
pub use config::{MuxConfig, StreamIdType};
pub use mux::{mux_connection, MuxAcceptor, MuxConnector, MuxStream};

#[cfg(test)]
mod tests {
    use std::{
        future::{poll_fn, Future},
        num::{NonZeroU64, NonZeroUsize},
        pin::Pin,
        sync::{
            atomic::{AtomicBool, AtomicUsize, Ordering},
            Arc, Mutex as StdMutex,
        },
        task::{Context, Poll, Waker},
        time::Duration,
    };

    use rand::Rng;
    use tokio::{
        io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt, ReadBuf},
        net::{TcpListener, TcpStream},
    };

    use crate::{builder::MuxBuilder, frame::MAX_PAYLOAD_SIZE, mux::TokioConn, MuxStream};

    async fn get_tcp_pair() -> (TcpStream, TcpStream) {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let h = tokio::spawn(async move {
            let (a, _) = listener.accept().await.unwrap();
            a
        });

        let b = TcpStream::connect(addr).await.unwrap();
        let a = h.await.unwrap();
        (a, b)
    }

    async fn get_duplex_mux_pair() -> (
        MuxStream<tokio::io::DuplexStream>,
        MuxStream<tokio::io::DuplexStream>,
    ) {
        let (a, b) = tokio::io::duplex(4096);
        let (connector_a, _acceptor_a, worker_a) = MuxBuilder::client().with_connection(a).build();
        let (_connector_b, mut acceptor_b, worker_b) =
            MuxBuilder::server().with_connection(b).build();
        tokio::spawn(worker_a);
        tokio::spawn(worker_b);

        let stream_a = connector_a.connect().unwrap();
        let stream_b = acceptor_b.accept().await.unwrap();
        (stream_a, stream_b)
    }

    fn raw_frame(command: u8, stream_id: u32, payload: &[u8]) -> Vec<u8> {
        let mut frame = Vec::with_capacity(8 + payload.len());
        frame.push(1); // smux v1
        frame.push(command);
        frame.extend_from_slice(&(payload.len() as u16).to_le_bytes());
        frame.extend_from_slice(&stream_id.to_le_bytes());
        frame.extend_from_slice(payload);
        frame
    }

    /// A transport whose read and shutdown paths deliberately share one waker
    /// slot. This is legal for `AsyncRead + AsyncWrite` implementations and
    /// lets the close tests catch actors that poll the same carrier with
    /// different task wakers during shutdown.
    #[derive(Default)]
    struct ShutdownGateState {
        shutdown_started: AtomicBool,
        allow_shutdown: AtomicBool,
        write_after_shutdown: AtomicBool,
        read_polls: AtomicUsize,
        io_waker: StdMutex<Option<Waker>>,
    }

    impl ShutdownGateState {
        fn register(&self, waker: &Waker) {
            *self.io_waker.lock().unwrap() = Some(waker.clone());
        }

        fn allow_shutdown(&self) {
            self.allow_shutdown.store(true, Ordering::SeqCst);
            if let Some(waker) = self.io_waker.lock().unwrap().take() {
                waker.wake();
            }
        }
    }

    struct ShutdownGate {
        state: Arc<ShutdownGateState>,
    }

    impl AsyncRead for ShutdownGate {
        fn poll_read(
            self: Pin<&mut Self>,
            cx: &mut Context<'_>,
            _buf: &mut ReadBuf<'_>,
        ) -> Poll<std::io::Result<()>> {
            self.state.read_polls.fetch_add(1, Ordering::SeqCst);
            self.state.register(cx.waker());
            Poll::Pending
        }
    }

    impl AsyncWrite for ShutdownGate {
        fn poll_write(
            self: Pin<&mut Self>,
            _cx: &mut Context<'_>,
            buf: &[u8],
        ) -> Poll<std::io::Result<usize>> {
            if self.state.shutdown_started.load(Ordering::SeqCst) {
                self.state
                    .write_after_shutdown
                    .store(true, Ordering::SeqCst);
                return Poll::Ready(Err(std::io::Error::new(
                    std::io::ErrorKind::BrokenPipe,
                    "write after shutdown started",
                )));
            }
            Poll::Ready(Ok(buf.len()))
        }

        fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
            Poll::Ready(Ok(()))
        }

        fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
            self.state.shutdown_started.store(true, Ordering::SeqCst);
            if self.state.allow_shutdown.load(Ordering::SeqCst) {
                Poll::Ready(Ok(()))
            } else {
                self.state.register(cx.waker());
                Poll::Pending
            }
        }
    }

    async fn test_stream<T: TokioConn>(mut a: MuxStream<T>, mut b: MuxStream<T>) {
        const LEN: usize = MAX_PAYLOAD_SIZE + 0x200;
        let mut data1 = vec![0; LEN];
        let mut data2 = vec![0; LEN];
        rand::rng().fill_bytes(&mut data1);
        rand::rng().fill_bytes(&mut data2);

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
        let (connector_a, mut acceptor_a, worker_a) =
            MuxBuilder::client().with_connection(a).build();
        let (connector_b, mut acceptor_b, worker_b) =
            MuxBuilder::server().with_connection(b).build();
        tokio::spawn(worker_a);
        tokio::spawn(worker_b);

        let stream1 = connector_a.clone().connect().unwrap();
        let stream2 = acceptor_b.accept().await.unwrap();
        test_stream(stream1, stream2).await;

        let stream1 = connector_b.connect().unwrap();
        let stream2 = acceptor_a.accept().await.unwrap();
        test_stream(stream1, stream2).await;

        assert_eq!(connector_a.get_num_streams(), 0);
        assert_eq!(connector_b.get_num_streams(), 0);

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

        assert_eq!(connector_a.get_num_streams(), 0);
        assert_eq!(connector_b.get_num_streams(), 0);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_worker_drop() {
        let (a, b) = get_tcp_pair().await;
        let (connector_a, mut acceptor_a, worker_a) =
            MuxBuilder::client().with_connection(a).build();
        let (connector_b, mut acceptor_b, worker_b) =
            MuxBuilder::server().with_connection(b).build();
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
        let (connector_a, acceptor_a, worker_a) = MuxBuilder::client().with_connection(a).build();
        let (connector_b, mut acceptor_b, worker_b) =
            MuxBuilder::server().with_connection(b).build();
        tokio::spawn(worker_a);
        tokio::spawn(worker_b);

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
        assert_eq!(buf, data);
        assert_eq!(stream1.read(&mut buf).await.unwrap(), 0);

        drop(acceptor_a);
        let mut stream = connector_b.connect().unwrap();
        assert_eq!(stream.read(&mut buf).await.unwrap(), 0);
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

    #[tokio::test]
    async fn test_timeout() {
        let (a, b) = get_tcp_pair().await;
        let (connector_a, _, worker_a) = MuxBuilder::client()
            .with_idle_timeout(NonZeroU64::new(3).unwrap())
            .with_connection(a)
            .build();
        let (_, mut acceptor_b, worker_b) = MuxBuilder::server().with_connection(b).build();
        tokio::spawn(async move {
            worker_a.await.unwrap();
        });
        tokio::spawn(async move {
            worker_b.await.unwrap();
        });

        let stream1 = connector_a.connect().unwrap();
        let stream2 = acceptor_b.accept().await.unwrap();
        tokio::time::sleep(Duration::from_secs(1)).await;
        assert!(!stream1.is_closed());
        assert!(!stream2.is_closed());

        tokio::time::sleep(Duration::from_secs(5)).await;

        assert!(stream1.is_closed());
        assert!(stream2.is_closed());
    }

    #[tokio::test]
    async fn test_recv_block() {
        let (a, b) = tokio::io::duplex(4096);
        let (connector_a, _, worker_a) = MuxBuilder::client().with_connection(a).build();
        let (_, mut acceptor_b, worker_b) = MuxBuilder::server()
            .with_max_rx_queue(NonZeroUsize::new(1).unwrap())
            .with_connection(b)
            .build();
        tokio::spawn(worker_a);
        tokio::spawn(worker_b);

        let mut stream_x1 = connector_a.connect().unwrap();
        let mut stream_x2 = acceptor_b.accept().await.unwrap();

        let mut stream_y1 = connector_a.connect().unwrap();
        let mut stream_y2 = acceptor_b.accept().await.unwrap();

        let data = &[1, 2, 3, 4];
        stream_x1.write_all(data).await.unwrap();
        stream_x1.flush().await.unwrap();
        // The single x frame fills the receive budget. The y frame reaches
        // the carrier but cannot be dispatched until x is consumed.
        stream_y1.write_all(data).await.unwrap();
        stream_y1.flush().await.unwrap();

        let y_is_pending = poll_fn(|cx| {
            let mut buf = [0; 128];
            let mut buf = ReadBuf::new(&mut buf);
            let res = Pin::new(&mut stream_y2).poll_read(cx, &mut buf);
            Poll::Ready(res.is_pending())
        })
        .await;
        assert!(y_is_pending, "y was dispatched despite a full RX budget");

        let mut buf = [0; 4];
        stream_x2.read_exact(&mut buf).await.unwrap();
        assert_eq!(&buf, data);

        tokio::time::timeout(Duration::from_secs(1), stream_y2.read_exact(&mut buf))
            .await
            .expect("dispatcher did not resume after RX consumption")
            .unwrap();
        assert_eq!(&buf, data);
    }

    #[tokio::test]
    async fn test_connection_drop() {
        let (a, b) = get_tcp_pair().await;
        let (connector_a, _, worker_a) = MuxBuilder::client().with_connection(a).build();
        let (_, mut acceptor_b, worker_b) = MuxBuilder::server().with_connection(b).build();
        tokio::spawn(worker_a);
        tokio::spawn(worker_b);

        let mut _stream1 = connector_a.connect().unwrap();
        let mut stream2 = acceptor_b.accept().await.unwrap();

        drop(_stream1);
        tokio::time::sleep(Duration::from_secs(1)).await;

        assert!(stream2.write_all(b"1234").await.is_err());
    }

    #[tokio::test]
    async fn test_inner_shutdown() {
        let (a, b) = get_tcp_pair().await;

        let (connector_a, mut acceptor_a, worker_a) =
            MuxBuilder::client().with_connection(a).build();
        let (connector_b, mut acceptor_b, worker_b) =
            MuxBuilder::server().with_connection(b).build();

        let a_res = tokio::spawn(worker_a);
        drop(worker_b);
        tokio::time::sleep(Duration::from_secs(2)).await;

        assert!(connector_b.connect().is_err());
        assert!(acceptor_b.accept().await.is_none());

        drop(connector_b);
        drop(acceptor_b);

        tokio::time::sleep(Duration::from_secs(2)).await;
        assert!(connector_a.connect().is_err());
        assert!(acceptor_a.accept().await.is_none());
        a_res.await.unwrap().unwrap_err();
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_drop_acceptor_cleans_unaccepted_incoming_streams() {
        let (a, b) = get_tcp_pair().await;
        let (connector_a, acceptor_a, worker_a) = MuxBuilder::client()
            .with_max_rx_queue(NonZeroUsize::new(8).unwrap())
            .with_connection(a)
            .build();
        let (connector_b, mut acceptor_b, worker_b) = MuxBuilder::server()
            .with_max_rx_queue(NonZeroUsize::new(8).unwrap())
            .with_connection(b)
            .build();

        tokio::spawn(worker_a);
        tokio::spawn(worker_b);

        const N: usize = 3;
        let mut outbound = Vec::with_capacity(N);
        for _ in 0..N {
            outbound.push(connector_b.connect().unwrap());
        }

        tokio::time::timeout(Duration::from_secs(1), async {
            while connector_a.get_num_streams() < N {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .unwrap();

        for stream in &mut outbound {
            for _ in 0..64 {
                stream.write_all(b"0123456789abcdef").await.unwrap();
            }
            stream.flush().await.unwrap();
        }

        drop(acceptor_a);
        assert_eq!(connector_a.get_num_streams(), 0);

        let mut c2s = connector_a.connect().unwrap();
        let mut s2c = acceptor_b.accept().await.unwrap();
        c2s.write_all(b"ping").await.unwrap();
        c2s.flush().await.unwrap();
        let mut buf = [0u8; 4];
        s2c.read_exact(&mut buf).await.unwrap();
        assert_eq!(&buf, b"ping");
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_drop_all_public_handles_stops_worker() {
        let (a, b) = get_tcp_pair().await;
        let (connector_a, acceptor_a, worker_a) = MuxBuilder::client().with_connection(a).build();
        let (_, _, worker_b) = MuxBuilder::server().with_connection(b).build();

        let h_a = tokio::spawn(worker_a);
        let _h_b = tokio::spawn(worker_b);

        drop(connector_a);
        drop(acceptor_a);

        let res = tokio::time::timeout(Duration::from_secs(2), h_a)
            .await
            .unwrap()
            .unwrap();
        assert!(
            res.is_ok(),
            "dropping the last public handle is an orderly local shutdown"
        );
    }

    // BUG: writing then dropping the stream (without explicit flush/shutdown)
    // loses everything still queued in the per-stream tx_queue, because Drop
    // calls remove_stream which drops the StreamHandle outright.
    #[tokio::test]
    async fn test_drop_after_write_preserves_queued_data() {
        let (a, b) = get_tcp_pair().await;
        let (connector_a, _, worker_a) = MuxBuilder::client().with_connection(a).build();
        let (_, mut acceptor_b, worker_b) = MuxBuilder::server().with_connection(b).build();
        tokio::spawn(worker_a);
        tokio::spawn(worker_b);

        let mut stream1 = connector_a.connect().unwrap();
        let mut stream2 = acceptor_b.accept().await.unwrap();

        let data = [1u8, 2, 3, 4, 5, 6, 7, 8];
        stream1.write_all(&data).await.unwrap();
        // No flush, no shutdown. The protocol contract should still deliver
        // anything already accepted by AsyncWrite::poll_write.
        drop(stream1);

        let mut buf = [0u8; 8];
        tokio::time::timeout(Duration::from_secs(2), stream2.read_exact(&mut buf))
            .await
            .expect("data should be delivered after drop")
            .expect("read should not error");
        assert_eq!(buf, data);
    }

    // BUG: After local shutdown we returned EOF, but a subsequent peer Push
    // (peer hadn't seen our FIN yet) would still populate rx_queue and a later
    // poll_read would surface that data, breaking AsyncRead EOF monotonicity.
    #[tokio::test(flavor = "multi_thread")]
    async fn test_eof_monotonic_after_local_shutdown() {
        let (a, b) = get_tcp_pair().await;
        let (connector_a, _, worker_a) = MuxBuilder::client().with_connection(a).build();
        let (_, mut acceptor_b, worker_b) = MuxBuilder::server().with_connection(b).build();
        tokio::spawn(worker_a);
        tokio::spawn(worker_b);

        let mut stream1 = connector_a.connect().unwrap();
        let mut stream2 = acceptor_b.accept().await.unwrap();

        // Local shutdown -> our handle.closed=true, FIN enqueued globally.
        stream1.shutdown().await.unwrap();

        // First read sees EOF immediately (rx_queue empty + handle.closed).
        let mut buf = [0u8; 4];
        let n = stream1.read(&mut buf).await.unwrap();
        assert_eq!(n, 0, "first read after shutdown must be EOF");

        // Peer races and writes data before processing our FIN. Inject a
        // PSH frame on the wire from b's side - but stream2's writer is
        // still open from b's perspective.
        let _ = stream2.write_all(b"late").await;
        let _ = stream2.flush().await;

        // Give the dispatcher time to receive the late PSH.
        tokio::time::sleep(Duration::from_millis(200)).await;

        // EOF must remain EOF.
        let n = stream1.read(&mut buf).await.unwrap();
        assert_eq!(
            n, 0,
            "subsequent read must remain EOF, not return late data"
        );
    }

    // BUG: poll_flush returns Err the moment the stream is locally closed,
    // even though there is still data queued in handle.tx_queue that
    // could be drained.
    #[tokio::test(flavor = "multi_thread")]
    async fn test_flush_drains_before_reporting_close() {
        let (a, b) = get_tcp_pair().await;
        let (connector_a, _, worker_a) = MuxBuilder::client().with_connection(a).build();
        let (_, mut acceptor_b, worker_b) = MuxBuilder::server().with_connection(b).build();
        tokio::spawn(worker_a);
        tokio::spawn(worker_b);

        let mut stream1 = connector_a.connect().unwrap();
        let mut stream2 = acceptor_b.accept().await.unwrap();

        let data = b"hello world";
        stream1.write_all(data).await.unwrap();
        // shutdown will mark our handle.closed=true and enqueue FIN. Pending
        // PSH frames from the write_all above should still go out.
        stream1.shutdown().await.unwrap();

        let mut buf = vec![0u8; data.len()];
        tokio::time::timeout(Duration::from_secs(2), stream2.read_exact(&mut buf))
            .await
            .expect("data must be delivered before EOF")
            .expect("read should succeed");
        assert_eq!(&buf, data);
    }

    // BUG (Display formatting): InvalidPeerStreamIdType used {0:?} where
    // {1:?} was meant, so the StreamIdType part of the message printed the
    // stream id again instead of the type.
    #[test]
    fn test_invalid_peer_stream_id_type_display() {
        use crate::config::StreamIdType;
        use crate::error::MuxError;
        let e = MuxError::InvalidPeerStreamIdType(7, StreamIdType::Even);
        let msg = format!("{}", e);
        assert!(
            msg.contains("Even"),
            "Display should mention type, got: {msg}"
        );
    }

    // BUG: MuxConnector::close used to call state.inner.poll_close_unpin
    // directly, racing with the worker's sender for the framed inner's
    // waker slots. The orderly-shutdown path lets the worker do the close
    // serially, so explicit close from the user must also drain pending
    // frames.
    #[tokio::test(flavor = "multi_thread")]
    async fn test_explicit_connector_close_flushes_pending() {
        let (a, b) = get_tcp_pair().await;
        let (mut connector_a, _, worker_a) = MuxBuilder::client().with_connection(a).build();
        let (_, mut acceptor_b, worker_b) = MuxBuilder::server().with_connection(b).build();
        let h_a = tokio::spawn(worker_a);
        tokio::spawn(worker_b);

        let mut stream1 = connector_a.connect().unwrap();
        let mut stream2 = acceptor_b.accept().await.unwrap();

        let data = b"close-then-flush";
        stream1.write_all(data).await.unwrap();
        drop(stream1);

        // Explicit close should not return until the inner sink has been
        // closed by the worker, not by us racing on it.
        connector_a.close().await.unwrap();

        let mut buf = vec![0u8; data.len()];
        tokio::time::timeout(Duration::from_secs(2), stream2.read_exact(&mut buf))
            .await
            .expect("data must arrive before close completes")
            .unwrap();
        assert_eq!(&buf, data);

        // And the worker future should now resolve cleanly.
        let res = tokio::time::timeout(Duration::from_secs(2), h_a)
            .await
            .expect("worker did not exit after explicit close");
        let _ = res.unwrap();
    }

    // BUG: when the last public handle (connector / acceptor / streams) was
    // dropped, dec_public_handles immediately called state.close(), which
    // set state.closed=true. The sender's next poll then errored out
    // without flushing the global tx_queue - so PSH/FIN frames already
    // queued by drops in flight were lost on the wire.
    #[tokio::test(flavor = "multi_thread")]
    async fn test_orderly_shutdown_flushes_pending_data() {
        let (a, b) = get_tcp_pair().await;
        let (connector_a, acceptor_a, worker_a) = MuxBuilder::client().with_connection(a).build();
        let (_, mut acceptor_b, worker_b) = MuxBuilder::server().with_connection(b).build();
        tokio::spawn(worker_a);
        tokio::spawn(worker_b);

        let mut stream1 = connector_a.connect().unwrap();
        let mut stream2 = acceptor_b.accept().await.unwrap();

        let data = b"hello orderly shutdown";
        stream1.write_all(data).await.unwrap();
        // Drop stream then *immediately* drop the remaining handles. This
        // races: per-stream tx_queue is moved to global by Drop, FIN is
        // enqueued, then dec_public_handles -> 0 -> shutdown.
        drop(stream1);
        drop(connector_a);
        drop(acceptor_a);

        let mut buf = vec![0u8; data.len()];
        tokio::time::timeout(Duration::from_secs(2), stream2.read_exact(&mut buf))
            .await
            .expect("data must still be delivered after orderly shutdown")
            .expect("read should succeed");
        assert_eq!(&buf, data);
    }

    // BUG: enabling keep_alive_interval only sent NOPs; nothing watched
    // whether the peer was still answering. With a silent-but-not-RST peer
    // (e.g. host died, NAT drop) the worker stayed alive forever. With
    // dead-peer detection the worker should error out within a few keep-
    // alive intervals.
    #[tokio::test(flavor = "multi_thread")]
    async fn test_keep_alive_detects_dead_peer() {
        // Use an in-memory duplex channel as the underlying transport, then
        // forget the b half: bytes go nowhere and nothing comes back, but
        // the socket is not closed - simulating a black-holed peer.
        let (a, _peer_held_open) = tokio::io::duplex(1024);

        let (connector_a, _acceptor_a, worker_a) = MuxBuilder::client()
            .with_keep_alive_interval(NonZeroU64::new(1).unwrap())
            .with_keep_alive_timeout(NonZeroU64::new(2).unwrap())
            .with_connection(a)
            .build();

        let h = tokio::spawn(worker_a);
        let mut stream = connector_a.connect().unwrap();

        // Within a few keep-alive intervals the worker should declare the
        // peer dead and exit.
        let res = tokio::time::timeout(Duration::from_secs(6), h).await;
        let outer = res.expect("worker did not exit on dead peer");
        let inner = outer.expect("worker task panicked");
        assert!(
            inner.is_err(),
            "worker should report ConnectionClosed on dead peer"
        );

        // Pending stream should also be unblocked / closed.
        let mut buf = [0u8; 1];
        let read = stream.read(&mut buf).await;
        assert!(read.is_err() || read.unwrap() == 0);
    }

    // BUG: poll_write checked back-pressure once at entry, then enqueued
    // arbitrarily many frames from a single buf. A 1 MiB write with
    // max_tx_queue=8 would still create ~16 frames and bypass the limit.
    // After the fix, a single poll_write should produce at most one frame
    // (MAX_PAYLOAD_SIZE), and write_all should drive multiple polls.
    #[tokio::test]
    async fn test_poll_write_respects_max_tx_queue_per_call() {
        use std::future::poll_fn;
        use std::pin::Pin;
        use tokio::io::AsyncWrite;

        let (a, _b) = get_tcp_pair().await;
        // Keep the worker future alive but never poll it - so the tx_queue
        // can never drain. This isolates what a single poll_write actually
        // enqueues from any concurrent draining.
        let (connector_a, _acceptor_a, _worker_a) = MuxBuilder::client()
            .with_max_tx_queue(NonZeroUsize::new(2).unwrap())
            .with_connection(a)
            .build();

        let mut stream = connector_a.connect().unwrap();
        let big = vec![0u8; MAX_PAYLOAD_SIZE * 8];
        let n = poll_fn(|cx| Pin::new(&mut stream).poll_write(cx, &big))
            .await
            .unwrap();
        assert!(
            n <= MAX_PAYLOAD_SIZE,
            "single poll_write should not exceed one frame's worth, got {}",
            n
        );
    }

    // BUG: poll_flush_frames drained one stream's whole tx_queue before
    // moving to the next, so a noisy stream could starve a small one. With
    // round-robin scheduling, frames from concurrent streams should
    // interleave on the wire instead of being grouped per stream.
    #[tokio::test(flavor = "multi_thread")]
    async fn test_flush_frames_round_robin_across_streams() {
        let (a, mut peer) = tokio::io::duplex(4096);
        let (connector, _acceptor, worker) = MuxBuilder::client().with_connection(a).build();

        // Queue all work before the worker starts so both streams are
        // continuously eligible during the sender's scheduling pass.
        let mut s1 = connector.connect().unwrap();
        let mut s2 = connector.connect().unwrap();
        let s1_id = s1.get_stream_id();
        let s2_id = s2.get_stream_id();

        const FRAMES_PER_STREAM: usize = 8;
        for _ in 0..FRAMES_PER_STREAM {
            s1.write_all(&[1]).await.unwrap();
            s2.write_all(&[2]).await.unwrap();
        }
        tokio::spawn(worker);

        // Two SYNs followed by 16 one-byte PSHs.
        let expected_len = 2 * 8 + 2 * FRAMES_PER_STREAM * 9;
        let mut wire = vec![0u8; expected_len];
        tokio::time::timeout(Duration::from_secs(1), peer.read_exact(&mut wire))
            .await
            .expect("sender did not drain queued streams")
            .unwrap();

        let mut push_ids = Vec::new();
        let mut offset = 0;
        while offset < wire.len() {
            let command = wire[offset + 1];
            let payload_len = u16::from_le_bytes([wire[offset + 2], wire[offset + 3]]) as usize;
            let stream_id = u32::from_le_bytes([
                wire[offset + 4],
                wire[offset + 5],
                wire[offset + 6],
                wire[offset + 7],
            ]);
            if command == 2 {
                push_ids.push(stream_id);
            }
            offset += 8 + payload_len;
        }

        assert_eq!(push_ids.len(), 2 * FRAMES_PER_STREAM);
        assert!(push_ids.iter().all(|id| *id == s1_id || *id == s2_id));
        assert!(
            push_ids.windows(2).all(|pair| pair[0] != pair[1]),
            "sender did not alternate ready streams: {push_ids:?}"
        );
    }

    // BUG: control frames (SYN/FIN/NOP) are spec'd as length=0, but the
    // decoder accepted any length and silently swallowed payload (NOP could
    // carry up to 64 KiB of garbage). Verify rejection.
    #[test]
    fn test_decode_rejects_nop_with_payload() {
        use crate::frame::MuxCodec;
        use bytes::BytesMut;
        use tokio_util::codec::Decoder;

        let mut codec = MuxCodec {};
        let mut buf = BytesMut::new();
        // version=1, cmd=NOP(3), length=4 LE, stream_id=0 LE, payload=4 bytes.
        buf.extend_from_slice(&[1, 3, 4, 0, 0, 0, 0, 0, 0xaa, 0xbb, 0xcc, 0xdd]);
        let res = codec.decode(&mut buf);
        assert!(res.is_err(), "NOP with non-zero length must be rejected");
    }

    #[test]
    fn test_decode_rejects_invalid_reserved_stream_ids() {
        use crate::frame::MuxCodec;
        use bytes::BytesMut;
        use tokio_util::codec::Decoder;

        let mut codec = MuxCodec {};

        let mut nop_with_stream = BytesMut::from(&raw_frame(3, 2, &[])[..]);
        assert!(
            codec.decode(&mut nop_with_stream).is_err(),
            "NOP must use reserved stream id 0"
        );

        let mut push_on_zero = BytesMut::from(&raw_frame(2, 0, b"x")[..]);
        assert!(
            codec.decode(&mut push_on_zero).is_err(),
            "PSH must not use reserved stream id 0"
        );
    }

    #[test]
    fn test_invalid_control_header_is_rejected_without_waiting_for_payload() {
        use crate::frame::MuxCodec;
        use bytes::BytesMut;
        use tokio_util::codec::Decoder;

        // A control frame can never carry a payload, so this header is
        // already invalid even though its claimed 64 KiB body has not arrived.
        let mut wire = BytesMut::from(&[1, 0, 0xff, 0xff, 2, 0, 0, 0][..]);
        let mut codec = MuxCodec {};
        assert!(codec.decode(&mut wire).is_err());
    }

    #[test]
    fn test_decoder_does_not_preallocate_max_frame_for_empty_input() {
        use crate::frame::{MuxCodec, HEADER_SIZE};
        use bytes::BytesMut;
        use tokio_util::codec::Decoder;

        let mut wire = BytesMut::new();
        let mut codec = MuxCodec {};
        assert!(codec.decode(&mut wire).unwrap().is_none());
        assert!(
            wire.capacity() <= HEADER_SIZE * 2,
            "empty decode preallocated {} bytes",
            wire.capacity()
        );
    }

    // BUG: close()'s `nothing_pending` shortcut to hard_close bypasses
    // Framed's BytesMut flush. With a slow underlying transport, the sender
    // may have moved bytes into Framed (start_send) but Framed::poll_flush
    // returned Pending. At that moment, tx_queue and per-stream queues are
    // both empty, so the shortcut fires and hard_close drops Framed -
    // taking the unflushed BytesMut content with it.
    #[tokio::test(flavor = "multi_thread")]
    async fn test_close_does_not_drop_framed_buffered_bytes() {
        use tokio::io::AsyncReadExt;

        // 32-byte duplex forces Framed::poll_flush to return Pending on
        // any non-trivial payload, so the producer side ends up with
        // unflushed bytes in Framed.
        let (a, b) = tokio::io::duplex(32);
        let (connector_a, _, worker_a) = MuxBuilder::client().with_connection(a).build();
        let _h = tokio::spawn(worker_a);

        // Slow drainer keeps the duplex mostly full while the sender is
        // working, then drains everything once the producer side closes.
        let drainer = tokio::spawn(async move {
            let mut all = Vec::new();
            let mut b = b;
            let mut tmp = [0u8; 16];
            loop {
                tokio::time::sleep(Duration::from_millis(5)).await;
                match b.read(&mut tmp).await {
                    Ok(0) => break,
                    Ok(n) => all.extend_from_slice(&tmp[..n]),
                    Err(_) => break,
                }
            }
            all
        });

        let mut stream = connector_a.connect().unwrap();
        let payload: Vec<u8> = (0..200u8).collect();
        stream.write_all(&payload).await.unwrap();
        stream.flush().await.unwrap();

        // After flush(), per-stream tx_queue is empty. Drop the stream to
        // enqueue FIN, then sleep so the sender drains FIN into Framed
        // (leaving global tx_queue empty too). After this sleep, both
        // queues are empty but Framed still holds unflushed bytes.
        drop(stream);
        tokio::time::sleep(Duration::from_millis(50)).await;

        // Now drop the connector. dec_public_handles -> close() shortcut
        // fires because both queues are empty - even though Framed still
        // holds unflushed bytes. With the bug, those bytes are lost.
        drop(connector_a);

        let bytes = tokio::time::timeout(Duration::from_secs(3), drainer)
            .await
            .expect("drainer never finished")
            .unwrap();

        // Wire layout: SYN(8) + PSH header(8) + payload(200) + FIN(8) = 224.
        let expected = 8 + 8 + payload.len() + 8;
        assert_eq!(
            bytes.len(),
            expected,
            "close() lost Framed-buffered bytes: got {}, expected {}",
            bytes.len(),
            expected,
        );
    }

    // BUG: MuxConnector::close uses close_waker to wait for the worker's
    // sender to drive shutdown. If the user forgets to spawn the worker
    // (or the worker has already exited), close hangs forever. This is a
    // regression vs. the pre-soft-close code, which drove poll_close_unpin
    // directly.
    #[tokio::test(flavor = "multi_thread")]
    async fn test_close_completes_without_running_worker() {
        let (a, _b) = tokio::io::duplex(1024);
        let (mut connector_a, _, _worker_a) = MuxBuilder::client().with_connection(a).build();
        // Worker NOT spawned. close() must still finish in finite time.

        tokio::time::timeout(Duration::from_secs(1), connector_a.close())
            .await
            .expect("close hung when worker isn't running")
            .unwrap();
    }

    // BUG: same hang as above, but with in-flight data. The "trivial"
    // version above passes today only because the close()-side shortcut
    // hits hard_close immediately when nothing is queued. Once anything
    // is in flight (per-stream tx_queue, global tx_queue, or Framed
    // BytesMut), close() needs the worker's sender to drain it - which
    // never runs here.
    #[tokio::test(flavor = "multi_thread")]
    async fn test_close_flushes_in_flight_data_without_worker() {
        let (a, b) = get_tcp_pair().await;
        let (mut connector_a, _acc, _worker_a) = MuxBuilder::client().with_connection(a).build();
        // Worker NOT spawned. close() must drive its own shutdown so the
        // bytes the user already accepted via write_all reach the wire.

        let reader = tokio::spawn(async move {
            let mut all = Vec::new();
            let mut b = b;
            let mut tmp = [0u8; 1024];
            loop {
                match b.read(&mut tmp).await {
                    Ok(0) => break,
                    Ok(n) => all.extend_from_slice(&tmp[..n]),
                    Err(_) => break,
                }
            }
            all
        });

        let mut stream = connector_a.connect().unwrap();
        let payload = b"hello-no-worker";
        stream.write_all(payload).await.unwrap();
        drop(stream);

        tokio::time::timeout(Duration::from_secs(2), connector_a.close())
            .await
            .expect("close hung with in-flight data and no worker")
            .unwrap();

        let received = tokio::time::timeout(Duration::from_secs(2), reader)
            .await
            .unwrap()
            .unwrap();

        // SYN(8) + PSH header(8) + payload + FIN(8) = 24 + payload.len()
        let expected = 8 + 8 + payload.len() + 8;
        assert_eq!(
            received.len(),
            expected,
            "in-flight data was lost during close without worker"
        );
    }

    // BUG: MuxConnector::close sets closing_inline=true to make the worker's
    // sender step out of the way. If the close() future is dropped before
    // it finishes (e.g. select! racing it against a timeout), the flag
    // would stick at true and the sender would be parked forever — silently
    // wedging the worker.
    #[tokio::test(flavor = "multi_thread")]
    async fn test_close_cancellation_resets_closing_inline() {
        // Hold the peer half so the duplex never drains. close() will park
        // in poll_close_unpin and never finish under the timeout.
        let (a, _b_held) = tokio::io::duplex(32);
        let (mut connector_a, _acc, _worker_a) = MuxBuilder::client().with_connection(a).build();

        let mut s = connector_a.connect().unwrap();
        s.write_all(&vec![0u8; 1024]).await.unwrap();
        drop(s);

        // The peer never reads, so close() cannot complete. tight timeout
        // wins and drops the close() future.
        tokio::time::timeout(Duration::from_millis(100), connector_a.close())
            .await
            .expect_err("close should hang while peer is jammed");

        assert!(
            !connector_a.is_closing_inline(),
            "closing_inline must be reset after close() future is cancelled"
        );
    }

    #[tokio::test]
    async fn test_stream_drop_during_connection_close_does_not_write_after_shutdown() {
        let gate_state = Arc::new(ShutdownGateState::default());
        let transport = ShutdownGate {
            state: gate_state.clone(),
        };
        let (mut connector, _acceptor, _worker) =
            MuxBuilder::client().with_connection(transport).build();
        let stream = connector.connect().unwrap();

        let mut close = Box::pin(connector.close());
        let first_poll = poll_fn(|cx| Poll::Ready(close.as_mut().poll(cx))).await;
        assert!(first_poll.is_pending());
        assert!(gate_state.shutdown_started.load(Ordering::SeqCst));

        // Once the carrier's shutdown has started, a concurrently dropped
        // stream must not append a FIN that forces a subsequent write.
        drop(stream);
        let second_poll = poll_fn(|cx| Poll::Ready(close.as_mut().poll(cx))).await;
        assert!(
            second_poll.is_pending(),
            "dropping a stream queued a frame after carrier shutdown: {second_poll:?}"
        );
        assert!(
            !gate_state.write_after_shutdown.load(Ordering::SeqCst),
            "connection close attempted a write after poll_shutdown"
        );

        gate_state.allow_shutdown();
        close.await.unwrap();
    }

    #[tokio::test]
    async fn test_dispatcher_does_not_overwrite_connection_close_waker() {
        let gate_state = Arc::new(ShutdownGateState::default());
        let transport = ShutdownGate {
            state: gate_state.clone(),
        };
        let (mut connector, _acceptor, worker) =
            MuxBuilder::client().with_connection(transport).build();
        let mut worker = Box::pin(worker);

        let initial_worker_poll = poll_fn(|cx| Poll::Ready(worker.as_mut().poll(cx))).await;
        assert!(initial_worker_poll.is_pending());
        assert_eq!(gate_state.read_polls.load(Ordering::SeqCst), 1);

        // close() installs its shutdown waker in the transport's shared slot.
        let mut close = Box::pin(connector.close());
        let close_poll = poll_fn(|cx| Poll::Ready(close.as_mut().poll(cx))).await;
        assert!(close_poll.is_pending());
        assert!(gate_state.shutdown_started.load(Ordering::SeqCst));

        // A worker poll during shutdown must not reach AsyncRead. Otherwise a
        // transport with one read/write waker slot can strand the close task.
        let closing_worker_poll = poll_fn(|cx| Poll::Ready(worker.as_mut().poll(cx))).await;
        assert!(closing_worker_poll.is_pending());
        assert_eq!(
            gate_state.read_polls.load(Ordering::SeqCst),
            1,
            "dispatcher polled the carrier after connection close began"
        );

        gate_state.allow_shutdown();
        close.await.unwrap();
    }

    #[tokio::test]
    async fn test_empty_write_does_not_create_false_eof() {
        let (mut tx, mut rx) = get_duplex_mux_pair().await;

        assert_eq!(tx.write(&[]).await.unwrap(), 0);
        tx.write_all(b"x").await.unwrap();
        tx.flush().await.unwrap();

        let mut byte = [0u8; 1];
        let n = tokio::time::timeout(Duration::from_secs(1), rx.read(&mut byte))
            .await
            .expect("read timed out")
            .unwrap();
        assert_eq!(n, 1, "an empty write must not surface as EOF");
        assert_eq!(byte, *b"x");
    }

    #[tokio::test]
    async fn test_peer_zero_length_push_does_not_create_false_eof() {
        let (a, mut peer) = tokio::io::duplex(4096);
        let (_connector, mut acceptor, worker) = MuxBuilder::client().with_connection(a).build();
        tokio::spawn(worker);

        let stream_id = 2; // peer ids are even when the local side is a client
        peer.write_all(&raw_frame(0, stream_id, &[])).await.unwrap();
        peer.write_all(&raw_frame(2, stream_id, &[])).await.unwrap();
        peer.write_all(&raw_frame(2, stream_id, b"x"))
            .await
            .unwrap();

        let mut stream = acceptor.accept().await.unwrap();
        let mut byte = [0u8; 1];
        let n = tokio::time::timeout(Duration::from_secs(1), stream.read(&mut byte))
            .await
            .expect("read timed out")
            .unwrap();
        assert_eq!(n, 1, "a zero-length PSH must not surface as EOF");
        assert_eq!(byte, *b"x");
    }

    #[tokio::test]
    async fn test_zero_capacity_read_returns_immediately() {
        let (_tx, mut rx) = get_duplex_mux_pair().await;
        let mut empty = [];

        let n = tokio::time::timeout(Duration::from_millis(100), rx.read(&mut empty))
            .await
            .expect("a zero-capacity read must not wait for network data")
            .unwrap();
        assert_eq!(n, 0);
    }

    #[tokio::test]
    async fn test_close_without_pending_data_closes_carrier() {
        let (a, mut peer) = tokio::io::duplex(1024);
        let (mut connector, acceptor, worker) = MuxBuilder::client().with_connection(a).build();

        connector.close().await.unwrap();

        let mut byte = [0u8; 1];
        let n = tokio::time::timeout(Duration::from_millis(100), peer.read(&mut byte))
            .await
            .expect("peer did not observe carrier shutdown")
            .unwrap();
        assert_eq!(n, 0);

        // Keep these alive through the assertion: close(), rather than Arc
        // teardown, must be what closes the carrier.
        drop((acceptor, worker));
    }

    #[tokio::test]
    async fn test_num_streams_excludes_closed_handles() {
        let (a, _peer) = tokio::io::duplex(1024);
        let (mut connector, _acceptor, _worker) = MuxBuilder::client().with_connection(a).build();
        let _stream = connector.connect().unwrap();
        assert_eq!(connector.get_num_streams(), 1);

        connector.close().await.unwrap();

        assert_eq!(
            connector.get_num_streams(),
            0,
            "closed handles must not be reported as open streams"
        );
    }

    #[tokio::test]
    async fn test_stream_flush_waits_for_carrier_flush() {
        let (a, peer_not_reading) = tokio::io::duplex(32);
        let (connector, _acceptor, worker) = MuxBuilder::client().with_connection(a).build();
        tokio::spawn(worker);

        let mut stream = connector.connect().unwrap();
        stream.write_all(&vec![1u8; 1024]).await.unwrap();

        let result = tokio::time::timeout(Duration::from_millis(100), stream.flush()).await;
        assert!(
            result.is_err(),
            "flush completed while the carrier was full and the peer was not reading"
        );

        drop(peer_not_reading);
    }

    #[tokio::test]
    async fn test_remote_fin_then_drop_preserves_accepted_writes() {
        let (a, mut peer) = tokio::io::duplex(32);
        let (connector, acceptor, worker) = MuxBuilder::client().with_connection(a).build();
        let mut stream = connector.connect().unwrap();
        let stream_id = stream.get_stream_id();
        let payload = vec![7u8; MAX_PAYLOAD_SIZE * 2 + 10];
        stream.write_all(&payload).await.unwrap();

        peer.write_all(&raw_frame(1, stream_id, &[])).await.unwrap();
        tokio::spawn(worker);

        tokio::time::timeout(Duration::from_secs(1), async {
            while !stream.is_closed() {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("remote FIN was not dispatched");

        drop(stream);
        drop(connector);
        drop(acceptor);

        let mut wire = Vec::new();
        tokio::time::timeout(Duration::from_secs(2), peer.read_to_end(&mut wire))
            .await
            .expect("carrier did not close")
            .unwrap();

        let payload_frames = payload.len().div_ceil(MAX_PAYLOAD_SIZE);
        let expected = 8 + payload_frames * 8 + payload.len(); // SYN + PSHs
        assert_eq!(
            wire.len(),
            expected,
            "dropping after remote FIN lost writes previously accepted by poll_write"
        );
    }

    #[tokio::test]
    async fn test_idle_timeout_sends_only_one_fin() {
        let (a, mut peer) = tokio::io::duplex(4096);
        let (connector, _acceptor, worker) = MuxBuilder::client()
            .with_idle_timeout(NonZeroU64::new(1).unwrap())
            .with_connection(a)
            .build();
        tokio::spawn(worker);
        let _held_stream = connector.connect().unwrap();

        let deadline = tokio::time::Instant::now() + Duration::from_millis(2200);
        let mut wire = Vec::new();
        let mut buf = [0u8; 128];
        while let Some(remaining) = deadline.checked_duration_since(tokio::time::Instant::now()) {
            match tokio::time::timeout(remaining, peer.read(&mut buf)).await {
                Ok(Ok(n)) if n > 0 => wire.extend_from_slice(&buf[..n]),
                _ => break,
            }
        }

        let fin_count = (0..wire.len() / 8)
            .filter(|frame_index| wire[frame_index * 8 + 1] == 1)
            .count();
        assert_eq!(fin_count, 1, "an idle stream must be finished only once");
    }

    #[tokio::test]
    async fn test_max_tx_queue_is_a_hard_limit() {
        let (a, _peer) = tokio::io::duplex(4096);
        let (connector, _acceptor, _worker) = MuxBuilder::client()
            .with_max_tx_queue(NonZeroUsize::new(1).unwrap())
            .with_connection(a)
            .build();
        let mut stream = connector.connect().unwrap();

        assert_eq!(stream.write(b"a").await.unwrap(), 1);
        let second_is_pending =
            poll_fn(|cx| Poll::Ready(Pin::new(&mut stream).poll_write(cx, b"b").is_pending()))
                .await;
        assert!(
            second_is_pending,
            "max_tx_queue=1 accepted a second queued frame"
        );
    }

    #[tokio::test]
    async fn test_unaccepted_syns_are_bounded_by_rx_queue() {
        let (a, mut peer) = tokio::io::duplex(4096);
        let (connector, mut acceptor, worker) = MuxBuilder::client()
            .with_max_rx_queue(NonZeroUsize::new(2).unwrap())
            .with_connection(a)
            .build();
        tokio::spawn(worker);

        let mut syns = Vec::new();
        syns.extend_from_slice(&raw_frame(0, 2, &[]));
        syns.extend_from_slice(&raw_frame(0, 4, &[]));
        syns.extend_from_slice(&raw_frame(0, 6, &[]));
        peer.write_all(&syns).await.unwrap();

        tokio::time::timeout(Duration::from_secs(1), async {
            while connector.get_num_streams() < 2 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("first two SYNs were not dispatched");
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert_eq!(
            connector.get_num_streams(),
            2,
            "unaccepted SYNs bypassed the configured receive bound"
        );

        drop(acceptor.accept().await.unwrap());
        tokio::time::timeout(Duration::from_secs(1), async {
            while connector.get_num_streams() < 2 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("dispatcher did not resume after an accept slot was consumed");
    }

    #[tokio::test]
    async fn test_rx_backpressure_suspends_keep_alive_timeout() {
        let (a, mut peer) = tokio::io::duplex(1024);
        let (connector, mut acceptor, worker) = MuxBuilder::client()
            .with_keep_alive_interval(NonZeroU64::new(1).unwrap())
            .with_keep_alive_timeout(NonZeroU64::new(2).unwrap())
            .with_max_rx_queue(NonZeroUsize::new(1).unwrap())
            .with_connection(a)
            .build();
        let worker_task = tokio::spawn(worker);

        // One unaccepted SYN fills the configured receive budget, so the
        // dispatcher intentionally stops polling the carrier.
        peer.write_all(&raw_frame(0, 2, &[])).await.unwrap();
        tokio::time::timeout(Duration::from_secs(1), async {
            while connector.get_num_streams() != 1 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("incoming SYN was not dispatched");

        // Not observing frames while intentionally backpressured is not proof
        // that the peer is dead. Keep the healthy carrier open beyond the
        // configured liveness timeout and verify the session survives.
        tokio::time::sleep(Duration::from_millis(2600)).await;
        assert!(
            connector.connect().is_ok(),
            "receive backpressure caused a false keep-alive timeout"
        );
        assert!(!worker_task.is_finished());

        drop(acceptor.accept().await.unwrap());
        drop(peer);
        let _ = worker_task.await;
    }

    #[tokio::test]
    async fn test_stream_shutdown_waits_until_fin_is_driven() {
        let (a, _peer) = tokio::io::duplex(1024);
        let (connector, _acceptor, _worker) = MuxBuilder::client().with_connection(a).build();
        let mut stream = connector.connect().unwrap();

        let shutdown_is_pending =
            poll_fn(|cx| Poll::Ready(Pin::new(&mut stream).poll_shutdown(cx).is_pending())).await;
        assert!(
            shutdown_is_pending,
            "shutdown completed before an unpolled worker could send FIN"
        );
    }

    #[tokio::test]
    async fn test_connection_close_rejects_new_streams_and_writes() {
        let (a, peer_not_reading) = tokio::io::duplex(32);
        let (mut closer, _acceptor, worker) = MuxBuilder::client().with_connection(a).build();
        let other_connector = closer.clone();
        let mut stream = closer.connect().unwrap();
        stream.write_all(&vec![1u8; 1024]).await.unwrap();

        let mut close = Box::pin(closer.close());
        let close_is_pending =
            poll_fn(|cx| Poll::Ready(close.as_mut().poll(cx).is_pending())).await;
        assert!(close_is_pending, "test carrier did not apply backpressure");

        assert!(
            other_connector.connect().is_err(),
            "connect succeeded after connection shutdown began"
        );
        let write_was_accepted = poll_fn(|cx| {
            let accepted = matches!(
                Pin::new(&mut stream).poll_write(cx, b"late"),
                Poll::Ready(Ok(_))
            );
            Poll::Ready(accepted)
        })
        .await;
        assert!(
            !write_was_accepted,
            "stream write succeeded after connection shutdown began"
        );

        drop(close);
        drop(peer_not_reading);
        drop(worker);
    }

    #[tokio::test]
    async fn test_connection_close_ends_acceptor_with_queued_streams() {
        let (a, mut peer) = tokio::io::duplex(32);
        let (mut connector, mut acceptor, worker) = MuxBuilder::client().with_connection(a).build();
        tokio::spawn(worker);

        peer.write_all(&raw_frame(0, 2, &[])).await.unwrap();
        tokio::time::timeout(Duration::from_secs(1), async {
            while connector.get_num_streams() != 1 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("incoming SYN was not queued");

        // Fill the opposite direction so connection close remains in progress
        // long enough to inspect the acceptor's soft-close behavior.
        let mut outgoing = connector.connect().unwrap();
        outgoing.write_all(&vec![1u8; 1024]).await.unwrap();
        let mut close = Box::pin(connector.close());
        let close_is_pending =
            poll_fn(|cx| Poll::Ready(close.as_mut().poll(cx).is_pending())).await;
        assert!(close_is_pending);

        assert!(
            acceptor.accept().await.is_none(),
            "acceptor yielded queued work after connection shutdown began"
        );

        drop(close);
        drop(peer);
    }

    #[tokio::test]
    async fn test_idle_timeout_reaps_unaccepted_stream() {
        let (a, mut peer) = tokio::io::duplex(1024);
        let (connector, _acceptor, worker) = MuxBuilder::client()
            .with_idle_timeout(NonZeroU64::new(1).unwrap())
            .with_max_rx_queue(NonZeroUsize::new(1).unwrap())
            .with_connection(a)
            .build();
        tokio::spawn(worker);

        peer.write_all(&raw_frame(0, 2, &[])).await.unwrap();
        tokio::time::timeout(Duration::from_secs(1), async {
            while connector.get_num_streams() != 1 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("incoming SYN was not dispatched");

        tokio::time::sleep(Duration::from_millis(1600)).await;
        assert_eq!(
            connector.get_num_streams(),
            0,
            "an expired unaccepted stream kept its receive slot"
        );
    }

    #[tokio::test]
    async fn test_idle_timeout_orders_fin_after_accepted_writes() {
        let (a, mut peer) = tokio::io::duplex(32);
        let (connector, _acceptor, worker) = MuxBuilder::client()
            .with_idle_timeout(NonZeroU64::new(1).unwrap())
            .with_connection(a)
            .build();
        tokio::spawn(worker);

        let mut stream = connector.connect().unwrap();
        let payload = vec![9u8; MAX_PAYLOAD_SIZE + 10];
        stream.write_all(&payload).await.unwrap();

        // Keep the tiny carrier full until idle timeout queues FIN while one
        // PSH is still waiting in the stream queue.
        tokio::time::sleep(Duration::from_millis(1600)).await;

        let expected_len = 8 + 2 * 8 + payload.len() + 8; // SYN + 2 PSHs + FIN
        let mut wire = vec![0u8; expected_len];
        tokio::time::timeout(Duration::from_secs(2), peer.read_exact(&mut wire))
            .await
            .expect("timed out draining carrier")
            .unwrap();

        let mut commands = Vec::new();
        let mut offset = 0;
        while offset < wire.len() {
            commands.push(wire[offset + 1]);
            let payload_len = u16::from_le_bytes([wire[offset + 2], wire[offset + 3]]) as usize;
            offset += 8 + payload_len;
        }
        assert_eq!(commands, vec![0, 2, 2, 1], "FIN overtook pending PSH");
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_concurrent_connector_close_wakes_every_waiter() {
        use tokio::sync::oneshot;

        let (a, mut peer) = tokio::io::duplex(32);
        let (connector, acceptor, worker) = MuxBuilder::client().with_connection(a).build();
        let first_connector = connector.clone();
        let second_connector = connector;

        let mut stream = first_connector.connect().unwrap();
        stream.write_all(&vec![1u8; 1024]).await.unwrap();
        drop(stream);

        let spawn_closer = |mut connector: crate::MuxConnector<tokio::io::DuplexStream>| {
            let (started_tx, started_rx) = oneshot::channel();
            let task = tokio::spawn(async move {
                let mut close = Box::pin(connector.close());
                let mut started_tx = Some(started_tx);
                poll_fn(|cx| match close.as_mut().poll(cx) {
                    Poll::Pending => {
                        if let Some(tx) = started_tx.take() {
                            let _ = tx.send(());
                        }
                        Poll::Ready(())
                    }
                    Poll::Ready(result) => {
                        panic!("close unexpectedly completed before peer draining: {result:?}")
                    }
                })
                .await;
                close.await
            });
            (started_rx, task)
        };

        let (first_started, first_close) = spawn_closer(first_connector);
        first_started.await.unwrap();
        let (second_started, second_close) = spawn_closer(second_connector);
        second_started.await.unwrap();

        let reader = tokio::spawn(async move {
            let mut wire = Vec::new();
            peer.read_to_end(&mut wire).await.unwrap();
            wire
        });

        let (first_result, second_result) = tokio::time::timeout(Duration::from_secs(2), async {
            tokio::join!(first_close, second_close)
        })
        .await
        .expect("one concurrent close waiter was never woken");
        first_result.unwrap().unwrap();
        second_result.unwrap().unwrap();
        reader.await.unwrap();

        // These handles deliberately remain alive until both close futures
        // complete, so hard-close must wake all waiters itself.
        drop((acceptor, worker));
    }
}
