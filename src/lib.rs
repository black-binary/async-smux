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
//! [`MuxStream`] supports TCP-style half-close. Calling
//! [`AsyncWriteExt::shutdown`](tokio::io::AsyncWriteExt::shutdown) sends FIN
//! and closes only the local write direction; reads remain open until the
//! peer sends its FIN. A peer FIN likewise produces read EOF without
//! preventing a response from being written. This does not change the wire
//! format; peers must also interpret FIN as directional EOF to write after
//! receiving it.
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

    use rand::{rngs::StdRng, Rng, RngExt, SeedableRng};
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

    fn next_stress_word(state: &mut u64) -> u64 {
        // xorshift64*: cheap, deterministic scheduling and chunk variation.
        // A fixed generator makes a failing stress run exactly reproducible.
        *state ^= *state >> 12;
        *state ^= *state << 25;
        *state ^= *state >> 27;
        state.wrapping_mul(0x2545_f491_4f6c_dd1d)
    }

    fn stress_payload(seed: u64, len: usize) -> Vec<u8> {
        let mut state = seed.max(1);
        (0..len)
            .map(|_| next_stress_word(&mut state) as u8)
            .collect()
    }

    async fn write_stress_chunks<W: AsyncWrite + Unpin>(writer: &mut W, data: &[u8], seed: u64) {
        let mut state = seed.max(1);
        let mut offset = 0;
        while offset < data.len() {
            let chunk_len = 1 + next_stress_word(&mut state) as usize % 4096;
            let end = (offset + chunk_len).min(data.len());
            writer.write_all(&data[offset..end]).await.unwrap();
            offset = end;
            if state & 3 == 0 {
                tokio::task::yield_now().await;
            }
        }
    }

    fn stress_message(seed: u64, round: usize) -> Vec<u8> {
        let mixed = seed
            .wrapping_add((round as u64).wrapping_mul(0x9e37_79b9_7f4a_7c15))
            .rotate_left((round % 63) as u32);
        let len = 1 + mixed as usize % 2048;
        stress_payload(mixed, len)
    }

    async fn run_full_duplex_stress(
        stream: MuxStream<tokio::io::DuplexStream>,
        write_seed: u64,
        read_seed: u64,
        rounds: usize,
        before_shutdown: Arc<tokio::sync::Barrier>,
    ) {
        let (mut reader, mut writer) = tokio::io::split(stream);
        let (write_result, ()) = tokio::join!(
            async {
                for round in 0..rounds {
                    let message = stress_message(write_seed, round);
                    write_stress_chunks(&mut writer, &message, write_seed ^ round as u64).await;
                    if round % 7 == 0 {
                        writer.flush().await.unwrap();
                    }
                }
                writer.flush().await
            },
            async {
                for round in 0..rounds {
                    let expected = stress_message(read_seed, round);
                    let mut actual = vec![0; expected.len()];
                    reader.read_exact(&mut actual).await.unwrap();
                    assert_eq!(actual, expected, "payload mismatch in round {round}");
                }
            }
        );
        write_result.unwrap();
        before_shutdown.wait().await;

        let mut stream = reader.unsplit(writer);
        stream.shutdown().await.unwrap();
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

        let mut buf = vec![0; 4];
        stream1.read_exact(&mut buf).await.unwrap();
        assert_eq!(buf, data);
        assert_eq!(stream1.read(&mut buf).await.unwrap(), 0);
        stream1.write_all(&[4, 5, 6, 7]).await.unwrap();
        stream1.shutdown().await.unwrap();
        stream2.read_exact(&mut buf).await.unwrap();
        assert_eq!(buf, [4, 5, 6, 7]);
        assert_eq!(stream2.read(&mut buf).await.unwrap(), 0);

        drop(acceptor_a);
        let mut stream = connector_b.connect().unwrap();
        assert_eq!(stream.read(&mut buf).await.unwrap(), 0);
        stream.flush().await.unwrap();
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
        stream2.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn test_half_close_shutdown_keeps_read_open() {
        let (client, mut server) = get_duplex_mux_pair().await;
        let (mut client_reader, mut client_writer) = tokio::io::split(client);

        client_writer.write_all(b"request").await.unwrap();
        client_writer.shutdown().await.unwrap();
        assert!(
            client_writer.write_all(b"late request").await.is_err(),
            "writes must fail after the local write half is shut down"
        );

        let mut request = Vec::new();
        tokio::time::timeout(Duration::from_secs(1), server.read_to_end(&mut request))
            .await
            .expect("server did not observe EOF from the client write half")
            .unwrap();
        assert_eq!(request, b"request");

        server.write_all(b"response").await.unwrap();
        server.shutdown().await.unwrap();

        let mut response = Vec::new();
        tokio::time::timeout(
            Duration::from_secs(1),
            client_reader.read_to_end(&mut response),
        )
        .await
        .expect("client read half closed before the server response")
        .unwrap();
        assert_eq!(response, b"response");
    }

    #[tokio::test]
    async fn test_half_close_local_fin_waits_for_remote_read_eof() {
        let (mut local, mut peer) = get_duplex_mux_pair().await;

        local.shutdown().await.unwrap();

        let read_is_pending = poll_fn(|cx| {
            let mut byte = [0u8; 1];
            let mut read_buf = ReadBuf::new(&mut byte);
            Poll::Ready(
                Pin::new(&mut local)
                    .poll_read(cx, &mut read_buf)
                    .is_pending(),
            )
        })
        .await;
        assert!(
            read_is_pending,
            "closing the local write half must not create local read EOF"
        );

        peer.write_all(b"x").await.unwrap();
        peer.flush().await.unwrap();
        let mut byte = [0u8; 1];
        local.read_exact(&mut byte).await.unwrap();
        assert_eq!(byte, *b"x");

        peer.shutdown().await.unwrap();
        assert_eq!(local.read(&mut byte).await.unwrap(), 0);
    }

    #[tokio::test]
    async fn test_half_close_remote_fin_only_closes_read_direction() {
        let (mut local, mut peer) = get_duplex_mux_pair().await;

        peer.write_all(b"done").await.unwrap();
        peer.shutdown().await.unwrap();

        let mut received = Vec::new();
        local.read_to_end(&mut received).await.unwrap();
        assert_eq!(received, b"done");

        local.write_all(b"reply after EOF").await.unwrap();
        local.shutdown().await.unwrap();

        let mut reply = Vec::new();
        peer.read_to_end(&mut reply).await.unwrap();
        assert_eq!(reply, b"reply after EOF");
    }

    #[tokio::test]
    async fn test_half_close_stream_is_closed_only_after_both_fins() {
        let (a, b) = tokio::io::duplex(4096);
        let (connector_a, _acceptor_a, worker_a) = MuxBuilder::client().with_connection(a).build();
        let (connector_b, mut acceptor_b, worker_b) =
            MuxBuilder::server().with_connection(b).build();
        tokio::spawn(worker_a);
        tokio::spawn(worker_b);

        let mut local = connector_a.connect().unwrap();
        let mut peer = acceptor_b.accept().await.unwrap();
        local.shutdown().await.unwrap();

        let mut byte = [0u8; 1];
        assert_eq!(peer.read(&mut byte).await.unwrap(), 0);
        assert!(!local.is_closed());
        assert!(!peer.is_closed());
        assert_eq!(connector_a.get_num_streams(), 1);
        assert_eq!(connector_b.get_num_streams(), 1);

        peer.shutdown().await.unwrap();
        assert_eq!(local.read(&mut byte).await.unwrap(), 0);
        assert!(local.is_closed());
        assert!(peer.is_closed());
        assert_eq!(connector_a.get_num_streams(), 0);
        assert_eq!(connector_b.get_num_streams(), 0);
    }

    #[tokio::test]
    async fn test_half_close_fin_before_accept_allows_response() {
        let (a, b) = tokio::io::duplex(4096);
        let (connector_a, _acceptor_a, worker_a) = MuxBuilder::client().with_connection(a).build();
        let (_connector_b, mut acceptor_b, worker_b) =
            MuxBuilder::server().with_connection(b).build();
        tokio::spawn(worker_a);
        tokio::spawn(worker_b);

        let mut client = connector_a.connect().unwrap();
        client.write_all(b"request before accept").await.unwrap();
        client.shutdown().await.unwrap();

        let mut server = acceptor_b.accept().await.unwrap();
        let mut request = Vec::new();
        server.read_to_end(&mut request).await.unwrap();
        assert_eq!(request, b"request before accept");

        server.write_all(b"response after accept").await.unwrap();
        server.shutdown().await.unwrap();
        let mut response = Vec::new();
        client.read_to_end(&mut response).await.unwrap();
        assert_eq!(response, b"response after accept");
    }

    #[tokio::test]
    async fn test_half_close_repeated_shutdown_sends_one_fin() {
        let (a, mut peer) = tokio::io::duplex(1024);
        let (connector, acceptor, worker) = MuxBuilder::client().with_connection(a).build();
        tokio::spawn(worker);

        let mut stream = connector.connect().unwrap();
        let stream_id = stream.get_stream_id();
        stream.shutdown().await.unwrap();
        stream.shutdown().await.unwrap();
        drop(stream);
        drop(connector);
        drop(acceptor);

        let mut wire = Vec::new();
        tokio::time::timeout(Duration::from_secs(1), peer.read_to_end(&mut wire))
            .await
            .expect("session did not close after dropping every public handle")
            .unwrap();
        assert_eq!(
            wire.len(),
            16,
            "repeated shutdown or Drop emitted extra data"
        );
        assert_eq!(wire[1], 0, "first frame was not SYN");
        assert_eq!(
            u32::from_le_bytes(wire[4..8].try_into().unwrap()),
            stream_id
        );
        assert_eq!(wire[9], 1, "second frame was not FIN");
        assert_eq!(
            u32::from_le_bytes(wire[12..16].try_into().unwrap()),
            stream_id
        );
    }

    #[tokio::test]
    async fn test_half_close_simultaneous_fin_preserves_crossed_data() {
        let (mut left, mut right) = get_duplex_mux_pair().await;

        left.write_all(b"from left").await.unwrap();
        right.write_all(b"from right").await.unwrap();
        let (left_shutdown, right_shutdown) = tokio::join!(left.shutdown(), right.shutdown());
        left_shutdown.unwrap();
        right_shutdown.unwrap();

        let mut received_left = Vec::new();
        let mut received_right = Vec::new();
        let (left_read, right_read) = tokio::time::timeout(Duration::from_secs(1), async {
            tokio::join!(
                left.read_to_end(&mut received_left),
                right.read_to_end(&mut received_right)
            )
        })
        .await
        .expect("crossed FINs did not produce EOF");
        left_read.unwrap();
        right_read.unwrap();

        assert_eq!(received_left, b"from right");
        assert_eq!(received_right, b"from left");
        assert!(left.is_closed());
        assert!(right.is_closed());
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
        let mut stream2 = acceptor_b.accept().await.unwrap();
        tokio::time::sleep(Duration::from_secs(1)).await;
        assert!(!stream1.is_closed());
        assert!(!stream2.is_closed());

        tokio::time::sleep(Duration::from_secs(5)).await;

        assert!(stream1.is_closed());
        assert!(!stream2.is_closed());
        let mut byte = [0u8; 1];
        assert_eq!(stream2.read(&mut byte).await.unwrap(), 0);
        stream2.shutdown().await.unwrap();
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
    async fn test_stream_drop_delivers_read_eof() {
        let (a, b) = get_tcp_pair().await;
        let (connector_a, _, worker_a) = MuxBuilder::client().with_connection(a).build();
        let (_, mut acceptor_b, worker_b) = MuxBuilder::server().with_connection(b).build();
        tokio::spawn(worker_a);
        tokio::spawn(worker_b);

        let stream1 = connector_a.connect().unwrap();
        let mut stream2 = acceptor_b.accept().await.unwrap();

        drop(stream1);

        let mut byte = [0u8; 1];
        assert_eq!(stream2.read(&mut byte).await.unwrap(), 0);
        assert!(
            !stream2.is_closed(),
            "peer drop closes only this stream's read direction"
        );

        stream2.shutdown().await.unwrap();
        assert!(stream2.is_closed());
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

    // Once a peer FIN has produced EOF, a protocol-invalid late PSH must not
    // make a subsequent read return data or implicitly close our write half.
    #[tokio::test(flavor = "multi_thread")]
    async fn test_eof_monotonic_after_remote_fin() {
        let (a, mut peer) = tokio::io::duplex(4096);
        let (_connector, mut acceptor, worker) = MuxBuilder::client().with_connection(a).build();
        tokio::spawn(worker);

        let stream_id = 2;
        peer.write_all(&raw_frame(0, stream_id, &[])).await.unwrap();
        let mut stream = acceptor.accept().await.unwrap();
        peer.write_all(&raw_frame(1, stream_id, &[])).await.unwrap();

        let mut buf = [0u8; 4];
        assert_eq!(stream.read(&mut buf).await.unwrap(), 0);

        peer.write_all(&raw_frame(2, stream_id, b"late"))
            .await
            .unwrap();
        tokio::time::sleep(Duration::from_millis(50)).await;

        assert_eq!(stream.read(&mut buf).await.unwrap(), 0);

        stream.write_all(b"reply").await.unwrap();
        stream.flush().await.unwrap();
        let mut wire = [0u8; 13];
        tokio::time::timeout(Duration::from_secs(1), peer.read_exact(&mut wire))
            .await
            .expect("reply was not sent after remote FIN")
            .unwrap();
        assert_eq!(wire[1], 2, "late PSH caused an implicit local FIN");
        assert_eq!(&wire[8..], b"reply");
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
        // shutdown closes the local write half and enqueues FIN. Pending PSH
        // frames from the write_all above should still go out first.
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

        let mut byte = [0u8; 1];
        let n = tokio::time::timeout(Duration::from_secs(1), stream.read(&mut byte))
            .await
            .expect("remote FIN was not dispatched")
            .unwrap();
        assert_eq!(n, 0);

        drop(stream);
        drop(connector);
        drop(acceptor);

        let mut wire = Vec::new();
        tokio::time::timeout(Duration::from_secs(2), peer.read_to_end(&mut wire))
            .await
            .expect("carrier did not close")
            .unwrap();

        let payload_frames = payload.len().div_ceil(MAX_PAYLOAD_SIZE);
        let expected = 8 + payload_frames * 8 + payload.len() + 8; // SYN + PSHs + FIN
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

    async fn run_randomized_lifecycle_stress(batches: usize, streams_per_batch: usize) {
        const SEED: u64 = 0x5eed_cafe_f00d_beef;

        let (a, b) = tokio::io::duplex(128);
        let (mut connector_a, _acceptor_a, worker_a) = MuxBuilder::client()
            .with_max_tx_queue(NonZeroUsize::new(2).unwrap())
            .with_max_rx_queue(NonZeroUsize::new(8).unwrap())
            .with_connection(a)
            .build();
        let (connector_b, mut acceptor_b, worker_b) = MuxBuilder::server()
            .with_max_tx_queue(NonZeroUsize::new(2).unwrap())
            .with_max_rx_queue(NonZeroUsize::new(8).unwrap())
            .with_connection(b)
            .build();
        let worker_a = tokio::spawn(worker_a);
        let worker_b = tokio::spawn(worker_b);
        let mut rng = StdRng::seed_from_u64(SEED);

        for batch in 0..batches {
            let mut clients = tokio::task::JoinSet::new();
            for slot in 0..streams_per_batch {
                let case_id = (batch * streams_per_batch + slot) as u64;
                let payload_len = match case_id % 16 {
                    0 => 0,
                    1 => 1,
                    2 => 7,
                    3 => 8,
                    4 => 1024,
                    5 => 8191,
                    6 => 8192,
                    7 => MAX_PAYLOAD_SIZE - 1,
                    8 => MAX_PAYLOAD_SIZE,
                    9 => MAX_PAYLOAD_SIZE + 1,
                    _ => rng.random_range(1..=16 * 1024),
                };
                let chunk_seed = rng.random::<u64>();
                let drop_without_flush = case_id.is_multiple_of(5);
                let connector = connector_a.clone();

                clients.spawn(async move {
                    if case_id.is_multiple_of(3) {
                        tokio::task::yield_now().await;
                    }
                    let mut stream = connector.connect().unwrap();
                    let mut header = [0u8; 16];
                    header[..8].copy_from_slice(&case_id.to_le_bytes());
                    header[8..12].copy_from_slice(&(payload_len as u32).to_le_bytes());
                    header[12] = u8::from(drop_without_flush);
                    header[13..].copy_from_slice(b"smx");
                    write_stress_chunks(&mut stream, &header, chunk_seed).await;

                    let payload_seed = SEED ^ case_id;
                    let payload = stress_payload(payload_seed, payload_len);
                    if case_id.is_multiple_of(7) {
                        stream.write_all(&payload).await.unwrap();
                    } else {
                        write_stress_chunks(&mut stream, &payload, chunk_seed ^ payload_seed).await;
                    }

                    if drop_without_flush {
                        // Exercise the path that transfers accepted frames to
                        // the global queue and puts FIN behind them.
                        drop(stream);
                        return;
                    }

                    if case_id.is_multiple_of(2) {
                        stream.flush().await.unwrap();
                    }
                    let response_len = payload_len / 2 + case_id as usize % 257;
                    let expected = stress_payload(!payload_seed, response_len);
                    let mut actual = vec![0; response_len];
                    stream.read_exact(&mut actual).await.unwrap();
                    assert_eq!(actual, expected, "response mismatch for case {case_id}");
                    // Coordinate completion so both FIN paths are exercised
                    // deterministically before the tasks finish.
                    stream.write_all(&[0xac]).await.unwrap();
                    stream.shutdown().await.unwrap();
                });
            }

            let mut servers = tokio::task::JoinSet::new();
            for _ in 0..streams_per_batch {
                let mut stream = acceptor_b
                    .accept()
                    .await
                    .expect("session closed during stress");
                servers.spawn(async move {
                    let mut header = [0u8; 16];
                    stream.read_exact(&mut header).await.unwrap();
                    assert_eq!(&header[13..], b"smx", "corrupted stress header");
                    let case_id = u64::from_le_bytes(header[..8].try_into().unwrap());
                    let payload_len =
                        u32::from_le_bytes(header[8..12].try_into().unwrap()) as usize;
                    let drop_without_flush = header[12] != 0;
                    assert!(
                        payload_len <= MAX_PAYLOAD_SIZE + 1,
                        "invalid payload length in case {case_id}: {payload_len}"
                    );

                    let payload_seed = SEED ^ case_id;
                    let expected = stress_payload(payload_seed, payload_len);
                    let mut actual = vec![0; payload_len];
                    stream.read_exact(&mut actual).await.unwrap();
                    assert_eq!(actual, expected, "request mismatch for case {case_id}");

                    if drop_without_flush {
                        let mut byte = [0u8; 1];
                        assert_eq!(
                            stream.read(&mut byte).await.unwrap(),
                            0,
                            "case {case_id} delivered data after FIN"
                        );
                    } else {
                        let response_len = payload_len / 2 + case_id as usize % 257;
                        let response = stress_payload(!payload_seed, response_len);
                        write_stress_chunks(&mut stream, &response, !case_id).await;
                        stream.flush().await.unwrap();
                        let mut ack = [0u8; 1];
                        stream.read_exact(&mut ack).await.unwrap();
                        assert_eq!(ack, [0xac], "invalid completion ack for case {case_id}");
                        stream.shutdown().await.unwrap();
                    }
                });
            }

            while let Some(result) = clients.join_next().await {
                result.unwrap();
            }
            while let Some(result) = servers.join_next().await {
                result.unwrap();
            }

            assert_eq!(connector_a.get_num_streams(), 0);
            assert_eq!(connector_b.get_num_streams(), 0);
            assert_eq!(
                connector_a.get_num_tracked_streams(),
                0,
                "client retained stream handles after batch {batch}"
            );
            assert_eq!(
                connector_b.get_num_tracked_streams(),
                0,
                "server retained stream handles after batch {batch}"
            );
            assert!(!worker_a.is_finished());
            assert!(!worker_b.is_finished());
        }

        connector_a.close().await.unwrap();
        let local_worker_result = tokio::time::timeout(Duration::from_secs(2), worker_a)
            .await
            .expect("local worker did not stop after stress close")
            .unwrap();
        assert!(matches!(
            local_worker_result,
            Ok(()) | Err(crate::error::MuxError::ConnectionClosed)
        ));
        let _ = tokio::time::timeout(Duration::from_secs(2), worker_b)
            .await
            .expect("peer worker did not observe stress close")
            .unwrap();
    }

    #[test]
    fn test_codec_randomized_fragmentation_round_trip() {
        use bytes::{Bytes, BytesMut};
        use tokio_util::codec::{Decoder, Encoder};

        use crate::frame::{MuxCodec, MuxCommand, MuxFrame};

        const SEED: u64 = 0xd15c_a11e_5eed_1234;
        const FRAMES: usize = 256;
        let mut rng = StdRng::seed_from_u64(SEED);
        let mut encoder = MuxCodec {};
        let mut wire = BytesMut::new();
        let mut expected = Vec::with_capacity(FRAMES);

        for index in 0..FRAMES {
            let command = match rng.random_range(0..4) {
                0 => MuxCommand::Sync,
                1 => MuxCommand::Finish,
                2 => MuxCommand::Push,
                _ => MuxCommand::Nop,
            };
            let stream_id = if command == MuxCommand::Nop {
                0
            } else {
                rng.random_range(1..=u32::MAX)
            };
            let payload_len = if command == MuxCommand::Push {
                match index % 16 {
                    0 => 0,
                    1 => 1,
                    2 => MAX_PAYLOAD_SIZE - 1,
                    3 => MAX_PAYLOAD_SIZE,
                    _ => rng.random_range(0..=4096),
                }
            } else {
                0
            };
            let payload = stress_payload(rng.random(), payload_len);
            encoder
                .encode(
                    MuxFrame::new(command, stream_id, Bytes::from(payload.clone())),
                    &mut wire,
                )
                .unwrap();
            expected.push((command, stream_id, payload));
        }

        let wire = wire.freeze();
        let mut decoder = MuxCodec {};
        let mut buffered = BytesMut::new();
        let mut actual = Vec::with_capacity(FRAMES);
        let mut offset = 0;
        while offset < wire.len() {
            let chunk_len = rng.random_range(1..=257).min(wire.len() - offset);
            buffered.extend_from_slice(&wire[offset..offset + chunk_len]);
            offset += chunk_len;
            while let Some(frame) = decoder.decode(&mut buffered).unwrap() {
                actual.push((
                    frame.header.command,
                    frame.header.stream_id,
                    frame.payload.to_vec(),
                ));
            }
        }
        while let Some(frame) = decoder.decode(&mut buffered).unwrap() {
            actual.push((
                frame.header.command,
                frame.header.stream_id,
                frame.payload.to_vec(),
            ));
        }

        assert!(buffered.is_empty());
        assert_eq!(actual, expected);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn test_stress_randomized_stream_lifecycles() {
        tokio::time::timeout(
            Duration::from_secs(20),
            run_randomized_lifecycle_stress(8, 48),
        )
        .await
        .expect("randomized lifecycle stress test deadlocked");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn test_stress_sustained_bidirectional_backpressure() {
        const STREAMS: usize = 24;
        const ROUNDS: usize = 96;
        const SEED_A: u64 = 0xa11c_e001_1234_5678;
        const SEED_B: u64 = 0xb0b0_0002_8765_4321;

        tokio::time::timeout(Duration::from_secs(20), async {
            let (a, b) = tokio::io::duplex(64);
            let (connector_a, _acceptor_a, worker_a) = MuxBuilder::client()
                .with_max_tx_queue(NonZeroUsize::new(1).unwrap())
                .with_max_rx_queue(NonZeroUsize::new(1).unwrap())
                .with_connection(a)
                .build();
            let (connector_b, mut acceptor_b, worker_b) = MuxBuilder::server()
                .with_max_tx_queue(NonZeroUsize::new(1).unwrap())
                .with_max_rx_queue(NonZeroUsize::new(1).unwrap())
                .with_connection(b)
                .build();
            tokio::spawn(worker_a);
            tokio::spawn(worker_b);

            let mut pairs = Vec::with_capacity(STREAMS);
            for _ in 0..STREAMS {
                let local = connector_a.connect().unwrap();
                let peer = acceptor_b.accept().await.unwrap();
                pairs.push((local, peer));
            }

            let mut tasks = tokio::task::JoinSet::new();
            for (index, (local, peer)) in pairs.into_iter().enumerate() {
                tasks.spawn(async move {
                    let a_seed = SEED_A ^ index as u64;
                    let b_seed = SEED_B ^ index as u64;
                    let before_shutdown = Arc::new(tokio::sync::Barrier::new(2));
                    tokio::join!(
                        run_full_duplex_stress(
                            local,
                            a_seed,
                            b_seed,
                            ROUNDS,
                            before_shutdown.clone()
                        ),
                        run_full_duplex_stress(peer, b_seed, a_seed, ROUNDS, before_shutdown)
                    );
                });
            }
            while let Some(result) = tasks.join_next().await {
                result.unwrap();
            }

            assert_eq!(connector_a.get_num_tracked_streams(), 0);
            assert_eq!(connector_b.get_num_tracked_streams(), 0);
        })
        .await
        .expect("sustained bidirectional backpressure test deadlocked");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn test_stress_concurrent_close_cancellation_and_stream_drop() {
        use tokio::sync::oneshot;

        const STREAMS: usize = 128;
        const CLOSERS: usize = 24;
        const PAYLOAD_LEN: usize = 257;

        tokio::time::timeout(Duration::from_secs(10), async {
            let (a, mut peer) = tokio::io::duplex(64);
            let (connector, acceptor, worker) = MuxBuilder::client().with_connection(a).build();
            let worker = tokio::spawn(worker);

            let mut streams = Vec::with_capacity(STREAMS);
            for index in 0..STREAMS {
                let mut stream = connector.connect().unwrap();
                let payload = stress_payload(index as u64 + 1, PAYLOAD_LEN);
                stream.write_all(&payload).await.unwrap();
                streams.push(stream);
            }
            drop(streams);

            let mut closers = Vec::with_capacity(CLOSERS);
            for _ in 0..CLOSERS {
                let mut connector = connector.clone();
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
                            panic!("close completed before the blocked carrier drained: {result:?}")
                        }
                    })
                    .await;
                    close.await
                });
                started_rx.await.unwrap();
                closers.push(task);
            }

            for (index, closer) in closers.iter().enumerate() {
                if index % 3 == 0 {
                    closer.abort();
                }
            }

            let reader = tokio::spawn(async move {
                let mut wire = Vec::new();
                peer.read_to_end(&mut wire).await.unwrap();
                wire
            });

            for (index, closer) in closers.into_iter().enumerate() {
                match closer.await {
                    Err(error) if index % 3 == 0 => assert!(error.is_cancelled()),
                    Ok(result) => result.unwrap(),
                    Err(error) => panic!("active close task failed: {error}"),
                }
            }
            let worker_result = worker.await.unwrap();
            assert!(matches!(
                worker_result,
                Ok(()) | Err(crate::error::MuxError::ConnectionClosed)
            ));
            let wire = reader.await.unwrap();

            let mut offset = 0;
            let mut push_bytes = 0;
            while offset < wire.len() {
                assert!(wire.len() - offset >= 8, "truncated frame header");
                let command = wire[offset + 1];
                let payload_len = u16::from_le_bytes([wire[offset + 2], wire[offset + 3]]) as usize;
                assert!(
                    wire.len() - offset >= 8 + payload_len,
                    "truncated frame payload"
                );
                if command == 2 {
                    push_bytes += payload_len;
                }
                offset += 8 + payload_len;
            }
            assert_eq!(
                push_bytes,
                STREAMS * PAYLOAD_LEN,
                "orderly close lost bytes accepted before shutdown"
            );

            drop((connector, acceptor));
        })
        .await
        .expect("concurrent close/cancellation stress test deadlocked");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    #[ignore = "long-running soak test; run explicitly in release mode"]
    async fn test_soak_randomized_stream_lifecycles() {
        tokio::time::timeout(
            Duration::from_secs(120),
            run_randomized_lifecycle_stress(64, 64),
        )
        .await
        .expect("randomized lifecycle soak test deadlocked");
    }
}
