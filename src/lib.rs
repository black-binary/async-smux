//! A lightweight and fast asynchronous [smux](https://github.com/xtaci/smux) (Simple MUltipleXing) library for Tokio async runtime.
//! # Quickstart
//!
//! ```ignore
//! ## Server
//! // Initialize a stream with `AsyncRead + AsyncWrite`, e.g. TcpStream
//! let tcp_connection = ...
//! // Spawn a smux server to multiplexing the tcp stream using `MuxBuilder`
//! let connector, acceptor, worker = MuxBuilder::server().with_connection(tcp_connection).build();
//! // Spawn the smux worker (or a worker `future`, more precisely)
//! // The worker keeps running and dispatch smux frames until you drop (or close) all streams, acceptors and connectors
//! tokio::spawn(worker);
//!
//! // Now we are ready to go!
//! // Both client and server can spawn and accept bi-directional streams
//! let outgoing_stream = connector.connect().unwrap();
//! let incoming_stream = acceptor.accept().await.unwrap();
//!
//! // Just use these smux streams like normal tcp streams :)
//! incoming_stream.read(...).await.unwrap();
//! incoming_stream.write_all(...).await.unwrap();
//! ```
//! ## Client
//! ```ignore
//! let tcp_connection = ...
//! // Just like what we do at the server side, except that we are calling the `client()` function this time
//! let (connector, acceptor, worker) = MuxBuilder::client().with_connection(tcp_connection).build();
//! tokio::spawn(worker);
//!
//! let outgoing_stream1 = connector.connect().unwrap();
//! ...
//! ```

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
        future::poll_fn,
        num::{NonZeroU64, NonZeroUsize},
        pin::Pin,
        task::Poll,
        time::Duration,
    };

    use rand::RngCore;
    use tokio::{
        io::{AsyncRead, AsyncReadExt, AsyncWriteExt, ReadBuf},
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

        let mut stream1 = connector_a.connect().unwrap();
        let mut stream2 = acceptor_b.accept().await.unwrap();
        tokio::time::sleep(Duration::from_secs(1)).await;
        assert!(!stream1.is_closed());
        assert!(!stream2.is_closed());

        tokio::time::sleep(Duration::from_secs(5)).await;

        assert!(stream1.is_closed());
        assert!(stream2.is_closed());
    }

    #[tokio::test]
    async fn test_recv_block() {
        let (a, b) = get_tcp_pair().await;
        let (connector_a, _, worker_a) = MuxBuilder::client().with_connection(a).build();
        let (_, mut acceptor_b, worker_b) = MuxBuilder::server()
            .with_max_rx_queue(12.try_into().unwrap())
            .with_connection(b)
            .build();
        tokio::spawn(async move {
            worker_a.await.unwrap();
        });
        tokio::spawn(async move {
            worker_b.await.unwrap();
        });

        let mut stream_x1 = connector_a.connect().unwrap();
        let mut stream_x2 = acceptor_b.accept().await.unwrap();

        let mut stream_y1 = connector_a.connect().unwrap();
        let mut stream_y2 = acceptor_b.accept().await.unwrap();

        let data = &[1, 2, 3, 4];
        for _ in 0..3 {
            stream_x1.write_all(data).await.unwrap();
        }
        // stream_x is full now
        stream_y1.write_all(data).await.unwrap();

        // stream_y should be blocked unless x incoming bytes is handled
        poll_fn(|cx| {
            let mut buf = [0; 128];
            let mut buf = ReadBuf::new(&mut buf);
            let res = Pin::new(&mut stream_y2).poll_read(cx, &mut buf);
            assert!(res.is_pending());
            Poll::Ready(())
        })
        .await;

        let mut buf = [0; 4];
        for _ in 0..3 {
            stream_x2.read_exact(&mut buf).await.unwrap();
            assert_eq!(&buf, data);
        }

        // stream_y is avaliable now
        poll_fn(|cx| {
            let mut buf_arr = [0; 128];
            let mut buf = ReadBuf::new(&mut buf_arr);
            let res = Pin::new(&mut stream_y2).poll_read(cx, &mut buf);
            assert!(res.is_ready());
            Poll::Ready(())
        })
        .await;
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
        assert!(res.is_err());
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
        assert_eq!(n, 0, "subsequent read must remain EOF, not return late data");
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
        assert!(msg.contains("Even"), "Display should mention type, got: {msg}");
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
        let (a, b) = tokio::io::duplex(1024);
        std::mem::forget(b);

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
        let (a, b) = get_tcp_pair().await;
        let (connector_a, _, worker_a) = MuxBuilder::client().with_connection(a).build();
        let (_, mut acceptor_b, worker_b) = MuxBuilder::server().with_connection(b).build();
        tokio::spawn(worker_a);
        tokio::spawn(worker_b);

        // Open two streams and immediately enqueue many small writes on each
        // before yielding to the worker. Use poll_fn so the writes happen
        // back-to-back in a single tick of the runtime.
        let mut s1_tx = connector_a.connect().unwrap();
        let mut s2_tx = connector_a.connect().unwrap();
        let mut s1_rx = acceptor_b.accept().await.unwrap();
        let mut s2_rx = acceptor_b.accept().await.unwrap();

        // Tag bytes so we know which stream they came from on a per-frame basis.
        const FRAMES: usize = 16;
        for i in 0..FRAMES {
            s1_tx.write_all(&[1u8; 8]).await.unwrap();
            s2_tx.write_all(&[2u8; 8]).await.unwrap();
            // Periodically flush one of them to fight runtime scheduling
            // luck without sequencing them strictly.
            if i % 4 == 3 {
                s1_tx.flush().await.unwrap();
                s2_tx.flush().await.unwrap();
            }
        }
        s1_tx.flush().await.unwrap();
        s2_tx.flush().await.unwrap();

        let mut buf1 = vec![0u8; FRAMES * 8];
        let mut buf2 = vec![0u8; FRAMES * 8];
        s1_rx.read_exact(&mut buf1).await.unwrap();
        s2_rx.read_exact(&mut buf2).await.unwrap();
        assert!(buf1.iter().all(|b| *b == 1));
        assert!(buf2.iter().all(|b| *b == 2));
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
        let (mut connector_a, _acc, _worker_a) =
            MuxBuilder::client().with_connection(a).build();
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
}
