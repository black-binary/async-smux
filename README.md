# async-smux

[![CI](https://github.com/black-binary/async-smux/actions/workflows/ci.yml/badge.svg)](https://github.com/black-binary/async-smux/actions/workflows/ci.yml)
[![crates.io](https://img.shields.io/crates/v/async_smux.svg)](https://crates.io/crates/async_smux)

A lightweight asynchronous [smux](https://github.com/xtaci/smux) (Simple
MUltipleXing) library for Tokio. Wraps any `AsyncRead + AsyncWrite +
Unpin` transport (`TcpStream`, `TlsStream`, …) and lets you spawn many
bi-directional `MuxStream`s over it — each one looks and behaves like a
plain TCP stream.

![img](https://raw.githubusercontent.com/xtaci/smux/master/mux.jpg)

## Quickstart

```rust,ignore
use async_smux::MuxBuilder;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

#[tokio::main]
async fn main() {
    let tcp = TcpStream::connect("127.0.0.1:12345").await.unwrap();

    // build() returns three pieces:
    //   connector — open new outgoing streams
    //   acceptor  — receive peer-initiated streams
    //   worker    — the future that drives I/O; spawn it
    let (connector, mut acceptor, worker) =
        MuxBuilder::client().with_connection(tcp).build();
    tokio::spawn(worker);

    // Outgoing
    let mut s = connector.connect().unwrap();
    s.write_all(b"hello").await.unwrap();
    let mut buf = [0u8; 5];
    s.read_exact(&mut buf).await.unwrap();

    // Incoming
    while let Some(mut peer) = acceptor.accept().await {
        // ...
    }
}
```

The server side is identical except for `MuxBuilder::server()`. Client
and server differ only in stream-id parity (odd vs. even) so two peers
never collide on locally-allocated ids.

A complete working example is in [`examples/echo.rs`](examples/echo.rs).

## Lifecycle

Three handles share ownership of the connection state:

- **`MuxConnector`** — opens streams. `Clone`-able.
- **`MuxAcceptor`** — receives peer-initiated streams. Implements
  `Stream<Item = MuxStream>`.
- **`MuxWorker`** — the future that drains the underlying transport
  and dispatches frames. Spawn it with `tokio::spawn(worker)`.

The worker exits when:

- All public handles (connectors + acceptors + streams) are dropped, or
- `MuxConnector::close().await` is called explicitly, or
- The peer closes the transport, or
- A keep-alive timeout fires (if configured).

`close()` performs an orderly shutdown: any frames already accepted by
`AsyncWrite::poll_write` are drained to the wire before the underlying
sink is closed. It also works without the worker being polled — useful
in test setups or sync teardown paths.

## Configuration

```rust,ignore
use async_smux::MuxBuilder;
use std::num::{NonZeroU64, NonZeroUsize};

let (connector, acceptor, worker) = MuxBuilder::server()
    // Send a NOP frame every N seconds to keep the connection alive.
    .with_keep_alive_interval(NonZeroU64::new(15).unwrap())
    // If we don't see any frame from the peer for this many seconds,
    // declare the connection dead and tear everything down.
    // Defaults to 3 × keep_alive_interval.
    .with_keep_alive_timeout(NonZeroU64::new(45).unwrap())
    // Per-stream idle timeout (seconds): close streams with no
    // recent traffic.
    .with_idle_timeout(NonZeroU64::new(60).unwrap())
    // Backpressure thresholds: cap how many frames may sit in the
    // tx/rx queues before poll_write / poll_read park.
    .with_max_tx_queue(NonZeroUsize::new(1024).unwrap())
    .with_max_rx_queue(NonZeroUsize::new(1024).unwrap())
    .with_connection(connection)
    .build();
```

All of these knobs are optional. Keep-alive and idle timeout are off
unless explicitly enabled.

## Frame format (smux v1)

```text
VERSION(1B) | CMD(1B) | LENGTH(2B LE) | STREAMID(4B LE) | DATA(LENGTH)

VERSION: 1
CMD:
    SYN(0)  open stream  (LENGTH must be 0)
    FIN(1)  close stream (LENGTH must be 0)
    PSH(2)  payload
    NOP(3)  keep-alive   (LENGTH must be 0; STREAMID is 0)
```

Stream id 0 is reserved for NOP; the library never hands it out and
rejects any peer SYN that uses it.

## Benchmark

| Implementation    | Throughput (TCP) | Handshake  |
| ----------------- | ---------------- | ---------- |
| smux (go)         | 0.4854 GiB/s     | 17.070 K/s |
| async-smux (rust) | 1.0550 GiB/s     | 81.774 K/s |

`benches/bench.rs` uses the unstable `test` crate so it requires a
nightly toolchain: `cargo +nightly bench`.

## License

MIT — see [LICENSE](LICENSE).
