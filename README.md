# async-smux

[crates.io](https://crates.io/crates/async_smux)

A lightweight asynchronous [smux](https://github.com/xtaci/smux) (Simple MUltipleXing) library for smol / async-std. 

![img](https://raw.githubusercontent.com/xtaci/smux/master/mux.jpg)

async-smux consumes a struct implementing `AsyncRead + AsyncWrite + Unpin + Send`, like TcpStream, TlsStream and anything you like. And then you may spawn multiple `MuxStream`s(up to 65535), which also implements `AsyncRead + AsyncWrite + Unpin + Send`.

## Benchmark

| Implementation    | Throughput (TCP) | Handshake  |
| ----------------- | ---------------- | ---------- |
| smux (go)         | 0.4854 GiB/s     | 17.070 K/s |
| async-smux (rust) | 1.1603 GiB/s     | 30.502 K/s |

Check out `/benches` directory for more details.

## Laziness

No thread or task will be spawned by this library. It just spawns a few `future`s. So it's runtime-independent.

`Mux` and `MuxStream` are completely lazy and will DO NOTHING if you don't `poll()` them.

Any polling operation, including `.read()` ,`.write()`, `accept()` and `connect()`, will push `Mux` and `MuxStream` working.

## Example

```rust
use async_smux::MuxDispatcher;
use smol::net::{TcpListener, TcpStream};
use smol::prelude::*;

async fn echo_server() {
    let listener = TcpListener::bind("0.0.0.0:12345").await.unwrap();
    let (stream, _) = listener.accept().await.unwrap();
    let mut mux = MuxDispatcher::new(stream);
    loop {
        let mut mux_stream = mux.accept().await.unwrap();
        let mut buf = [0u8; 1024];
        let size = mux_stream.read(&mut buf).await.unwrap();
        mux_stream.write(&buf[..size]).await.unwrap();
    }
}

fn main() {
    smol::spawn(echo_server()).detach();
    smol::block_on(async {
        smol::Timer::after(std::time::Duration::from_secs(1)).await;
        let stream = TcpStream::connect("127.0.0.1:12345").await.unwrap();
        let mut mux = MuxDispatcher::new(stream);
        for i in 0..100 {
            let mut mux_stream = mux.connect().await.unwrap();
            let mut buf = [0u8; 1024];
            mux_stream.write(b"hello").await.unwrap();
            let size = mux_stream.read(&mut buf).await.unwrap();
            let reply = String::from_utf8(buf[..size].to_vec()).unwrap();
            println!("{}: {}", i, reply);
        }
    });
}

```


