use async_smux::{Mux, MuxConfig};
use smol::net::{TcpListener, TcpStream};
use smol::prelude::*;

async fn echo_server() {
    let listener = TcpListener::bind("0.0.0.0:12345").await.unwrap();
    let (stream, _) = listener.accept().await.unwrap();
    let mux = Mux::new(stream, MuxConfig::default());
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
        let mux = Mux::new(stream, MuxConfig::default());
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
