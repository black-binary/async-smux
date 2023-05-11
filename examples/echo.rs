use std::time::Duration;

use async_smux::MuxBuilder;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
};

async fn echo_server() {
    let listener = TcpListener::bind("127.0.0.1:12345").await.unwrap();
    let (stream, _) = listener.accept().await.unwrap();

    let (_, mut acceptor, worker) = MuxBuilder::server().with_connection(stream).build();
    tokio::spawn(worker);

    println!("server launched");
    while let Some(mut mux_stream) = acceptor.accept().await {
        println!("accepted mux stream {}", mux_stream.get_stream_id());

        let mut buf = [0u8; 100];
        let size = mux_stream.read(&mut buf).await.unwrap();
        mux_stream.write_all(&buf[..size]).await.unwrap();
        mux_stream.flush().await.unwrap();
        mux_stream.shutdown().await.unwrap();
    }
}

#[tokio::main]
async fn main() {
    tokio::spawn(echo_server());
    tokio::time::sleep(Duration::from_secs(3)).await;

    let stream = TcpStream::connect("127.0.0.1:12345").await.unwrap();
    let (connector, _, worker) = MuxBuilder::client().with_connection(stream).build();
    tokio::spawn(worker);

    for i in 0..10 {
        let mut mux_stream = connector.connect().unwrap();
        let mut buf = [0u8; 5];
        mux_stream.write_all(b"hello").await.unwrap();
        mux_stream.read_exact(&mut buf).await.unwrap();
        let reply = String::from_utf8(buf[..].to_vec()).unwrap();
        println!("{}: reply = {}", i, reply);
        mux_stream.shutdown().await.unwrap();
    }
}
