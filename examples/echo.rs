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

fn init() {
    let _ = env_logger::builder()
        .filter_level(log::LevelFilter::Debug)
        .try_init();
    std::env::set_var("SMOL_THREADS", "4");
}

fn main() {
    init();
    smol::spawn(echo_server()).detach();
    smol::block_on(async {
        smol::Timer::after(std::time::Duration::from_secs(1)).await;
        let stream = TcpStream::connect("127.0.0.1:12345").await.unwrap();
        let mut mux = MuxDispatcher::new(stream);
        for i in 0..100 {
            let mut mux_stream = mux.connect().await.unwrap();
            let mut buf = [0u8; 1024];
            let payload = format!("hello {}", i);
            mux_stream.write(&payload.as_bytes()).await.unwrap();
            let size = mux_stream.read(&mut buf).await.unwrap();
            let reply = String::from_utf8(buf[..size].to_vec()).unwrap();
            println!("{}: {}", i, reply);
        }
    });
}
