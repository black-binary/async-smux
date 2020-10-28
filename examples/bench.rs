use async_smux::{MuxDispatcher, MuxStream};
use smol::{channel, net::TcpListener, net::TcpStream, prelude::*};

async fn get_tcp_stream_pair() -> (TcpStream, TcpStream) {
    let listener = TcpListener::bind("0.0.0.0:0").await.unwrap();
    let local_addr = listener.local_addr().unwrap();
    let (tx, rx) = channel::bounded(8);
    smol::spawn(async move {
        let (stream, _) = listener.accept().await.unwrap();
        tx.send(stream).await.unwrap();
    })
    .detach();
    let client_stream = TcpStream::connect(local_addr).await.unwrap();
    let server_stream = rx.recv().await.unwrap();
    (client_stream, server_stream)
}

async fn get_mux_stream_pair() -> (MuxDispatcher, MuxDispatcher, MuxStream, MuxStream) {
    let (stream1, stream2) = get_tcp_stream_pair().await;
    let mut mux1 = MuxDispatcher::new(stream1);
    let mut mux2 = MuxDispatcher::new(stream2);
    let stream1 = mux1.connect().await.unwrap();
    let stream2 = mux2.accept().await.unwrap();
    (mux1, mux2, stream1, stream2)
}

fn main() {
    std::env::set_var("SMOL_THREADS", "8");
    smol::block_on(async {
        const PAYLOAD_SIZE: usize = 0xffff;
        const SEND_ROUND: usize = 0x10000;
        let (_mux1, _mux2, mut stream1, mut stream2) = get_mux_stream_pair().await;
        let _t1 = smol::spawn(async move {
            let payload = [0u8; PAYLOAD_SIZE];
            for _ in 0..SEND_ROUND {
                stream1.write(&payload).await.unwrap();
            }
        });
        let t2 = smol::spawn(async move {
            let mut payload = [0u8; PAYLOAD_SIZE];
            let i1 = std::time::Instant::now();
            for _ in 0..SEND_ROUND {
                stream2.read(&mut payload).await.unwrap();
            }
            let i2 = std::time::Instant::now();
            let duration: std::time::Duration = i2 - i1;
            println!("time: {} ms", duration.as_millis());
            println!("data: {} bytes", PAYLOAD_SIZE * SEND_ROUND);
            let speed = (PAYLOAD_SIZE * SEND_ROUND / 1024 / 1024) as f32
                / (duration.as_millis() as f32 / 1000.0);
            println!("speed: {} MB/s", speed);
        });
        t2.await;
    });
}
