use async_smux::{MuxDispatcher, MuxStream};
use smol::{channel, net::TcpListener, net::TcpStream, prelude::*};

async fn get_tcp_stream_pair() -> (TcpStream, TcpStream) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
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

const PAYLOAD_SIZE: usize = 0x40000;
const SEND_ROUND: usize = 1024 * 64;

fn init() {
    let _ = env_logger::builder()
        .filter_level(log::LevelFilter::Debug)
        .try_init();
    std::env::set_var("SMOL_THREADS", "4");
}

fn main() {
    init();
    smol::block_on(async {
        let (_mux1, _mux2, mut stream1, mut stream2) = get_mux_stream_pair().await;
        let _t1 = smol::spawn(async move {
            let mut payload = Vec::new();
            payload.resize(PAYLOAD_SIZE, 0);
            for i in 0..SEND_ROUND {
                stream1.write_all(&payload).await.unwrap();
                println!("sent: {}", i);
            }
            stream1.close().await.unwrap();
        });
        let mut payload = Vec::new();
        payload.resize(PAYLOAD_SIZE, 0);
        for i in 0..SEND_ROUND {
            stream2.read_exact(&mut payload).await.unwrap();
            println!("recv: {}", i);
        }
        _t1.await;
    });
}
