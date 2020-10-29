use async_smux::{MuxDispatcher, MuxStream};
use criterion::{criterion_group, criterion_main, Criterion, Throughput};
use smol::{channel, net::TcpListener, net::TcpStream, prelude::*};
use std::time::Duration;

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

const PAYLOAD_SIZE: usize = 128 * 1024;
const ROUND: usize = 1024;

pub fn tcp_throughput() {
    smol::block_on(async {
        let (mut stream1, mut stream2) = get_tcp_stream_pair().await;
        let _t1 = smol::spawn(async move {
            let payload = [0u8; PAYLOAD_SIZE];
            for _ in 0..ROUND {
                stream1.write_all(&payload).await.unwrap();
            }
            stream1.close().await.unwrap();
        });
        let t2 = smol::spawn(async move {
            let mut payload = [0u8; PAYLOAD_SIZE];
            loop {
                if let Ok(_) = stream2.read(&mut payload).await {
                } else {
                    break;
                }
            }
        });
        t2.await;
    });
}

pub fn smux_throughput() {
    smol::block_on(async {
        let (_mux1, _mux2, mut stream1, mut stream2) = get_mux_stream_pair().await;
        let _t1 = smol::spawn(async move {
            let payload = [0u8; PAYLOAD_SIZE];
            for _ in 0..ROUND {
                stream1.write_all(&payload).await.unwrap();
            }
            stream1.close().await.unwrap();
        });
        let t2 = smol::spawn(async move {
            let mut payload = [0u8; PAYLOAD_SIZE];
            loop {
                if let Ok(_) = stream2.read(&mut payload).await {
                } else {
                    break;
                }
            }
        });
        t2.await;
    });
}

pub fn criterion_benchmark(c: &mut Criterion) {
    std::env::set_var("SMOL_THREADS", "8");
    let mut group = c.benchmark_group("throughput");
    group.throughput(Throughput::Bytes((PAYLOAD_SIZE * ROUND) as u64));
    group.bench_function("tcp", |b| b.iter(|| smux_throughput()));
    group.bench_function("smux", |b| b.iter(|| smux_throughput()));
    group.finish()
}

criterion_group! {
    name = benches;
    config = Criterion::default().sample_size(20).measurement_time(Duration::from_secs(10));
    targets = criterion_benchmark
}
criterion_main!(benches);
