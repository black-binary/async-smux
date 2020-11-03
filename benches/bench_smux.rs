use async_smux::{MuxDispatcher, MuxStream};
use criterion::{criterion_group, criterion_main, Criterion, Throughput};
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

const PAYLOAD_SIZE: usize = 128 * 1024;
const SEND_ROUND: usize = 1024;

fn tcp_throughput() {
    smol::block_on(async {
        let (mut stream1, mut stream2) = get_tcp_stream_pair().await;
        let _t1 = smol::spawn(async move {
            let mut payload = Vec::new();
            payload.resize(PAYLOAD_SIZE, 0);
            for _ in 0..SEND_ROUND {
                stream1.write_all(&payload).await.unwrap();
            }
            stream1.close().await.unwrap();
        });
        let mut payload = Vec::new();
        payload.resize(PAYLOAD_SIZE, 0);
        loop {
            if stream2.read_exact(&mut payload).await.is_err() {
                stream2.close().await.unwrap();
                break;
            }
        }
    });
}

fn smux_throughput() {
    smol::block_on(async {
        let (_mux1, _mux2, mut stream1, mut stream2) = get_mux_stream_pair().await;
        let _t1 = smol::spawn(async move {
            let mut payload = Vec::new();
            payload.resize(PAYLOAD_SIZE, 0);
            for _ in 0..SEND_ROUND {
                stream1.write_all(&payload).await.unwrap();
            }
            stream1.close().await.unwrap();
        });
        let mut payload = Vec::new();
        payload.resize(PAYLOAD_SIZE, 0);
        loop {
            if stream2.read_exact(&mut payload).await.is_err() {
                stream2.close().await.unwrap();
                return;
            };
        }
    });
}

const HANDSHAKE_ROUND: usize = 1024 * 64;

fn smux_handshake() {
    smol::block_on(async {
        let (stream1, stream2) = get_tcp_stream_pair().await;
        let mut mux1 = MuxDispatcher::new(stream1);
        let mut mux2 = MuxDispatcher::new(stream2);
        let _t1 = smol::spawn(async move {
            for _ in 0..HANDSHAKE_ROUND {
                let mut stream = mux1.accept().await.unwrap();
                stream.close().await.unwrap();
            }
        });
        for _ in 0..HANDSHAKE_ROUND {
            let mut stream = mux2.connect().await.unwrap();
            stream.close().await.unwrap();
        }
    });
}

pub fn throughput_benchmark(c: &mut Criterion) {
    std::env::set_var("SMOL_THREADS", "8");
    let mut group = c.benchmark_group("throughput");
    group.throughput(Throughput::Bytes((PAYLOAD_SIZE * SEND_ROUND) as u64));
    group.bench_function("smux", |b| b.iter(|| smux_throughput()));
    group.bench_function("tcp", |b| b.iter(|| tcp_throughput()));
    group.finish();
}

pub fn handshake_benchmark(c: &mut Criterion) {
    std::env::set_var("SMOL_THREADS", "8");
    let mut group = c.benchmark_group("handshake");
    group.throughput(Throughput::Elements(HANDSHAKE_ROUND as u64));
    group.bench_function("handhsake", |b| b.iter(|| smux_handshake()));
    group.finish();
}

criterion_group! {
    name = throughput_benches;
    config = Criterion::default().sample_size(20);
    targets = throughput_benchmark
}

criterion_group! {
    name = handhsake_benches;
    config = Criterion::default().sample_size(20);
    targets = handshake_benchmark
}

criterion_main!(throughput_benches, handhsake_benches);
