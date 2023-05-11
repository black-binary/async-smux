#![feature(test)]

use tokio::net::{TcpListener, TcpStream};

extern crate test;

pub fn add_two(a: i32) -> i32 {
    a + 2
}

async fn get_tcp_pair() -> (TcpStream, TcpStream) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let h = tokio::spawn(async move {
        let (a, _) = listener.accept().await.unwrap();
        a
    });

    let b = TcpStream::connect(addr).await.unwrap();
    let a = h.await.unwrap();
    a.set_nodelay(true).unwrap();
    b.set_nodelay(true).unwrap();
    (a, b)
}

#[cfg(test)]
mod tests {
    use super::*;
    use async_smux::{MuxBuilder, MuxStream};
    use test::Bencher;
    use tokio::{
        io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt},
        runtime::Runtime,
    };

    lazy_static::lazy_static! {
        static ref RT: Runtime = tokio::runtime::Builder::new_multi_thread().enable_all().build().unwrap();
    }

    fn get_mux_pair() -> (MuxStream<TcpStream>, MuxStream<TcpStream>) {
        RT.block_on(async {
            let (a, b) = get_tcp_pair().await;
            let (connector, _, worker) = MuxBuilder::client().with_connection(a).build();
            RT.spawn(worker);
            let (_, mut acceptor, worker) = MuxBuilder::server().with_connection(b).build();
            RT.spawn(worker);
            let a = connector.connect().unwrap();
            let b = acceptor.accept().await.unwrap();
            (a, b)
        })
    }

    #[inline]
    async fn send<T: AsyncWrite + Unpin>(data: &[u8], a: &mut T) {
        a.write_all(data).await.unwrap();
        a.flush().await.unwrap();
    }

    #[inline]
    async fn recv<T: AsyncRead + Unpin>(buf: &mut [u8], a: &mut T) -> std::io::Result<()> {
        a.read_exact(buf).await?;
        Ok(())
    }

    const DATA_SIZE: usize = 0x20000;

    fn bench_send<T: AsyncRead + AsyncWrite + Send + Unpin + 'static>(
        bencher: &mut Bencher,
        mut a: T,
        mut b: T,
    ) {
        let data = vec![0; DATA_SIZE];
        let mut buf = vec![0; DATA_SIZE];
        RT.spawn(async move {
            loop {
                if recv(&mut buf, &mut b).await.is_err() {
                    break;
                }
            }
        });
        bencher.bytes = DATA_SIZE as u64;

        // Warm up
        for _ in 0..10 {
            RT.block_on(async {
                send(&data, &mut a).await;
            });
        }

        for _ in 0..10 {
            bencher.iter(|| {
                RT.block_on(async {
                    send(&data, &mut a).await;
                });
            });
        }
    }

    #[bench]
    fn bench_tcp_send(bencher: &mut Bencher) {
        let (a, b) = RT.block_on(async { get_tcp_pair().await });
        bench_send(bencher, a, b);
    }

    #[bench]
    fn bench_mux_send(bencher: &mut Bencher) {
        let (a, b) = get_mux_pair();
        bench_send(bencher, a, b);
    }
}
