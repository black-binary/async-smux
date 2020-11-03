pub(crate) mod config;
pub(crate) mod dispatcher;
pub(crate) mod error;
pub(crate) mod frame;
pub(crate) mod stream;

pub use config::MuxConfig;
pub use dispatcher::MuxDispatcher;
pub use error::{Error, Result};
pub use stream::MuxStream;

pub mod prelude {
    pub use crate::config::MuxConfig;
    pub use crate::dispatcher::MuxDispatcher;
    pub use crate::stream::MuxStream;
}

#[cfg(test)]
mod test {
    use std::time::Duration;

    use super::prelude::*;
    use rand::prelude::*;
    use smol::net::{TcpListener, TcpStream};
    use smol::prelude::*;

    fn init() {
        let _ = env_logger::builder()
            .filter_level(log::LevelFilter::Error)
            .try_init();
        std::env::set_var("SMOL_THREADS", "4");
    }

    #[test]
    fn drop() {
        init();
        smol::block_on(async {
            let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
            let local_addr = listener.local_addr().unwrap();

            let t = smol::spawn(async move {
                let (stream, _) = listener.accept().await.unwrap();
                let mut dispatcher = MuxDispatcher::new(stream);

                let _stream1 = dispatcher.accept().await.unwrap();
                let _stream2 = dispatcher.accept().await.unwrap();
                let _stream3 = dispatcher.accept().await.unwrap();

                let mut invalid_streams = Vec::new();

                for _ in 0..100 {
                    let stream = dispatcher.accept().await.unwrap();
                    invalid_streams.push(stream);
                }

                smol::Timer::after(Duration::from_secs(5)).await;
                assert_eq!(dispatcher.get_streams_count().await, 3);
            });

            let stream = TcpStream::connect(local_addr).await.unwrap();
            let mut dispatcher = MuxDispatcher::new(stream);

            let _stream1 = dispatcher.connect().await.unwrap();
            assert_eq!(dispatcher.get_streams_count().await, 1);
            let _stream2 = dispatcher.connect().await.unwrap();
            assert_eq!(dispatcher.get_streams_count().await, 2);
            let _stream3 = dispatcher.connect().await.unwrap();
            assert_eq!(dispatcher.get_streams_count().await, 3);

            for _ in 0..100 {
                let _ = dispatcher.connect().await.unwrap();
            }

            smol::Timer::after(Duration::from_secs(3)).await;

            assert_eq!(dispatcher.get_streams_count().await, 3);
            t.await
        });
    }

    #[test]
    fn concurrent() {
        init();
        smol::block_on(async {
            let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
            let local_addr = listener.local_addr().unwrap();

            let mut random_payload = [0u8; 0x1000];
            for i in 0..random_payload.len() {
                random_payload[i] = random();
            }

            smol::spawn(async move {
                let (stream, _) = listener.accept().await.unwrap();
                let mut mux = MuxDispatcher::new(stream);
                let mut tasks = Vec::new();
                for i in 0..100 {
                    let mut stream = mux.accept().await.unwrap();
                    let id = i;
                    let task = smol::spawn(async move {
                        let send_buf = [id as u8; 1024];
                        stream.write(&send_buf).await.unwrap();
                        stream.close().await.unwrap();
                        println!("thread {} closed", id);
                    });
                    tasks.push(task);
                }
                for t in tasks {
                    t.await;
                }
                let mut streams = Vec::new();
                for _ in 0i32..3 {
                    streams.push(mux.connect().await.unwrap());
                }
                streams[0].write_all(&random_payload).await.unwrap();
                for s in streams.iter_mut() {
                    let mut send_buf = [0u8; 1024];
                    assert!(s.read(&mut send_buf).await.is_err());
                }
                smol::Timer::after(Duration::from_secs(5)).await;
            })
            .detach();

            let stream = TcpStream::connect(local_addr).await.unwrap();
            let mut mux = MuxDispatcher::new(stream);

            let mut tasks = Vec::new();

            for i in 0i32..100 {
                let mut stream: MuxStream = mux.connect().await.unwrap();
                let id = i;
                let task = smol::spawn(async move {
                    let mut recv_buf = [0u8; 1024];
                    stream.read_exact(&mut recv_buf).await.unwrap();
                    println!(
                        "id={}, buf={}, stream_id = {:08X}",
                        id,
                        recv_buf[0],
                        stream.get_stream_id()
                    );
                });
                tasks.push(task);
            }

            for i in tasks {
                i.await;
            }

            let mut streams = Vec::new();
            for _ in 0i32..3 {
                streams.push(mux.accept().await.unwrap());
            }

            let mut payload = Vec::new();

            let mut my_stream = streams.remove(0);
            while payload.len() != 0x1000 {
                let mut small_buf = [0u8; 0x100];
                my_stream.read_exact(&mut small_buf).await.unwrap();
                payload.extend_from_slice(&small_buf[..]);
            }

            assert_eq!(&payload, &random_payload);

            for s in streams.iter_mut() {
                s.close().await.unwrap();
            }
        });
    }

    #[test]
    fn simple_parsing() {
        init();
        smol::block_on(async {
            let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
            let local_addr = listener.local_addr().unwrap();
            smol::spawn(async move {
                let (server_stream, _) = listener.accept().await.unwrap();
                let mut server_dispatcher = MuxDispatcher::new(server_stream);
                let mut server_mux_stream = server_dispatcher.accept().await.unwrap();
                server_mux_stream
                    .write_all(b"testtest12345678")
                    .await
                    .unwrap();
            })
            .detach();
            let client_stream = TcpStream::connect(local_addr).await.unwrap();
            let mut client_dispatcher = MuxDispatcher::new(client_stream);
            let mut client_mux_stream = client_dispatcher.connect().await.unwrap();
            let mut buf = [0u8; 1024];
            let size = client_mux_stream.read(&mut buf).await.unwrap();
            let payload = String::from_utf8(buf[..size].to_vec()).unwrap();
            assert_eq!(payload, "testtest12345678");
        });
    }

    #[test]
    fn huge_payload() {
        init();
        smol::block_on(async {
            let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
            let local_addr = listener.local_addr().unwrap();
            let mut payload = Vec::new();
            payload.resize(0x30000, 0);
            rand::thread_rng().fill_bytes(&mut payload);

            let payload_bak = payload.clone();
            smol::spawn(async move {
                let (server_stream, _) = listener.accept().await.unwrap();
                let mut server_dispatcher = MuxDispatcher::new(server_stream);
                let mut server_mux_stream = server_dispatcher.accept().await.unwrap();
                server_mux_stream.write_all(&payload).await.unwrap();
                server_mux_stream.close().await.unwrap();
            })
            .detach();
            let client_stream = TcpStream::connect(local_addr).await.unwrap();
            let mut client_dispatcher = MuxDispatcher::new(client_stream);
            let mut client_mux_stream = client_dispatcher.connect().await.unwrap();
            let mut buf2 = Vec::new();
            buf2.resize(0x30000, 0);
            for i in 0..0x10 {
                client_mux_stream
                    .read_exact(&mut buf2[i * 0x3000..(i + 1) * 0x3000])
                    .await
                    .unwrap();
            }
            assert_eq!(buf2, payload_bak);
        });
    }
}
