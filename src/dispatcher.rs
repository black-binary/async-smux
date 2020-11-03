use std::{collections::HashMap, sync::Arc};

use bytes::Bytes;
use futures::lock::Mutex;
use futures::prelude::*;
use smol::{
    channel::{bounded, Receiver, Sender},
    future::FutureExt,
    Task,
};

use crate::config::MuxConfig;
use crate::error::{Error, Result};
use crate::frame::{Command, Frame};
use crate::stream::MuxStream;

/// A smux dispatcher.
///
/// A dispatcher can establish multiple `MuxStream` over the underlying stream.
///
#[derive(Clone)]
pub struct MuxDispatcher {
    local_tx_map: Arc<Mutex<HashMap<u32, Sender<Frame>>>>,
    global_tx: Sender<Frame>,
    stream_rx: Arc<Mutex<Receiver<MuxStream>>>,
    config: Arc<MuxConfig>,
    _task: Arc<Task<()>>,
}

impl MuxDispatcher {
    /// Gets the number of connections handled by this dispatcher.
    pub async fn get_streams_count(&self) -> usize {
        self.local_tx_map.lock().await.len()
    }

    /// Consumes an async stream to crate a new dispatcher.
    /// # Panics
    /// Panics if the config is invalid.
    pub fn new<T: AsyncRead + AsyncWrite + Send + 'static>(inner: T) -> Self {
        Self::new_from_config(inner, MuxConfig::default())
    }

    /// Consumes an async stream to crate a new dispatcher, with customized config.
    /// # Panics
    /// Panics if the config is invalid.
    pub fn new_from_config<T: AsyncRead + AsyncWrite + Send + 'static>(
        inner: T,
        config: MuxConfig,
    ) -> Self {
        config.check().expect("Invalid mux config");

        let local_tx_map = Arc::new(Mutex::new(HashMap::new()));
        let (mut reader, mut writer) = inner.split();
        let (stream_tx, stream_rx) = bounded(config.stream_buffer);
        let (global_tx, global_rx) = bounded(config.dispatcher_frame_buffer);

        let read_worker = {
            let local_tx_map = local_tx_map.clone();
            let global_tx = global_tx.clone();
            async move {
                log::debug!("read_worker spawned");
                loop {
                    let frame = Frame::read_from(&mut reader).await;
                    if let Err(e) = frame {
                        log::error!("closing underlying stream: {}", e);
                        return;
                    }
                    let frame = frame.unwrap();
                    match frame.header.command {
                        Command::Sync => {
                            let stream_id = frame.header.stream_id;
                            let (local_tx, local_rx) = bounded(config.stream_frame_buffer);
                            let stream = MuxStream {
                                stream_id: stream_id,
                                tx: global_tx.clone(),
                                rx: local_rx,
                                read_buf: Bytes::new(),
                                max_payload_length: config.max_payload_length,
                                closed: false,
                            };
                            if stream_tx.send(stream).await.is_err() {
                                log::debug!("dispatcher closed");
                                return;
                            }
                            local_tx_map.lock().await.insert(stream_id, local_tx);
                            log::debug!("read_worker, syn, {:08X} inserted", stream_id);
                        }
                        Command::Push => {
                            let tx = {
                                if let Some(tx) =
                                    local_tx_map.lock().await.get(&frame.header.stream_id)
                                {
                                    tx.clone()
                                } else {
                                    log::error!(
                                        "stream id {:08X} not found",
                                        frame.header.stream_id
                                    );
                                    continue;
                                }
                            };
                            let stream_id = frame.header.stream_id;
                            if tx.send(frame).await.is_err() {
                                local_tx_map.lock().await.remove(&stream_id);
                                log::debug!(
                                    "read_worker: stream object dropped, {:08X} removed",
                                    stream_id
                                );
                            }
                        }
                        Command::Nop => {}
                        Command::Finish => {
                            local_tx_map.lock().await.remove(&frame.header.stream_id);
                            log::debug!("read worker: fin, {:08X} removed", frame.header.stream_id);
                        }
                    };
                }
            }
        };

        let write_worker = {
            log::debug!("write_worker spawned");
            let local_tx_map = local_tx_map.clone();
            async move {
                loop {
                    let frame = global_rx.recv().await;
                    if frame.is_err() {
                        log::debug!("global_rx drained");
                        break;
                    }
                    let frame = frame.unwrap();
                    if let Command::Finish = frame.header.command {
                        log::debug!("write worker, fin: {:08X}", frame.header.stream_id);
                        local_tx_map.lock().await.remove(&frame.header.stream_id);
                    }
                    let result = frame.write_to(&mut writer).await;
                    if let Err(e) = result {
                        log::error!(
                            "failed to write frame bytes to the underlying stream: {}",
                            e
                        );
                        break;
                    }
                }
            }
        };

        let race_task = smol::spawn(async move {
            let read_task = smol::spawn(read_worker);
            let write_task = smol::spawn(write_worker);
            read_task.race(write_task).await;
            log::debug!("race task done");
        });

        Self {
            global_tx: global_tx,
            stream_rx: Arc::new(Mutex::new(stream_rx)),
            local_tx_map: local_tx_map,
            config: Arc::new(config),
            _task: Arc::new(race_task),
        }
    }

    /// Accepts a new smux stream from the peer.
    pub async fn accept(&mut self) -> Result<MuxStream> {
        if let Some(stream) = self.stream_rx.lock().await.next().await {
            Ok(stream)
        } else {
            Err(Error::DispatcherClosed)
        }
    }

    /// Spawns a new smux stream to the peer.
    pub async fn connect(&mut self) -> Result<MuxStream> {
        let mut stream_id: u32;
        loop {
            stream_id = rand::random();
            if !self.local_tx_map.lock().await.contains_key(&stream_id) {
                break;
            }
        }
        let (local_tx, local_rx) = bounded(self.config.stream_frame_buffer);
        let global_tx = self.global_tx.clone();
        let stream = MuxStream {
            stream_id: stream_id,
            tx: global_tx,
            rx: local_rx,
            read_buf: Bytes::new(),
            max_payload_length: self.config.max_payload_length,
            closed: false,
        };
        let frame = Frame::new_sync_frame(stream_id);
        if self.global_tx.send(frame).await.is_err() {
            return Err(Error::DispatcherClosed);
        }
        self.local_tx_map.lock().await.insert(stream_id, local_tx);
        log::debug!("connect syn insert: {:08X}", stream_id);
        Ok(stream)
    }
}
