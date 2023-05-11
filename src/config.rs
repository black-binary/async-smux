use std::num::{NonZeroU64, NonZeroUsize};

#[derive(Clone, Copy, Debug)]
pub enum StreamIdType {
    Even = 0,
    Odd = 1,
}

#[derive(Clone, Copy, Debug)]
pub struct MuxConfig {
    pub stream_id_type: StreamIdType,
    pub keep_alive_interval: Option<NonZeroU64>,
    pub idle_timeout: Option<NonZeroU64>,
    pub max_tx_queue: NonZeroUsize,
    pub max_rx_queue: NonZeroUsize,
}
