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
    /// Maximum gap (seconds) between any two received frames before the
    /// peer is declared dead and the connection closed. Only consulted
    /// when `keep_alive_interval` is also set. Defaults to 3 *
    /// `keep_alive_interval`.
    pub keep_alive_timeout: Option<NonZeroU64>,
    pub idle_timeout: Option<NonZeroU64>,
    pub max_tx_queue: NonZeroUsize,
    pub max_rx_queue: NonZeroUsize,
}
