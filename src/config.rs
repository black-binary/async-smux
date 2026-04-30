use std::num::{NonZeroU64, NonZeroUsize};

/// Parity used for locally-allocated stream ids. Server uses `Even`
/// (0, 2, 4, …) and client uses `Odd` (1, 3, 5, …) so the two sides
/// never collide. Stream id 0 is reserved for NOP keep-alive frames
/// and is never handed out as a real stream id.
#[derive(Clone, Copy, Debug)]
pub enum StreamIdType {
    Even = 0,
    Odd = 1,
}

/// Session configuration. Construct via [`crate::MuxBuilder`] for the
/// fluent API, or build one directly and pass it to
/// [`crate::mux_connection`].
#[derive(Clone, Copy, Debug)]
pub struct MuxConfig {
    /// Parity for locally-allocated stream ids. See [`StreamIdType`].
    pub stream_id_type: StreamIdType,
    /// If set, send a NOP frame this often (seconds) and enable
    /// dead-peer detection.
    pub keep_alive_interval: Option<NonZeroU64>,
    /// Maximum gap (seconds) between any two received frames before
    /// the peer is declared dead and the connection closed. Only
    /// consulted when `keep_alive_interval` is also set. Defaults to
    /// 3 × `keep_alive_interval`.
    pub keep_alive_timeout: Option<NonZeroU64>,
    /// If set, close any stream that sees no traffic for this many
    /// seconds.
    pub idle_timeout: Option<NonZeroU64>,
    /// Backpressure threshold for outbound frames per stream.
    pub max_tx_queue: NonZeroUsize,
    /// Backpressure threshold for inbound frames across the session.
    pub max_rx_queue: NonZeroUsize,
}
