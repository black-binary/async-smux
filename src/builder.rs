//! Type-state builder for constructing a smux session.
//!
//! Start with [`MuxBuilder::client`] or [`MuxBuilder::server`], chain
//! optional `with_*` calls to configure timeouts and queue limits, then
//! call `with_connection(...)` followed by `build()` to obtain the
//! `(MuxConnector, MuxAcceptor, MuxWorker)` triple.

use std::num::{NonZeroU64, NonZeroUsize};

use crate::{
    config::{MuxConfig, StreamIdType},
    mux::{MuxWorker, TokioConn},
    mux_connection, MuxAcceptor, MuxConnector,
};

pub struct WithConnection<T> {
    config: MuxConfig,
    connection: T,
}

pub struct WithConfig {
    config: MuxConfig,
}

pub struct Begin {}

/// Type-state builder. Move through `Begin → WithConfig → WithConnection`
/// by calling `client()` / `server()`, optional `with_*`, then
/// `with_connection(...)`. Finalize with `build()`.
pub struct MuxBuilder<State> {
    state: State,
}

impl MuxBuilder<Begin> {
    /// Configure as a client: locally-allocated stream ids are odd.
    pub fn client() -> MuxBuilder<WithConfig> {
        MuxBuilder {
            state: WithConfig {
                config: MuxConfig {
                    stream_id_type: StreamIdType::Odd,
                    keep_alive_interval: None,
                    keep_alive_timeout: None,
                    idle_timeout: None,
                    max_tx_queue: NonZeroUsize::new(1024).unwrap(),
                    max_rx_queue: NonZeroUsize::new(1024).unwrap(),
                },
            },
        }
    }

    /// Configure as a server: locally-allocated stream ids are even.
    pub fn server() -> MuxBuilder<WithConfig> {
        MuxBuilder {
            state: WithConfig {
                config: MuxConfig {
                    stream_id_type: StreamIdType::Even,
                    keep_alive_interval: None,
                    keep_alive_timeout: None,
                    idle_timeout: None,
                    max_tx_queue: NonZeroUsize::new(1024).unwrap(),
                    max_rx_queue: NonZeroUsize::new(1024).unwrap(),
                },
            },
        }
    }
}

impl MuxBuilder<WithConfig> {
    /// Send a NOP keep-alive frame every `interval_secs` seconds. When
    /// set, the dead-peer timeout (see [`Self::with_keep_alive_timeout`])
    /// also becomes active.
    pub fn with_keep_alive_interval(&mut self, interval_secs: NonZeroU64) -> &mut Self {
        self.state.config.keep_alive_interval = Some(interval_secs);
        self
    }

    /// Maximum gap between received frames before the peer is declared dead
    /// and the connection closed. Only consulted when keep-alive is also
    /// enabled. Defaults to 3 * `keep_alive_interval`.
    pub fn with_keep_alive_timeout(&mut self, timeout_secs: NonZeroU64) -> &mut Self {
        self.state.config.keep_alive_timeout = Some(timeout_secs);
        self
    }

    /// Per-stream idle timeout: if a stream sees no traffic for this
    /// many seconds, it is closed and its handle is reaped.
    pub fn with_idle_timeout(&mut self, timeout_secs: NonZeroU64) -> &mut Self {
        self.state.config.idle_timeout = Some(timeout_secs);
        self
    }

    /// Backpressure threshold for outbound frames. `poll_write` parks
    /// once a stream's pending tx queue exceeds this value. Defaults
    /// to 1024.
    pub fn with_max_tx_queue(&mut self, size: NonZeroUsize) -> &mut Self {
        self.state.config.max_tx_queue = size;
        self
    }

    /// Backpressure threshold for inbound frames. The dispatcher parks
    /// once total pending rx exceeds this value, propagating
    /// backpressure to the peer's tx side. Defaults to 1024.
    pub fn with_max_rx_queue(&mut self, size: NonZeroUsize) -> &mut Self {
        self.state.config.max_rx_queue = size;
        self
    }

    /// Bind the underlying transport. The transport must implement
    /// `AsyncRead + AsyncWrite + Unpin`.
    pub fn with_connection<T: TokioConn>(
        &mut self,
        connection: T,
    ) -> MuxBuilder<WithConnection<T>> {
        MuxBuilder {
            state: WithConnection {
                config: self.state.config,
                connection,
            },
        }
    }
}

impl<T: TokioConn> MuxBuilder<WithConnection<T>> {
    /// Finalize the builder. Returns the three handles that share the
    /// session: the connector for outgoing streams, the acceptor for
    /// peer-initiated streams, and the worker future that must be
    /// polled (e.g. via `tokio::spawn`) to drive I/O.
    pub fn build(self) -> (MuxConnector<T>, MuxAcceptor<T>, MuxWorker<T>) {
        mux_connection(self.state.connection, self.state.config)
    }
}
