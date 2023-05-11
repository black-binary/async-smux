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

pub struct MuxBuilder<State> {
    state: State,
}

impl MuxBuilder<Begin> {
    pub fn client() -> MuxBuilder<WithConfig> {
        MuxBuilder {
            state: WithConfig {
                config: MuxConfig {
                    stream_id_type: StreamIdType::Odd,
                    keep_alive_interval: None,
                    idle_timeout: None,
                    max_tx_queue: NonZeroUsize::new(512).unwrap(),
                    max_rx_queue: NonZeroUsize::new(512).unwrap(),
                },
            },
        }
    }

    pub fn server() -> MuxBuilder<WithConfig> {
        MuxBuilder {
            state: WithConfig {
                config: MuxConfig {
                    stream_id_type: StreamIdType::Even,
                    keep_alive_interval: None,
                    idle_timeout: None,
                    max_tx_queue: NonZeroUsize::new(512).unwrap(),
                    max_rx_queue: NonZeroUsize::new(512).unwrap(),
                },
            },
        }
    }
}

impl MuxBuilder<WithConfig> {
    pub fn with_keep_alive_interval(&mut self, interval_secs: NonZeroU64) -> &mut Self {
        self.state.config.keep_alive_interval = Some(interval_secs);
        self
    }

    pub fn with_idle_timeout(&mut self, timeout_secs: NonZeroU64) -> &mut Self {
        self.state.config.idle_timeout = Some(timeout_secs);
        self
    }

    pub fn with_max_tx_queue(&mut self, size: NonZeroUsize) -> &mut Self {
        self.state.config.max_tx_queue = size;
        self
    }

    pub fn with_max_rx_queue(&mut self, size: NonZeroUsize) -> &mut Self {
        self.state.config.max_rx_queue = size;
        self
    }

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
    pub fn build(self) -> (MuxConnector<T>, MuxAcceptor<T>, MuxWorker<T>) {
        mux_connection(self.state.connection, self.state.config)
    }
}
