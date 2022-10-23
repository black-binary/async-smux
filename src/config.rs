use std::time::Duration;

/// `Mux` Configurations
#[derive(Copy, Clone)]
pub struct MuxConfig {
    pub idle_timeout: Option<Duration>,
    pub ping_interval: Option<Duration>,
}
