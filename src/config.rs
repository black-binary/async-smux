use crate::frame;
use std::io;
use std::io::{Error, ErrorKind};

/// `Mux` Configurations
#[derive(Copy, Clone)]
pub struct MuxConfig {
    /// Maximum payload size of each smux frame.
    pub max_payload_size: usize,

    /// The buffer size for reading frames.
    pub frame_buffer_size: usize,

    /// The buffer size for accepting new `MuxStream`s.
    pub stream_buffer_size: usize,

    /// Trying to clean dropped `MuxStream` records every `clean_duration` seconds.
    pub clean_duration: u64,
}

impl Default for MuxConfig {
    fn default() -> Self {
        Self {
            max_payload_size: 0xffff,
            frame_buffer_size: 0x100,
            stream_buffer_size: 0x100,
            clean_duration: 30,
        }
    }
}

impl MuxConfig {
    /// Validates the config
    pub fn check(&self) -> io::Result<()> {
        if self.max_payload_size == 0 {
            return Err(Error::new(
                ErrorKind::InvalidData,
                "max_payload_size must be positive",
            ));
        }
        if self.max_payload_size > frame::MAX_PAYLOAD_SIZE {
            return Err(Error::new(
                ErrorKind::InvalidData,
                "max_payload_size must not be larger than 65535",
            ));
        }
        if self.frame_buffer_size == 0 {
            return Err(Error::new(
                ErrorKind::InvalidData,
                "frame_buffer_size must be positive",
            ));
        }
        if self.stream_buffer_size == 0 {
            return Err(Error::new(
                ErrorKind::InvalidData,
                "frame_buffer_size must be positive",
            ));
        }
        Ok(())
    }
}
