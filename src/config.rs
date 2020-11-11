use crate::frame;
use std::io;
use std::io::{Error, ErrorKind};

/// `Mux` Configurations
#[derive(Copy, Clone)]
pub struct MuxConfig {
    /// Maximum payload size of each smux frame
    pub max_payload_size: usize,

    /// The buffer size for reading frames
    pub frame_buffer_size: usize,

    /// The buffer size for accepting new `MuxStream`
    pub stream_buffer_size: usize,

    /// Trying to clean dropped `MuxStream` records every `clean_duration` seconds
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
                "max_payload_length == 0",
            ));
        }
        if self.max_payload_size > frame::MAX_PAYLOAD_SIZE {
            return Err(Error::new(
                ErrorKind::InvalidData,
                "max_frame_length > 0xffff",
            ));
        }
        Ok(())
    }
}
