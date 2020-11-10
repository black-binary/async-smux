use crate::frame;
use std::io;
use std::io::{Error, ErrorKind};

#[derive(Copy, Clone)]
pub struct MuxConfig {
    pub max_payload_size: usize,
    pub frame_buffer_size: usize,
    pub stream_buffer_size: usize,
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
    /// Validate the config
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
