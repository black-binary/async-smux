use crate::frame;
use std::io;
use std::io::{Error, ErrorKind};

#[derive(Copy, Clone)]
pub struct MuxConfig {
    pub max_payload_length: usize,
    pub dispatcher_frame_buffer: usize,
    pub stream_frame_buffer: usize,
    pub stream_buffer: usize,
}

impl Default for MuxConfig {
    fn default() -> Self {
        Self {
            max_payload_length: 0xffff,
            dispatcher_frame_buffer: 0x500,
            stream_frame_buffer: 0x100,
            stream_buffer: 0x100,
        }
    }
}

impl MuxConfig {
    // Validate the config
    pub fn check(&self) -> io::Result<()> {
        if self.max_payload_length == 0 {
            return Err(Error::new(
                ErrorKind::InvalidData,
                "max_payload_length == 0",
            ));
        }
        if self.max_payload_length > frame::MAX_PAYLOAD_LENGTH {
            return Err(Error::new(
                ErrorKind::InvalidData,
                "max_frame_length > 0xffff",
            ));
        }
        Ok(())
    }
}
