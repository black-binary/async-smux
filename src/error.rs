use thiserror::Error;

use crate::config::StreamIdType;

#[derive(Debug, Error)]
pub enum MuxError {
    #[error("Invalid command {0}")]
    InvalidCommand(u8),
    #[error("Invalid version {0}")]
    InvalidVersion(u8),
    #[error("Payload too large {0}")]
    PayloadTooLarge(usize),
    #[error("Duplicated stream id {0}")]
    DuplicatedStreamId(u32),

    #[error("Invalid stream ID from peer: {0}, local stream ID type: {0:?}")]
    InvalidPeerStreamIdType(u32, StreamIdType),

    #[error("Too many streams")]
    TooManyStreams,

    #[error("Inner connection closed")]
    ConnectionClosed,
    #[error("Mux stream closed: {0:x}")]
    StreamClosed(u32),

    #[error("IO error: {0}")]
    IoError(#[from] std::io::Error),
}

pub type MuxResult<T> = Result<T, MuxError>;
