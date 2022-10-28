use bytes::{Buf, BufMut, Bytes, BytesMut};
use tokio_util::codec::{Decoder, Encoder};

use std::io::Cursor;

use crate::error::{MuxError, MuxResult};

pub const SMUX_VERSION: u8 = 1;
pub const HEADER_SIZE: usize = 8;
pub const MAX_PAYLOAD_SIZE: usize = 0xffff;

#[derive(Eq, PartialEq, Debug, Clone, Copy)]
pub(crate) enum MuxCommand {
    Sync = 0,
    Finish = 1,
    Push = 2,
    Nop = 3,
}

impl TryFrom<u8> for MuxCommand {
    type Error = MuxError;

    fn try_from(value: u8) -> Result<Self, Self::Error> {
        match value {
            0 => Ok(MuxCommand::Sync),
            1 => Ok(MuxCommand::Finish),
            2 => Ok(MuxCommand::Push),
            3 => Ok(MuxCommand::Nop),
            _ => Err(MuxError::InvalidCommand(value)),
        }
    }
}

#[derive(Copy, Clone, Eq, PartialEq, Debug)]
pub(crate) struct MuxFrameHeader {
    pub version: u8,
    pub command: MuxCommand,
    pub length: u16,
    pub stream_id: u32,
}

impl MuxFrameHeader {
    #[inline]
    fn encode(&self, buf: &mut BytesMut) {
        buf.put_u8(self.version);
        buf.put_u8(self.command as u8);
        buf.put_u16(self.length);
        buf.put_u32(self.stream_id);
    }

    #[inline]
    fn decode(buf: &[u8]) -> MuxResult<Self> {
        let mut cursor = Cursor::new(buf);
        let version = cursor.get_u8();
        if version != SMUX_VERSION {
            return Err(MuxError::InvalidVersion(version));
        }
        let command = MuxCommand::try_from(cursor.get_u8())?;
        let length = cursor.get_u16();
        let stream_id = cursor.get_u32();
        Ok(Self {
            version,
            command,
            length,
            stream_id,
        })
    }
}

#[derive(Clone)]
pub(crate) struct MuxFrame {
    pub header: MuxFrameHeader,
    pub payload: Bytes,
}

impl MuxFrame {
    pub fn new(command: MuxCommand, stream_id: u32, payload: Bytes) -> Self {
        assert!(payload.len() <= MAX_PAYLOAD_SIZE);
        Self {
            header: MuxFrameHeader {
                version: SMUX_VERSION,
                command,
                length: payload.len() as u16,
                stream_id,
            },
            payload,
        }
    }
}

pub(crate) struct MuxCodec {}

impl Decoder for MuxCodec {
    type Item = MuxFrame;
    type Error = MuxError;

    fn decode(&mut self, src: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        if src.len() < HEADER_SIZE {
            return Ok(None);
        }
        let header = MuxFrameHeader::decode(src)?;
        let len = header.length as usize;
        if src.len() < HEADER_SIZE + len {
            return Ok(None);
        }
        src.advance(HEADER_SIZE);
        let payload = src.split_to(len).freeze();

        debug_assert!(payload.len() == len);
        let frame = MuxFrame { header, payload };

        Ok(Some(frame))
    }
}

impl Encoder<MuxFrame> for MuxCodec {
    type Error = MuxError;

    fn encode(&mut self, item: MuxFrame, dst: &mut BytesMut) -> Result<(), Self::Error> {
        if item.header.version != SMUX_VERSION {
            return Err(MuxError::InvalidVersion(item.header.version));
        }

        if item.payload.len() > MAX_PAYLOAD_SIZE {
            return Err(MuxError::PayloadTooLarge(item.payload.len()));
        }

        item.header.encode(dst);
        dst.put_slice(&item.payload);

        Ok(())
    }
}
