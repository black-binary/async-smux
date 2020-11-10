use bytes::{Buf, BufMut, Bytes, BytesMut};
use futures::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};

use std::io::{self, Cursor, Error, ErrorKind};

use crate::Result;

pub const SMUX_VERSION: u8 = 1;
pub const HEADER_LENGTH: usize = 8;
pub const MAX_PAYLOAD_SIZE: usize = 0xffff;

#[derive(Eq, PartialEq, Debug, Clone, Copy)]
pub enum Command {
    Sync,
    Finish,
    Push,
    Nop,
}

impl Command {
    #[inline]
    fn from_u8(val: u8) -> io::Result<Self> {
        match val {
            0 => Ok(Command::Sync),
            1 => Ok(Command::Finish),
            2 => Ok(Command::Push),
            3 => Ok(Command::Nop),
            _ => Err(Error::new(ErrorKind::InvalidData, "invalid command")),
        }
    }

    #[inline]
    fn to_u8(&self) -> u8 {
        match self {
            Command::Sync => 0,
            Command::Finish => 1,
            Command::Push => 2,
            Command::Nop => 3,
        }
    }
}

#[derive(Copy, Clone, Eq, PartialEq, Debug)]
pub struct FrameHeader {
    pub version: u8,
    pub command: Command,
    pub length: usize,
    pub stream_id: u32,
}

impl FrameHeader {
    #[inline]
    fn write_to<B: BufMut>(&self, buf: &mut B) {
        buf.put_u8(self.version);
        buf.put_u8(self.command.to_u8());
        buf.put_u16(self.length as u16);
        buf.put_u32(self.stream_id);
    }

    #[inline]
    async fn read_from<R: AsyncRead + Unpin>(reader: &mut R) -> io::Result<Self> {
        let mut buf = [0u8; HEADER_LENGTH];
        reader.read_exact(&mut buf).await?;
        Self::read_from_buf(&buf)
    }

    #[inline]
    pub fn read_from_buf(buf: &[u8; 8]) -> io::Result<Self> {
        let mut buf = Cursor::new(buf);
        let version = buf.get_u8();
        if version != SMUX_VERSION {
            return Err(Error::new(ErrorKind::InvalidData, "invalid protocol"));
        }
        let command = Command::from_u8(buf.get_u8())?;
        let length = buf.get_u16() as usize;
        let stream_id = buf.get_u32();
        Ok(Self {
            version,
            command,
            length,
            stream_id,
        })
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Frame {
    pub header: FrameHeader,
    pub payload: Bytes,
}

impl Frame {
    async fn read_from<R: AsyncRead + Unpin>(reader: &mut R) -> Result<Self> {
        let header = FrameHeader::read_from(reader).await?;
        let mut payload = BytesMut::new();
        payload.resize(header.length, 0);
        reader.read_exact(&mut payload).await?;
        let payload = payload.freeze();
        Ok(Self { header, payload })
    }

    async fn write_to<W: AsyncWrite + Unpin>(&self, writer: &mut W) -> Result<()> {
        let packet = self.encode();
        writer.write_all(&packet).await?;
        Ok(())
    }

    pub fn encode(&self) -> Bytes {
        let mut buf = BytesMut::with_capacity(HEADER_LENGTH + self.header.length);
        self.header.write_to(&mut buf);
        buf.extend_from_slice(&self.payload);
        buf.freeze()
    }

    pub fn new_sync_frame(stream_id: u32) -> Self {
        let header = FrameHeader {
            version: SMUX_VERSION,
            command: Command::Sync,
            length: 0,
            stream_id: stream_id,
        };
        Self {
            header,
            payload: Bytes::new(),
        }
    }

    pub fn new_push_frame(stream_id: u32, payload: Bytes) -> Self {
        assert!(payload.len() <= MAX_PAYLOAD_SIZE);
        let header = FrameHeader {
            version: SMUX_VERSION,
            command: Command::Push,
            length: payload.len(),
            stream_id: stream_id,
        };
        Self { header, payload }
    }

    pub fn new_finish_frame(stream_id: u32) -> Self {
        let header = FrameHeader {
            version: SMUX_VERSION,
            command: Command::Finish,
            length: 0,
            stream_id: stream_id,
        };
        Self {
            header,
            payload: Bytes::new(),
        }
    }
}

pub struct FrameIo<T> {
    inner: T,
}

impl<T: AsyncRead + Unpin> FrameIo<T> {
    pub async fn read_frame(&mut self) -> Result<Frame> {
        let frame = Frame::read_from(&mut self.inner).await?;
        Ok(frame)
    }
}

impl<T: AsyncWrite + Unpin> FrameIo<T> {
    pub async fn write_frame(&mut self, frame: &Frame) -> Result<()> {
        frame.write_to(&mut self.inner).await?;
        Ok(())
    }
}

impl<T> FrameIo<T> {
    pub fn new(inner: T) -> Self {
        Self { inner }
    }
}
