use bytes::{Buf, BufMut, Bytes, BytesMut};
use futures::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};

use std::io::{self, Cursor, Error, ErrorKind};

use crate::Result;

pub const SMUX_VERSION: u8 = 1;
pub const HEADER_SIZE: usize = 8;
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
            _ => Err(Error::new(
                ErrorKind::InvalidData,
                format!("invalid command {:x}", val),
            )),
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
    fn encode(&self) -> Bytes {
        let mut buf = BytesMut::new();
        buf.resize(8, 0);
        self.write_to_buf(&mut buf);
        buf.freeze()
    }

    #[inline]
    fn write_to_buf(&self, header_buf: &mut [u8]) {
        let mut buf = header_buf;
        buf.put_u8(self.version);
        buf.put_u8(self.command.to_u8());
        buf.put_u16(self.length as u16);
        buf.put_u32(self.stream_id);
    }

    #[inline]
    async fn read_from<R: AsyncRead + Unpin>(reader: &mut R) -> io::Result<Self> {
        let mut buf = [0u8; HEADER_SIZE];
        reader.read_exact(&mut buf).await?;
        Self::read_from_buf(&buf)
    }

    #[inline]
    pub fn read_from_buf(buf: &[u8; 8]) -> io::Result<Self> {
        let mut cursor = Cursor::new(buf);
        let version = cursor.get_u8();
        if version != SMUX_VERSION {
            return Err(Error::new(
                ErrorKind::InvalidData,
                format!("invalid smux protocl version {:x}, {:?}", version, &buf[..]),
            ));
        }
        let command = Command::from_u8(cursor.get_u8())?;
        let length = cursor.get_u16() as usize;
        let stream_id = cursor.get_u32();
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
    pub packet: Bytes,
}

impl Frame {
    pub fn get_payload(&self) -> Bytes {
        let mut payload = self.packet.clone();
        payload.advance(8);
        payload
    }

    async fn read_from<R: AsyncRead + Unpin>(reader: &mut R) -> Result<Self> {
        let header = FrameHeader::read_from(reader).await?;
        let mut packet = BytesMut::with_capacity(HEADER_SIZE + header.length);
        unsafe {
            packet.set_len(HEADER_SIZE + header.length);
        }
        header.write_to_buf(&mut packet[..HEADER_SIZE]);
        if header.length != 0 {
            reader.read_exact(&mut packet[HEADER_SIZE..]).await?;
        }
        let packet = packet.freeze();
        Ok(Self { header, packet })
    }

    async fn write_to<W: AsyncWrite + Unpin>(&self, writer: &mut W) -> Result<()> {
        debug_assert_eq!(self.packet.len(), self.header.length + HEADER_SIZE);
        writer.write_all(&self.packet).await?;
        Ok(())
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
            packet: header.encode(),
        }
    }

    pub fn new_push_frame(stream_id: u32, mut payload: &[u8]) -> Self {
        let len = payload.len();
        assert!(len <= MAX_PAYLOAD_SIZE);
        let header = FrameHeader {
            version: SMUX_VERSION,
            command: Command::Push,
            length: len,
            stream_id: stream_id,
        };
        let mut packet = BytesMut::with_capacity(HEADER_SIZE + header.length);
        unsafe {
            packet.set_len(HEADER_SIZE + header.length);
        }
        header.write_to_buf(&mut packet[..HEADER_SIZE]);
        payload.copy_to_slice(&mut packet[HEADER_SIZE..]);
        let packet = packet.freeze();
        Self { header, packet }
    }

    pub fn new_finish_frame(stream_id: u32) -> Self {
        let header = FrameHeader {
            version: SMUX_VERSION,
            command: Command::Finish,
            length: 0,
            stream_id: stream_id,
        };
        let packet = header.encode();
        Self { header, packet }
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
