use bytes::{Buf, BufMut, Bytes, BytesMut};
use futures::prelude::*;
use std::io::{Cursor, Error, ErrorKind, Result};

pub const SMUX_VERSION: u8 = 1;
pub const MAX_PAYLOAD_LENGTH: usize = 0xffff;

#[derive(Eq, PartialEq, Debug, Clone)]
pub enum Command {
    Sync,
    Finish,
    Push,
    Nop,
}

impl Command {
    #[inline]
    fn from_u8(val: u8) -> Result<Self> {
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

#[derive(Clone)]
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
    fn read_from_buf(buf: &[u8; 8]) -> Result<Self> {
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

#[derive(Clone)]
pub struct Frame {
    pub header: FrameHeader,
    packet: Bytes,
}

impl Frame {
    pub fn into_payload(mut self) -> Bytes {
        self.packet.advance(8);
        self.packet
    }

    pub async fn read_from<R: AsyncRead + Unpin>(reader: &mut R) -> Result<Self> {
        let mut header_buf = [0u8; 8];
        reader.read_exact(&mut header_buf[..]).await?;
        let header = FrameHeader::read_from_buf(&header_buf)?;
        let mut packet = BytesMut::with_capacity(8 + header.length);
        packet.extend_from_slice(&header_buf);
        if header.length != 0 {
            unsafe {
                packet.set_len(8 + header.length);
            }
            reader.read_exact(&mut packet[8..]).await?;
        }
        let packet = packet.freeze();
        Ok(Self { header, packet })
    }

    pub async fn write_to<W: AsyncWrite + Unpin>(&self, writer: &mut W) -> Result<()> {
        writer.write_all(&self.packet).await?;
        Ok(())
    }

    pub fn new_sync_frame(stream_id: u32) -> Self {
        let mut packet = BytesMut::with_capacity(8);
        let header = FrameHeader {
            version: SMUX_VERSION,
            command: Command::Sync,
            length: 0,
            stream_id: stream_id,
        };
        header.write_to(&mut packet);
        let packet = packet.freeze();
        Self { header, packet }
    }

    pub fn new_push_frame(stream_id: u32, payload: &[u8]) -> Self {
        assert!(payload.len() <= MAX_PAYLOAD_LENGTH);
        let mut packet = BytesMut::with_capacity(8 + payload.len());
        let header = FrameHeader {
            version: SMUX_VERSION,
            command: Command::Push,
            length: payload.len(),
            stream_id: stream_id,
        };
        header.write_to(&mut packet);
        packet.extend_from_slice(payload);
        let packet = packet.freeze();
        Self { header, packet }
    }

    pub fn new_finish_frame(stream_id: u32) -> Self {
        let mut packet = BytesMut::with_capacity(8);
        let header = FrameHeader {
            version: SMUX_VERSION,
            command: Command::Finish,
            length: 0,
            stream_id: stream_id,
        };
        header.write_to(&mut packet);
        let packet = packet.freeze();
        Self { header, packet }
    }
}

#[cfg(test)]
mod test {
    use super::{Command, Frame, FrameHeader, SMUX_VERSION};
    use bytes::BytesMut;
    use smol::io::Cursor;
    #[test]
    fn test_command() {
        assert_eq!(
            Command::from_u8(Command::Push.to_u8()).unwrap(),
            Command::Push,
        );
        assert_eq!(
            Command::from_u8(Command::Sync.to_u8()).unwrap(),
            Command::Sync,
        );
        assert_eq!(
            Command::from_u8(Command::Finish.to_u8()).unwrap(),
            Command::Finish,
        );
        assert_eq!(
            Command::from_u8(Command::Nop.to_u8()).unwrap(),
            Command::Nop,
        );
        assert!(Command::from_u8(100).is_err());
    }

    #[test]
    fn frame_parsing() {
        smol::block_on(async {
            let mut cursor = Cursor::new(Vec::<u8>::new());
            let payload = b"payload12345678";

            let header = FrameHeader {
                version: SMUX_VERSION,
                command: Command::Sync,
                length: payload.len(),
                stream_id: 1234,
            };
            let mut packet = BytesMut::new();
            header.write_to(&mut packet);
            packet.extend_from_slice(payload);

            let frame = Frame {
                header: header,
                packet: packet.freeze(),
            };

            frame.write_to(&mut cursor).await.unwrap();

            let mut cursor = Cursor::new(cursor.get_ref());
            let new_frame = Frame::read_from(&mut cursor).await.unwrap();
            assert_eq!(new_frame.header.version, frame.header.version);
            assert_eq!(new_frame.header.stream_id, frame.header.stream_id);
            assert_eq!(
                new_frame.header.command.to_u8(),
                frame.header.command.to_u8()
            );
            assert_eq!(new_frame.header.length, frame.header.length);
            assert_eq!(new_frame.into_payload(), frame.into_payload());

            let invalid_buf = [1u8, 4, 5, 6, 7, 1, 1, 1, 1, 1, 1, 1];
            let mut cursor = Cursor::new(&invalid_buf);
            assert!(Frame::read_from(&mut cursor).await.is_err());
            let invalid_buf = [3u8, 4, 8, 6, 7, 1, 1, 1, 1, 1, 11, 1, 1];
            let mut cursor = Cursor::new(&invalid_buf);
            assert!(Frame::read_from(&mut cursor).await.is_err());
            let invalid_buf = [0u8];
            let mut cursor = Cursor::new(&invalid_buf);
            assert!(Frame::read_from(&mut cursor).await.is_err());
        });
    }
}
