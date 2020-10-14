use bytes::{Buf, BufMut, Bytes, BytesMut};
use futures::prelude::*;
use std::io::{Cursor, Error, ErrorKind, Result};

#[derive(Eq, PartialEq, Debug)]
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

pub struct Frame {
    pub version: u8,
    pub command: Command,
    pub length: u16,
    pub stream_id: u32,
    pub payload: Bytes,
}

impl Frame {
    pub async fn read_from<R: AsyncRead + Unpin>(reader: &mut R) -> Result<Self> {
        let mut header_buf = [0u8; 8];
        reader.read_exact(&mut header_buf[..]).await?;
        let mut cursor = Cursor::new(&header_buf);
        let version = cursor.get_u8();
        if version != 1 {
            return Err(Error::new(ErrorKind::InvalidData, "invalid protocol"));
        }
        let command = Command::from_u8(cursor.get_u8())?;
        let length = cursor.get_u16();
        let stream_id = cursor.get_u32();
        let mut payload = BytesMut::with_capacity(length as usize);
        if length != 0 {
            payload.resize(length as usize, 0);
            reader.read_exact(&mut payload).await?;
        }
        let payload = payload.freeze();
        Ok(Self {
            version,
            command,
            length,
            stream_id,
            payload,
        })
    }

    pub async fn write_to<W: AsyncWrite + Unpin>(&self, writer: &mut W) -> Result<()> {
        let mut header_buf = BytesMut::with_capacity(8);
        header_buf.put_u8(self.version);
        header_buf.put_u8(self.command.to_u8());
        header_buf.put_u16(self.length);
        header_buf.put_u32(self.stream_id);
        writer.write_all(&header_buf).await?;
        if self.payload.len() != 0 {
            writer.write_all(&self.payload).await?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod test {
    use super::{Command, Frame};
    use bytes::Bytes;
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
    fn test_frame() {
        smol::block_on(async {
            let mut cursor = Cursor::new(Vec::<u8>::new());
            let payload = Bytes::from_static(b"payload12345678");

            let frame = Frame {
                version: 1,
                stream_id: 1234,
                command: Command::Sync,
                length: payload.len() as u16,
                payload: payload,
            };

            frame.write_to(&mut cursor).await.unwrap();

            let mut cursor = Cursor::new(cursor.get_ref());
            let new_frame = Frame::read_from(&mut cursor).await.unwrap();
            assert_eq!(new_frame.version, frame.version);
            assert_eq!(new_frame.stream_id, frame.stream_id);
            assert_eq!(new_frame.command.to_u8(), frame.command.to_u8());
            assert_eq!(new_frame.length, frame.length);
            assert_eq!(new_frame.payload.to_vec(), frame.payload.to_vec());

            let invalid_buf = [1u8, 4, 5, 6, 7, 1, 1, 1, 1, 1, 1, 1];
            let mut cursor = Cursor::new(&invalid_buf);
            assert!(Frame::read_from(&mut cursor).await.is_err());
            let invalid_buf = [3u8, 4, 8, 6, 7, 1, 1, 1, 1, 1, 11, 1, 1];
            let mut cursor = Cursor::new(&invalid_buf);
            assert!(Frame::read_from(&mut cursor).await.is_err());
        });
    }
}
