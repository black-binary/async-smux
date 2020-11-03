use std::{
    io,
    pin::Pin,
    task::Context,
    task::Poll::{self, Ready},
};

use bytes::{Buf, Bytes};
use futures::prelude::*;
use futures::ready;
use smol::channel::{Receiver, Sender};

use crate::error::{Error, Result};
use crate::frame::Frame;
/// A smux data stream.
///
/// A `MuxStream` is created by `MuxDispatcher`. It implements the AsyncRead and AsyncWrite traits.
pub struct MuxStream {
    pub(crate) stream_id: u32,
    pub(crate) rx: Receiver<Frame>,
    pub(crate) tx: Sender<Frame>,
    pub(crate) read_buf: Bytes,
    pub(crate) max_payload_length: usize,
    pub(crate) closed: bool,
}

impl MuxStream {
    pub fn get_stream_id(&self) -> u32 {
        self.stream_id
    }
}

impl Drop for MuxStream {
    fn drop(&mut self) {
        if !self.closed {
            let stream_id = self.stream_id;
            let frame = Frame::new_finish_frame(stream_id);
            let _ = smol::block_on(self.tx.send(frame));
            log::debug!("stream {:08X} droped", self.stream_id);
        }
        drop(self)
    }
}

impl AsyncRead for MuxStream {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut [u8],
    ) -> Poll<io::Result<usize>> {
        if self.read_buf.remaining() > 0 {
            let read_buf = &mut self.read_buf;
            if buf.len() >= read_buf.len() {
                let len = read_buf.len();
                buf[..len].copy_from_slice(&read_buf[..]);
                read_buf.clear();
                return Ready(Ok(len));
            } else {
                let buf_len = buf.len();
                buf[..].copy_from_slice(&read_buf[..buf_len]);
                read_buf.advance(buf_len);
                return Ready(Ok(buf_len));
            }
        }

        if let Some(frame) = ready!(self.rx.poll_next_unpin(cx)) {
            let mut payload = frame.into_payload();
            if buf.len() >= payload.len() {
                buf[..payload.len()].copy_from_slice(&payload);
                Ready(Ok(payload.len()))
            } else {
                let buf_len = buf.len();
                buf[..].copy_from_slice(&payload[..buf_len]);
                payload.advance(buf_len);
                self.read_buf = payload;
                Ready(Ok(buf_len))
            }
        } else {
            Ready(Err(Error::StreamClosed.into()))
        }
    }
}

impl MuxStream {
    #[inline]
    fn send_frame(&mut self, frame: Frame) -> Result<()> {
        smol::block_on(self.tx.send(frame)).map_err(|_| Error::DispatcherClosed)?;
        Ok(())
    }
}

impl AsyncWrite for MuxStream {
    fn poll_write(
        mut self: Pin<&mut Self>,
        _: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        let max_payload_length = self.max_payload_length;
        if buf.len() <= max_payload_length {
            let frame = Frame::new_push_frame(self.stream_id, buf);
            self.send_frame(frame)?;
        } else {
            let frame_count = buf.len() / max_payload_length + 1;
            for i in 0..frame_count - 1 {
                let frame = Frame::new_push_frame(
                    self.stream_id,
                    &buf[i * max_payload_length..(i + 1) * max_payload_length],
                );
                self.send_frame(frame)?;
            }
            let frame = Frame::new_push_frame(
                self.stream_id,
                &buf[(frame_count - 1) * max_payload_length..],
            );
            self.send_frame(frame)?;
        }
        Ready(Ok(buf.len()))
    }

    fn poll_flush(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<io::Result<()>> {
        Ready(Ok(()))
    }

    fn poll_close(mut self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<io::Result<()>> {
        let frame = Frame::new_finish_frame(self.stream_id);
        self.send_frame(frame)?;
        self.rx.close();
        self.closed = true;
        Ready(Ok(()))
    }
}
