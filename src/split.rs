use crate::*;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt, ReadHalf, WriteHalf};

pub struct WebSocketRead<Stream> {
    pub(crate) stream: ReadHalf<Stream>,
    pub(crate) max_payload_len: usize,
    pub(crate) role: Role,
    pub(crate) is_closed: bool,
    pub(crate) fragment: Option<MessageType>,
}

pub struct WebSocketWrite<Stream> {
    pub(crate) stream: WriteHalf<Stream>,
    pub(crate) max_payload_len: usize,
    pub(crate) role: Role,
    pub(crate) is_closed: bool,
}

impl<R> WebSocketRead<R>
where
    R: AsyncRead + Unpin,
{
    pub async fn recv(&mut self) -> std::io::Result<Event> {
        if self.is_closed {
            return Err(std::io::Error::new(
                std::io::ErrorKind::NotConnected,
                "read after close",
            ));
        }
        let event = self.recv_event().await;
        if let Ok(Event::Close { .. } | Event::Error(..)) | Err(..) = event {
            self.is_closed = true;
        }
        event
    }

    pub async fn recv_event(&mut self) -> std::io::Result<Event> {
        let [b1, b2] = crate::ws::read_buf(&mut self.stream).await?;

        let fin = b1 & 0b_1000_0000 != 0;
        let rsv = b1 & 0b_111_0000;
        let opcode = b1 & 0b_1111;
        let len = (b2 & 0b_111_1111) as usize;
        let is_masked = b2 & 0b_1000_0000 != 0;

        if rsv != 0 {
            err!("reserve bit must be `0`");
        }

        match self.role {
            Role::Server => {
                if !is_masked {
                    err!("expected masked frame");
                }
            }
            Role::Client => {
                if is_masked {
                    err!("expected unmasked frame");
                }
            }
        }

        if opcode >= 8 {
            if !fin {
                err!("control frame must not be fragmented");
            }
            if len > 125 {
                err!("control frame must have a payload length of 125 bytes or less");
            }
            let msg = self.read_payload(len).await?;
            match opcode {
                8 => Ok(crate::ws::on_close(&msg)),
                9 => Ok(Event::Ping(msg)),
                10 => Ok(Event::Pong(msg)),
                _ => err!("unknown opcode"),
            }
        } else {
            let ty = match (opcode, fin, self.fragment) {
                (2, true, None) => DataType::Complete(MessageType::Binary),
                (1, true, None) => DataType::Complete(MessageType::Text),
                (2, false, None) => {
                    self.fragment = Some(MessageType::Binary);
                    DataType::Stream(Stream::Start(MessageType::Binary))
                }
                (1, false, None) => {
                    self.fragment = Some(MessageType::Text);
                    DataType::Stream(Stream::Start(MessageType::Text))
                }
                (0, false, Some(ty)) => DataType::Stream(Stream::Next(ty)),
                (0, true, Some(ty)) => {
                    self.fragment = None;
                    DataType::Stream(Stream::End(ty))
                }
                _ => err!("invalid data frame"),
            };

            let len = match len {
                126 => u16::from_be_bytes(crate::ws::read_buf(&mut self.stream).await?) as usize,
                127 => u64::from_be_bytes(crate::ws::read_buf(&mut self.stream).await?) as usize,
                len => len,
            };

            if len > self.max_payload_len {
                err!("payload too large");
            }
            let data = self.read_payload(len).await?;
            Ok(Event::Data { ty, data })
        }
    }

    async fn read_payload(&mut self, len: usize) -> std::io::Result<Box<[u8]>> {
        let mut data = vec![0; len].into_boxed_slice();
        match self.role {
            Role::Server => {
                let mask: [u8; 4] = crate::ws::read_buf(&mut self.stream).await?;
                self.stream.read_exact(&mut data).await?;
                for i in 0..data.len() {
                    data[i] ^= mask[i & 3];
                }
            }
            Role::Client => {
                self.stream.read_exact(&mut data).await?;
            }
        }
        Ok(data)
    }
}

impl<W> WebSocketWrite<W>
where
    W: AsyncWrite + Unpin,
{
    pub async fn send(&mut self, data: impl Into<Frame<'_>>) -> std::io::Result<()> {
        self.send_raw(data.into()).await
    }

    pub async fn close<T>(mut self, reason: T) -> std::io::Result<()>
    where
        T: CloseReason,
        T::Bytes: AsRef<[u8]>,
    {
        self.send_raw(Frame {
            fin: true,
            opcode: 8,
            data: reason.to_bytes().as_ref(),
        })
            .await?;
        self.stream.flush().await
    }

    pub async fn send_ping(&mut self, data: impl AsRef<[u8]>) -> std::io::Result<()> {
        self.send_raw(Frame {
            fin: true,
            opcode: 9,
            data: data.as_ref(),
        })
            .await
    }

    pub async fn send_pong(&mut self, data: impl AsRef<[u8]>) -> std::io::Result<()> {
        self.send_raw(Frame {
            fin: true,
            opcode: 10,
            data: data.as_ref(),
        })
            .await
    }

    pub async fn send_raw(&mut self, frame: Frame<'_>) -> std::io::Result<()> {
        let buf = match self.role {
            Role::Server => frame.encode_without_mask(),
            Role::Client => frame.encode_with(rand::random::<u32>().to_ne_bytes()),
        };
        self.stream.write_all(&buf).await
    }

    pub async fn flush(&mut self) -> std::io::Result<()> {
        self.stream.flush().await
    }
}