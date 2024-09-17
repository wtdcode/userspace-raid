//! Basic NBD client that works with this crate's server.
//!
//! See the documentation for [`Client`].
#![deny(missing_docs)]

use color_eyre::eyre::bail;
use color_eyre::Result;
use tokio::{
    io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt},
    net::TcpStream,
    runtime::Handle,
    sync::{Mutex, RwLock},
};

use std::{
    borrow::BorrowMut,
    io::prelude::*,
    os::{
        fd::AsRawFd,
        unix::io::{IntoRawFd, RawFd},
    },
    sync::Arc,
};

use byteorder::{ReadBytesExt, WriteBytesExt, BE};

use crate::nbd::proto::*;

#[derive(Debug)]
struct Export {
    size: u64,
}

/// Client provides an interface to an export from a remote NBD server.
#[derive(Debug)]
pub struct Client<IO> {
    conn: RwLock<IO>,
    export: Export,
}

impl<IO: AsyncRead + AsyncWrite + Unpin> Client<IO> {
    async fn initial_handshake(stream: &mut (impl AsyncRead + AsyncWrite + Unpin)) -> Result<()> {
        let magic = stream.read_u64().await?;
        if magic != MAGIC {
            bail!(ProtocolError::new(format!("unexpected magic {}", magic)));
        }
        let opt_magic = stream.read_u64().await?;
        if opt_magic != IHAVEOPT {
            bail!(ProtocolError::new(format!(
                "unexpected IHAVEOPT value {opt_magic}",
            )))
        }
        let server_flags = stream.read_u16().await?;
        let server_flags = HandshakeFlags::from_bits(server_flags)
            .ok_or_else(|| ProtocolError::new(format!("unexpected server flags {server_flags}")))?;
        if !server_flags.contains(HandshakeFlags::FIXED_NEWSTYLE | HandshakeFlags::NO_ZEROES) {
            bail!(ProtocolError::new("server does not support NO_ZEROES"));
        }
        let client_flags =
            ClientHandshakeFlags::C_FIXED_NEWSTYLE | ClientHandshakeFlags::C_NO_ZEROES;
        stream.write_u32(client_flags.bits()).await?;
        Ok(())
    }

    async fn get_export_info(
        stream: &mut (impl AsyncRead + Unpin),
    ) -> Result<(Export, TransmitFlags)> {
        let size = stream.read_u64().await?;
        let transmit_flags = stream.read_u16().await?;
        let transmit_flags = TransmitFlags::from_bits(transmit_flags)
            .ok_or_else(|| ProtocolError::new("invalid transmit flags {transmit_flags}"))?;
        let export = Export { size };
        Ok((export, transmit_flags))
    }

    async fn handshake_haggle(
        stream: &mut (impl AsyncRead + AsyncWrite + Unpin),
    ) -> Result<Export> {
        Opt {
            typ: OptType::EXPORT_NAME,
            data: b"default".to_vec(),
        }
        .put(stream)
        .await?;
        // ignore transmit flags for now (we don't send anything fancy anyway)
        let (export, _transmit_flags) = Self::get_export_info(stream).await?;
        Ok(export)
    }

    /// Establish a handshake with stream and return a `Client` ready for use.
    pub async fn new(mut stream: IO) -> Result<Self> {
        Self::initial_handshake(&mut stream).await?;
        let export = Self::handshake_haggle(&mut stream).await?;
        Ok(Self {
            conn: RwLock::new(stream),
            export,
        })
    }

    /// Return the size of this export, as reported by the server during the
    /// handshake.
    pub fn size(&self) -> u64 {
        self.export.size
    }

    async fn get_reply_data(&self, req: &Request, buf: &mut [u8], t: &mut IO) -> Result<()> {
        let reply = SimpleReply::get(t, buf).await?;
        if reply.handle != req.handle {
            bail!(format!(
                "reply for wrong handle {} != {}",
                reply.handle, req.handle
            ))
        }
        if reply.err != ErrorType::OK {
            bail!(format!("{:?} failed: {:?}", req.typ, reply.err))
        }
        Ok(())
    }

    async fn get_ack(&self, req: &Request, t: &mut IO) -> Result<()> {
        self.get_reply_data(req, &mut [], t).await
    }

    /// Send a read command to the NBD server.
    pub async fn read(&self, offset: u64, len: u32) -> Result<Vec<u8>> {
        let mut t = self.conn.write().await;
        let req = Request::new(Cmd::READ, offset, len);
        req.put(&[], &mut *t).await?;
        let mut buf = vec![0; len as usize];
        self.get_reply_data(&req, &mut buf, &mut *t).await?;
        Ok(buf)
    }

    /// Send a read command to the NBD server given a buf
    pub async fn read_at(&self, offset: u64, buf: &mut [u8]) -> Result<()> {
        let mut t = self.conn.write().await;
        let req = Request::new(Cmd::READ, offset, buf.len() as u32);
        req.put(&[], &mut *t).await?;
        self.get_reply_data(&req, buf, &mut *t).await?;
        Ok(())
    }

    /// Send a write command to the NBD server.
    pub async fn write(&self, offset: u64, data: &[u8]) -> Result<()> {
        let mut t = self.conn.write().await;
        let req = Request::new(Cmd::WRITE, offset, data.len() as u32);
        req.put(data, &mut *t).await?;
        self.get_ack(&req, &mut *t).await?;
        Ok(())
    }

    /// Send a flush command to the NBD server.
    pub async fn flush(&self) -> Result<()> {
        let mut t = self.conn.write().await;
        let req = Request::new(Cmd::FLUSH, 0, 0);
        req.put(&[], &mut *t).await?;
        self.get_ack(&req, &mut *t).await?;
        Ok(())
    }

    /// Disconnect from server cleanly and consume this client.
    pub async fn disconnect(mut self) -> Result<()> {
        Request::new(Cmd::DISCONNECT, 0, 0)
            .put(&[], self.conn.get_mut())
            .await?;
        Ok(())
    }
}

impl Client<TcpStream> {
    /// Connect to a server, run handshake, and return a `Client` prepared for
    /// the transmission phase.
    pub async fn connect(host: &str) -> Result<Self> {
        let stream = TcpStream::connect((host, TCP_PORT)).await?;
        Self::new(stream).await
    }
}

impl<IO: AsRawFd> AsRawFd for Client<IO> {
    fn as_raw_fd(&self) -> RawFd {
        let handle = Handle::current();
        tokio::task::block_in_place(move || {
            handle.block_on(async move { self.conn.read().await.as_raw_fd() })
        })
    }
}
