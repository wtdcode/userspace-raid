//! NBD protocol constants and struct definitions.
//!
//! See <https://github.com/NetworkBlockDevice/nbd/blob/master/doc/proto.md> for
//! the protocol description.
#![deny(missing_docs)]
#![allow(clippy::upper_case_acronyms)]
#![allow(non_camel_case_types)]
use bitflags::bitflags;
use color_eyre::eyre::{bail, ensure, WrapErr};
use color_eyre::Result;
use num_enum::{IntoPrimitive, TryFromPrimitive};
use rand::Rng;
use std::error::Error;
use std::fmt;
use std::io;
use std::io::ErrorKind;
use tokio::io::AsyncReadExt;
use tokio::io::AsyncWriteExt;
use tokio::io::{AsyncRead, AsyncWrite};
use tracing::{error, warn};

pub(crate) const TCP_PORT: u16 = 10809;

pub(crate) const MAGIC: u64 = 0x4e42444d41474943; // b"NBDMAGIC"
pub(crate) const IHAVEOPT: u64 = 0x49484156454F5054; // b"IHAVEOPT"
pub(crate) const REPLY_MAGIC: u64 = 0x3e889045565a9;

// transmission constants
pub(crate) const REQUEST_MAGIC: u32 = 0x25609513;
pub(crate) const SIMPLE_REPLY_MAGIC: u32 = 0x67446698;

#[derive(Debug, Clone)]
pub(crate) struct ProtocolError(String);

impl ProtocolError {
    pub fn new<S: AsRef<str>>(s: S) -> Self {
        ProtocolError(s.as_ref().to_string())
    }
}

impl fmt::Display for ProtocolError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "nbd protocol error: {}", self.0)?;
        Ok(())
    }
}

impl Error for ProtocolError {}

bitflags! {
#[derive(Clone, PartialEq, Eq, Debug, Copy)]
  pub(crate) struct HandshakeFlags: u16 {
    const FIXED_NEWSTYLE = 0b01;
    const NO_ZEROES = 0b10;
  }

  #[derive(Clone, PartialEq, Eq, Debug)]
  pub(crate) struct ClientHandshakeFlags: u32 {
    const C_FIXED_NEWSTYLE = 0b01;
    const C_NO_ZEROES = 0b10;
  }

  #[derive(Clone, PartialEq, Eq, Debug)]
  pub(crate) struct TransmitFlags: u16 {
    const HAS_FLAGS = 1 << 0;
    const READ_ONLY = 1 << 1;
    const SEND_FLUSH = 1 << 2;
    const SEND_FUA = 1 << 3;
    const ROTATIONAL = 1 << 4;
    const SEND_TRIM = 1 << 5;
    const SEND_WRITE_ZEROES = 1 << 6;
    const SEND_DF = 1 << 7;
    const CAN_MULTI_CONN = 1 << 8;
    const SEND_RESIZE = 1 << 9;
    const SEND_CACHE = 1 << 10;
    const SEND_FAST_ZERO = 1 << 11;
  }
}

#[derive(IntoPrimitive, TryFromPrimitive, Debug, Copy, Clone, PartialEq, Eq)]
#[repr(u32)]
pub(crate) enum OptType {
    EXPORT_NAME = 1,
    ABORT = 2,
    LIST = 3,
    PEEK_EXPORT = 4,
    STARTTLS = 5,
    INFO = 6,
    GO = 7,
}

#[derive(IntoPrimitive, TryFromPrimitive, Debug, Copy, Clone, PartialEq, Eq)]
#[repr(u16)]
pub(crate) enum InfoType {
    EXPORT = 0,
    NAME = 1,
    DESCRIPTION = 2,
    BLOCK_SIZE = 3,
}

#[derive(IntoPrimitive, TryFromPrimitive, Debug, Copy, Clone, PartialEq, Eq)]
#[repr(u32)]
pub(crate) enum ReplyType {
    ACK = 1,
    SERVER = 2,
    INFO = 3,
    ERR_UNSUP = (1 << 31) + 1,
    ERR_POLICY = (1 << 31) + 2,
    ERR_INVALID = (1 << 31) + 3,
    ERR_TLS_REQD = (1 << 31) + 5,
    ERR_UNKNOWN = (1 << 31) + 6,
    ERR_SHUTDOWN = (1 << 31) + 7,
    ERR_BLOCK_SIZE_REQD = (1 << 31) + 8,
    ERR_TOO_BIG = (1 << 31) + 9,
}

/// Builder for replying to an option
#[must_use]
pub(crate) struct OptReply {
    opt: OptType,
    reply_type: ReplyType,
    data: Vec<u8>,
}

impl OptReply {
    pub fn ack(opt: OptType) -> Self {
        Self {
            opt,
            reply_type: ReplyType::ACK,
            data: vec![],
        }
    }

    pub fn new(opt: OptType, reply_type: ReplyType, data: Vec<u8>) -> Self {
        Self {
            opt,
            reply_type,
            data,
        }
    }

    pub async fn put<IO: AsyncWrite + Unpin>(self, stream: &mut IO) -> io::Result<()> {
        //     The server will reply to any option apart from NBD_OPT_EXPORT_NAME with reply packets in the following format:
        //
        // S: 64 bits, 0x3e889045565a9 (magic number for replies)
        // S: 32 bits, the option as sent by the client to which this is a reply
        // S: 32 bits, reply type (e.g., NBD_REP_ACK for successful completion, or NBD_REP_ERR_UNSUP to mark use of an option not known by this server
        // S: 32 bits, length of the reply. This MAY be zero for some replies, in which case the next field is not sent
        // S: any data as required by the reply (e.g., an export name in the case of NBD_REP_SERVER)
        stream.write_u64(REPLY_MAGIC).await?;
        stream.write_u32(self.opt.into()).await?;
        stream.write_u32(self.reply_type.into()).await?;
        stream.write_u32(self.data.len() as u32).await?;
        stream.write_all(&self.data).await?;
        stream.flush().await?;
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Opt {
    pub typ: OptType,
    pub data: Vec<u8>,
}

impl Opt {
    pub async fn get<IO: AsyncRead + Unpin>(stream: &mut IO) -> Result<Self> {
        // C: 64 bits, 0x49484156454F5054 (ASCII 'IHAVEOPT') (note same newstyle handshake's magic number)
        // C: 32 bits, option
        // C: 32 bits, length of option data (unsigned)
        // C: any data needed for the chosen option, of length as specified above.
        let magic = stream.read_u64().await?;
        if magic != IHAVEOPT {
            bail!(ProtocolError(format!("unexpected option magic {magic}")));
        }
        let option = stream.read_u32().await?;
        let typ = OptType::try_from(option)
            .map_err(|_| ProtocolError(format!("unexpected option {option}")))?;
        let option_len = stream.read_u32().await?;
        ensure!(
            option_len < 10_000,
            ProtocolError(format!("option length {option_len} is too large"))
        );
        let mut data = vec![0u8; option_len as usize];
        stream
            .read_exact(&mut data)
            .await
            .wrap_err_with(|| format!("reading option {:?} of size {option_len}", typ))?;
        Ok(Self { typ, data })
    }

    pub async fn put<IO: AsyncWrite + Unpin>(self, stream: &mut IO) -> Result<()> {
        stream.write_u64(IHAVEOPT).await?;
        stream.write_u32(self.typ.into()).await?;
        stream.write_u32(self.data.len() as u32).await?;
        stream.write_all(&self.data).await?;
        Ok(())
    }
}

/// Builder for reply to a OptType::LIST option request
#[must_use]
pub(crate) struct ExportList {
    export_names: Vec<String>,
}

impl ExportList {
    pub fn new(export_names: Vec<String>) -> Self {
        Self { export_names }
    }
    pub async fn put<IO: AsyncWrite + Unpin>(self, stream: &mut IO) -> Result<()> {
        // Return zero or more NBD_REP_SERVER replies, one for each export,
        // followed by NBD_REP_ACK or an error (such as NBD_REP_ERR_SHUTDOWN).
        // The server MAY omit entries from this list if TLS has not been
        // negotiated, the server is operating in SELECTIVETLS mode, and the
        // entry concerned is a TLS-only export.
        for name in self.export_names.into_iter() {
            let mut data = vec![];
            data.write_u32(name.len() as u32).await?;
            data.write_all(name.as_bytes()).await?;
            OptReply::new(OptType::LIST, ReplyType::SERVER, data)
                .put(stream)
                .await?;
            OptReply::ack(OptType::LIST).put(stream).await?;
        }
        Ok(())
    }
}

#[derive(Debug, Clone)]
pub(crate) struct InfoRequest {
    // we just ignore the requested export names in general
    #[allow(dead_code)]
    pub name: String,
    pub typs: Vec<InfoType>,
}

impl InfoRequest {
    pub async fn get<IO: AsyncRead + Unpin>(stream: &mut IO) -> Result<Self> {
        let name_len = stream.read_u32().await?;
        let mut buf = vec![0; name_len as usize];
        stream.read_exact(&mut buf).await?;
        let name = String::from_utf8(buf)
            .wrap_err(ProtocolError::new("invalid UTF-8 in requested export"))?;
        let num_requests = stream.read_u16().await?;
        let mut typs = vec![];
        for _ in 0..num_requests {
            let typ = stream.read_u16().await?;
            let typ = InfoType::try_from(typ)
                .map_err(|_| ProtocolError::new("invalid info type {typ}"))?;
            typs.push(typ);
        }
        Ok(InfoRequest { name, typs })
    }
}

// -------------------
// Transmission phase
// -------------------

#[derive(IntoPrimitive, TryFromPrimitive, Debug, PartialEq, Eq, Copy, Clone)]
#[repr(u16)]
pub(crate) enum Cmd {
    READ = 0,
    WRITE = 1,
    // NBD_CMD_DISC
    DISCONNECT = 2,
    FLUSH = 3,
    TRIM = 4,
    CACHE = 5,
    WRITE_ZEROES = 6,
    BLOCK_STATUS = 7,
    RESIZE = 8,
}

bitflags! {
    #[derive(Clone, PartialEq, Eq, Debug)]
    pub(crate) struct CmdFlags: u16 {
        const FUA = 1 << 0;
        const NO_HOLE = 1 << 1;
        // "don't fragment"
        const DF = 1 << 2;
        const REQ_ONE = 1 << 3;
        const FAST_ZERO = 1 << 4;
    }
}

#[derive(Clone, PartialEq, Eq)]
pub(crate) struct Request {
    // parsed in case we need them later
    pub flags: CmdFlags,
    pub typ: Cmd,
    pub handle: u64,
    pub offset: u64,
    // used for READ (redundant for WRITE)
    pub len: u32,
    // actual data is stored into caller-provided buffer
    pub data_len: usize,
}

impl fmt::Debug for Request {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut f = &mut f.debug_struct("Request");
        if !self.flags.is_empty() {
            f = f.field("flags", &self.flags);
        }
        f = f.field("typ", &self.typ);
        // .field("handle", &self.handle);
        if self.typ == Cmd::READ
            || self.typ == Cmd::WRITE
            || self.typ == Cmd::TRIM
            || self.typ == Cmd::CACHE
        {
            f = f.field("offset", &self.offset);
        }
        if self.len != 0 {
            f = f.field("len", &self.len);
        }
        f.finish_non_exhaustive()
    }
}

impl Request {
    pub fn new(typ: Cmd, offset: u64, len: u32) -> Self {
        let handle = rand::thread_rng().gen::<u64>();
        let data_len = if typ == Cmd::WRITE { len as usize } else { 0 };
        Request {
            flags: CmdFlags::empty(),
            typ,
            handle,
            offset,
            len,
            data_len,
        }
    }

    /// Send this request.
    ///
    /// data (required only for a Cmd::WRITE) is not part of a Request and must
    /// be included separately.
    pub async fn put<IO: AsyncWrite + Unpin>(&self, data: &[u8], stream: &mut IO) -> Result<()> {
        assert!(
            self.data_len <= data.len(),
            "not enough data passed for request {} > {}",
            self.data_len,
            data.len(),
        );
        stream.write_u32(REQUEST_MAGIC).await?;
        stream.write_u16(self.flags.bits()).await?;
        stream.write_u16(self.typ.into()).await?;
        stream.write_u64(self.handle).await?;
        stream.write_u64(self.offset).await?;
        stream.write_u32(self.len).await?;
        stream.write_all(&data[..self.data_len]).await?;
        Ok(())
    }

    /// Get reads the next request, storing the data for a write request in buf.
    pub async fn get<IO: AsyncRead + Unpin>(stream: &mut IO, buf: &mut [u8]) -> Result<Self> {
        // C: 32 bits, 0x25609513, magic (NBD_REQUEST_MAGIC)
        // C: 16 bits, command flags
        // C: 16 bits, type
        // C: 64 bits, handle
        // C: 64 bits, offset (unsigned)
        // C: 32 bits, length (unsigned)
        // C: (length bytes of data if the request is of type NBD_CMD_WRITE)
        let magic = stream.read_u32().await?;
        if magic != REQUEST_MAGIC {
            bail!(ProtocolError(format!("wrong request magic {}", magic)));
        }
        let flags = stream.read_u16().await?;
        let flags = CmdFlags::from_bits(flags)
            .ok_or_else(|| ProtocolError(format!("unexpected command flags {}", flags)))?;
        let typ = stream.read_u16().await?;
        let typ =
            Cmd::try_from(typ).map_err(|_| ProtocolError(format!("unexpected command {}", typ)))?;
        let handle = stream.read_u64().await?;
        let offset = stream.read_u64().await?;
        let len = stream.read_u32().await?;
        let data_len;
        if typ == Cmd::WRITE {
            data_len = (len as usize).min(buf.len());
            stream
                .read_exact(&mut buf[..data_len])
                .await
                .wrap_err_with(|| format!("parsing write request of length {data_len}"))?;
        } else {
            data_len = 0;
        };
        Ok(Self {
            flags,
            typ,
            handle,
            offset,
            len,
            data_len,
        })
    }
}

#[derive(IntoPrimitive, TryFromPrimitive, Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u32)]
pub(crate) enum ErrorType {
    OK = 0,
    EPERM = 1,
    EIO = 5,
    ENOMEM = 12,
    EINVAL = 22,
    ENOSPC = 28,
    EOVERFLOW = 75,
    ENOTSUP = 95,
    ESHUTDOWN = 108,
}

impl ErrorType {
    pub fn from_io_kind(kind: io::ErrorKind) -> Self {
        match kind {
            ErrorKind::PermissionDenied => Self::EPERM,
            ErrorKind::InvalidInput => Self::EOVERFLOW,
            ErrorKind::UnexpectedEof => Self::EOVERFLOW,
            _ => {
                warn!("unexpected error {}", kind);
                Self::EIO
            }
        }
    }
}

#[derive(Debug)]
#[must_use]
pub(crate) struct SimpleReply<'a> {
    pub err: ErrorType,
    pub handle: u64,
    pub data: &'a [u8],
}

impl<'a> SimpleReply<'a> {
    pub fn data(req: &Request, data: &'a [u8]) -> Self {
        SimpleReply {
            err: ErrorType::OK,
            handle: req.handle,
            data,
        }
    }

    pub fn ok(req: &Request) -> Self {
        Self::data(req, &[])
    }

    pub fn err(err: ErrorType, req: &Request) -> Self {
        SimpleReply {
            err,
            handle: req.handle,
            data: &[],
        }
    }

    pub async fn get<IO: AsyncRead + Unpin>(stream: &mut IO, buf: &'a mut [u8]) -> Result<Self> {
        let mut magic_buf = [0u8; 4];
        let n = stream.read(&mut magic_buf).await?;
        if n == 0 {
            error!("socket is closed for reading");
        }
        let magic = u32::from_be_bytes(magic_buf);
        if magic != SIMPLE_REPLY_MAGIC {
            bail!(ProtocolError::new(format!("wrong reply magic {magic}")));
        }
        let err = stream.read_u32().await?;
        let err = ErrorType::try_from(err)
            .map_err(|_| ProtocolError::new(format!("invalid error type {err}")))?;
        let handle = stream.read_u64().await?;
        stream.read_exact(buf).await?;
        Ok(Self {
            err,
            handle,
            data: buf,
        })
    }

    pub async fn put<IO: AsyncWrite + Unpin>(self, stream: &mut IO) -> Result<()> {
        // The simple reply message MUST be sent by the server in response to all requests if structured replies have not been negotiated using NBD_OPT_STRUCTURED_REPLY. If structured replies have been negotiated, a simple reply MAY be used as a reply to any request other than NBD_CMD_READ, but only if the reply has no data payload. The message looks as follows:
        //
        // S: 32 bits, 0x67446698, magic (NBD_SIMPLE_REPLY_MAGIC; used to be NBD_REPLY_MAGIC)
        // S: 32 bits, error (MAY be zero)
        // S: 64 bits, handle
        // S: (length bytes of data if the request is of type NBD_CMD_READ and error is zero)
        stream.write_u32(SIMPLE_REPLY_MAGIC).await?;
        stream.write_u32(self.err.into()).await?;
        stream.write_u64(self.handle).await?;
        stream.write_all(self.data).await?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_opt_get_put() -> Result<()> {
        let opt = Opt {
            typ: OptType::INFO,
            data: vec![2, 3, 4, 5],
        };
        let mut buf = vec![];
        opt.clone().put(&mut buf).await?;
        assert_eq!(Opt::get(&mut &buf[..]).await?, opt);
        Ok(())
    }

    #[tokio::test]
    async fn test_request_get_put_read() -> Result<()> {
        let req = Request {
            flags: CmdFlags::empty(),
            typ: Cmd::READ,
            handle: 1234,
            offset: 5123,
            len: 698123,
            data_len: 0,
        };
        let mut buf = vec![];
        req.put(&[], &mut buf).await?;
        assert_eq!(Request::get(&mut &buf[..], &mut []).await?, req);
        Ok(())
    }

    #[tokio::test]
    async fn test_request_get_put_write() -> Result<()> {
        let req = Request {
            flags: CmdFlags::FUA,
            typ: Cmd::WRITE,
            handle: 1234,
            offset: 5123,
            len: 12,
            data_len: 12,
        };
        let data = vec![1; 12];
        let mut buf = vec![];
        req.put(&data, &mut buf).await?;
        let mut data_read = vec![0; 12];
        assert_eq!(Request::get(&mut &buf[..], &mut data_read).await?, req);
        assert_eq!(data, data_read);
        Ok(())
    }
}
