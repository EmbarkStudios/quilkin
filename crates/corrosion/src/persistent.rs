//! Implementation for a persistent connection between a client (agent) and
//! server (relay).

pub mod client;
mod error;
pub mod server;

use bytes::{BufMut, BytesMut};
pub use corro_api_types::ExecResult;
use quilkin_types::{Endpoint, IcaoCode, TokenSet};
use serde::{Deserialize, Serialize};

pub const MAGIC: [u8; 4] = 0xf0cacc1au32.to_ne_bytes();

#[derive(thiserror::Error, Debug)]
pub enum HandshakeError {
    #[error("handshake response from peer was invalid")]
    InvalidResponse,
    #[error("handshake response had an invalid magic number")]
    InvalidMagic,
    #[error("our version {} is not supported by the peer {}", ours, theirs)]
    UnsupportedVersion { ours: u16, theirs: u16 },
    #[error("expected length of {} but only received {}", expected, length)]
    InsufficientLength { length: usize, expected: usize },

    #[error(transparent)]
    InvalidIcao(#[from] quilkin_types::IcaoError),
}

#[inline]
fn write_magic_and_version(buf: &mut [u8], version: u16) {
    debug_assert!(buf.len() >= 6);
    buf[..4].copy_from_slice(&MAGIC);
    // Version comes after magic so that the server can determine how to
    // deserialize how to deserialize the rest of the handshake if it changes
    // in the future
    buf[4..6].copy_from_slice(&version.to_ne_bytes());
}

#[inline]
fn explicit_size<const N: usize>(buf: &[u8]) -> Result<[u8; N], HandshakeError> {
    if buf.len() < N {
        return Err(HandshakeError::InsufficientLength {
            length: buf.len(),
            expected: N,
        });
    }

    // For now we won't care about the length being larger than what we want
    let mut es = [0u8; N];
    es.copy_from_slice(&buf[..N]);
    Ok(es)
}

pub struct ClientHandshakeRequestV1 {
    pub qcmp_port: u16,
    pub icao: IcaoCode,
}

impl ClientHandshakeRequestV1 {
    #[inline]
    pub fn write(self) -> [u8; 12] {
        let mut req = [0u8; 12];
        write_magic_and_version(&mut req, 1);

        req[6..8].copy_from_slice(&self.qcmp_port.to_ne_bytes());
        req[8..12].copy_from_slice(self.icao.as_bytes());
        req
    }

    #[inline]
    pub fn read(buf: [u8; 6]) -> Result<Self, HandshakeError> {
        let qcmp_port = buf[0] as u16 | (buf[1] as u16) << 8;
        let icao = buf[2..].try_into()?;
        Ok(Self { qcmp_port, icao })
    }
}

pub enum ClientHandshake {
    V1(ClientHandshakeRequestV1),
}

impl ClientHandshake {
    pub fn read(server_version: u16, mut buf: &[u8]) -> Result<(u16, Self), HandshakeError> {
        if buf.len() < 4 || &buf[..4] != &MAGIC {
            return Err(HandshakeError::InvalidMagic);
        }

        let version = buf[4] as u16 | (buf[5] as u16) << 8;
        buf = &buf[6..];

        let this = match version {
            1 => {
                let fixed = explicit_size(buf)?;
                Self::V1(ClientHandshakeRequestV1::read(fixed)?)
            }
            theirs => {
                return Err(HandshakeError::UnsupportedVersion {
                    ours: server_version,
                    theirs,
                });
            }
        };

        Ok((version, this))
    }
    pub fn client_details(self) -> (u16, IcaoCode) {
        let Self::V1(req) = self;
        (req.qcmp_port, req.icao)
    }
}

pub struct ServerHandshakeResponseV1 {
    pub accept: bool,
}

impl ServerHandshakeResponseV1 {
    #[inline]
    pub fn write(self) -> [u8; 7] {
        let mut res = [0u8; 7];
        write_magic_and_version(&mut res, 1);
        res[6] = if self.accept { 1 } else { 0 };
        res
    }

    #[inline]
    pub fn read(buf: [u8; 1]) -> Result<Self, HandshakeError> {
        match buf[0] {
            0 => Ok(Self { accept: false }),
            1 => Ok(Self { accept: true }),
            _ => return Err(HandshakeError::InvalidResponse),
        }
    }
}

pub enum ServerHandshake {
    V1(ServerHandshakeResponseV1),
}

impl ServerHandshake {
    pub fn read(client_version: u16, mut buf: &[u8]) -> Result<Self, HandshakeError> {
        if buf.len() < 4 || &buf[..4] != &MAGIC {
            return Err(HandshakeError::InvalidMagic);
        }

        let version = buf[4] as u16 | (buf[5] as u16) << 8;
        buf = &buf[6..];

        match version {
            1 => {
                let fixed = explicit_size(buf)?;
                Ok(Self::V1(ServerHandshakeResponseV1::read(fixed)?))
            }
            theirs => Err(HandshakeError::UnsupportedVersion {
                ours: client_version,
                theirs,
            }),
        }
    }
}

#[inline]
fn update_length_prefix(buf: &mut bytes::BytesMut) {
    assert!(buf.len() - 2 <= u16::MAX as usize);

    let len = (buf.len() - 2) as u16;

    let len_slice = buf.get_mut(0..2).unwrap();
    len_slice[0] = len as u8;
    len_slice[1] = (len >> 8) as u8;
}

#[inline]
pub fn write_length_prefixed_jsonb<T: serde::Serialize>(
    item: &T,
) -> Result<BytesMut, serde_json::Error> {
    let mut buf = bytes::BytesMut::new();
    buf.put_u16(0);
    {
        let mut w = buf.writer();
        serde_json::to_writer(&mut w, item)?;
        buf = w.into_inner();
    }

    update_length_prefix(&mut buf);
    Ok(buf)
}

#[inline]
pub fn write_length_prefixed(bytes: &[u8]) -> BytesMut {
    let mut buf = bytes::BytesMut::with_capacity(bytes.len() + 2);
    // Reserve the length prefix
    buf.put_u16(0);
    buf.extend_from_slice(bytes);

    update_length_prefix(&mut buf);

    buf
}

#[derive(thiserror::Error, Debug)]
pub enum LengthReadError {
    #[error(transparent)]
    ReadExact(#[from] quinn::ReadExactError),
    #[error(transparent)]
    Read(#[from] quinn::ReadError),
    #[error("stream ended")]
    StreamEnded,
    #[error(
        "expected a chunk of JSON length {} but received {}",
        expected,
        received
    )]
    LengthMismatch { expected: usize, received: usize },
    #[error(transparent)]
    Json(#[from] serde_json::Error),
}

use error::ErrorCode as Ec;

impl<'s> From<&'s LengthReadError> for Ec {
    fn from(value: &'s LengthReadError) -> Self {
        match value {
            LengthReadError::Read(_)
            | LengthReadError::ReadExact(quinn::ReadExactError::ReadError(_))
            | LengthReadError::StreamEnded => Ec::ClientClosed,
            LengthReadError::ReadExact(quinn::ReadExactError::FinishedEarly(_)) => {
                Ec::LengthRequired
            }
            LengthReadError::LengthMismatch { expected, received } => {
                if received > expected {
                    Ec::PayloadTooLarge
                } else {
                    Ec::PayloadInsufficient
                }
            }
            LengthReadError::Json(_) => Ec::BadRequest,
        }
    }
}

#[inline]
pub async fn read_length_prefixed(
    recv: &mut quinn::RecvStream,
) -> Result<bytes::Bytes, LengthReadError> {
    let mut len = [0u8; 2];
    recv.read_exact(&mut len).await?;
    let len = u16::from_ne_bytes(len) as usize;

    let Some(chunk) = recv.read_chunk(len, true).await? else {
        return Err(LengthReadError::StreamEnded);
    };

    if chunk.bytes.len() != len {
        return Err(LengthReadError::LengthMismatch {
            expected: len,
            received: chunk.bytes.len(),
        });
    }

    Ok(chunk.bytes)
}

#[inline]
pub async fn read_length_prefixed_jsonb<T: serde::de::DeserializeOwned>(
    recv: &mut quinn::RecvStream,
) -> Result<T, LengthReadError> {
    let bytes = read_length_prefixed(recv).await?;
    Ok(serde_json::from_slice(&bytes)?)
}

#[derive(Deserialize, Serialize)]
pub struct ServerUpsert {
    #[serde(rename = "a")]
    pub endpoint: Endpoint,
    #[serde(rename = "i")]
    pub icao: IcaoCode,
    #[serde(rename = "t")]
    pub tokens: TokenSet,
}

#[derive(Deserialize, Serialize)]
pub struct ServerUpdate {
    #[serde(rename = "a")]
    pub endpoint: Endpoint,
    #[serde(rename = "i")]
    pub icao: Option<IcaoCode>,
    #[serde(rename = "t")]
    pub tokens: Option<TokenSet>,
}

#[derive(Deserialize, Serialize)]
#[serde(tag = "ty", content = "a")]
pub enum ServerChange {
    #[serde(rename = "i")]
    Insert(Vec<ServerUpsert>),
    #[serde(rename = "r")]
    Remove(Vec<Endpoint>),
    #[serde(rename = "u")]
    Update(Vec<ServerUpdate>),
}
