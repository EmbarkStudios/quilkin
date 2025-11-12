use crate::codec;
use bytes::BufMut;
use quilkin_types::{Endpoint, IcaoCode};
use serde::{Deserialize, Serialize};

#[derive(thiserror::Error, Debug)]
pub enum Error {
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
    #[error(transparent)]
    Json(#[from] serde_json::Error),
}

pub const MAGIC: [u8; 4] = 0xf0cacc1au32.to_ne_bytes();
pub const VERSION: u16 = 1;

pub trait VersionedRequest: Serialize {
    const VERSION: u16;

    fn write(&self) -> Result<bytes::Bytes, serde_json::Error> {
        let mut buf = bytes::BytesMut::with_capacity(128);

        codec::reserve_length_prefix(&mut buf);
        buf.extend_from_slice(&MAGIC);
        buf.extend_from_slice(&Self::VERSION.to_le_bytes());

        {
            let mut w = buf.writer();
            serde_json::to_writer(&mut w, self)?;
            buf = w.into_inner();
        }

        codec::update_length_prefix(&mut buf)
            .expect("unreachable, unless self is ridiculously huge");

        Ok(buf.freeze())
    }
}

pub struct VersionedBuf<'buf> {
    pub version: u16,
    pub buf: &'buf [u8],
}

impl<'buf> VersionedBuf<'buf> {
    #[inline]
    pub fn try_parse(buf: &'buf [u8]) -> Result<Self, Error> {
        if buf.len() <= 6 {
            return Err(Error::InsufficientLength {
                length: req.len(),
                expected: 6,
            });
        }

        if buf[..4] != &MAGIC {
            return Err(Error::InvalidMagic);
        }

        let version = buf[4] as u16 | (buf[5] as u16) << 8;

        Ok(Self {
            version,
            buf: &buf[6..],
        })
    }

    #[inline]
    pub fn deserialize_request(self) -> Result<Request, Error> {
        match self.version {
            1 => {
                let req = serde_json::from_slice::<v1::Request>(self.request)?;
                Ok(Request::V1(req))
            }
            theirs => Err(Error::UnsupportedVersion {
                ours: VERSION,
                theirs,
            }),
        }
    }

    #[inline]
    pub fn deserialize_response(self) -> Result<Response, Error> {
        match self.version {
            1 => {
                let res = serde_json::from_slice::<v1::Response>(self.request)?;
                Ok(Response::V1(res))
            }
            theirs => Err(Error::UnsupportedVersion {
                ours: VERSION,
                theirs,
            }),
        }
    }
}

pub enum Request {
    V1(v1::Request),
}

#[derive(Serialize, Deserialize)]
#[serde(tag = "t", content = "r")]
pub enum Response {
    V1(v1::Response),
}

/// The contents of a response
#[derive(Serialize, Deserialize)]
#[serde(tag = "t", content = "r")]
pub enum ResponseResult<T> {
    #[serde(rename = "o")]
    Ok(T),
    #[serde(rename = "e")]
    Err {
        #[serde(rename = "c")]
        code: u16,
        #[serde(rename = "m")]
        message: String,
    },
}

/// Requests and responses for version 1 of persistent streams
pub mod v1 {
    use super::*;

    /// A request to open a persistent connection that can mutate the database
    #[derive(Serialize, Deserialize)]
    pub struct MutateRequest {
        /// The QCMP port on the server that can be pinged for latency information
        #[serde(rename = "q")]
        pub qcmp_port: u16,
        /// The ICAO of the server
        #[serde(rename = "i")]
        pub icao: IcaoCode,
    }

    /// A request to subscribe to a database query, receiving a stream of mutations
    #[derive(Serialize, Deserialize)]
    #[serde(transparent)]
    pub struct SubscribeRequest(pub crate::pubsub::SubParamsv1);

    #[derive(Serialize, Deserialize)]
    #[serde(tag = "t", content = "r")]
    pub enum Request {
        #[serde(rename = "m")]
        Mutate(MutateRequest),
        #[serde(rename = "s")]
        Subscribe(SubscribeRequest),
    }

    impl VersionedRequest for Request {
        const VERSION: u16 = 1;
    }

    /// Response to a [`MutateRequest`]
    #[derive(Serialize, Deserialize)]
    pub struct MutateResponse;

    /// Response to a [`SubscribeRequest`]
    #[derive(Serialize, Deserialize)]
    pub struct SubscribeResponse {
        #[serde(rename = "i")]
        pub id: Uuid,
        #[serde(rename = "q")]
        pub query_hash: String,
    }

    /// A response to a [`Request`]
    #[derive(Serialize, Deserialize)]
    #[serde(tag = "t", content = "r")]
    pub enum OkResponse {
        #[serde(rename = "m")]
        Mutate(MutateResponse),
        #[serde(rename = "s")]
        Subscribe(SubscribeResponse),
    }

    pub type Response = ResponseResult<OkResponse>;

    /// A DB mutation request to upsert a server
    #[derive(Deserialize, Serialize)]
    pub struct ServerUpsert {
        /// The unique server endpoint to upsert
        #[serde(rename = "a")]
        pub endpoint: Endpoint,
        /// The ICAO of the server
        #[serde(rename = "i")]
        pub icao: IcaoCode,
        /// The server's token set
        #[serde(rename = "t")]
        pub tokens: TokenSet,
    }

    /// A DB mutation request to update a server
    #[derive(Deserialize, Serialize)]
    pub struct ServerUpdate {
        /// The unique server endpoint to update
        #[serde(rename = "a")]
        pub endpoint: Endpoint,
        /// If present, updates the ICAO of the server
        #[serde(rename = "i")]
        pub icao: Option<IcaoCode>,
        /// If present, updates the server's token set
        #[serde(rename = "t")]
        pub tokens: Option<TokenSet>,
    }

    /// Updates the details for the mutator connected to the server
    #[derive(Deserialize, Serialize)]
    pub struct MutatorUpdate {
        /// If not null, the new ICAO to use for the mutator
        #[serde(default, rename = "i", skip_serializing_if = "Option::is_none")]
        pub icao: Option<IcaoCode>,
        /// If not null, the new QCMP port for the mutator
        #[serde(default, rename = "q", skip_serializing_if = "Option::is_none")]
        pub qcmp_port: Option<u16>,
    }

    /// A DB mutation request
    #[derive(Deserialize, Serialize)]
    #[serde(tag = "ty", content = "a")]
    pub enum ServerChange {
        /// One or more servers to upsert
        #[serde(rename = "i")]
        Upsert(Vec<ServerUpsert>),
        /// One or more servers to remove
        #[serde(rename = "r")]
        Remove(Vec<Endpoint>),
        /// One or more servers to update
        #[serde(rename = "u")]
        Update(Vec<ServerUpdate>),
        #[serde(rename = "m")]
        UpdateMutator(MutatorUpdate),
    }
}
