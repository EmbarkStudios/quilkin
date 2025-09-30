use bytes::Bytes;
use corro_api_types::ExecResult;
use quilkin_types::IcaoCode;
use std::net::SocketAddr;
use tokio::sync::{mpsc, oneshot};

type ResponseTx = oneshot::Sender<Result<ExecResult, StreamError>>;

#[derive(thiserror::Error, Debug)]
pub enum StreamError {
    #[error(transparent)]
    Connect(#[from] quinn::ConnectionError),
    #[error(transparent)]
    Write(#[from] quinn::WriteError),
    #[error(transparent)]
    Read(#[from] quinn::ReadError),
    #[error(transparent)]
    ReadExact(#[from] quinn::ReadExactError),
    #[error(transparent)]
    Reset(#[from] quinn::ResetError),
    #[error(transparent)]
    Json(serde_json::Error),
    #[error(
        "expected a chunk of JSON length {} but received {}",
        expected,
        received
    )]
    LengthMismatch { expected: usize, received: usize },
    #[error("stream ended")]
    StreamEnded,
}

use super::LengthReadError as Lre;

impl From<Lre> for StreamError {
    fn from(value: Lre) -> Self {
        match value {
            Lre::Json(json) => Self::Json(json),
            Lre::LengthMismatch { expected, received } => {
                Self::LengthMismatch { expected, received }
            }
            Lre::ReadExact(re) => Self::ReadExact(re),
            Lre::Read(r) => Self::Read(r),
            Lre::StreamEnded => Self::StreamEnded,
        }
    }
}

#[derive(thiserror::Error, Debug)]
pub enum ConnectError {
    #[error(transparent)]
    Connect(#[from] quinn::ConnectError),
    #[error(transparent)]
    Connection(#[from] quinn::ConnectionError),
    #[error(transparent)]
    Creation(#[from] std::io::Error),
    #[error(transparent)]
    Handshake(#[from] super::HandshakeError),
    #[error(transparent)]
    Write(#[from] StreamError),
}

#[derive(thiserror::Error, Debug)]
pub enum TransactionError {
    #[error(transparent)]
    Json(#[from] serde_json::Error),
    #[error(transparent)]
    Stream(#[from] StreamError),
    #[error("the I/O task for this client was shutdown")]
    TaskShutdown,
}

/// The current version of the client stream
///
/// - 0: Invalid
/// - 1: The initial version
///   Requests are 16-bit length-prefixed JSON, where the JSON is [`ServerChange`]
///   Responses are the JSON of [`ExecResult`]
pub const VERSION: u16 = 1;

/// A persistent connection to a corrosion agent
pub struct Client {
    inner: quinn::Connection,
    local_addr: SocketAddr,
    tx: mpsc::UnboundedSender<(Bytes, ResponseTx)>,
    task: tokio::task::JoinHandle<Result<Option<quinn::VarInt>, StreamError>>,
}

impl Client {
    /// Connects using a non-encrypted session
    pub async fn connect_insecure(
        addr: SocketAddr,
        qcmp_port: u16,
        icao: IcaoCode,
    ) -> Result<Self, ConnectError> {
        let ep = quinn::Endpoint::client((std::net::Ipv6Addr::LOCALHOST, 0).into())?;

        let inner = ep
            .connect_with(
                quinn_plaintext::client_config(),
                addr,
                &addr.ip().to_string(),
            )?
            .await?;

        // This is really infallible
        let local_addr = ep.local_addr()?;

        let client = inner.clone();
        let (mut send, mut recv) = client.open_bi().await?;

        // Handshake
        // We need to actually send something for the connection to be fully established
        let peer_version = {
            let req = super::ClientHandshakeRequestV1 { qcmp_port, icao }.write();

            send.write_chunk(super::write_length_prefixed(&req).freeze())
                .await
                .map_err(StreamError::from)?;

            let res = super::read_length_prefixed(&mut recv)
                .await
                .map_err(StreamError::from)?;
            match super::ServerHandshake::read(VERSION, &res[..])? {
                super::ServerHandshake::V1(shs) => {
                    if !shs.accept {
                        return Err(ConnectError::Handshake(
                            crate::persistent::HandshakeError::UnsupportedVersion {
                                ours: VERSION,
                                theirs: 1,
                            },
                        ));
                    }

                    1
                }
            }
        };

        let (tx, mut reqrx) = mpsc::unbounded_channel();

        let task = tokio::task::spawn(async move {
            let func = async || -> Result<Option<quinn::VarInt>, StreamError> {
                match peer_version {
                    1 => loop {
                        let (msg, comp): (_, ResponseTx) = tokio::select! {
                            res = recv.received_reset() => {
                                return res.map_err(StreamError::Reset);
                            }
                            req = reqrx.recv() => {
                                let Some(req) = req else {
                                    let _ = send.reset(quinn::VarInt::from_u32(1));
                                    let _ = send.finish();
                                    // We need to drop the recv stream so that the server
                                    // knows we don't care and it can finish closing the connection
                                    drop(recv);
                                    tracing::debug!("waiting for server to received buffered stream...");
                                    drop(send.stopped().await);
                                    tracing::debug!("client finished");
                                    break;
                                };

                                req
                            }
                        };

                        send.write_chunk(msg).await?;
                        let res = super::read_length_prefixed_jsonb::<ExecResult>(&mut recv)
                            .await
                            .map_err(StreamError::from);

                        if let Err(error) = &res {
                            tracing::error!(%error, "error occurred reading response to transaction");
                        }

                        if comp.send(res).is_err() {
                            tracing::warn!("transaction response could not be sent to queuer");
                        }
                    },
                    _invalid => {
                        return Err(StreamError::Connect(
                            quinn::ConnectionError::VersionMismatch,
                        ));
                    }
                }

                Ok(None)
            };

            func().await
        });

        Ok(Self {
            inner,
            tx,
            task,
            local_addr,
        })
    }

    pub fn local_addr(&self) -> SocketAddr {
        self.local_addr
    }

    pub fn remote_addr(&self) -> SocketAddr {
        self.inner.remote_address()
    }

    pub async fn transactions(
        &self,
        change: &[super::ServerChange],
    ) -> Result<ExecResult, TransactionError> {
        let buf = super::write_length_prefixed_jsonb(&change)?;

        let (tx, rx) = oneshot::channel();
        self.tx
            .send((buf.freeze(), tx))
            .map_err(|_| TransactionError::TaskShutdown)?;
        Ok(rx.await.map_err(|_| TransactionError::TaskShutdown)??)
    }

    /// Closes the connection to the upstream server
    pub async fn shutdown(self) {
        drop(self.tx);
        if let Ok(Err(error)) = self.task.await {
            tracing::warn!(%error, "stream exited with error");
        }
        drop(self.inner);
    }
}
