use crate::Peer;
use quilkin_types::IcaoCode;
use quinn::{RecvStream, SendStream};
use std::net::{IpAddr, SocketAddr};

use super::error::ErrorCode;

/// The current version of the server stream
///
/// - 0: Invalid
/// - 1: The initial version
///   All frames are prefixed with a u16 length of the frame
pub const VERSION: u16 = 1;

#[async_trait::async_trait]
pub trait AgentExecutor: Sync + Send + Clone {
    async fn connected(&self, peer: Peer, icao: IcaoCode, qcmp_port: u16);
    async fn execute(
        &self,
        peer: Peer,
        statements: &[super::ServerChange],
    ) -> corro_types::api::ExecResult;
    async fn disconnected(&self, peer: Peer);
}

pub struct Server {
    endpoint: quinn::Endpoint,
    task: tokio::task::JoinHandle<()>,
    local_addr: SocketAddr,
}

struct ValidClientHandshake {
    send: SendStream,
    recv: RecvStream,
    peer: Peer,
}

#[derive(thiserror::Error, Debug)]
enum InitialConnectionError {
    #[error(transparent)]
    Connection(#[from] quinn::ConnectionError),
    #[error(transparent)]
    Read(#[from] super::LengthReadError),
    #[error(transparent)]
    Handshake(#[from] super::HandshakeError),
    #[error(transparent)]
    Write(#[from] quinn::WriteError),
}

impl From<quinn::ReadError> for InitialConnectionError {
    fn from(value: quinn::ReadError) -> Self {
        Self::Read(super::LengthReadError::Read(value))
    }
}

#[derive(thiserror::Error, Debug)]
enum IoLoopError {
    #[error(transparent)]
    Read(#[from] super::LengthReadError),
    #[error(transparent)]
    Jsonb(#[from] serde_json::Error),
    #[error(transparent)]
    Write(#[from] quinn::WriteError),
}

impl From<IoLoopError> for ErrorCode {
    fn from(value: IoLoopError) -> Self {
        match value {
            IoLoopError::Read(read) => (&read).into(),
            IoLoopError::Write(_) => Self::ClientClosed,
            IoLoopError::Jsonb(_) => Self::InternalServerError,
        }
    }
}

impl Server {
    pub fn new_unencrypted(
        addr: SocketAddr,
        executor: impl AgentExecutor + 'static,
    ) -> std::io::Result<Self> {
        let endpoint = quinn::Endpoint::server(quinn_plaintext::server_config(), addr)?;

        let local_addr = endpoint.local_addr()?;
        let ep = endpoint.clone();
        let task = tokio::task::spawn(async move {
            while let Some(conn) = ep.accept().await {
                if !conn.remote_address_validated() {
                    let _impossible = conn.retry();
                    continue;
                }

                let peer_ip = conn.remote_address();

                let exec = executor.clone();
                tokio::spawn(async move {
                    match Self::complete_handshake(conn, &exec).await {
                        Ok(vch) => {
                            let ValidClientHandshake {
                                mut send,
                                mut recv,
                                peer,
                            } = vch;

                            let mut io_loop = async || -> Result<(), IoLoopError> {
                                loop {
                                    let to_exec: Vec<super::ServerChange> =
                                        super::read_length_prefixed_jsonb(&mut recv).await?;

                                    let response = exec.execute(peer, &to_exec).await;
                                    let response = super::write_length_prefixed_jsonb(&response)?;
                                    send.write_chunk(response.freeze()).await?;
                                }
                            };

                            let code = if let Err(error) = io_loop().await {
                                tracing::warn!(%peer, %error, "error handling peer connection");
                                error.into()
                            } else {
                                ErrorCode::Ok
                            };

                            exec.disconnected(peer).await;
                            Self::close(peer, code, send, recv).await;
                        }
                        Err(error) => {
                            tracing::warn!(%peer_ip, %error, "error handling peer handshake");
                        }
                    }
                });
            }
        });

        Ok(Self {
            endpoint,
            task,
            local_addr,
        })
    }

    async fn complete_handshake<AE>(
        conn: quinn::Incoming,
        exec: &AE,
    ) -> Result<ValidClientHandshake, InitialConnectionError>
    where
        AE: AgentExecutor + 'static,
    {
        let peer = match conn.remote_address().ip() {
            IpAddr::V4(v4) => v4.to_ipv6_mapped(),
            IpAddr::V6(v6) => v6,
        };

        let peer = std::net::SocketAddrV6::new(peer, conn.remote_address().port(), 0, 0);
        tracing::debug!(%peer, "accepting peer connection");

        let connection = conn.await?;
        let (mut send, mut recv) = connection.accept_bi().await?;

        let handshake_request = match super::read_length_prefixed(&mut recv).await {
            Ok(bytes) => bytes,
            Err(error) => {
                Self::close(peer, (&error).into(), send, recv).await;
                return Err(error.into());
            }
        };

        use super::ClientHandshake;

        let (_version, info) = match ClientHandshake::read(VERSION, &handshake_request) {
            Ok(ch) => ch,
            Err(err) => {
                Self::close(peer, ErrorCode::BadHandshake, send, recv).await;
                return Err(err.into());
            }
        };

        let chunk = match &info {
            ClientHandshake::V1(_v1) => {
                let hs = super::ServerHandshakeResponseV1 { accept: true }.write();
                super::write_length_prefixed(&hs)
            }
        };

        let (qcmp_port, icao) = info.client_details();
        exec.connected(peer, icao, qcmp_port).await;
        send.write_chunk(chunk.freeze()).await?;

        Ok(ValidClientHandshake { send, recv, peer })
    }

    #[inline]
    async fn close(peer: Peer, code: ErrorCode, mut send: SendStream, recv: RecvStream) {
        tracing::debug!(%peer, %code, "closing peer connection...");
        let _ = send.finish();
        let _ = send.reset(code.into());
        drop(recv);
        tracing::debug!(%peer, "waiting for peer to stop");
        drop(send.stopped().await);
        tracing::debug!(%peer, "peer connection closed");
    }

    pub async fn shutdown(self, reason: &str) {
        self.endpoint
            .close(quinn::VarInt::from_u32(0), reason.as_bytes());
        drop(self.task.await);
    }

    #[inline]
    pub fn local_addr(&self) -> SocketAddr {
        self.local_addr
    }
}
