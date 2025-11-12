use crate::{Peer, codec, persistent::proto, pubsub};
use quilkin_types::IcaoCode;
use quinn::{RecvStream, SendStream};
use std::net::{IpAddr, SocketAddr};
use tokio_stream::StreamExt;

use super::error::ErrorCode;

/// The current version of the server stream
///
/// - 0: Invalid
/// - 1: The initial version
///   The first frame is a request with a magic number and version
///   All frames after are u16 length prefixed JSON
pub const VERSION: u16 = 1;

/// Trait used by a server implementation to perform database mutations
#[async_trait::async_trait]
pub trait Mutator: Sync + Send + Clone {
    /// A new mutation client has connected
    async fn connected(&self, peer: Peer, icao: IcaoCode, qcmp_port: u16);
    /// A mutation client wants to perform 1 or more database mutations
    async fn execute(
        &self,
        peer: Peer,
        statements: &[super::ServerChange],
    ) -> corro_types::api::ExecResponse;
    /// A mutation client has disconnected
    async fn disconnected(&self, peer: Peer);
}

/// Trait used by a server implementation to perform database subscriptions
#[async_trait::async_trait]
pub trait SubManager: Sync + Sync + Clone {
    async fn subscribe(
        &self,
        subp: pubsub::SubParamsv1,
    ) -> Result<pubsub::Subscription, pubsub::MatcherUpsertError>;
}

pub struct Server {
    endpoint: quinn::Endpoint,
    task: tokio::task::JoinHandle<()>,
    local_addr: SocketAddr,
}

struct ValidRequest {
    send: SendStream,
    recv: RecvStream,
    peer: Peer,
    request: proto::Request,
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
        executor: impl Mutator + 'static,
        subs: impl SubManager + 'static,
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
                let usbs = subs.clone();

                tokio::spawn(async move {
                    match Self::read_request(conn).await {
                        Ok(vr) => {
                            Self::handle_request(vr, exec, usbs).await;
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

    async fn read_request(conn: quinn::Incoming) -> Result<ValidRequest, InitialConnectionError> {
        let peer = match conn.remote_address().ip() {
            IpAddr::V4(v4) => v4.to_ipv6_mapped(),
            IpAddr::V6(v6) => v6,
        };

        let peer = std::net::SocketAddrV6::new(peer, conn.remote_address().port(), 0, 0);
        tracing::debug!(%peer, "accepting peer connection");

        let connection = conn.await?;
        let (mut send, mut recv) = connection.accept_bi().await?;

        let request = match codec::read_length_prefixed(&mut recv).await {
            Ok(bytes) => bytes,
            Err(error) => {
                Self::close(peer, (&error).into(), send, recv).await;
                return Err(error.into());
            }
        };

        let rr = proto::RawRequest::try_parse(&request)?;
        let request = rr.deserialize()?;

        Ok(ValidRequest {
            send,
            recv,
            peer,
            request,
        })
    }

    /// Handles a single request (really, stream)
    async fn handle_request(
        req: ValidRequest,
        exec: impl Mutator + 'static,
        subs: impl SubManager + 'static,
    ) {
        let ValidRequest {
            mut send,
            mut recv,
            peer,
            request,
        } = req;

        async fn accept<T: Serialize>(ss: &mut SendStream, contents: T) -> Result<(), IoLoopError> {
            let res = proto::Response {
                code: 200,
                version: proto::VERSION,
                contents: proto::ResponseContents::Ok(contents),
            };

            let bytes = codec::write_length_prefixed_jsonb(&res)?.freeze();
            Ok(ss.write_chunk(bytes).await?)
        }

        async fn reject(ss: &mut SendStream, code: u16, err: String) -> Result<(), IoLoopError> {
            let res = proto::Response {
                code,
                version: proto::VERSION,
                contents: proto::ResponseContents::Err(err),
            };

            let bytes = codec::write_length_prefixed_jsonb(&res)?.freeze();
            Ok(ss.write_chunk(bytes).await?)
        }

        let result = 'io: {
            match req.request {
                proto::Request::V1(inner) => {
                    match inner {
                        proto::v1::Request::Mutate(mreq) => {
                            exec.connected(peer, mreq.icao, mreq.qcmp_port).await;

                            if let Err(err) = accept(
                                &mut send,
                                proto::v1::Response::Mutate(proto::v1::MutateResponse),
                            )
                            .await
                            {
                                break 'io Err(err);
                            }

                            let io_loop = async || -> Result<(), IoLoopError> {
                                loop {
                                    let to_exec = codec::read_length_prefixed_jsonb::<
                                        proto::v1::ServerChange,
                                    >(&mut recv)
                                    .await?;
                                    let response = exec.execute(peer, &[to_exec]).await;
                                    let response = codec::write_length_prefixed_jsonb(&response)?;
                                    send.write_chunk(response.freeze()).await?;
                                }
                            };

                            let res = io_loop().await;
                            exec.disconnected(peer).await;
                            res
                        }
                        proto::v1::Request::Subscribe(sreq) => {
                            let max_buffer = sreq.0.max_buffer;
                            let max_time = sreq.0.max_time;

                            match subs.subscribe(sreq.0).await {
                                Ok(sub) => {
                                    if let Err(err) = accept(
                                        &mut send,
                                        proto::v1::Response::Subscribe(
                                            proto::v1::SubscribeResponse {
                                                id: sub.id,
                                                query_hash: sub.query_hash.clone(),
                                            },
                                        ),
                                    )
                                    .await
                                    {
                                        break 'io Err(err);
                                    }

                                    let sub_id = sub.id;

                                    let mut bs =
                                        pubsub::BufferingSubStream::new(sub, max_buffer, max_time);

                                    let io_loop = async || -> Result<(), IoLoopError> {
                                        loop {
                                            tokio::select! {
                                                buf = bs.next() => {
                                                    send.write_chunk(buf).await?;
                                                }
                                                res = send.stopped() => {
                                                    match res {
                                                        Ok(code) => {
                                                            tracing::info!(%peer, %sub_id, ?code, "subscriber disconnected with code");
                                                        }
                                                        Err(_) => {
                                                            tracing::info!(%peer, %sub_id, "subscriber disconnected");
                                                        }
                                                    }
                                                    return Ok(());
                                                }
                                            }
                                        }
                                    };

                                    let res = io_loop().await;
                                }
                                Err(err) => {
                                    use pubsub::{MatcherError as Me, MatcherUpsertError as E};

                                    let err_str = err.to_string();
                                    let code = match err {
                                        E::Pool(_) | E::Sqlite(_) | E::MissingBroadcaster => {
                                            ErrorCode::InternalServerError
                                        }
                                        E::CouldNotExpand
                                        | E::NormalizeStatement(_)
                                        | E::SubFromWithoutMatcher => ErrorCode::BadRequest,
                                        E::Matcher(me) => {
                                            match me {
                                                Me::Lexer(_) | Me::StatementRequired |  Me::TableRequired | Me::QualificationRequired { .. } | Me::AggPrimaryKeyMissing(_, _) => ErrorCode::BadRequest,
                                                Me::TableNotFound(_) | Me::TableForColumnNotFound { .. }=> ErrorCode::NotFound,
                                                Me::UnsupportedStatement | Me::UnsupportedExpr { .. } | Me::JoinOnExprUnsupported { .. } => ErrorCode::Unsupported,
                                                Me::Sqlite(_) | Me::NoPrimaryKey(_) | Me::TableStarNotFound { .. } | Me::MissingPrimaryKeys | Me::ChangeQueueClosedOrFull /* this could maybe be serviceunavailable */ | Me::NoChangeInserted | Me::EventReceiverClosed | Me::Unpack(_) | Me::InsertSub | Me::FromSql(_) | Me::Io(_) | Me::ChangesetEncode(_) | Me::QueueFull | Me::CannotRestoreExisting | Me::WritePermitAcquire(_) | Me::NotRunning | Me::MissingSql => ErrorCode::InternalServerError,
                                            }
                                        }
                                    };

                                    reject(&mut send, code, err_str).await
                                }
                            }
                        }
                    }
                }
            }
        };

        let code = if let Err(error) = result {
            tracing::warn!(%peer, %error, "error handling peer connection");
            error.into()
        } else {
            ErrorCode::Ok
        };

        Self::close(peer, code, send, recv).await;
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
