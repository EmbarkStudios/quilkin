use crate::{Peer, codec, persistent::proto, pubsub};
use quilkin_types::IcaoCode;
use quinn::{RecvStream, SendStream};
use std::net::{IpAddr, SocketAddr};
use tokio_stream::StreamExt;
use tracing::Instrument as _;

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
        statements: &[proto::v1::ServerChange],
    ) -> corro_types::api::ExecResponse;
    /// A mutation client has disconnected
    async fn disconnected(&self, peer: Peer);
}

/// Trait used by a server implementation to perform database subscriptions
#[async_trait::async_trait]
pub trait SubManager: Sync + Send + Clone {
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
    Read(#[from] codec::LengthReadError),
    #[error(transparent)]
    Handshake(#[from] proto::Error),
    #[error(transparent)]
    Write(#[from] quinn::WriteError),
}

impl From<quinn::ReadError> for InitialConnectionError {
    fn from(value: quinn::ReadError) -> Self {
        Self::Read(codec::LengthReadError::Read(value))
    }
}

#[derive(thiserror::Error, Debug)]
enum IoLoopError {
    #[error(transparent)]
    Read(#[from] codec::LengthReadError),
    #[error(transparent)]
    Serialization(#[from] std::io::Error),
    #[error(transparent)]
    Write(#[from] quinn::WriteError),
}

impl From<IoLoopError> for ErrorCode {
    fn from(value: IoLoopError) -> Self {
        match value {
            IoLoopError::Read(read) => (&read).into(),
            IoLoopError::Write(_) => Self::ClientClosed,
            IoLoopError::Serialization(_) => Self::InternalServerError,
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
            while let Some(inc) = ep.accept().await {
                if !inc.remote_address_validated() {
                    let _impossible = inc.retry();
                    continue;
                }

                let peer_ip = inc.remote_address();
                let exec = executor.clone();
                let usbs = subs.clone();

                tokio::spawn(async move {
                    let peer = match inc.remote_address().ip() {
                        IpAddr::V4(v4) => v4.to_ipv6_mapped(),
                        IpAddr::V6(v6) => v6,
                    };

                    let peer = std::net::SocketAddrV6::new(peer, inc.remote_address().port(), 0, 0);
                    tracing::debug!("accepting peer connection");

                    let connection = match inc.await {
                        Ok(c) => c,
                        Err(error) => {
                            tracing::warn!(%error, "failed to establish connection from remote peer");
                            return;
                        }
                    };

                    loop {
                        tokio::select! {
                            res = Self::read_request(peer, &connection) => {
                                match res {
                                    Ok(vr) => {
                                        let exec = exec.clone();
                                        let usbs = usbs.clone();
                                        tokio::spawn(async move {
                                            Self::handle_request(vr, exec, usbs).await;
                                        });
                                    }
                                    Err(error) => {
                                        tracing::warn!(%peer_ip, %error, "error handling peer handshake");
                                    }
                                }
                            }
                            reason = connection.closed() => {
                                tracing::info!(%reason, "peer closed connection");
                                break;
                            }
                        }
                    }
                }.instrument(tracing::info_span!("remote connection", %peer_ip))
                );
            }
        });

        Ok(Self {
            endpoint,
            task,
            local_addr,
        })
    }

    async fn read_request(
        peer: std::net::SocketAddrV6,
        conn: &quinn::Connection,
    ) -> Result<ValidRequest, InitialConnectionError> {
        let (send, mut recv) = conn.accept_bi().await?;

        macro_rules! bad_request {
            ($op:expr) => {
                match $op {
                    Ok(o) => o,
                    Err(error) => {
                        Self::close(peer, ErrorCode::BadRequest, send, recv).await;
                        return Err(error.into());
                    }
                }
            };
        }

        let request = bad_request!(codec::read_length_prefixed(&mut recv).await);
        let vb = bad_request!(proto::VersionedBuf::try_parse(&request));
        let request = bad_request!(vb.deserialize_request());

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

        let result = match request {
            proto::Request::V1(inner) => {
                v1_impl::handle_stream(inner, peer, &mut send, &mut recv, exec, subs).await
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

#[inline]
async fn send_response<T: proto::VersionedResponse>(
    ss: &mut SendStream,
    response: T,
) -> Result<(), IoLoopError> {
    let bytes = response.write()?;
    Ok(ss.write_chunk(bytes).await?)
}

mod v1_impl {
    use super::*;
    use proto::v1;

    async fn handle_mutate(
        req: v1::MutateRequest,
        peer: Peer,
        send: &mut SendStream,
        recv: &mut RecvStream,
        exec: impl Mutator + 'static,
    ) -> Result<(), IoLoopError> {
        exec.connected(peer, req.icao, req.qcmp_port).await;

        send_response(
            send,
            v1::Response::Ok(v1::OkResponse::Mutate(v1::MutateResponse)),
        )
        .await?;

        let mut io_loop = async || -> Result<(), IoLoopError> {
            loop {
                let to_exec =
                    codec::read_length_prefixed_jsonb::<Vec<v1::ServerChange>>(recv).await?;

                let response = exec.execute(peer, &to_exec).await;
                let response = codec::write_length_prefixed_jsonb(&response)?;
                send.write_chunk(response.freeze()).await?;
            }
        };

        let res = io_loop().await;
        exec.disconnected(peer).await;
        res
    }

    async fn handle_subscribe(
        req: v1::SubscribeRequest,
        peer: Peer,
        send: &mut SendStream,
        subs: impl SubManager + 'static,
    ) -> Result<(), IoLoopError> {
        let max_buffer = req.0.max_buffer;
        let max_time = req.0.max_time;

        match subs.subscribe(req.0).await {
            Ok(sub) => {
                send_response(
                    send,
                    v1::Response::Ok(v1::OkResponse::Subscribe(v1::SubscribeResponse {
                        id: sub.id,
                        query_hash: sub.query_hash.clone(),
                    })),
                )
                .await?;

                let sub_id = sub.id;

                let mut bs = pubsub::BufferingSubStream::new(sub, max_buffer, max_time);

                let mut io_loop = async || -> Result<(), IoLoopError> {
                    loop {
                        tokio::select! {
                            buf = bs.next() => {
                                if let Some(buf) = buf {
                                    send.write_chunk(buf).await?;
                                } else {
                                    tracing::info!(%peer, %sub_id, "subscription sender was closed");
                                    // This should maybe be an error?
                                    return Ok(());
                                }
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
                tracing::debug!(result = ?res, "subscription stream ended");
                res
            }
            Err(err) => {
                use pubsub::{MatcherError as Me, MatcherUpsertError as E};

                let err_str = err.to_string();
                let code = match err {
                    E::Pool(_) | E::Sqlite(_) | E::MissingBroadcaster => {
                        ErrorCode::InternalServerError
                    }
                    E::CouldNotExpand | E::NormalizeStatement(_) | E::SubFromWithoutMatcher => {
                        ErrorCode::BadRequest
                    }
                    E::Matcher(me) => {
                        match me {
                            Me::Lexer(_) | Me::StatementRequired |  Me::TableRequired | Me::QualificationRequired { .. } | Me::AggPrimaryKeyMissing(_, _) => ErrorCode::BadRequest,
                            Me::TableNotFound(_) | Me::TableForColumnNotFound { .. }=> ErrorCode::NotFound,
                            Me::UnsupportedStatement | Me::UnsupportedExpr { .. } | Me::JoinOnExprUnsupported { .. } => ErrorCode::Unsupported,
                            Me::Sqlite(_) | Me::NoPrimaryKey(_) | Me::TableStarNotFound { .. } | Me::MissingPrimaryKeys | Me::ChangeQueueClosedOrFull /* this could maybe be serviceunavailable */ | Me::NoChangeInserted | Me::EventReceiverClosed | Me::Unpack(_) | Me::InsertSub | Me::FromSql(_) | Me::Io(_) | Me::ChangesetEncode(_) | Me::QueueFull | Me::CannotRestoreExisting | Me::WritePermitAcquire(_) | Me::NotRunning | Me::MissingSql => ErrorCode::InternalServerError,
                        }
                    }
                };

                send_response(
                    send,
                    v1::Response::Err {
                        code: code as u16,
                        message: err_str,
                    },
                )
                .await
            }
        }
    }

    pub(super) async fn handle_stream(
        request: proto::v1::Request,
        peer: Peer,
        send: &mut SendStream,
        recv: &mut RecvStream,
        exec: impl Mutator + 'static,
        subs: impl SubManager + 'static,
    ) -> Result<(), IoLoopError> {
        match request {
            v1::Request::Mutate(mreq) => handle_mutate(mreq, peer, send, recv, exec).await,
            v1::Request::Subscribe(sreq) => handle_subscribe(sreq, peer, send, subs).await,
        }
    }
}
