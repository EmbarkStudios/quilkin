//! Code for clients that connect to a remote [`crate::server::Server`] and either
//! send mutations, or subscribe to said mutations

use crate::{
    codec,
    persistent::{
        ErrorCode,
        proto::{self, VersionedRequest},
    },
    pubsub,
};
use bytes::Bytes;
pub use corro_api_types::{ExecResponse, ExecResult, QueryEvent};
use quilkin_types::IcaoCode;
use std::net::SocketAddr;
use tokio::sync::{mpsc, oneshot};
use tracing::Instrument as _;
use uuid::Uuid;

type ResponseTx = oneshot::Sender<Result<ExecResponse, StreamError>>;

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

use codec::LengthReadError as Lre;

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
    Json(#[from] serde_json::Error),
    #[error(transparent)]
    Write(#[from] StreamError),
    #[error(transparent)]
    Proto(#[from] proto::Error),
}

#[derive(thiserror::Error, Debug)]
pub enum TransactionError {
    #[error(transparent)]
    Serialization(#[from] std::io::Error),
    #[error(transparent)]
    Stream(#[from] StreamError),
    #[error("the I/O task for this client was shutdown")]
    TaskShutdown,
}

/// A persistent connection to a corrosion server
#[derive(Clone)]
pub struct Client {
    conn: quinn::Connection,
    local_addr: SocketAddr,
    /// A lazy notifying reference count
    #[allow(dead_code)]
    tx: mpsc::Sender<()>,
}

impl Client {
    /// Connects using a non-encrypted session
    pub async fn connect_insecure(
        addr: SocketAddr,
        metrics: super::Metrics,
    ) -> Result<Self, ConnectError> {
        let ep = quinn::Endpoint::client((std::net::Ipv6Addr::LOCALHOST, 0).into())?;

        let conn = ep
            .connect_with(
                quinn_plaintext::client_config(),
                addr,
                &addr.ip().to_string(),
            )?
            .await?;

        // This is really infallible
        let local_addr = ep.local_addr()?;

        // Spawn a task to periodically update the connection metrics
        let mconn = conn.clone();
        let (tx, mut rx) = mpsc::channel(1);
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(std::time::Duration::from_secs(10));

            // TODO: might be more interesting to use active for streams instead
            // of connections as there will (at the moment) only ever be 1
            metrics.active.with_label_values::<&str>(&[]).inc();

            let mut tx_count = 0;
            let mut tx_bytes = 0;
            let mut rx_count = 0;
            let mut rx_bytes = 0;

            use super::update_metric as um;

            let stats = mconn.stats();
            um(&metrics.tx_count, &mut tx_count, stats.udp_tx.datagrams);
            um(&metrics.tx_bytes, &mut tx_bytes, stats.udp_tx.bytes);
            um(&metrics.rx_count, &mut rx_count, stats.udp_rx.datagrams);
            um(&metrics.rx_bytes, &mut rx_bytes, stats.udp_rx.bytes);

            loop {
                tokio::select! {
                    _ = interval.tick() => {
                        let stats = mconn.stats();
                        um(&metrics.tx_count, &mut tx_count, stats.udp_tx.datagrams);
                        um(&metrics.tx_bytes, &mut tx_bytes,stats.udp_tx.bytes);
                        um(&metrics.rx_count, &mut rx_count, stats.udp_rx.datagrams);
                        um(&metrics.rx_bytes, &mut rx_bytes,stats.udp_rx.bytes);
                    }
                    over = rx.recv() => {
                        if over.is_none() {
                            break;
                        }
                    }
                }
            }

            metrics.active.with_label_values::<&str>(&[]).dec();
        });

        Ok(Self {
            conn,
            local_addr,
            tx,
        })
    }

    #[inline]
    pub fn local_addr(&self) -> SocketAddr {
        self.local_addr
    }

    #[inline]
    pub fn remote_addr(&self) -> SocketAddr {
        self.conn.remote_address()
    }

    /// Closes the connection to the upstream server
    #[inline]
    pub async fn shutdown(self) {
        drop(self.conn);
    }
}

pub struct MutationClient {
    inner: Client,
    tx: mpsc::UnboundedSender<(Bytes, ResponseTx)>,
    task: tokio::task::JoinHandle<Result<Option<quinn::VarInt>, StreamError>>,
}

impl MutationClient {
    pub async fn connect(
        inner: Client,
        qcmp_port: u16,
        icao: IcaoCode,
    ) -> Result<Self, ConnectError> {
        let (mut send, mut recv) = inner.conn.open_bi().await?;

        // We need to actually send something for the connection to be fully established
        let req_buf =
            proto::v1::Request::Mutate(proto::v1::MutateRequest { qcmp_port, icao }).write()?;

        send.write_chunk(req_buf).await.map_err(StreamError::from)?;

        let res = codec::read_length_prefixed(&mut recv)
            .await
            .map_err(StreamError::from)?;

        let vb = proto::VersionedBuf::try_parse(&res)?;
        let peer_version = vb.version;
        let response = vb.deserialize_response()?;

        match response {
            proto::Response::V1(res) => {
                use proto::v1;

                match res {
                    v1::Response::Ok(v1::OkResponse::Mutate(_)) => {}
                    v1::Response::Ok(invalid) => {
                        tracing::error!(
                            ?invalid,
                            peer_version,
                            "received a non-mutate response to a mutate request"
                        );
                        return Err(proto::Error::InvalidResponse.into());
                    }
                    v1::Response::Err { code, message } => {
                        return Err(proto::Error::ErrorResponse { code, message }.into());
                    }
                }
            }
        }

        let (tx, mut reqrx) = mpsc::unbounded_channel();

        let task = tokio::task::spawn(async move {
            let mut func = async || -> Result<Option<quinn::VarInt>, StreamError> {
                match peer_version {
                    1 => loop {
                        let (msg, comp): (_, ResponseTx) = tokio::select! {
                            res = recv.received_reset() => {
                                return res.map_err(StreamError::Reset);
                            }
                            req = reqrx.recv() => {
                                let Some(req) = req else {
                                    // Initiate a shutdown, the server will see we've reset its receiver
                                    // and it will exit the loop when it has finished all messages, closing
                                    // its sender, which we will know when we receive a reset (or it times out)
                                    let _ = send.reset(ErrorCode::Ok.into());
                                    let _ = send.finish();

                                    tracing::debug!("waiting for server to shutdown this stream...");
                                    drop(recv.received_reset().await);
                                    tracing::debug!("client finished");
                                    break;
                                };

                                req
                            }
                        };

                        send.write_chunk(msg).await?;
                        let res = codec::read_length_prefixed_jsonb::<ExecResponse>(&mut recv)
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

        Ok(Self { inner, tx, task })
    }

    #[inline]
    pub async fn update(
        &self,
        qcmp_port: Option<u16>,
        icao: Option<IcaoCode>,
    ) -> Result<ExecResponse, TransactionError> {
        if qcmp_port.is_none() && icao.is_none() {
            return Ok(ExecResponse {
                results: vec![ExecResult::Error {
                    error: "neither the QCMP port nor ICAO were provided".into(),
                }],
                time: 0.,
                version: None,
                actor_id: None,
            });
        }

        self.transactions(&[proto::v1::ServerChange::UpdateMutator(
            proto::v1::MutatorUpdate { qcmp_port, icao },
        )])
        .await
    }

    #[inline]
    pub async fn transactions(
        &self,
        change: &[proto::v1::ServerChange],
    ) -> Result<ExecResponse, TransactionError> {
        let buf = codec::write_length_prefixed_jsonb(&change)?;
        self.send_raw(buf).await
    }

    #[inline]
    pub async fn send_raw(&self, buf: bytes::BytesMut) -> Result<ExecResponse, TransactionError> {
        let (tx, rx) = oneshot::channel();
        self.tx
            .send((buf.freeze(), tx))
            .map_err(|_e| TransactionError::TaskShutdown)?;
        Ok(rx.await.map_err(|_e| TransactionError::TaskShutdown)??)
    }

    pub async fn shutdown(self) {
        drop(self.tx);
        if let Ok(Err(error)) = self.task.await {
            tracing::warn!(%error, "stream exited with error");
        }
        self.inner.shutdown().await;
    }
}

pub enum SubscriptionStop {}

pub struct SubscriptionClient {
    inner: Client,
    tx: oneshot::Sender<ErrorCode>,
    task: tokio::task::JoinHandle<Result<Option<quinn::VarInt>, StreamError>>,
}

pub struct SubClientStream {
    /// The unique identifier for this subscription
    pub id: Uuid,
    /// The hash of the query that was subscribed to
    pub query_hash: String,
    /// The stream of changes that match the subscription query
    ///
    /// Dropping this will terminate the subscription on the remote server,
    /// but ideally one would call [`SubscriptionClient::shutdown`] to gracefully
    /// close the stream
    pub rx: mpsc::UnboundedReceiver<pubsub::SubscriptionStream>,
}

impl SubscriptionClient {
    pub async fn connect(
        inner: Client,
        params: crate::pubsub::SubParamsv1,
    ) -> Result<(Self, SubClientStream), ConnectError> {
        let (mut send, mut recv) = inner.conn.open_bi().await?;

        let res = Self::handshake(&mut send, &mut recv, params).await?;

        let (tx, reqrx) = mpsc::unbounded_channel();
        let (stx, srx) = oneshot::channel();

        let sub_id = res.id;

        let scs = SubClientStream {
            id: res.id,
            query_hash: res.query_hash,
            rx: reqrx,
        };

        let task = tokio::task::spawn(async move {
            Self::run_subscription_loop(recv, send, tx, srx)
                .instrument(tracing::info_span!("subscription", %sub_id))
                .await
        });

        Ok((
            Self {
                inner,
                tx: stx,
                task,
            },
            scs,
        ))
    }

    async fn run_subscription_loop(
        mut recv: quinn::RecvStream,
        send: quinn::SendStream,
        tx: mpsc::UnboundedSender<pubsub::SubscriptionStream>,
        mut srx: oneshot::Receiver<ErrorCode>,
    ) -> Result<Option<quinn::VarInt>, StreamError> {
        tracing::info!("started subscription stream");

        'io: loop {
            tokio::select! {
                res = codec::read_length_prefixed(&mut recv) => {
                    match res {
                        Ok(buf) => {
                            let stream = pubsub::SubscriptionStream::new(buf);
                            if tx.send(stream).is_err() {
                                tracing::info!("subscription stream receiver closed, closing stream");
                                let _ = recv.stop(ErrorCode::Unknown.into());
                                break 'io;
                            }
                        }
                        Err(_err) => {
                            match recv.received_reset().await {
                                Ok(code) => {
                                    tracing::warn!(code = ?code.map(ErrorCode::from), "server shutdown subscription stream");
                                }
                                Err(error) => {
                                    tracing::warn!(%error, "server shutdown subscription stream abnormally");
                                }
                            }

                            break;
                        }
                    }
                }
                code = &mut srx => {
                    let code = code.unwrap_or_else(|_| {
                        tracing::error!("SubscriptionClient dropped without calling shutdown");
                            ErrorCode::Unknown
                    });
                    drop(recv.stop(code.into()));
                    break;
                }
            }
        }

        tracing::info!("waiting for remote shutdown");
        let stime = std::time::Instant::now();
        let res = send.stopped().await;
        tracing::info!(time = ?stime.elapsed(), result = ?res, "remote shutdown complete");

        Ok(None)
    }

    async fn handshake(
        send: &mut quinn::SendStream,
        recv: &mut quinn::RecvStream,
        params: crate::pubsub::SubParamsv1,
    ) -> Result<proto::v1::SubscribeResponse, ConnectError> {
        // We need to actually send something for the connection to be fully established
        let req_buf = proto::v1::Request::Subscribe(proto::v1::SubscribeRequest(params)).write()?;

        send.write_chunk(req_buf).await.map_err(StreamError::from)?;

        let res = codec::read_length_prefixed(recv)
            .await
            .map_err(StreamError::from)?;

        macro_rules! bad_response {
            ($op:expr) => {
                match $op {
                    Ok(r) => r,
                    Err(error) => {
                        let _ = recv.stop(ErrorCode::BadResponse.into());
                        return Err(error.into());
                    }
                }
            };
        }

        let vb = bad_response!(proto::VersionedBuf::try_parse(&res));
        let peer_version = vb.version;
        let response = bad_response!(vb.deserialize_response());

        use proto::v1;

        match response {
            proto::Response::V1(v1::Response::Ok(v1::OkResponse::Subscribe(sr))) => Ok(sr),
            proto::Response::V1(v1::Response::Ok(invalid)) => {
                tracing::error!(
                    ?invalid,
                    peer_version,
                    "received a non-subscribe response to a subscribe request"
                );
                Err(proto::Error::InvalidResponse.into())
            }
            proto::Response::V1(v1::Response::Err { code, message }) => {
                Err(proto::Error::ErrorResponse { code, message }.into())
            }
        }
    }

    /// Shuts down this client, sending the specified code to the server if it
    /// is able
    pub async fn shutdown(self, code: ErrorCode) {
        let _ = self.tx.send(code);
        if let Ok(Err(error)) = self.task.await {
            tracing::warn!(%error, "stream exited with error");
        }
        self.inner.shutdown().await;
    }
}
