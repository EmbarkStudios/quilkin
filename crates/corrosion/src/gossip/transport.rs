use super::GossipMetrics;
use bytes::Bytes;
use quinn::{self as q, Connection};
use std::{
    collections::HashMap,
    net::SocketAddr,
    sync::{Arc, Mutex as StdMutex},
    time::{Duration, Instant},
};
use tokio::sync::{Mutex, RwLock, mpsc};
use tracing::Instrument;

#[derive(Debug, Clone)]
pub struct Transport(Arc<TransportInner>);

#[derive(Debug)]
struct TransportInner {
    endpoint: q::Endpoint,
    conns: RwLock<HashMap<SocketAddr, Arc<Mutex<Option<Connection>>>>>,
    rtt_tx: mpsc::Sender<(SocketAddr, Duration)>,
    metrics: &'static GossipMetrics,
}

#[derive(Clone, Copy)]
pub(crate) enum TrafficClass {
    Sync,
    Broadcast,
    Foca,
}

impl TrafficClass {
    #[inline]
    pub(crate) fn as_str(self) -> &'static str {
        match self {
            Self::Sync => "sync",
            Self::Broadcast => "broadcast",
            Self::Foca => "foca",
        }
    }
}

#[derive(Debug, thiserror::Error)]
pub enum TransportError {
    #[error(transparent)]
    Connect(#[from] q::ConnectError),
    #[error(transparent)]
    Connection(#[from] q::ConnectionError),
    #[error(transparent)]
    Datagram(#[from] q::SendDatagramError),
    #[error(transparent)]
    SendStreamWrite(#[from] q::WriteError),
    #[error(transparent)]
    TimedOut(#[from] tokio::time::error::Elapsed),
    #[error(transparent)]
    Stopped(#[from] q::StoppedError),
}

impl Transport {
    /// Creates a new transport that communicates in plaintext
    pub fn new_insecure(
        metrics: &'static GossipMetrics,
        rtt_tx: mpsc::Sender<(SocketAddr, Duration)>,
    ) -> eyre::Result<Self> {
        let mut endpoint = q::Endpoint::client((std::net::Ipv6Addr::UNSPECIFIED, 0).into())?;

        // The original corrosion code puts timeouts on the client side, but IMO it makes more sense to put timeouts
        // on the server side even though the effect is the same
        endpoint.set_default_client_config(quinn_plaintext::client_config());

        Ok(Self(Arc::new(TransportInner {
            endpoint,
            conns: Default::default(),
            rtt_tx,
            metrics,
        })))
    }

    pub async fn send_datagram(&self, addr: SocketAddr, data: Bytes) -> Result<(), TransportError> {
        let conn = self.connect(addr, TrafficClass::Foca).await?;

        match conn.send_datagram(data.clone()) {
            Ok(()) => {}
            Err(q::SendDatagramError::ConnectionLost(_)) => {
                let conn = self.connect(addr, TrafficClass::Foca).await?;
                conn.send_datagram(data.clone())?;
            }
            Err(error) => {
                self.0
                    .metrics
                    .client_datagram_errors_inc(datagram_error_kind(&error));
                if matches!(error, q::SendDatagramError::TooLarge) {
                    tracing::warn!(%addr, len = data.len(), max = conn.max_datagram_size(), "attempted to send a larger-than-PMTU datagram");
                }
                return Err(error.into());
            }
        }

        self.0.metrics.client_datagrams_sent_inc(data.len());

        Ok(())
    }

    pub async fn send_uni(&self, addr: SocketAddr, data: Bytes) -> Result<(), TransportError> {
        let len = data.len();
        let conn = self.connect(addr, TrafficClass::Broadcast).await?;

        let mut stream = match conn
            .open_uni()
            .instrument(debug_span!("quic_open_uni"))
            .await
        {
            Ok(stream) => stream,
            Err(e @ ConnectionError::VersionMismatch) => {
                return Err(e.into());
            }
            Err(e) => {
                debug!("retryable error attempting to open unidirectional stream: {e}");
                let conn = self.connect(addr, TrafficClass::Broadcast).await?;
                conn.open_uni()
                    .instrument(debug_span!("quic_open_uni"))
                    .await?
            }
        };

        stream
            .write_chunk(data)
            .instrument(debug_span!("quic_write_chunk"))
            .await?;

        stream
            .finish()
            .expect("unreachable, the stream does not leave this method");

        stream
            .stopped()
            .instrument(debug_span!("quic_stopped"))
            .await?;

        counter!(
            "corro.transport.tx.bytes.v2.total",
            "traffic" => TrafficClass::Broadcast.as_str()
        )
        .increment(len as u64);

        Ok(())
    }

    pub async fn open_bi(
        &self,
        addr: SocketAddr,
    ) -> Result<(q::SendStream, q::RecvStream), TransportError> {
        let conn = self.connect(addr, TrafficClass::Sync).await?;

        match conn.open_bi().await {
            Ok(s) => return Ok(s),
            Err(e @ q::ConnectionError::VersionMismatch) => {
                return Err(e.into());
            }
            Err(error) => {
                tracing::debug!(%error, "retryable error attempting to open bidirectional stream");
            }
        }

        // retry, it should reconnect!
        let conn = self.connect(addr, TrafficClass::Sync).await?;
        Ok(conn.open_bi().await?)
    }

    async fn measured_connect(
        &self,
        addr: SocketAddr,
        server_name: String,
        traffic: TrafficClass,
    ) -> Result<Connection, TransportError> {
        let start = Instant::now();

        match tokio::time::timeout(
            Duration::from_secs(5),
            self.0.endpoint.connect(addr, &server_name)?,
        )
        .await
        {
            Ok(Ok(conn)) => {
                histogram!(
                    "corro.transport.connect.time.v2.seconds",
                    "traffic" => traffic.as_str()
                )
                .record(start.elapsed().as_secs_f64());
                tracing::Span::current().record("rtt", conn.rtt().as_secs_f64());
                Ok(conn)
            }
            Ok(Err(e)) => {
                counter!(
                    "corro.transport.connect.errors.v2",
                    "traffic" => traffic.as_str(),
                    "kind" => "connect_error"
                )
                .increment(1);
                Err(e.into())
            }
            Err(e) => {
                counter!(
                    "corro.transport.connect.errors.v2",
                    "traffic" => traffic.as_str(),
                    "kind" => "timed_out"
                )
                .increment(1);
                Err(e.into())
            }
        }
    }

    // this shouldn't block for long...
    async fn get_lock(&self, addr: SocketAddr) -> Arc<Mutex<Option<Connection>>> {
        {
            let r = self.0.conns.read().await;
            if let Some(lock) = r.get(&addr) {
                return lock.clone();
            }
        }

        let mut w = self.0.conns.write().await;
        w.entry(addr).or_default().clone()
    }

    async fn connect(
        &self,
        addr: SocketAddr,
        traffic: TrafficClass,
    ) -> Result<Connection, TransportError> {
        let conn_lock = self.get_lock(addr).await;

        let mut lock = conn_lock.lock().await;

        if let Some(conn) = lock.as_ref()
            && test_conn(conn)
        {
            if self.0.rtt_tx.try_send((addr, conn.rtt())).is_err() {
                tracing::debug!("could not send RTT for connection through sender");
            }
            return Ok(conn.clone());
        }

        // clear it, if there was one it didn't pass the test.
        *lock = None;

        let conn = self
            .measured_connect(addr, addr.ip().to_string(), traffic)
            .await?;
        *lock = Some(conn.clone());
        Ok(conn)
    }
}

const NO_ERROR: q::VarInt = q::VarInt::from_u32(0);

fn datagram_error_kind(e: &q::SendDatagramError) -> &'static str {
    match e {
        q::SendDatagramError::UnsupportedByPeer => "unsupported_by_peer",
        q::SendDatagramError::Disabled => "disabled",
        q::SendDatagramError::TooLarge => "too_large",
        q::SendDatagramError::ConnectionLost(_) => "connection_lost",
    }
}

#[inline]
fn test_conn(conn: &Connection) -> bool {
    use q::ConnectionError;

    match conn.close_reason() {
        None => true,
        Some(
            ConnectionError::TimedOut
            | ConnectionError::Reset
            | ConnectionError::LocallyClosed
            | ConnectionError::ApplicationClosed(q::ApplicationClose {
                error_code: NO_ERROR,
                ..
            }),
        ) => {
            // don't log, pretty normal stuff
            false
        }
        Some(error) => {
            tracing::warn!(%error, "cached connection was closed abnormally, reconnecting");
            false
        }
    }
}
