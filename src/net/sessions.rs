/*
 * Copyright 2020 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

use std::{
    collections::{HashMap, HashSet},
    fmt,
    net::SocketAddr,
    sync::{Arc, atomic},
    time::Duration,
};

use tokio::time::Instant;

use crate::{
    Loggable,
    config::filter::CachedFilterChain,
    filters::Filter,
    metrics,
    net::{
        PacketMut, PacketQueueSender,
        maxmind_db::{IpNetEntry, MetricsIpNetEntry},
        queue::SendPacket,
    },
    time::UtcTimestamp,
};

use parking_lot::RwLock;

pub(crate) mod inner_metrics;
pub mod quality;

pub type SessionMap = crate::collections::ttl::TtlMap<SessionKey, Session>;

/// What the send path needs from a session, all of it resolved when the session
/// was created.
pub(crate) struct SessionRoute {
    /// `GeoIP` information for the client, for metrics.
    pub asn_info: Option<MetricsIpNetEntry>,
    /// The cluster routing selected the destination from, as a label value.
    /// Empty for a cluster configured without a locality, or for a destination
    /// that didn't come from the cluster map at all.
    pub destination: Arc<str>,
    pub pending_sends: PacketQueueSender,
}

/// Responsible for managing sending processed traffic to its destination and
/// tracking metrics and other information about the session.
pub trait SessionManager {
    /// Sends `contents` upstream, `cluster` being the locality routing selected
    /// the destination from.
    fn send(
        &self,
        key: SessionKey,
        contents: bytes::Bytes,
        cluster: Option<crate::net::endpoint::Locality>,
    ) -> Result<(), super::PipelineError>;
}

#[derive(PartialEq, Eq, Hash)]
pub enum SessionError {
    SocketAddressUnavailable,
    MissingAllocatedSocket,
    MissingDestinationSocket,
}

impl std::error::Error for SessionError {}

impl fmt::Display for SessionError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::SocketAddressUnavailable => {
                f.write_str("couldn't get socket address from raw socket")
            }
            Self::MissingAllocatedSocket => {
                f.write_str("couldn't obtain any allocated socket, should be unreachable")
            }
            Self::MissingDestinationSocket => {
                f.write_str("couldn't obtain any socket for destination, should be unreachable")
            }
        }
    }
}

impl fmt::Debug for SessionError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{self}")
    }
}

/// A data structure that is responsible for holding sessions, and pooling
/// sockets between them. This means that we only provide new unique sockets
/// to new connections to the same gameserver, and we share sockets across
/// multiple gameservers.
///
/// Traffic from different gameservers is then demuxed using their address to
/// send back to the original client.
pub struct SessionPool {
    ports_to_sockets: RwLock<HashMap<u16, PacketQueueSender>>,
    storage: Arc<RwLock<SocketStorage>>,
    session_map: SessionMap,
    downstream_sends: Vec<PacketQueueSender>,
    downstream_index: atomic::AtomicUsize,
    cached_filter_chain: CachedFilterChain,
    /// Used to tell an endpoint disappearing from underneath a session apart from
    /// a player going quiet. Sessions are never reported as `endpoint_gone` when
    /// unset.
    clusters: Option<crate::config::Watch<crate::net::ClusterMap>>,
    max_sessions: usize,
    backend: crate::net::io::UdpBackend,
    pub ring_buffer_len: u16,
}

/// The wrapper struct responsible for holding all of the socket related mappings.
#[derive(Default)]
struct SocketStorage {
    destination_to_sockets: HashMap<SocketAddr, HashSet<u16>>,
    destination_to_sources: HashMap<(SocketAddr, u16), SocketAddr>,
    sources_to_asn_info: HashMap<SocketAddr, IpNetEntry>,
    sockets_to_destination: HashMap<u16, HashSet<SocketAddr>>,
    /// The cluster each destination belongs to, recorded when a session to it is
    /// created, since a packet arriving from upstream carries no routing decision
    /// to read it from.
    destination_to_cluster: HashMap<SocketAddr, Arc<str>>,
}

impl SessionPool {
    /// Constructs a new session pool, it's created with an `Arc` as that's
    /// required for the pool to provide a reference to the children to be able
    /// to release their sockets back to the parent.
    pub fn new(
        downstream_sends: Vec<PacketQueueSender>,
        cached_filter_chain: CachedFilterChain,
        clusters: Option<crate::config::Watch<crate::net::ClusterMap>>,
        max_sessions: usize,
        backend: crate::net::io::UdpBackend,
        ring_buffer_len: u16,
    ) -> Arc<Self> {
        const SESSION_TIMEOUT_SECONDS: Duration = Duration::from_secs(60);
        const SESSION_EXPIRY_POLL_INTERVAL: Duration = Duration::from_secs(60);

        Arc::new(Self {
            ports_to_sockets: <_>::default(),
            storage: <_>::default(),
            session_map: SessionMap::new(SESSION_TIMEOUT_SECONDS, SESSION_EXPIRY_POLL_INTERVAL),
            downstream_sends,
            downstream_index: atomic::AtomicUsize::new(0),
            cached_filter_chain,
            clusters,
            max_sessions,
            backend,
            ring_buffer_len,
        })
    }

    /// Allocates a new upstream socket from a new socket from the system.
    fn create_new_session_from_new_socket(
        self: &Arc<Self>,
        key: SessionKey,
        cluster: Option<crate::net::endpoint::Locality>,
    ) -> Result<SessionRoute, super::PipelineError> {
        tracing::trace!(source=%key.source, dest=%key.dest, "creating new socket for session");
        let raw_socket = crate::net::raw_socket_with_reuse(0)?;
        let port = raw_socket
            .local_addr()?
            .as_socket()
            .ok_or(SessionError::SocketAddressUnavailable)?
            .port();

        let (pending_sends, srecv) = crate::net::queue(15, self.backend)?;
        self.clone().spawn_session(
            raw_socket,
            port,
            (pending_sends.clone(), srecv),
            self.cached_filter_chain.clone(),
        )?;

        self.ports_to_sockets
            .write()
            .insert(port, pending_sends.clone());
        self.create_session_from_existing_socket(key, pending_sends, port, cluster)
    }

    pub(crate) fn process_received_upstream_packet(
        self: &Arc<Self>,
        packet: impl PacketMut,
        mut recv_addr: SocketAddr,
        port: u16,
        last_received_at: &mut Option<UtcTimestamp>,
        filters: &crate::filters::FilterChain,
    ) {
        let received_at = UtcTimestamp::now();
        recv_addr.set_ip(recv_addr.ip().to_canonical());
        let (downstream_addr, asn_info, cluster) = {
            let storage = self.storage.read();
            let Some(downstream_addr) = storage.destination_to_sources.get(&(recv_addr, port))
            else {
                tracing::debug!(address=%recv_addr, "received traffic from a server that has no downstream");
                return;
            };
            let asn_info = storage.sources_to_asn_info.get(downstream_addr);

            (
                *downstream_addr,
                asn_info.map(MetricsIpNetEntry::from),
                storage
                    .destination_to_cluster
                    .get(&recv_addr)
                    .cloned()
                    .unwrap_or_else(|| Arc::from("")),
            )
        };

        let asn_metric_info = asn_info.as_ref().into();

        if let Some(last_received_at) = last_received_at {
            metrics::set_packet_jitter(
                metrics::WRITE,
                &asn_metric_info,
                (received_at - *last_received_at).nanos(),
            );
        }
        *last_received_at = Some(received_at);

        let result = {
            let _timer = metrics::processing_time(metrics::WRITE).start_timer();
            Self::process_recv_packet(
                recv_addr,
                downstream_addr,
                asn_info,
                cluster,
                packet,
                filters,
            )
        };

        match result {
            Ok(packet) => {
                let index = self
                    .downstream_index
                    .fetch_add(1, atomic::Ordering::Relaxed)
                    % self.downstream_sends.len();
                // SAFETY: we've ensured it's within bounds via the %
                unsafe {
                    self.downstream_sends.get_unchecked(index).push(packet);
                }
            }
            Err((asn_info, cluster, error)) => {
                error.log();
                let asn_metric_info = asn_info.as_ref().into();

                metrics::packets_dropped_total(
                    metrics::WRITE,
                    error.drop_reason(),
                    error.filter_name(),
                    &cluster,
                )
                .inc();
                metrics::errors_total(metrics::WRITE, error.discriminant(), &asn_metric_info).inc();
            }
        }
    }

    /// Returns a reference to an existing session mapped to `key`, otherwise
    /// creates a new session either from a fresh socket, or if there are sockets
    /// allocated that are not reserved by an existing destination, using the
    /// existing socket.
    pub(crate) fn get(
        self: &Arc<Self>,
        key @ SessionKey { dest, .. }: SessionKey,
        cluster: Option<crate::net::endpoint::Locality>,
    ) -> Result<SessionRoute, super::PipelineError> {
        tracing::trace!(source=%key.source, dest=%key.dest, "SessionPool::get");
        // If we already have a session for the key pairing, return that session.
        if let Some(entry) = self.session_map.get(&key) {
            // The only point on the downstream path holding the session, so also
            // where its jitter estimate is updated
            entry.quality.record_arrival();

            return Ok(SessionRoute {
                asn_info: entry.asn_info.as_ref().map(MetricsIpNetEntry::from),
                destination: entry.destination.clone(),
                pending_sends: entry.pending_sends.clone(),
            });
        }

        if self.session_map.len() >= self.max_sessions {
            tracing::warn!(
                limit = self.max_sessions,
                source = %key.source,
                dest = %key.dest,
                "session limit reached, dropping packet"
            );
            inner_metrics::sessions_rejected_total().inc();
            return Err(super::PipelineError::SessionLimit);
        }

        // If there's a socket_set available, it means there are sockets
        // allocated to the address that we want to avoid.
        let storage = self.storage.read();
        let Some(socket_set) = storage.destination_to_sockets.get(&dest) else {
            drop(storage);
            let no_sockets = self.ports_to_sockets.read().is_empty();
            return if no_sockets {
                // Initial case where we have no allocated or reserved sockets.
                self.create_new_session_from_new_socket(key, cluster)
            } else {
                // Where we have no allocated sockets for a destination, assign
                // the first available one.
                let (port, sender) = self
                    .ports_to_sockets
                    .read()
                    .iter()
                    .next()
                    .map(|(port, socket)| (*port, socket.clone()))
                    .ok_or(SessionError::MissingAllocatedSocket)?;

                self.create_session_from_existing_socket(key, sender, port, cluster)
            };
        };

        let available_socket = self
            .ports_to_sockets
            .read()
            .iter()
            .find(|(port, _)| !socket_set.contains(port))
            .map(|(port, socket)| (*port, socket.clone()));

        if let Some((port, socket)) = available_socket {
            drop(storage);
            self.storage
                .write()
                .destination_to_sockets
                .get_mut(&dest)
                .ok_or(SessionError::MissingDestinationSocket)?
                .insert(port);
            self.create_session_from_existing_socket(key, socket, port, cluster)
        } else {
            drop(storage);
            self.create_new_session_from_new_socket(key, cluster)
        }
    }

    /// Using an existing socket, reserves the socket for a new session.
    fn create_session_from_existing_socket(
        self: &Arc<Self>,
        key: SessionKey,
        pending_sends: PacketQueueSender,
        socket_port: u16,
        cluster: Option<crate::net::endpoint::Locality>,
    ) -> Result<SessionRoute, super::PipelineError> {
        tracing::trace!(source=%key.source, dest=%key.dest, "reusing socket for session");
        // Interned once per session rather than per packet: the label is the
        // cluster routing chose, and a session's destination doesn't change
        let destination: Arc<str> = cluster
            .as_ref()
            .map_or("", |locality| locality.as_str())
            .into();

        // Resolved once here rather than at close, so a destination that was never
        // in the cluster map can't later look like one that vanished from it
        let tracked = self
            .clusters
            .as_ref()
            .is_some_and(|clusters| clusters.read().contains_endpoint(&key.dest));

        let asn_info = {
            let mut storage = self.storage.write();
            storage
                .destination_to_sockets
                .entry(key.dest)
                .or_default()
                .insert(socket_port);
            storage
                .sockets_to_destination
                .entry(socket_port)
                .or_default()
                .insert(key.dest);
            storage
                .destination_to_sources
                .insert((key.dest, socket_port), key.source);

            // The upstream receive path has no routing decision to read the
            // cluster from, so it reads it back from here
            drop(
                storage
                    .destination_to_cluster
                    .insert(key.dest, destination.clone()),
            );

            let asn_info = crate::net::maxmind_db::MaxmindDb::lookup(key.source.ip());

            if let Some(asn_info) = &asn_info {
                storage
                    .sources_to_asn_info
                    .insert(key.source, asn_info.clone());
            }

            asn_info
        };

        let asn_metrics_info = asn_info.as_ref().map(MetricsIpNetEntry::from);

        let session = Session::new(
            key,
            pending_sends.clone(),
            socket_port,
            self.clone(),
            asn_info,
            destination.clone(),
            tracked,
        );
        tracing::trace!("inserting session into map");
        self.session_map.insert(key, session);
        tracing::trace!("session inserted");
        Ok(SessionRoute {
            asn_info: asn_metrics_info,
            destination,
            pending_sends,
        })
    }

    /// Processes a packet that is received by this session.
    #[allow(clippy::type_complexity)]
    fn process_recv_packet<P: PacketMut>(
        source: SocketAddr,
        dest: SocketAddr,
        asn_info: Option<MetricsIpNetEntry>,
        cluster: Arc<str>,
        packet: P,
        filters: &crate::filters::FilterChain,
    ) -> Result<SendPacket, (Option<MetricsIpNetEntry>, Arc<str>, Error)> {
        tracing::trace!(%source, %dest, length = packet.len(), "received packet from upstream");

        let mut context = crate::filters::WriteContext::new(source.into(), dest.into(), packet);

        if let Err(err) = filters.write(&mut context) {
            return Err((asn_info, cluster, err.into()));
        }

        Ok(SendPacket {
            data: context.contents.freeze(),
            destination: dest,
            asn_info,
            // The traffic is from the gameserver this session is routed to, so it
            // belongs to the same cluster as the downstream direction
            cluster,
        })
    }

    /// Returns a map of active sessions.
    pub fn sessions(&self) -> &SessionMap {
        &self.session_map
    }

    /// Sends packet data to the appropiate session based on its `key`.
    #[inline]
    pub fn send(
        self: &Arc<Self>,
        key: SessionKey,
        packet: bytes::Bytes,
        cluster: Option<crate::net::endpoint::Locality>,
    ) -> Result<(), super::PipelineError> {
        self.send_inner(key, packet, cluster)?;
        Ok(())
    }

    /// A separate function for a unit test below
    #[inline]
    fn send_inner(
        self: &Arc<Self>,
        key: SessionKey,
        packet: bytes::Bytes,
        cluster: Option<crate::net::endpoint::Locality>,
    ) -> Result<PacketQueueSender, super::PipelineError> {
        let SessionRoute {
            asn_info,
            destination,
            pending_sends,
        } = self.get(key, cluster)?;

        pending_sends.push(SendPacket {
            destination: key.dest,
            data: packet,
            asn_info,
            cluster: destination,
        });
        Ok(pending_sends)
    }

    /// Spawns a session I/O loop for the given socket, dispatching to the
    /// correct backend based on the pool's configured backend.
    pub(crate) fn spawn_session(
        self: Arc<Self>,
        raw_socket: socket2::Socket,
        port: u16,
        pending_sends: crate::net::PacketQueue,
        filters: crate::config::filter::CachedFilterChain,
    ) -> Result<(), super::PipelineError> {
        use crate::net::io::UdpBackend;
        match self.backend {
            #[cfg(target_os = "linux")]
            UdpBackend::Completion => crate::net::io::completion::io_uring::spawn_session(
                self,
                raw_socket,
                port,
                pending_sends,
                filters,
            ),
            _ => crate::net::io::poll::tokio::spawn_session(
                self,
                raw_socket,
                port,
                pending_sends,
                filters,
            ),
        }
    }

    /// Returns whether the pool contains any sockets allocated to a destination.
    #[cfg(test)]
    fn has_no_allocated_sockets(&self) -> bool {
        let storage = self.storage.read();
        let is_empty = storage.destination_to_sockets.is_empty();
        // These should always be the same.
        debug_assert!(!(is_empty ^ storage.sockets_to_destination.is_empty()));
        is_empty
    }

    /// Forces removal of session to make testing quicker.
    #[cfg(test)]
    async fn drop_session(&self, key: SessionKey) -> bool {
        let is_removed = self.session_map.remove(key);
        // Sleep because there's no async drop.
        tokio::time::sleep(Duration::from_millis(100)).await;
        is_removed
    }

    /// Handles the logic of releasing a socket back into the pool.
    fn release_socket(
        self: Arc<Self>,
        SessionKey {
            ref source,
            ref dest,
        }: SessionKey,
        port: u16,
    ) {
        tracing::trace!("releasing socket");
        let mut storage = self.storage.write();
        let Some(socket_set) = storage.destination_to_sockets.get_mut(dest) else {
            return;
        };

        socket_set.remove(&port);

        if socket_set.is_empty() {
            storage.destination_to_sockets.remove(dest);
        }

        let Some(dest_set) = storage.sockets_to_destination.get_mut(&port) else {
            return;
        };

        dest_set.remove(dest);

        if dest_set.is_empty() {
            storage.sockets_to_destination.remove(&port);
        }

        // Not asserted because the source might not have GeoIP info.
        storage.sources_to_asn_info.remove(source);
        storage.destination_to_sources.remove(&(*dest, port));

        // Only once no session is left using the destination, since the locality
        // is shared by all of them
        if !storage.destination_to_sockets.contains_key(dest) {
            storage.destination_to_cluster.remove(dest);
        }
        tracing::trace!("socket released");
    }
}

impl SessionManager for Arc<SessionPool> {
    fn send(
        &self,
        key: SessionKey,
        contents: bytes::Bytes,
        cluster: Option<crate::net::endpoint::Locality>,
    ) -> Result<(), super::PipelineError> {
        SessionPool::send(self, key, contents, cluster)
    }
}

impl Drop for SessionPool {
    fn drop(&mut self) {
        let map = std::mem::take(&mut self.session_map);
        std::thread::spawn(move || {
            drop(map);
        });
    }
}

/// Session encapsulates a UDP stream session
pub struct Session {
    /// The time at which the session was created
    created_at: Instant,
    /// The source and destination pair.
    key: SessionKey,
    /// The socket port of the session.
    socket_port: u16,
    /// The queue of packets being sent to the upstream (server)
    pending_sends: PacketQueueSender,
    /// The `GeoIP` information of the source.
    asn_info: Option<IpNetEntry>,
    /// The cluster routing selected the destination from, as a label value.
    destination: Arc<str>,
    /// Whether the destination was in the cluster map when the session was
    /// created. When it wasn't, its absence later says nothing.
    tracked: bool,
    /// Interarrival jitter of this session, folded into aggregate metrics by
    /// [`quality::spawn_aggregator`].
    quality: quality::SessionQualityHandle,
    /// The socket pool of the session.
    pool: Arc<SessionPool>,
}

impl Session {
    pub(crate) fn new(
        key: SessionKey,
        pending_sends: PacketQueueSender,
        socket_port: u16,
        pool: Arc<SessionPool>,
        asn_info: Option<IpNetEntry>,
        destination: Arc<str>,
        tracked: bool,
    ) -> Self {
        let s = Self {
            key,
            pending_sends,
            pool,
            socket_port,
            quality: quality::SessionQualityHandle::register(asn_info.as_ref()),
            asn_info,
            destination,
            tracked,
            created_at: Instant::now(),
        };

        if let Some(asn) = &s.asn_info {
            tracing::debug!(
                number = asn.id,
                organization = asn.as_name,
                country_code = asn.as_cc,
                prefix = asn.prefix,
                prefix_entity = asn.prefix_entity,
                prefix_name = asn.prefix_name,
                "maxmind information"
            );
        }

        inner_metrics::total_sessions().inc();
        s.active_session_metric().inc();
        tracing::debug!(source = %key.source, dest = %key.dest, "Session created");
        s
    }

    fn active_session_metric(&self) -> prometheus::IntGauge {
        inner_metrics::active_sessions(self.asn_info.as_ref())
    }

    /// Why this session is ending.
    ///
    /// UDP has no close, so a player leaving is indistinguishable from one going
    /// quiet and is reported as an idle timeout. What is worth separating out is
    /// the endpoint having gone away underneath the session, and the proxy itself
    /// going away.
    fn close_reason(&self) -> inner_metrics::CloseReason {
        if crate::metrics::shutdown_initiated().get() != 0 {
            return inner_metrics::CloseReason::Shutdown;
        }

        // Only meaningful for a destination that was in the cluster map when the
        // session was created
        if self.tracked
            && let Some(clusters) = &self.pool.clusters
            && !clusters.read().contains_endpoint(&self.key.dest)
        {
            return inner_metrics::CloseReason::EndpointGone;
        }

        inner_metrics::CloseReason::IdleTimeout
    }

    fn release(&mut self) {
        let reason = self.close_reason();
        self.active_session_metric().dec();
        inner_metrics::sessions_closed_total(reason).inc();
        inner_metrics::duration_secs().observe(self.created_at.elapsed().as_secs() as f64);
        tracing::debug!(source = %self.key.source, dest_address = %self.key.dest, ?reason, "Session closed");
        SessionPool::release_socket(self.pool.clone(), self.key, self.socket_port);
    }
}

impl Drop for Session {
    fn drop(&mut self) {
        self.release();
    }
}

// A (source, destination) address pair that uniquely identifies a session.
#[derive(Clone, Copy, Eq, Hash, PartialEq, Debug, PartialOrd, Ord)]
pub struct SessionKey {
    pub source: SocketAddr,
    pub dest: SocketAddr,
}

impl From<(SocketAddr, SocketAddr)> for SessionKey {
    fn from((source, dest): (SocketAddr, SocketAddr)) -> Self {
        Self { source, dest }
    }
}

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("filter {0}")]
    Filter(#[from] crate::filters::FilterError),
}

impl Error {
    /// Bounded value suitable for use as a metric label
    #[inline]
    pub fn discriminant(&self) -> &'static str {
        match self {
            Self::Filter(fe) => fe.discriminant(),
        }
    }

    #[inline]
    pub fn drop_reason(&self) -> crate::metrics::DropReason {
        match self {
            Self::Filter(fe) => fe.drop_reason(),
        }
    }

    #[inline]
    pub fn filter_name(&self) -> &'static str {
        match self {
            Self::Filter(fe) => fe.filter_name(),
        }
    }
}

impl Loggable for Error {
    #[inline]
    fn log(&self) {
        tracing::error!("{self}");
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test::{AddressType, TestHelper, available_addr};
    use std::sync::Arc;

    async fn new_pool() -> (Arc<SessionPool>, PacketQueueSender) {
        let backend = crate::net::io::UdpBackend::default();
        let (pending_sends, _srecv) = crate::net::queue(1, backend).unwrap();
        let fake = crate::config::filter::FilterChainConfig::default();
        (
            SessionPool::new(
                vec![pending_sends.clone()],
                fake.cached(),
                None,
                usize::MAX,
                backend,
                64,
            ),
            pending_sends,
        )
    }

    /// A pool with a cluster map holding `dest`, so destination attribution and
    /// endpoint-gone detection are both live.
    async fn new_pool_with_cluster(
        dest: SocketAddr,
        locality: Option<crate::net::endpoint::Locality>,
    ) -> (
        Arc<SessionPool>,
        crate::config::Watch<crate::net::ClusterMap>,
    ) {
        let backend = crate::net::io::UdpBackend::default();
        let (pending_sends, _srecv) = crate::net::queue(1, backend).unwrap();
        let fake = crate::config::filter::FilterChainConfig::default();

        let clusters = crate::config::Watch::new(crate::net::ClusterMap::default());
        clusters.read().insert(
            None,
            locality,
            [crate::net::endpoint::Endpoint::new(dest.into())].into(),
        );

        let pool = SessionPool::new(
            vec![pending_sends],
            fake.cached(),
            Some(clusters.clone()),
            usize::MAX,
            backend,
            64,
        );

        (pool, clusters)
    }

    #[tokio::test]
    async fn sessions_carry_the_cluster_routing_chose() {
        let dest: SocketAddr = (std::net::Ipv4Addr::LOCALHOST, 8090u16).into();
        let locality = crate::net::endpoint::Locality::with_region("session-locality-test");
        let (pool, _clusters) = new_pool_with_cluster(dest, Some(locality.clone())).await;

        let key: SessionKey = ((std::net::Ipv4Addr::LOCALHOST, 8091u16).into(), dest).into();
        let route = pool.get(key, Some(locality.clone())).unwrap();

        assert_eq!(&*route.destination, locality.to_string().as_str());
    }

    #[tokio::test]
    async fn a_destination_outside_the_cluster_map_is_not_tracked() {
        let known: SocketAddr = (std::net::Ipv4Addr::LOCALHOST, 8092u16).into();
        let (pool, _clusters) = new_pool_with_cluster(known, None).await;

        let unknown: SocketAddr = (std::net::Ipv4Addr::LOCALHOST, 8093u16).into();
        let key: SessionKey = ((std::net::Ipv4Addr::LOCALHOST, 8094u16).into(), unknown).into();
        drop(pool.get(key, None).unwrap());

        // Never in the cluster map, so its absence at close says nothing
        assert!(!pool.session_map.get(&key).unwrap().tracked);
    }

    #[tokio::test]
    async fn a_destination_routed_without_a_cluster_still_detects_it_vanishing() {
        let dest: SocketAddr = (std::net::Ipv4Addr::LOCALHOST, 8099u16).into();
        let locality = crate::net::endpoint::Locality::with_region("no-cluster-gone-test");
        let (pool, clusters) = new_pool_with_cluster(dest, Some(locality.clone())).await;

        // Routed without a cluster, as the decryptor filter does when it decodes a
        // destination out of the packet itself
        let key: SessionKey = ((std::net::Ipv4Addr::LOCALHOST, 8100u16).into(), dest).into();
        let route = pool.get(key, None).unwrap();

        assert!(route.destination.is_empty());

        clusters.read().remove_locality(None, &Some(locality));
        assert_eq!(
            pool.session_map.get(&key).unwrap().close_reason(),
            inner_metrics::CloseReason::EndpointGone
        );
    }

    #[tokio::test]
    async fn a_session_whose_endpoint_vanished_closes_as_endpoint_gone() {
        let dest: SocketAddr = (std::net::Ipv4Addr::LOCALHOST, 8095u16).into();
        let locality = crate::net::endpoint::Locality::with_region("endpoint-gone-test");
        let (pool, clusters) = new_pool_with_cluster(dest, Some(locality.clone())).await;

        let key: SessionKey = ((std::net::Ipv4Addr::LOCALHOST, 8096u16).into(), dest).into();
        drop(pool.get(key, None).unwrap());

        let session = pool.session_map.get(&key).unwrap();
        assert_eq!(
            session.close_reason(),
            inner_metrics::CloseReason::IdleTimeout
        );

        clusters.read().remove_locality(None, &Some(locality));
        assert_eq!(
            session.close_reason(),
            inner_metrics::CloseReason::EndpointGone
        );
    }

    #[tokio::test]
    async fn a_session_records_arrivals_for_its_jitter_estimate() {
        let (pool, _receiver) = new_pool().await;
        let key: SessionKey = (
            (std::net::Ipv4Addr::LOCALHOST, 8097u16).into(),
            (std::net::Ipv4Addr::UNSPECIFIED, 8098u16).into(),
        )
            .into();

        // The first `get` creates the session, subsequent ones are packets
        drop(pool.get(key, None).unwrap());
        drop(pool.get(key, None).unwrap());
        drop(pool.get(key, None).unwrap());

        let session = pool.session_map.get(&key).unwrap();
        assert_eq!(session.quality.packets_since_last_sample(), 2);
    }

    #[tokio::test]
    async fn insert_and_release_single_socket() {
        let (pool, _receiver) = new_pool().await;
        let key = (
            (std::net::Ipv4Addr::LOCALHOST, 8080u16).into(),
            (std::net::Ipv4Addr::UNSPECIFIED, 8080u16).into(),
        )
            .into();

        let _session = pool.get(key, None).unwrap();

        assert!(pool.drop_session(key).await);

        assert!(pool.has_no_allocated_sockets());
    }

    #[tokio::test]
    async fn insert_and_release_multiple_sockets() {
        let (pool, _receiver) = new_pool().await;
        let key1 = (
            (std::net::Ipv4Addr::LOCALHOST, 8080u16).into(),
            (std::net::Ipv4Addr::UNSPECIFIED, 8080u16).into(),
        )
            .into();
        let key2 = (
            (std::net::Ipv4Addr::LOCALHOST, 8081u16).into(),
            (std::net::Ipv4Addr::UNSPECIFIED, 8080u16).into(),
        )
            .into();

        let _session1 = pool.get(key1, None).unwrap();
        let _session2 = pool.get(key2, None).unwrap();

        assert!(pool.drop_session(key1).await);
        assert!(!pool.has_no_allocated_sockets());
        assert!(pool.drop_session(key2).await);

        assert!(pool.has_no_allocated_sockets());
        drop(pool);
    }

    #[tokio::test]
    async fn same_address_uses_different_sockets() {
        let (pool, _receiver) = new_pool().await;
        let key1 = (
            (std::net::Ipv4Addr::LOCALHOST, 8080u16).into(),
            (std::net::Ipv4Addr::UNSPECIFIED, 8080u16).into(),
        )
            .into();
        let key2 = (
            (std::net::Ipv4Addr::LOCALHOST, 8081u16).into(),
            (std::net::Ipv4Addr::UNSPECIFIED, 8080u16).into(),
        )
            .into();

        let _socket1 = pool.get(key1, None).unwrap();
        let _socket2 = pool.get(key2, None).unwrap();
        assert_ne!(
            pool.session_map.get(&key1).unwrap().socket_port,
            pool.session_map.get(&key2).unwrap().socket_port
        );

        assert!(pool.drop_session(key1).await);
        assert!(pool.drop_session(key2).await);
    }

    #[tokio::test]
    async fn different_addresses_uses_same_socket() {
        let (pool, _receiver) = new_pool().await;
        let key1 = (
            (std::net::Ipv4Addr::LOCALHOST, 8080u16).into(),
            (std::net::Ipv4Addr::UNSPECIFIED, 8080u16).into(),
        )
            .into();
        let key2 = (
            (std::net::Ipv4Addr::LOCALHOST, 8081u16).into(),
            (std::net::Ipv4Addr::UNSPECIFIED, 8081u16).into(),
        )
            .into();

        let _socket1 = pool.get(key1, None).unwrap();
        let _socket2 = pool.get(key2, None).unwrap();

        assert_eq!(
            pool.session_map.get(&key1).unwrap().socket_port,
            pool.session_map.get(&key2).unwrap().socket_port
        );
    }

    #[tokio::test]
    async fn spawn_safe_same_destination() {
        let (pool, _receiver) = new_pool().await;
        let key1 = (
            (std::net::Ipv4Addr::LOCALHOST, 8080u16).into(),
            (std::net::Ipv4Addr::UNSPECIFIED, 8080u16).into(),
        )
            .into();
        let key2 = (
            (std::net::Ipv4Addr::LOCALHOST, 8080u16).into(),
            (std::net::Ipv4Addr::UNSPECIFIED, 8080u16).into(),
        )
            .into();

        let socket1 = pool.get(key1, None).unwrap();

        let task = tokio::spawn(async move {
            drop(socket1);
        });

        let _socket2 = pool.get(key2, None).unwrap();

        task.await.unwrap();
    }

    #[tokio::test]
    async fn spawn_safe_different_destination() {
        let (pool, _receiver) = new_pool().await;
        let key1 = (
            (std::net::Ipv4Addr::LOCALHOST, 8080u16).into(),
            (std::net::Ipv4Addr::UNSPECIFIED, 8080u16).into(),
        )
            .into();
        let key2 = (
            (std::net::Ipv4Addr::LOCALHOST, 8081u16).into(),
            (std::net::Ipv4Addr::UNSPECIFIED, 8081u16).into(),
        )
            .into();

        let socket1 = pool.get(key1, None).unwrap();

        let task = tokio::spawn(async move {
            drop(socket1);
        });

        let _socket2 = pool.get(key2, None).unwrap();

        task.await.unwrap();
    }

    fn pool_with_limit(limit: usize) -> (Arc<SessionPool>, PacketQueueSender) {
        let backend = crate::net::io::UdpBackend::default();
        let (pending_sends, _srecv) = crate::net::queue(1, backend).unwrap();
        let fake = crate::config::filter::FilterChainConfig::default();
        (
            SessionPool::new(
                vec![pending_sends.clone()],
                fake.cached(),
                None,
                limit,
                backend,
                64,
            ),
            pending_sends,
        )
    }

    fn key(source_port: u16, dest_port: u16) -> SessionKey {
        (
            (std::net::Ipv4Addr::LOCALHOST, source_port).into(),
            (std::net::Ipv4Addr::UNSPECIFIED, dest_port).into(),
        )
            .into()
    }

    #[tokio::test]
    async fn session_limit_rejects_new_sessions_at_capacity() {
        let (pool, _receiver) = pool_with_limit(2);

        assert!(pool.get(key(8080, 9000), None).is_ok());
        assert!(pool.get(key(8081, 9000), None).is_ok());

        assert!(matches!(
            pool.get(key(8082, 9000), None),
            Err(super::super::PipelineError::SessionLimit)
        ));
    }

    #[tokio::test]
    async fn session_limit_allows_existing_session_at_capacity() {
        let (pool, _receiver) = pool_with_limit(1);

        assert!(pool.get(key(8080, 9000), None).is_ok());

        // Limit hit — new session rejected.
        assert!(matches!(
            pool.get(key(8081, 9000), None),
            Err(super::super::PipelineError::SessionLimit)
        ));

        // But re-fetching the existing session must still succeed.
        assert!(pool.get(key(8080, 9000), None).is_ok());
    }

    #[tokio::test]
    async fn session_limit_recovers_after_session_removed() {
        let (pool, _receiver) = pool_with_limit(1);

        assert!(pool.get(key(8080, 9000), None).is_ok());
        assert!(matches!(
            pool.get(key(8081, 9000), None),
            Err(super::super::PipelineError::SessionLimit)
        ));

        pool.drop_session(key(8080, 9000)).await;

        // Slot freed — a new session should be accepted.
        assert!(pool.get(key(8081, 9000), None).is_ok());
    }

    #[tokio::test]
    #[cfg_attr(target_os = "macos", ignore)]
    async fn send_and_recv() {
        let mut t = TestHelper::default();
        let dest = t.run_echo_server(AddressType::Ipv6).await;
        let mut dest = dest.to_socket_addr().unwrap();
        crate::test::map_addr_to_localhost(&mut dest);
        let source = available_addr(AddressType::Ipv6).await;
        let socket = tokio::net::UdpSocket::bind(source).await.unwrap();
        let mut source = socket.local_addr().unwrap();
        crate::test::map_addr_to_localhost(&mut source);
        let (pool, _pending_sends) = new_pool().await;

        let key: SessionKey = (source, dest).into();
        let msg = b"helloworld";

        let pending = pool
            .send_inner(key, bytes::Bytes::from_static(msg), None)
            .unwrap();
        let pending = pending.swap(Vec::new());

        assert_eq!(msg, &*pending[0].data);
    }
}
