use crate::{
    filters::{self, Filter as _},
    metrics::{self, AsnInfo},
    net::{
        EndpointAddress,
        error::PipelineError,
        maxmind_db::{self, IpNetEntry},
        sessions::inner_metrics as session_metrics,
    },
    time::UtcTimestamp,
};
pub use quilkin_xdp::xdp;
use quilkin_xdp::xdp::{
    Umem,
    packet::{
        Packet, PacketError, csum,
        net_types::{IpAddresses, IpHdr, Ipv4Hdr, NetworkU16, UdpHdr, UdpHeaders},
    },
    slab::{Slab, StackSlab},
};
use std::{
    collections::hash_map::Entry,
    net::{IpAddr, SocketAddr},
    sync::{
        Arc,
        atomic::{AtomicU16, Ordering},
    },
    time::Instant,
};

/// Wrapper around the actual packet buffer and the UDP metadata it parsed to
/// so that we can satisify the filter traits
struct PacketWrapper {
    buffer: Packet,
    headers: UdpHeaders,
    /// A modification a filter requested that couldn't be applied, the packet is
    /// dropped rather than forwarded partially modified
    failure: Option<&'static str>,
}

impl PacketWrapper {
    #[inline]
    fn new(buffer: Packet, headers: UdpHeaders) -> Self {
        Self {
            buffer,
            headers,
            failure: None,
        }
    }

    /// Records a modification that couldn't be applied, keeping the first
    #[inline]
    fn fail(&mut self, failure: &'static str) {
        self.failure.get_or_insert(failure);
    }

    /// Shrinks the data payload, recording `failure` if that would move the tail
    /// into the headers
    #[inline]
    fn trim(&mut self, length: usize, failure: &'static str) {
        if length > self.headers.data_length() || self.buffer.adjust_tail(-(length as i32)).is_err()
        {
            self.fail(failure);
            return;
        }

        self.headers.data.end -= length;
    }
}

impl filters::Packet for PacketWrapper {
    #[inline]
    fn as_slice(&self) -> &[u8] {
        &self.buffer[self.headers.data.start..self.headers.data.end]
    }

    #[inline]
    fn len(&self) -> usize {
        self.headers.data_length()
    }
}

impl filters::PacketMut for PacketWrapper {
    #[inline]
    fn extend_head(&mut self, bytes: &[u8]) {
        if self.buffer.insert(self.headers.data.start, bytes).is_err() {
            self.fail("filter::extend head");
            return;
        }

        self.headers.data.end += bytes.len();
    }

    #[inline]
    fn extend_tail(&mut self, bytes: &[u8]) {
        if self.buffer.append(bytes).is_err() {
            self.fail("filter::extend tail");
            return;
        }

        self.headers.data.end += bytes.len();
    }

    #[inline]
    fn remove_head(&mut self, length: usize) {
        if length == 0 {
            return;
        }

        if length > self.headers.data_length() || self.headers.data.end > self.buffer.len() {
            self.fail("filter::remove head");
            return;
        }

        // Shift the payload down over the removed bytes, the headers are rewritten
        // before the packet is sent
        self.buffer.copy_within(
            self.headers.data.start + length..self.headers.data.end,
            self.headers.data.start,
        );
        self.trim(length, "filter::remove head");
    }

    #[inline]
    fn remove_tail(&mut self, length: usize) {
        self.trim(length, "filter::remove tail");
    }

    // Only used in the io-uring/reference implementations
    fn freeze(self) -> bytes::Bytes {
        unreachable!();
    }
}

use crate::config;

#[derive(Clone)]
pub struct ConfigState {
    pub filters: config::filter::CachedFilterChain,
    pub clusters: config::Watch<crate::net::ClusterMap>,
}

/// Matches the default session TTL
const ASN_CACHE_TTL: i64 = 60 * 1_000_000_000;

struct AsnCacheEntry {
    asn: Option<(IpNetEntry, maxmind_db::Asn)>,
    last_access: std::cell::Cell<i64>,
}

/// Cache of maxmind lookups keyed by client IP, entries not accessed within
/// [`ASN_CACHE_TTL`] are evicted by [`Self::sweep`]
#[derive(Default)]
pub struct AsnCache {
    map: std::collections::HashMap<IpAddr, AsnCacheEntry>,
    next_sweep: i64,
}

impl AsnCache {
    #[inline]
    fn get(&self, ip: &IpAddr, now: i64) -> Option<&(IpNetEntry, maxmind_db::Asn)> {
        let entry = self.map.get(ip)?;
        entry.last_access.set(now);
        entry.asn.as_ref()
    }

    #[inline]
    fn get_or_insert_with(
        &mut self,
        ip: IpAddr,
        now: i64,
        lookup: impl FnOnce() -> Option<(IpNetEntry, maxmind_db::Asn)>,
    ) -> Option<&(IpNetEntry, maxmind_db::Asn)> {
        let entry = self.map.entry(ip).or_insert_with(|| AsnCacheEntry {
            asn: lookup(),
            last_access: std::cell::Cell::new(now),
        });
        entry.last_access.set(now);
        entry.asn.as_ref()
    }

    /// Evicts idle entries, at most once per TTL interval
    #[inline]
    fn sweep(&mut self, now: i64) {
        if now < self.next_sweep {
            return;
        }
        self.next_sweep = now + ASN_CACHE_TTL;
        self.map
            .retain(|_, entry| now - entry.last_access.get() < ASN_CACHE_TTL);
    }

    #[cfg(test)]
    fn len(&self) -> usize {
        self.map.len()
    }
}

pub struct State {
    /// The external port is how we determine if packets come from clients (downstream)
    /// or servers (upstream)
    pub external_port: NetworkU16,
    pub qcmp_port: NetworkU16,
    pub destinations: Vec<EndpointAddress>,
    pub addr_to_asn: AsnCache,
    pub sessions: Arc<SessionState>,
    pub local_ipv4: std::net::Ipv4Addr,
    pub local_ipv6: std::net::Ipv6Addr,
    pub last_receive: UtcTimestamp,
}

impl State {
    /// Maps a remote server (upstream) endpoint back to the client endpoint
    /// that initiated the session
    #[inline]
    fn lookup_client(
        &self,
        server_addr: SocketAddr,
        port: NetworkU16,
    ) -> Option<(SocketAddr, AsnInfo<'_>)> {
        let addr = self.sessions.lookup_client(server_addr, port)?;
        let entry = self
            .addr_to_asn
            .get(&addr.ip(), self.last_receive.unix_nanos())
            .map_or(metrics::EMPTY, |(ipe, asn)| AsnInfo {
                prefix: &ipe.prefix,
                asn: asn.as_str(),
            });

        Some((addr, entry))
    }

    /// Retrieves or creates a session, ie a mapping of a server endpoint + port
    /// to a client endpoint
    #[inline]
    fn session(
        &mut self,
        client_addr: SocketAddr,
        server_addr: SocketAddr,
    ) -> (NetworkU16, AsnInfo<'_>, IpAddresses) {
        let ips = self.ips(server_addr.ip());
        let asn = self.addr_to_asn.get_or_insert_with(
            client_addr.ip(),
            self.last_receive.unix_nanos(),
            || {
                let ipe = maxmind_db::MaxmindDb::lookup(client_addr.ip());
                ipe.map(|ipe| {
                    let asn = maxmind_db::Asn::new(ipe.id);
                    (ipe, asn)
                })
            },
        );

        let port = self
            .sessions
            .get_or_create(client_addr, server_addr, asn.map(|(ipe, _)| ipe));

        (
            port,
            asn.map_or(metrics::EMPTY, |(ipe, asn)| AsnInfo {
                prefix: &ipe.prefix,
                asn: asn.as_str(),
            }),
            ips,
        )
    }

    #[inline]
    fn ips(&self, destination: IpAddr) -> IpAddresses {
        match destination {
            IpAddr::V4(destination) => IpAddresses::V4 {
                source: self.local_ipv4,
                destination,
            },
            IpAddr::V6(destination) => IpAddresses::V6 {
                source: self.local_ipv6,
                destination,
            },
        }
    }
}

/// Linux by default only allocates ephemeral ports between 32768..=60999
/// (see `/proc/sys/net/ipv4/ip_local_port_range`), so we take advantage and only
/// allocate ports above that range. Note that we check that this range hasn't
/// been modified during XDP initialization, if that changes the port mapping
/// code could cause issues
const EPHEMERAL_RANGE_END: u16 = 61000;
/// With 18 bytes per address, this lets each bucket fit in < 2k
const BUCKET_SIZE: usize = 112;

#[repr(C)]
struct Item {
    octets: [u8; 16],
    port: u16,
}

impl Item {
    #[inline]
    fn set(&mut self, addr: SocketAddr) {
        match addr {
            SocketAddr::V4(v4) => {
                // We'll never be sending to multicast addresses, so use that
                // fact to encode that this is an ipv4 address
                self.octets[0] = 0xff;
                self.octets[12..].copy_from_slice(&v4.ip().octets());
            }
            SocketAddr::V6(v6) => {
                self.octets = v6.ip().octets();
            }
        }

        self.port = addr.port();
    }

    #[inline]
    fn get(&self) -> SocketAddr {
        if self.octets[0] == 0xff {
            (
                std::net::Ipv4Addr::new(
                    self.octets[12],
                    self.octets[13],
                    self.octets[14],
                    self.octets[15],
                ),
                self.port,
            )
                .into()
        } else {
            (std::net::Ipv6Addr::from(self.octets), self.port).into()
        }
    }
}

struct PortMap {
    buckets: Vec<[Item; BUCKET_SIZE]>,
}

impl PortMap {
    #[inline]
    fn new() -> Self {
        Self {
            // SAFETY: Item is POD
            buckets: vec![unsafe { std::mem::zeroed() }],
        }
    }

    #[inline]
    fn get(&self, port: NetworkU16) -> Option<SocketAddr> {
        // The eBPF program only routes ports we allocated, but don't rely on it
        let i = port.host().checked_sub(EPHEMERAL_RANGE_END)? as usize;
        let bucket = i / BUCKET_SIZE;

        let bucket = self.buckets.get(bucket)?;

        // SAFETY: We know the index is valid
        unsafe {
            let item = bucket.get_unchecked(i % BUCKET_SIZE);

            // A zero port means this item was never initialized
            if item.port == 0 {
                return None;
            }

            Some(item.get())
        }
    }

    #[inline]
    fn insert(&mut self, client_addr: SocketAddr, port: u16) {
        let i = (port - EPHEMERAL_RANGE_END) as usize;
        let bucket = i / BUCKET_SIZE;
        if self.buckets.len() == bucket {
            // SAFETY: POD
            self.buckets.push(unsafe { std::mem::zeroed() });
        }

        // SAFETY: We've guaranteed we have a bucket at the index, and the
        // bucket has a fixed size of initialized bytes ready
        unsafe {
            self.buckets
                .get_unchecked_mut(bucket)
                .get_unchecked_mut(i % BUCKET_SIZE)
                .set(client_addr);
        }
    }
}

struct ClientInfo {
    asn_info: Option<IpNetEntry>,
    created_at: Instant,
    /// The port used to identify this unique session to the IP owning this map
    port: NetworkU16,
}

struct PortMapper {
    /// Maps a client endpoint to the port used as the source port for sending
    /// to the server endpoint `Self` is associated with
    client_to_port: Arc<parking_lot::Mutex<std::collections::HashMap<SocketAddr, ClientInfo>>>,
    port_to_client: Arc<parking_lot::RwLock<PortMap>>,
    port: AtomicU16,
}

impl PortMapper {
    #[inline]
    fn new() -> Self {
        Self {
            client_to_port: Arc::new(Default::default()),
            port_to_client: Arc::new(parking_lot::RwLock::new(PortMap::new())),
            port: AtomicU16::new(EPHEMERAL_RANGE_END),
        }
    }

    #[inline]
    fn get_or_alloc(
        &self,
        client_addr: SocketAddr,
        asn: Option<&IpNetEntry>,
    ) -> Option<NetworkU16> {
        match self.client_to_port.lock().entry(client_addr) {
            Entry::Occupied(entry) => Some(entry.get().port),
            Entry::Vacant(entry) => {
                let port = self.port.fetch_add(1, Ordering::Relaxed);

                if port < EPHEMERAL_RANGE_END {
                    // This means we've overflowed
                    return None;
                }

                session_metrics::total_sessions().inc();
                session_metrics::active_sessions(asn).inc();

                self.port_to_client.write().insert(client_addr, port);

                let port = port.into();
                entry.insert(ClientInfo {
                    asn_info: asn.cloned(),
                    created_at: Instant::now(),
                    port,
                });
                Some(port)
            }
        }
    }

    #[inline]
    fn get_client(&self, port: NetworkU16) -> Option<SocketAddr> {
        self.port_to_client.read().get(port)
    }
}

impl Drop for PortMapper {
    fn drop(&mut self) {
        let lock = self.client_to_port.lock();

        let now = Instant::now();

        for client_info in lock.values() {
            session_metrics::active_sessions(client_info.asn_info.as_ref()).dec();
            session_metrics::duration_secs()
                .observe(now.duration_since(client_info.created_at).as_secs_f64());
        }
    }
}

pub struct SessionState {
    sessions: crate::collections::ttl::TtlMap<SocketAddr, PortMapper>,
}

#[allow(clippy::derivable_impls)]
impl Default for SessionState {
    fn default() -> Self {
        Self {
            sessions: Default::default(),
        }
    }
}

impl SessionState {
    /// Attempts to lookup a client endpoint based on the server endpoint that sent
    /// the packet to the specified port
    #[inline]
    fn lookup_client(&self, server_addr: SocketAddr, port: NetworkU16) -> Option<SocketAddr> {
        self.sessions
            .get(&server_addr)
            .and_then(|pm| pm.get_client(port))
    }

    /// Retrieves the port used to forward packets from the specified client
    /// endpoint to the specified server endpoint, pairing the port to the client
    /// for forwarding packets back from the server to the client
    #[inline]
    fn get_or_create(
        &self,
        client_addr: SocketAddr,
        server_addr: SocketAddr,
        asn: Option<&IpNetEntry>,
    ) -> NetworkU16 {
        let port = match self.sessions.entry(server_addr) {
            crate::collections::ttl::Entry::Occupied(entry) => {
                entry.get().get_or_alloc(client_addr, asn)
            }
            crate::collections::ttl::Entry::Vacant(entry) => {
                let pm = PortMapper::new();
                let port = pm.get_or_alloc(client_addr, asn);
                entry.insert(pm);
                port
            }
        };

        if let Some(port) = port {
            return port;
        }

        // This means that this server has allocated over 4535 ports, which...could?
        // happen in some scenarios, but for now we just emit a warning, nuke the current
        // mapping. This means that if the server is still active and sends packets
        // in the future, they will either be dropped since we don't know what
        // the client endpoint is any longer, or, slightly worse, a packet gets
        // redirected to a different client.
        self.sessions.remove(server_addr);
        self.get_or_create(client_addr, server_addr, asn)
    }
}

/// The minimum ethernet frame size, the 4 byte frame check sequence is stripped
/// before we see it
const MIN_ETHERNET_FRAME: usize = 60;

/// Parses the headers of a received frame, returning the reason it couldn't be
/// parsed, which is used as the drop metric label.
///
/// Ethernet pads frames to [`MIN_ETHERNET_FRAME`], so an ipv4 packet with a
/// payload below 18 bytes (eg. a 17 byte QCMP ping) carries trailing padding that
/// isn't part of the datagram, which we trim. Anything else the eBPF program
/// couldn't rule out, eg. a truncated frame, is rejected.
#[inline]
fn parse_headers(buffer: &mut Packet) -> Result<UdpHeaders, &'static str> {
    // Two iterations at most, the reparse is against an exactly sized frame
    for _ in 0..2 {
        let error = match UdpHeaders::parse_packet(buffer) {
            // Both the parse above and the checksums we calculate read the UDP
            // header at the offset a 20 byte ipv4 header puts it at, which the
            // parse itself doesn't validate
            Ok(Some(headers)) => {
                return match &headers.ip {
                    IpHdr::V4(v4) if usize::from(v4.internet_header_length()) != Ipv4Hdr::LEN => {
                        Err("ipv4 header length")
                    }
                    _ => Ok(headers),
                };
            }
            Ok(None) => return Err("non-UDP packet"),
            Err(error) => error,
        };

        // `offset` is where the UDP header starts and `size` the datagram length,
        // so anything past their sum is padding
        let PacketError::InsufficientData {
            offset,
            size,
            length,
        } = error
        else {
            return Err(error.discriminant());
        };

        let Some(padding) = length.checked_sub(offset + size) else {
            return Err("truncated packet");
        };

        // Padding only ever exists to reach the minimum frame size
        if padding == 0 || length > MIN_ETHERNET_FRAME {
            return Err("trailing data");
        }

        if let Err(error) = buffer.adjust_tail(-(padding as i32)) {
            return Err(error.discriminant());
        }
    }

    Err("malformed packet")
}

/// Returns the frame to be dropped if a filter failed, or requested a
/// modification that couldn't be applied
#[inline]
fn filtered(
    result: Result<(), filters::FilterError>,
    packet: PacketWrapper,
) -> Result<PacketWrapper, (PipelineError, Packet)> {
    let error = match result {
        Ok(()) => match packet.failure {
            None => return Ok(packet),
            Some(failure) => filters::FilterError::Custom(failure),
        },
        Err(error) => error,
    };

    Err((PipelineError::Filter(error), packet.buffer))
}

#[inline]
pub fn process_packets<const RXN: usize, const TXN: usize>(
    rx_slab: &mut StackSlab<RXN>,
    umem: &mut Umem,
    tx_slab: &mut StackSlab<TXN>,
    config_state: &mut ConfigState,
    state: &mut State,
) {
    let filters = config_state.filters.load();
    let cm = config_state.clusters.clone_value();

    let now = UtcTimestamp::now();
    let jitter = (now - state.last_receive).nanos();
    state.last_receive = now;
    state.addr_to_asn.sweep(now.unix_nanos());
    let mut had_read = false;

    while let Some(mut buffer) = rx_slab.pop_back() {
        // This indicates a packet that is split, which we don't handle _at all_
        // right now, and only the first buffer has headers, so check before parsing
        if buffer.is_continued() {
            metrics::packets_dropped_total(metrics::READ, "split packet", &metrics::EMPTY).inc();
            umem.free_packet(buffer);
            continue;
        }

        let headers = match parse_headers(&mut buffer) {
            Ok(headers) => headers,
            Err(reason) => {
                tracing::debug!(reason, length = buffer.len(), "dropped unparsable packet");
                metrics::packets_dropped_total(metrics::READ, reason, &metrics::EMPTY).inc();
                umem.free_packet(buffer);
                continue;
            }
        };

        if headers.udp.destination == state.qcmp_port {
            process_qcmp_packet(buffer, headers, umem, tx_slab);
            continue;
        }

        let is_client = headers.udp.destination == state.external_port;
        let direction = if is_client {
            had_read = true;
            metrics::READ
        } else {
            metrics::WRITE
        };

        let packet = PacketWrapper::new(buffer, headers);

        let res = {
            let _timer = metrics::processing_time(direction).start_timer();

            if is_client {
                process_client_packet(packet, umem, filters, &cm, state, tx_slab)
            } else {
                process_server_packet(packet, umem, filters, state, tx_slab, jitter)
            }
        };

        match res {
            Ok(None) => {}
            Ok(Some(packet)) => {
                umem.free_packet(packet);
            }
            Err((error, packet)) => {
                let discriminant = error.discriminant();
                error.inc_system_errors_total(direction, &metrics::EMPTY);
                metrics::packets_dropped_total(direction, discriminant, &metrics::EMPTY).inc();

                umem.free_packet(packet);
            }
        }
    }

    if had_read {
        metrics::packet_jitter(metrics::READ, &metrics::EMPTY).set(jitter);
    }
}

#[inline]
fn push_packet<const TXN: usize>(
    direction: metrics::Direction,
    packet: Packet,
    asn: AsnInfo<'_>,
    data_length: usize,
    res: Result<(), PacketError>,
    tx_slab: &mut StackSlab<TXN>,
    umem: &mut Umem,
) {
    match res {
        Ok(()) => {
            if let Some(packet) = tx_slab.push_front(packet) {
                metrics::packets_dropped_total(direction, "tx slab full", &metrics::EMPTY).inc();
                umem.free_packet(packet);
            } else {
                metrics::packets_total(direction, &asn).inc();
                metrics::bytes_total(direction, &asn).inc_by(data_length as u64);
            }
        }
        Err(err) => {
            let discriminant = err.discriminant();
            metrics::errors_total(direction, discriminant, &metrics::EMPTY).inc();
            metrics::packets_dropped_total(direction, discriminant, &metrics::EMPTY).inc();
            umem.free_packet(packet);
        }
    }
}

#[inline]
fn process_client_packet<const TXN: usize>(
    packet: PacketWrapper,
    umem: &mut Umem,
    filters: &filters::FilterChain,
    cm: &crate::net::ClusterMap,
    state: &mut State,
    tx_slab: &mut StackSlab<TXN>,
) -> Result<Option<Packet>, (PipelineError, Packet)> {
    let mut source_addr = packet.headers.source_address();
    source_addr.set_ip(source_addr.ip().to_canonical());

    let mut ctx =
        filters::ReadContext::new(cm, source_addr.into(), packet, &mut state.destinations);

    let result = filters.read(&mut ctx);
    let mut packet = filtered(result, ctx.contents)?;

    let Some(dest_addr) = state.destinations.pop() else {
        return Ok(Some(packet.buffer));
    };

    let data = &packet.buffer[packet.headers.data];

    // TODO: We _could_ be more clever with this and do a running checksum calculation
    // as the packet data is modified by the filters, but for now we just do the
    // full checksum for the sake of simplicity
    let data_checksum = csum::DataChecksum::calculate_if_needed(data, &packet.buffer);
    let data_length = data.len();

    let eth = packet.headers.eth.swapped();

    // If we have more than 1 destination we need to clone the packet data to
    // a new packet for each destination, only modifying the headers
    if !state.destinations.is_empty() {
        while let Some(daddr) = state.destinations.pop() {
            let Ok(dest_addr) = daddr.to_socket_addr() else {
                continue;
            };
            let (source, asn, ips) = state.session(source_addr, dest_addr);

            let mut headers = UdpHeaders {
                eth,
                ip: ips.with_header(&packet.headers.ip),
                udp: UdpHdr {
                    source,
                    destination: dest_addr.port().into(),
                    check: 0,
                    length: NetworkU16(0),
                },
                data: packet.headers.data,
            };

            // SAFETY: the umem outlives the frame
            let mut new_packet = unsafe {
                let Some(new_packet) = umem.alloc() else {
                    continue;
                };
                new_packet
            };

            let res = fill_packet(&mut headers, data, data_checksum, &mut new_packet);
            push_packet(
                metrics::Direction::Read,
                new_packet,
                asn,
                data_length,
                res,
                tx_slab,
                umem,
            );
        }
    }

    let Ok(dest_addr) = dest_addr.to_socket_addr() else {
        return Ok(Some(packet.buffer));
    };
    let (source, asn, ips) = state.session(source_addr, dest_addr);

    let mut headers = UdpHeaders {
        eth,
        ip: ips.with_header(&packet.headers.ip),
        udp: UdpHdr {
            source,
            destination: dest_addr.port().into(),
            check: 0,
            length: NetworkU16(0),
        },
        data: packet.headers.data,
    };

    headers.calc_checksum(data_checksum);

    let res = modify_packet_headers(&packet.headers, &mut headers, &mut packet.buffer);
    push_packet(
        metrics::Direction::Read,
        packet.buffer,
        asn,
        data_length,
        res,
        tx_slab,
        umem,
    );

    Ok(None)
}

#[inline]
fn process_server_packet<const TXN: usize>(
    packet: PacketWrapper,
    umem: &mut Umem,
    filters: &crate::filters::FilterChain,
    state: &mut State,
    tx_slab: &mut StackSlab<TXN>,
    jitter: i64,
) -> Result<Option<Packet>, (PipelineError, Packet)> {
    let mut server_addr = packet.headers.source_address();
    server_addr.set_ip(server_addr.ip().to_canonical());

    let Some((client_addr, asn)) = state.lookup_client(server_addr, packet.headers.udp.destination)
    else {
        tracing::debug!(address = %server_addr, "received traffic from a server that has no downstream");
        return Ok(Some(packet.buffer));
    };

    metrics::packet_jitter(metrics::Direction::Write, &asn).set(jitter);

    let mut ctx = filters::WriteContext::new(server_addr.into(), client_addr.into(), packet);

    let result = filters.write(&mut ctx);
    let mut packet = filtered(result, ctx.contents)?;

    let mut headers = UdpHeaders {
        eth: packet.headers.eth.swapped(),
        ip: state.ips(client_addr.ip()).with_header(&packet.headers.ip),
        udp: UdpHdr {
            source: state.external_port,
            destination: client_addr.port().into(),
            length: NetworkU16(0),
            check: 0,
        },
        data: packet.headers.data,
    };

    let res = modify_packet_headers(&packet.headers, &mut headers, &mut packet.buffer);
    if res.is_ok() {
        let _ = packet.buffer.calc_udp_checksum();
    }

    push_packet(
        metrics::Direction::Write,
        packet.buffer,
        asn,
        packet.headers.data_length(),
        res,
        tx_slab,
        umem,
    );
    Ok(None)
}

/// Modifies the headers of an existing well formed packet to a new source and destination,
/// resizing the header portion as needed if changing between ipv4 and ipv6
#[inline]
fn modify_packet_headers(
    original: &UdpHeaders,
    new: &mut UdpHeaders,
    packet: &mut Packet,
) -> Result<(), PacketError> {
    match (original.is_ipv4(), new.is_ipv4()) {
        (true, false) => packet.adjust_head(-20)?,
        (false, true) => packet.adjust_head(20)?,
        (_, _) => {}
    }

    new.set_packet_headers(packet)?;
    Ok(())
}

#[inline]
fn fill_packet(
    headers: &mut UdpHeaders,
    data: &[u8],
    data_checksum: csum::DataChecksum,
    frame: &mut Packet,
) -> Result<(), PacketError> {
    let hdr_len = headers.header_length();
    frame.adjust_tail(hdr_len as i32)?;
    headers.calc_checksum(data_checksum);
    headers.set_packet_headers(frame)?;
    frame.insert(hdr_len, data)?;
    Ok(())
}

fn process_qcmp_packet<const TXN: usize>(
    mut packet: Packet,
    headers: UdpHeaders,
    umem: &mut Umem,
    tx_slab: &mut StackSlab<TXN>,
) {
    use crate::{codec::qcmp, time::UtcTimestamp};

    fn inner(packet: &mut Packet, headers: UdpHeaders) -> bool {
        let received_at = UtcTimestamp::now();
        let Some(data) = packet.get(headers.data.start..headers.data.end) else {
            tracing::debug!("corrupt UDP packet, data payload is out of range");
            return false;
        };
        let command = match qcmp::Protocol::parse(data) {
            Ok(Some(command)) => command,
            Ok(None) => {
                tracing::debug!("rejected non-qcmp packet");
                return false;
            }
            Err(error) => {
                tracing::debug!(%error, "rejected malformed packet");
                return false;
            }
        };

        let qcmp::Protocol::Ping {
            client_timestamp,
            nonce,
        } = command
        else {
            tracing::warn!("rejected unsupported QCMP packet");
            return false;
        };

        let mut ob = qcmp::QcmpPacket::default();
        let buf = qcmp::Protocol::ping_reply(nonce, client_timestamp, received_at).encode(&mut ob);

        if let Err(error) = packet.adjust_tail(-(headers.data_length() as i32)) {
            tracing::debug!(%error, "unable to trim QCMP ping data");
            return false;
        }

        if let Err(error) = packet.insert(headers.data.start, buf) {
            tracing::debug!(%error, "unable to write QCMP pong data");
            return false;
        }

        let mut new = UdpHeaders::new(
            headers.eth.swapped(),
            headers.ip.swapped(),
            headers.udp.swapped(),
            headers.data.start..headers.data.start + buf.len(),
        );
        new.decrement_hop();

        if let Err(error) = modify_packet_headers(&headers, &mut new, packet) {
            tracing::debug!(%error, "unable to modify QCMP packet headers");
            return false;
        }

        if let Err(error) = packet.calc_udp_checksum() {
            tracing::debug!(%error, "failed to calculate QCMP packet checksum");
            return false;
        }

        true
    }

    let packet = if inner(&mut packet, headers) {
        tracing::debug!("sending QCMP pong");

        if let Some(packet) = tx_slab.push_front(packet) {
            tracing::debug!("tx slab full, unable to send QCMP pong");
            packet
        } else {
            return;
        }
    } else {
        packet
    };

    umem.free_packet(packet);
}

#[cfg(test)]
mod test {
    use super::*;
    use quilkin_xdp::xdp::packet::Pod;
    use xdp::packet::net_types as nt;

    #[test]
    fn asn_cache_evicts_idle_entries() {
        let mut cache = AsnCache::default();
        let one = IpAddr::from([1, 1, 1, 1]);
        let two = IpAddr::from([2, 2, 2, 2]);

        cache.get_or_insert_with(one, 0, || None);
        cache.get_or_insert_with(two, 0, || None);
        cache.sweep(1);
        assert_eq!(cache.len(), 2);

        // Only `one` is accessed within the TTL.
        cache.get(&one, ASN_CACHE_TTL);
        cache.sweep(ASN_CACHE_TTL + 1);
        assert_eq!(cache.len(), 1);
        assert!(cache.map.contains_key(&one));

        cache.sweep(ASN_CACHE_TTL * 3);
        assert_eq!(cache.len(), 0);
    }

    /// Builds an ipv4 UDP packet with `padding` trailing bytes
    fn ipv4_packet(
        data: &mut [u8; 2048],
        proto: nt::IpProto::Enum,
        payload: &[u8],
        padding: usize,
    ) -> Packet {
        let mut v4 = nt::Ipv4Hdr::zeroed();
        v4.reset(64, proto);
        v4.source = u32::from_be_bytes([5; 4]).into();
        v4.destination = u32::from_be_bytes([2; 4]).into();

        let start = nt::EthHdr::LEN + nt::Ipv4Hdr::LEN + nt::UdpHdr::LEN;
        let mut headers = UdpHeaders::new(
            nt::EthHdr {
                source: nt::MacAddress([1; 6]),
                destination: nt::MacAddress([2; 6]),
                ether_type: nt::EtherType::Ipv4,
            },
            nt::IpHdr::V4(v4),
            UdpHdr {
                source: 8888.into(),
                destination: 7777.into(),
                length: NetworkU16(0),
                check: 0,
            },
            start..start + payload.len(),
        );

        let mut packet = xdp::Packet::testing_new(data);
        packet.adjust_tail(start as _).unwrap();
        headers.set_packet_headers(&mut packet).unwrap();
        packet.insert(start, payload).unwrap();
        if proto == nt::IpProto::Udp {
            packet.calc_udp_checksum().unwrap();
        }
        packet.append(&vec![0; padding]).unwrap();

        packet
    }

    #[test]
    fn trims_ethernet_padding() {
        // The size of a QCMP ping, which pads to the 60 byte minimum frame size
        let payload = [0xfdu8; 17];
        let mut data = [0u8; 2048];
        let mut packet = ipv4_packet(&mut data, nt::IpProto::Udp, &payload, 1);

        assert_eq!(packet.len(), MIN_ETHERNET_FRAME);
        let headers = parse_headers(&mut packet).expect("failed to parse padded packet");

        assert_eq!(headers.data_length(), payload.len());
        assert_eq!(&packet[headers.data], &payload[..]);
        assert_eq!(packet.len(), MIN_ETHERNET_FRAME - 1);
    }

    #[test]
    fn rejects_unparsable_packets() {
        let mut data = [0u8; 2048];

        // Not UDP
        let mut packet = ipv4_packet(&mut data, nt::IpProto::Tcp, &[0xfd; 17], 0);
        assert_eq!(parse_headers(&mut packet).err(), Some("non-UDP packet"));

        // A datagram larger than the frame that holds it
        let mut packet = ipv4_packet(&mut data, nt::IpProto::Udp, &[0xfd; 17], 0);
        packet.adjust_tail(-4).unwrap();
        assert_eq!(parse_headers(&mut packet).err(), Some("truncated packet"));

        // Trailing data on a frame too large to have been padded
        let mut packet = ipv4_packet(&mut data, nt::IpProto::Udp, &[0xfd; 32], 4);
        assert_eq!(parse_headers(&mut packet).err(), Some("trailing data"));

        // An ipv4 header length the fixed header offsets don't hold for, 4 is
        // below the 5 that means no options, both put the UDP header elsewhere
        for ihl in [4, 6] {
            let mut packet = ipv4_packet(&mut data, nt::IpProto::Udp, &[0xfd; 17], 0);
            packet[nt::EthHdr::LEN] = 0x40 | ihl;
            assert_eq!(parse_headers(&mut packet).err(), Some("ipv4 header length"));
        }

        // Too small to even hold the ethernet header
        let mut packet = xdp::Packet::testing_new(&mut data);
        packet.append(&[0xab; 8]).unwrap();
        assert_eq!(parse_headers(&mut packet).err(), Some("truncated packet"));
    }

    #[test]
    fn xdp_buffer_manipulation() {
        let payload = [0xfdu8; 21];

        let mut v6 = nt::Ipv6Hdr::zeroed();
        v6.reset(64, nt::IpProto::Udp);
        v6.source = [13; 16];
        v6.destination = [8; 16];
        let mut headers = UdpHeaders::new(
            nt::EthHdr {
                source: nt::MacAddress([1; 6]),
                destination: nt::MacAddress([2; 6]),
                ether_type: nt::EtherType::Ipv6,
            },
            nt::IpHdr::V6(v6),
            UdpHdr {
                source: 22.into(),
                destination: 20021.into(),
                length: NetworkU16(0),
                check: 0,
            },
            nt::EthHdr::LEN + nt::Ipv6Hdr::LEN + nt::UdpHdr::LEN
                ..nt::EthHdr::LEN + nt::Ipv6Hdr::LEN + nt::UdpHdr::LEN + payload.len(),
        );

        let mut data = [0u8; 2048];
        let mut buffer = xdp::Packet::testing_new(&mut data);
        buffer.adjust_tail(headers.data.start as _).unwrap();
        headers.set_packet_headers(&mut buffer).unwrap();
        buffer.insert(headers.data.start, &payload).unwrap();
        buffer.calc_udp_checksum().unwrap();

        let mut wrapper = PacketWrapper::new(buffer, headers);

        use crate::filters::{Packet, PacketMut};

        assert_eq!(wrapper.as_slice(), payload);

        {
            const HEAD: &[u8] = &[1; 3];
            wrapper.extend_head(HEAD);
            assert_eq!(&wrapper.as_slice()[..HEAD.len()], HEAD);
            assert_eq!(wrapper.as_slice()[HEAD.len()..], payload);
            assert_eq!(wrapper.headers.data_length(), payload.len() + HEAD.len());
            wrapper.remove_head(HEAD.len());
        }

        assert_eq!(wrapper.as_slice(), payload);

        {
            const TAIL: &[u8] = &[8; 20];
            wrapper.extend_tail(TAIL);
            assert_eq!(wrapper.as_slice()[..payload.len()], payload);
            assert_eq!(&wrapper.as_slice()[payload.len()..], TAIL);
            assert_eq!(wrapper.headers.data_length(), payload.len() + TAIL.len());
            wrapper.remove_tail(TAIL.len());
        }

        assert_eq!(wrapper.as_slice(), payload);
    }
}
