/*
 * Copyright 2024 Google LLC All Rights Reserved.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

pub mod queue;

use crate::{
    Config,
    filters::{Filter as _, ReadContext},
    metrics,
    net::{
        PipelineError,
        sessions::{SessionKey, SessionManager, SessionPool},
    },
};
use std::{
    net::{IpAddr, SocketAddr},
    sync::Arc,
};

pub use self::queue::{PacketQueue, PacketQueueReceiver, PacketQueueSender, queue};

/// The size of a single receive buffer, for a presumed MTU of 1500 rounded up
/// to the next power of 2, which the io-uring buffer ring requires
///
/// A backend accepts this minus whatever metadata it keeps in the same buffer,
/// see its `MAX_PACKET_LEN`. Larger datagrams are dropped.
pub const RECV_BUFFER_LEN: usize = 2048;

/// How often [`empty_packet_dropped`] may log
const EMPTY_PACKET_LOG_INTERVAL: i64 = 5 * 60 * 1_000_000_000;

/// When [`empty_packet_dropped`] last logged, as unix nanos
static EMPTY_PACKET_LOGGED_AT: std::sync::atomic::AtomicI64 =
    std::sync::atomic::AtomicI64::new(i64::MIN);

/// Records an empty datagram being dropped
///
/// Counted every time, but logged at most once per [`EMPTY_PACKET_LOG_INTERVAL`],
/// as a client that sends these sends them at whatever rate it ticks at.
pub fn empty_packet_dropped() {
    use crate::service::ALLOW_EMPTY_PACKETS_FLAG;

    metrics::packets_dropped(metrics::READ, metrics::DropReason::EmptyPacket).inc();
    crate::net::metrics::observe_dropped_packet(
        metrics::READ,
        "",
        0,
        metrics::DropReason::EmptyPacket,
    );

    let now = crate::time::UtcTimestamp::now().unix_nanos();

    if claim_log_slot(&EMPTY_PACKET_LOGGED_AT, now, EMPTY_PACKET_LOG_INTERVAL) {
        tracing::warn!(
            "empty packets are dropped by default, enable --{ALLOW_EMPTY_PACKETS_FLAG} to allow them"
        );
    }
}

/// Whether `now` is at least `interval` past the timestamp in `last`, taking
/// the slot for `now` when it is
///
/// Every worker reaches this for the same packet storm, so the slot is taken by
/// a swap and only the thread that wins it logs.
fn claim_log_slot(last: &std::sync::atomic::AtomicI64, now: i64, interval: i64) -> bool {
    use std::sync::atomic::Ordering;

    let previous = last.load(Ordering::Relaxed);

    now.saturating_sub(previous) >= interval
        && last
            .compare_exchange(previous, now, Ordering::Relaxed, Ordering::Relaxed)
            .is_ok()
}

/// Returns the canonical form of `ip` when it is a source we never forward, as
/// a reply to a spoofed one of these would make the proxy a UDP amplifier
///
/// Dual stack sockets report ipv4 peers as ipv4-mapped ipv6, which matches none
/// of the ipv6 predicates, so the mapping is undone before the check.
pub fn disallowed_source_ip(ip: IpAddr) -> Option<IpAddr> {
    let ip = ip.to_canonical();

    let disallowed = match ip {
        IpAddr::V4(v4) => {
            v4.is_loopback() || v4.is_multicast() || v4.is_broadcast() || v4.is_unspecified()
        }
        IpAddr::V6(v6) => v6.is_loopback() || v6.is_multicast() || v6.is_unspecified(),
    };

    disallowed.then_some(ip)
}

/// Representation of an immutable set of bytes pulled from the network, this trait
/// provides an abstraction over however the packet was received (epoll, io-uring, xdp)
///
/// Use [`PacketMut`] if you need a mutable representation.
pub trait Packet: Sized {
    /// Returns the underlying slice of bytes representing the packet.
    fn as_slice(&self) -> &[u8];

    /// Returns the size of the packet.
    fn len(&self) -> usize;

    /// Returns whether the given packet is empty.
    fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

/// Representation of an mutable set of bytes pulled from the network, this trait
/// provides an abstraction over however the packet was received (epoll, io-uring, xdp)
pub trait PacketMut: Sized + Packet {
    /// Truncates the head by the specified number of bytes
    fn remove_head(&mut self, length: usize);
    /// Truncates the tail by the specified number of bytes
    fn remove_tail(&mut self, length: usize);
    /// Prepend the specified bytes
    fn extend_head(&mut self, bytes: &[u8]);
    /// Append the specified bytes
    fn extend_tail(&mut self, bytes: &[u8]);
    /// Returns an immutable version of the packet, this allows certain types
    /// return a type that can be more cheaply cloned and shared.
    fn freeze(self) -> bytes::Bytes;
}

impl PacketMut for bytes::BytesMut {
    fn remove_head(&mut self, length: usize) {
        drop(self.split_to(length));
    }

    fn extend_head(&mut self, bytes: &[u8]) {
        let rest = self.split();
        self.extend_from_slice(bytes);
        self.unsplit(rest);
    }

    fn extend_tail(&mut self, bytes: &[u8]) {
        self.extend_from_slice(bytes);
    }

    fn remove_tail(&mut self, length: usize) {
        self.truncate(self.len().saturating_sub(length));
    }

    fn freeze(self) -> bytes::Bytes {
        self.freeze()
    }
}

impl Packet for bytes::BytesMut {
    fn as_slice(&self) -> &[u8] {
        self.as_ref()
    }

    fn is_empty(&self) -> bool {
        self.is_empty()
    }

    fn len(&self) -> usize {
        self.len()
    }
}

/// Packet received from local port
pub(crate) struct DownstreamPacket<'stack, P> {
    pub(crate) contents: P,
    pub(crate) source: SocketAddr,
    pub(crate) filters: &'stack crate::filters::FilterChain,
    pub(crate) allow_empty: bool,
}

impl<P: PacketMut> DownstreamPacket<'_, P> {
    #[inline]
    pub(crate) fn process<S: SessionManager>(
        self,
        worker_id: usize,
        config: &Arc<Config>,
        sessions: &S,
        destinations: &mut Vec<crate::net::Destination>,
    ) {
        let payload_size = self.contents.len();
        tracing::trace!(
            id = worker_id,
            size = payload_size,
            source = %self.source,
            "received packet from downstream"
        );

        let timer = metrics::processing_time(metrics::READ).start_timer();
        if let Err(error) = self.process_inner(config, sessions, destinations) {
            error.inc_system_errors_total(metrics::READ, &metrics::EMPTY);
            metrics::packets_dropped_total(
                metrics::READ,
                error.drop_reason(),
                error.filter_name(),
                "",
            )
            .inc();
            crate::net::metrics::observe_dropped_packet_with_filter(
                metrics::READ,
                "",
                payload_size,
                error.drop_reason(),
                error.filter_name(),
            );
        }

        timer.stop_and_record();
    }

    /// Processes a packet by running it through the filter chain.
    #[inline]
    fn process_inner<S: SessionManager>(
        self,
        config: &Arc<Config>,
        sessions: &S,
        destinations: &mut Vec<crate::net::Destination>,
    ) -> Result<(), PipelineError> {
        if self.contents.is_empty() && !self.allow_empty {
            empty_packet_dropped();
            return Ok(());
        }

        let Some(clusters) = config
            .dyn_cfg
            .clusters()
            .filter(|c| c.read().has_endpoints())
        else {
            tracing::trace!("no upstream endpoints");
            return Err(PipelineError::NoUpstreamEndpoints);
        };

        #[cfg(not(debug_assertions))]
        if let Some(ip) = disallowed_source_ip(self.source.ip()) {
            return Err(PipelineError::DisallowedSourceIP(ip));
        }

        let cm = clusters.clone_value();
        let mut context = ReadContext::new(&cm, self.source.into(), self.contents, destinations);
        self.filters
            .read(&mut context)
            .map_err(PipelineError::Filter)?;

        let ReadContext { contents, .. } = context;

        if destinations.is_empty() {
            return Ok(());
        }

        // Similar to bytes::BytesMut::freeze, we turn the mutable pool buffer
        // into an immutable one with its own internal arc so it can be cloned
        // cheaply and returned to the pool once all references are dropped
        let contents = contents.freeze();

        // A single destination is the most likely outcome for a typical workload
        // so we can avoid an unnecessary buffer clone
        if destinations.len() == 1
            && let Some(dest) = destinations.pop()
        {
            let session_key = SessionKey {
                source: self.source,
                dest: dest.address.to_socket_addr()?,
            };

            sessions.send(session_key, contents, dest.cluster)?;
        } else {
            for dest in destinations.drain(0..) {
                let session_key = SessionKey {
                    source: self.source,
                    dest: dest.address.to_socket_addr()?,
                };

                sessions.send(session_key, contents.clone(), dest.cluster)?;
            }
        }

        Ok(())
    }
}

/// Runs a packet through the filter chain with a no-op session manager.
/// Used by benchmarks to measure pipeline overhead without any IO.
pub fn bench_process_packet(
    payload: bytes::BytesMut,
    source: std::net::SocketAddr,
    config: &Arc<Config>,
    filter_chain: &crate::filters::FilterChain,
    destinations: &mut Vec<crate::net::Destination>,
) {
    struct Noop;
    impl crate::net::sessions::SessionManager for Noop {
        fn send(
            &self,
            _key: crate::net::sessions::SessionKey,
            _contents: bytes::Bytes,
            _cluster: Option<crate::net::endpoint::Locality>,
        ) -> Result<(), crate::net::PipelineError> {
            Ok(())
        }
    }
    DownstreamPacket {
        contents: payload,
        source,
        filters: filter_chain,
        allow_empty: true,
    }
    .process(0, config, &Noop, destinations);
}

/// Spawns a background task that sits in a loop, receiving packets from the passed in socket.
/// Each received packet is placed on a queue to be processed by a worker task.
/// This function also spawns the set of worker tasks responsible for consuming packets
/// off the aforementioned queue and processing them through the filter chain and session
/// pipeline.
pub fn spawn_receivers(
    config: Arc<Config>,
    socket: socket2::Socket,
    worker_sends: Vec<crate::net::PacketQueue>,
    sessions: &Arc<SessionPool>,
    backend: crate::net::io::UdpBackend,
    recv_ring_len: u16,
    allow_empty_packets: bool,
) -> crate::Result<()> {
    let port = crate::net::socket_port(&socket);

    let Some(pfc) = config.dyn_cfg.cached_filter_chain() else {
        eyre::bail!("the ProxyFilterChain state was not configured");
    };

    for (worker_id, ws) in worker_sends.into_iter().enumerate() {
        let worker = crate::net::io::Listener {
            worker_id,
            port,
            config: config.clone(),
            sessions: sessions.clone(),
            backend,
            allow_empty_packets,
        };

        worker.spawn_io_loop(ws, pfc.clone(), recv_ring_len)?;
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use std::net::{Ipv4Addr, Ipv6Addr};

    use super::*;

    /// A dual stack socket reports an ipv4 peer in ipv4-mapped form, so every
    /// ipv4 source has to be caught in both spellings
    #[test]
    fn disallows_amplification_sources() {
        for v4 in [
            Ipv4Addr::LOCALHOST,
            Ipv4Addr::BROADCAST,
            Ipv4Addr::UNSPECIFIED,
            // multicast = 224.0.0.0/4
            Ipv4Addr::new(224, 0, 0, 0),
            Ipv4Addr::new(239, 255, 255, 255),
        ] {
            let canonical = Some(IpAddr::V4(v4));
            assert_eq!(disallowed_source_ip(v4.into()), canonical, "{v4}");
            assert_eq!(
                disallowed_source_ip(v4.to_ipv6_mapped().into()),
                canonical,
                "::ffff:{v4}"
            );
        }

        for v6 in [
            Ipv6Addr::LOCALHOST,
            Ipv6Addr::UNSPECIFIED,
            // multicast = any address starting with 0xff
            Ipv6Addr::new(0xff00, 0, 0, 0, 0, 0, 0, 0),
        ] {
            assert_eq!(
                disallowed_source_ip(v6.into()),
                Some(IpAddr::V6(v6)),
                "{v6}"
            );
        }

        // Ordinary clients, in every spelling they can arrive in
        let routable = Ipv4Addr::new(203, 0, 113, 1);
        for allowed in [
            IpAddr::V4(routable),
            IpAddr::V6(routable.to_ipv6_mapped()),
            IpAddr::V6(Ipv6Addr::new(0x2001, 0xdb8, 0, 0, 0, 0, 0, 1)),
        ] {
            assert_eq!(disallowed_source_ip(allowed), None, "{allowed}");
        }
    }
}

/// Covers the wiring the unit tests above can't reach, since the guard is only
/// compiled into release builds. Which addresses are refused is covered there.
#[cfg(all(test, not(debug_assertions)))]
mod release_tests {
    use super::*;
    use crate::{net::Endpoint, test::alloc_buffer};
    use std::net::{Ipv4Addr, Ipv6Addr, SocketAddrV6};

    /// A disallowed source is refused by the pipeline, in the ipv4-mapped form
    /// a dual stack socket reports it in
    #[tokio::test]
    async fn disallowed_ips() {
        let endpoint = Endpoint::new((Ipv4Addr::LOCALHOST, 7777).into());

        let providers = crate::Providers::default();
        let mut service = crate::Service::default();
        let mut config = Config::new(None, Default::default(), &providers, &mut service);
        // FilterChain is not inserted by Service::default() unless a network
        // service is enabled; insert it manually for this test.
        crate::config::insert_default::<crate::filters::FilterChain>(&mut config.dyn_cfg.typemap);
        config
            .dyn_cfg
            .clusters()
            .unwrap()
            .modify(|clusters| clusters.insert_default([endpoint].into()));
        let config = Arc::new(config);

        let session_manager = SessionPool::new(
            vec![],
            config.dyn_cfg.cached_filter_chain().unwrap(),
            None,
            usize::MAX,
            crate::net::io::UdpBackend::default(),
            4,
        );

        let filter_chain = crate::filters::FilterChain::default();

        for (source, expected) in [
            (
                SocketAddrV6::new(Ipv4Addr::LOCALHOST.to_ipv6_mapped(), 7777, 0, 0),
                IpAddr::V4(Ipv4Addr::LOCALHOST),
            ),
            (
                SocketAddrV6::new(Ipv6Addr::LOCALHOST, 7777, 0, 0),
                IpAddr::V6(Ipv6Addr::LOCALHOST),
            ),
        ] {
            let packet = DownstreamPacket {
                contents: alloc_buffer([1, 2, 3, 4]),
                source: SocketAddr::V6(source),
                filters: &filter_chain,
            };

            let res = packet.process_inner(&config, &session_manager, &mut Vec::new());

            assert_eq!(res, Err(PipelineError::DisallowedSourceIP(expected)));
        }
    }
}

#[cfg(test)]
mod empty_packets {
    use std::sync::atomic::{AtomicI64, AtomicUsize, Ordering};

    use super::*;

    #[derive(Default)]
    struct CountingSessions(AtomicUsize);

    impl SessionManager for CountingSessions {
        fn send(
            &self,
            _key: SessionKey,
            _contents: bytes::Bytes,
            _cluster: Option<crate::net::endpoint::Locality>,
        ) -> Result<(), PipelineError> {
            self.0.fetch_add(1, Ordering::Relaxed);
            Ok(())
        }
    }

    /// An empty datagram reaches the upstream only when it is allowed to
    #[test]
    fn forwarded_only_when_allowed() {
        let providers = crate::Providers::default();
        let mut service = crate::Service::default();
        let mut config = Config::new(None, Default::default(), &providers, &mut service);
        crate::config::insert_default::<crate::filters::FilterChain>(&mut config.dyn_cfg.typemap);
        config.dyn_cfg.clusters().unwrap().modify(|clusters| {
            clusters
                .insert_default([crate::net::Endpoint::new(([1, 1, 1, 1], 7777).into())].into());
        });
        let config = Arc::new(config);
        let filters = crate::filters::FilterChain::default();

        for (allow_empty, forwarded) in [(false, 0), (true, 1)] {
            let sessions = CountingSessions::default();
            let packet = DownstreamPacket {
                contents: crate::test::alloc_buffer([]),
                source: ([198, 51, 100, 1], 9999).into(),
                filters: &filters,
                allow_empty,
            };

            packet
                .process_inner(&config, &sessions, &mut Vec::new())
                .expect("an empty packet is dropped, not an error");

            assert_eq!(
                sessions.0.load(Ordering::Relaxed),
                forwarded,
                "{allow_empty}"
            );
        }
    }

    #[test]
    fn log_slot_is_claimed_once_per_interval() {
        const INTERVAL: i64 = 5 * 60 * 1_000_000_000;
        let last = AtomicI64::new(i64::MIN);

        // The first ever packet logs, however long the process has been up
        assert!(claim_log_slot(&last, 0, INTERVAL));
        assert!(!claim_log_slot(&last, 0, INTERVAL));
        assert!(!claim_log_slot(&last, INTERVAL - 1, INTERVAL));
        assert!(claim_log_slot(&last, INTERVAL, INTERVAL));
        assert!(!claim_log_slot(&last, INTERVAL, INTERVAL));
    }
}
