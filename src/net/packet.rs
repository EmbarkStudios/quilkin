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
use std::{net::SocketAddr, sync::Arc};

pub use self::queue::{PacketQueue, PacketQueueSender, queue};

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
    type FrozenPacket: Packet;
    fn remove_head(&mut self, length: usize);
    fn remove_tail(&mut self, length: usize);
    fn extend_head(&mut self, bytes: &[u8]);
    fn extend_tail(&mut self, bytes: &[u8]);
    /// Returns an immutable version of the packet, this allows certain types
    /// return a type that can be more cheaply cloned and shared.
    fn freeze(self) -> Self::FrozenPacket;
}

/// Packet received from local port
pub(crate) struct DownstreamPacket<'stack, P> {
    pub(crate) contents: P,
    pub(crate) source: SocketAddr,
    pub(crate) filters: &'stack crate::filters::FilterChain,
}

impl<P: PacketMut> DownstreamPacket<'_, P> {
    #[inline]
    pub(crate) fn process<S: SessionManager<Packet = P::FrozenPacket>>(
        self,
        worker_id: usize,
        config: &Arc<Config>,
        sessions: &S,
        destinations: &mut Vec<crate::net::EndpointAddress>,
    ) {
        tracing::trace!(
            id = worker_id,
            size = self.contents.len(),
            source = %self.source,
            "received packet from downstream"
        );

        let timer = metrics::processing_time(metrics::READ).start_timer();
        if let Err(error) = self.process_inner(config, sessions, destinations) {
            let discriminant = error.discriminant();

            error.inc_system_errors_total(metrics::READ, &metrics::EMPTY);
            metrics::packets_dropped_total(metrics::READ, discriminant, &metrics::EMPTY).inc();
        }

        timer.stop_and_record();
    }

    /// Processes a packet by running it through the filter chain.
    #[inline]
    fn process_inner<S: SessionManager<Packet = P::FrozenPacket>>(
        self,
        config: &Arc<Config>,
        sessions: &S,
        destinations: &mut Vec<crate::net::EndpointAddress>,
    ) -> Result<(), PipelineError> {
        let Some(clusters) = config
            .dyn_cfg
            .clusters()
            .filter(|c| c.read().has_endpoints())
        else {
            tracing::trace!("no upstream endpoints");
            return Err(PipelineError::NoUpstreamEndpoints);
        };

        #[cfg(not(debug_assertions))]
        {
            match self.source.ip() {
                std::net::IpAddr::V4(ipv4) => {
                    if ipv4.is_loopback() || ipv4.is_multicast() || ipv4.is_broadcast() {
                        return Err(PipelineError::DisallowedSourceIP(self.source.ip()));
                    }
                }
                std::net::IpAddr::V6(ipv6) => {
                    if ipv6.is_loopback() || ipv6.is_multicast() {
                        return Err(PipelineError::DisallowedSourceIP(self.source.ip()));
                    }
                }
            }
        }

        let cm = clusters.clone_value();
        let mut context = ReadContext::new(&cm, self.source.into(), self.contents, destinations);
        self.filters
            .read(&mut context)
            .map_err(PipelineError::Filter)?;

        let ReadContext { contents, .. } = context;

        // Similar to bytes::BytesMut::freeze, we turn the mutable pool buffer
        // into an immutable one with its own internal arc so it can be cloned
        // cheaply and returned to the pool once all references are dropped
        let contents = contents.freeze();

        for epa in destinations.drain(0..) {
            let session_key = SessionKey {
                source: self.source,
                dest: epa.to_socket_addr()?,
            };

            sessions.send(session_key, &contents)?;
        }

        Ok(())
    }
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
    buffer_pool: Arc<crate::collections::BufferPool>,
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
            buffer_pool: buffer_pool.clone(),
        };

        worker.spawn_io_loop(ws, pfc.clone())?;
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    #![cfg(not(debug_assertions))]

    use quilkin_xds::locality::Locality;

    use crate::collections::BufferPool;
    use crate::net::Endpoint;
    use crate::test::alloc_buffer;

    use super::*;
    use std::net::{IpAddr, Ipv4Addr, Ipv6Addr};
    use std::net::{SocketAddrV4, SocketAddrV6};

    // Ensure we disallow certain source IP addresses to protect against UDP amplification attacks
    #[tokio::test]
    async fn disallowed_ips() {
        let nl1 = Locality::with_region("nl-1");
        let endpoint = Endpoint::new((Ipv4Addr::LOCALHOST, 7777).into());

        let config = Arc::new(Config::default_agent().cluster(
            None,
            Some(nl1.clone()),
            [endpoint.clone()].into(),
        ));
        let buffer_pool = Arc::new(BufferPool::new(1, 10));
        let session_manager = SessionPool::new(config.clone(), vec![], buffer_pool.clone());

        let packet_data: [u8; 4] = [1, 2, 3, 4];
        for ip in [
            IpAddr::V4(Ipv4Addr::LOCALHOST),
            IpAddr::V4(Ipv4Addr::BROADCAST),
            // multicast = 224.0.0.0/4
            IpAddr::V4(Ipv4Addr::new(224, 0, 0, 0)),
            IpAddr::V4(Ipv4Addr::new(239, 255, 255, 255)),
            IpAddr::V6(Ipv6Addr::LOCALHOST),
            // multicast = any address starting with 0xff
            IpAddr::V6(Ipv6Addr::new(0xff00, 0, 0, 0, 0, 0, 0, 0)),
        ] {
            let packet = DownstreamPacket {
                contents: alloc_buffer(packet_data),
                source: match ip {
                    IpAddr::V4(ipv4) => SocketAddr::V4(SocketAddrV4::new(ipv4, 0)),
                    IpAddr::V6(ipv6) => SocketAddr::V6(SocketAddrV6::new(ipv6, 0, 0, 0)),
                },
            };

            let mut endpoints = vec![endpoint.address.clone()];
            let res = packet.process_inner(&config, &session_manager, &mut endpoints);

            assert_eq!(res, Err(PipelineError::DisallowedSourceIP(ip)));
        }
    }
}
