pub mod icmp;
pub mod io;
pub mod router;
pub mod types;

pub use crossbeam_channel;

use quilkin_uring::eventfd;
use std::{io::Error, sync::Arc};
use types::{Ip, LinkLayerAddr};

#[derive(thiserror::Error, Debug)]
pub enum CacheSpawnError {
    #[error("failed to create ICMP socket")]
    Icmp(#[source] Error),
    #[error("failed to create buffer ring")]
    BufferRing(#[source] Error),
    #[error("failed to create io ring")]
    IoRing(#[source] Error),
    #[error("failed to create event fd for MAC requests")]
    MacRequest(#[source] Error),
    #[error("failed to create event fd for shutdown")]
    Shutdown(#[source] Error),
    #[error("failed to create epoll instance")]
    Epoll(#[source] Error),
    #[error("failed to add eBPF ringbuf to epoll")]
    EpollRingBuf(#[source] Error),
    #[error("failed to spawn thread")]
    Thread(#[source] Error),
}

/// A cache of IP -> MAC addresses
pub struct L2Cache {
    map: dashmap::DashMap<Ip, LinkLayerAddr>,
    tx: types::RequestSender,
    shutdown: eventfd::EventFdWriter,
}

impl L2Cache {
    pub fn new(
        nic: xdp::nic::NicIndex,
        ebpf_ring: aya::maps::RingBuf<aya::maps::MapData>,
    ) -> Result<(Arc<Self>, std::thread::JoinHandle<()>), CacheSpawnError> {
        // Create an ICMP socket that we can use to send pings to L3 IP addresses to prod the kernel into doing ARP/ICMPv6
        // requests to resolve the L2 Ethernet MAC addresses
        let icmp = icmp::IcmpSocket::new(nic).map_err(CacheSpawnError::Icmp)?;

        let br = quilkin_uring::ring::BufferRing::new(
            QUEUE_SIZE as u16,
            // we only deal with small ICMP packets here, but we get an -EFAULT for ipv6 if we use any power of 2 lower
            // than 256, which is irritating but fine
            256,
        )
        .map_err(CacheSpawnError::BufferRing)?;

        const QUEUE_SIZE: u32 = 256;

        let ring = quilkin_uring::io_uring::IoUring::builder()
            .setup_cqsize(QUEUE_SIZE)
            .build(QUEUE_SIZE >> 1)
            .map_err(CacheSpawnError::IoRing)?;

        let (tx, rx) = crossbeam_channel::bounded(128);

        let event = eventfd::EventFd::new().map_err(CacheSpawnError::MacRequest)?;

        let tx = types::RequestSender {
            sender: tx,
            writer: event.writer(),
        };

        let rr = types::RequestReceiver { rx, event };

        let shutdown = eventfd::EventFd::new().map_err(CacheSpawnError::Shutdown)?;

        let this = Arc::new(Self {
            map: Default::default(),
            tx,
            shutdown: shutdown.writer(),
        });

        let ebpf_ring = types::EbpfRing::new(ebpf_ring)?;

        let cache = this.clone();

        let jh = std::thread::Builder::new()
            .name("xdp-layer2-cache".into())
            .spawn(move || {
                if let Err(error) =
                    io::cache_io_loop(icmp, rr, ring, ebpf_ring, br, cache, shutdown)
                {
                    tracing::error!(%error, "error caused L2 cache io loop to exit");
                }
            })
            .map_err(CacheSpawnError::Thread)?;

        Ok((this, jh))
    }

    #[inline]
    pub fn shutdown(&self) {
        self.shutdown.write(1);
    }

    #[inline]
    pub fn mac_for_ip(&self, ip: std::net::IpAddr) -> Option<LinkLayerAddr> {
        let ip = ip.into();
        let ll = self.map.get(&ip).map(|ll| *ll);
        if ll.is_none() {
            self.tx.send(ip);
        }

        ll
    }

    #[inline]
    fn update_mac(&self, ip: Ip, lladdr: LinkLayerAddr) {
        self.map.insert(ip, lladdr);
    }
}
