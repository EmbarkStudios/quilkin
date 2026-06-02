pub mod icmp;
pub mod io;
pub mod types;

use crate::net::io::completion::{self as comp, eventfd};
use quilkin_xdp::aya;
use std::{io::Error, sync::Arc};
use types::{Ip, LinkLayerAddr};

enum CacheEntry {
    Unknown(u128),
    Known {
        interested: u128,
        addr: LinkLayerAddr,
    },
}

#[derive(thiserror::Error, Debug)]
pub enum CacheSpawnError {
    #[error("{0} invalid number of XDP RX queues, must be >= 1 and <= 128")]
    InvalidRxQueueCount(usize),
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
    #[error("failed to retrieve gateway MAC address")]
    Gateway(#[source] eyre::Error),
}

/// A cache of IP -> MAC addresses
pub struct L2Cache {
    map: dashmap::DashMap<Ip, CacheEntry>,
    channels: Vec<CacheChannel>,
    tx: types::RequestSender,
    shutdown: eventfd::EventFdWriter,
}

pub type CacheChannel = crossbeam_channel::Sender<(Ip, LinkLayerAddr)>;

impl L2Cache {
    pub fn with_channels(
        channels: Vec<CacheChannel>,
        ebpf_ring: aya::maps::RingBuf<aya::maps::MapData>,
    ) -> Result<(Arc<Self>, std::thread::JoinHandle<()>), CacheSpawnError> {
        if channels.len() > 128 || channels.len() < 1 {
            return Err(CacheSpawnError::InvalidRxQueueCount(channels.len()));
        }

        // Create an ICMP socket that we can use to send pings to L3 IP addresses to prod the kernel into doing ARP/ICMPv6
        // requests to resolve the L2 Ethernet MAC addresses
        let icmp = icmp::IcmpSocket::new().map_err(CacheSpawnError::Icmp)?;

        let br = comp::ring::BufferRing::new(
            QUEUE_SIZE as u16,
            // we only deal with ICMP echo responses
            64,
        )
        .map_err(CacheSpawnError::BufferRing)?;

        const QUEUE_SIZE: u32 = 256;

        let ring = io_uring::IoUring::builder()
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
            channels,
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
    pub fn mac_for_ip(&self, ip: std::net::IpAddr, channel: u8) -> Option<LinkLayerAddr> {
        let ip = ip.into();
        match self.map.entry(ip) {
            dashmap::Entry::Occupied(mut ent) => match ent.get_mut() {
                CacheEntry::Known { addr, interested } => {
                    *interested = *interested | 1 << channel as u32;
                    Some(*addr)
                }
                CacheEntry::Unknown(ws) => {
                    *ws = *ws | 1 << channel as u32;
                    None
                }
            },
            dashmap::Entry::Vacant(ent) => {
                ent.insert(CacheEntry::Unknown(1 << channel as u32));
                self.tx.send(ip);
                None
            }
        }
    }

    #[inline]
    fn update_mac(&self, ip: Ip, lladdr: LinkLayerAddr) {
        let int = {
            let Some(mut entry) = self.map.get_mut(&ip) else {
                tracing::warn!(ip = %ip.0, %lladdr, "attempted to update link layer address for an entry not in the map");
                return;
            };

            match entry.value_mut() {
                CacheEntry::Known { interested, addr } => {
                    if addr == &lladdr {
                        tracing::debug!(ip = %ip.0, %lladdr, "skipping update for identical mac");
                        return;
                    }

                    // Notify all of the interested XDP threads of the new mac
                    *addr = lladdr;
                    *interested
                }
                CacheEntry::Unknown(int) => {
                    let int = *int;
                    *entry.value_mut() = CacheEntry::Known {
                        interested: int,
                        addr: lladdr,
                    };
                    int
                }
            }
        };

        self.notify(int, ip, lladdr);
    }

    #[inline]
    fn notify(&self, int: u128, ip: Ip, lladdr: LinkLayerAddr) {
        for (i, c) in self.channels.iter().enumerate() {
            if int & 1 << i as u32 == 0 {
                continue;
            }

            if c.try_send((ip, lladdr)).is_err() {
                // TODO: counter instead
                tracing::warn!(index = i, "XDP channel full");
            }
        }
    }
}

/// Retrieves the MAC address of the default gateway
pub fn determine_gateway_mac(nic: &str) -> Result<types::MacAddr, CacheSpawnError> {
    use eyre::WrapErr;

    // This is extremely ugly, but netlink is even uglier, this issues an arp command, to get the default gateway address's
    // link layer address. This uses the special `_gateway` identifier which is systemd specific, so this would need to be
    // modified if people want to run it on non-systemd systems

    fn get(nic: &str) -> eyre::Result<types::MacAddr> {
        let output = std::process::Command::new("arp")
            .args(["-i", nic, "-n", "_gateway"])
            .output()
            .context("failed to resolve link layer address of default gateway")?;

        eyre::ensure!(
            output.status.success(),
            "arp returned failure status {}",
            output.status
        );

        let out = String::from_utf8(output.stdout).context("arp output was not utf-8")?;
        let l = out
            .lines()
            .nth(1)
            .ok_or("arp output didn't have 2 or more lines of output")?;
        let lladdr = l
            .split_whitespace()
            .nth(2)
            .ok_or("arp output didn't have a standard line")?;

        let mut la = [0u8; 6];
        let mut i = 0;
        for c in lladdr.split(':') {
            eyre::ensure!(i < la.len(), "mac address contained too many components");

            la[i] = u8::from_str_radix(c, 16).with_context(|| format!("failed to parse {c}"))?;

            i += 1;
        }

        eyre::ensure!(i == 6, "mac address did not contain enough components");
        Ok(types::MacAddr(la))
    }

    get(nic).map_err(CacheSpawnError::Gateway)
}

#[cfg(test)]
mod test {
    #[test]
    fn gateway() {
        assert_eq!(
            super::determine_gateway_mac("enp5s0").unwrap().0,
            [0x04, 0xf4, 0x1c, 0xea, 0x7f, 0x17]
        );
    }
}
