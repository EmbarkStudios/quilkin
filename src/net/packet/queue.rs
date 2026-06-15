use std::sync::Arc;

pub type PacketQueue = (PacketQueueSender, PacketQueueReceiver);

pub fn queue(capacity: usize, backend: crate::net::io::UdpBackend) -> std::io::Result<PacketQueue> {
    cfg_select! {
        target_os = "linux" => {
            if matches!(backend, crate::net::io::UdpBackend::Completion) {
                let efd = crate::net::io::completion::io_uring::EventFd::new()?;
                return Ok((
                    PacketQueueSender {
                        packets: Arc::new(parking_lot::Mutex::new(Vec::with_capacity(capacity))),
                        notify: Notify::EventFd(efd.writer()),
                    },
                    PacketQueueReceiver::EventFd(efd),
                ));
            }
            make_watch_queue(capacity)
        }
        _ => {
            let _ = backend;
            make_watch_queue(capacity)
        }
    }
}

fn make_watch_queue(capacity: usize) -> std::io::Result<PacketQueue> {
    let (tx, rx) = tokio::sync::watch::channel(true);
    Ok((
        PacketQueueSender {
            packets: Arc::new(parking_lot::Mutex::new(Vec::with_capacity(capacity))),
            notify: {
                cfg_select! {
                    target_os = "linux" => { Notify::Watch(tx) }
                    _ => { tx }
                }
            },
        },
        {
            cfg_select! {
                target_os = "linux" => { PacketQueueReceiver::Watch(rx) }
                _ => { rx }
            }
        },
    ))
}

cfg_select! {
    target_os = "linux" => {
        enum Notify {
            EventFd(crate::net::io::completion::io_uring::EventFdWriter),
            Watch(tokio::sync::watch::Sender<bool>),
        }

        pub enum PacketQueueReceiver {
            EventFd(crate::net::io::completion::io_uring::EventFd),
            Watch(tokio::sync::watch::Receiver<bool>),
        }
    }
    _ => {
        type Notify = tokio::sync::watch::Sender<bool>;

        pub type PacketQueueReceiver = tokio::sync::watch::Receiver<bool>;
    }
}

/// A simple packet queue that signals when a packet is pushed
///
/// For `io_uring` this notifies an eventfd that will be processed on the next
/// completion loop
#[derive(Clone)]
pub struct PacketQueueSender {
    packets: Arc<parking_lot::Mutex<Vec<SendPacket>>>,
    notify: Notify,
}

impl PacketQueueSender {
    #[inline]
    pub(crate) fn capacity(&self) -> usize {
        self.packets.lock().capacity()
    }

    /// Pushes a packet onto the queue to be sent, signalling a sender that
    /// it's available
    #[inline]
    pub fn push(&self, packet: SendPacket) {
        self.packets.lock().push(packet);
        cfg_select! {
            target_os = "linux" => {
                match &self.notify {
                    Notify::EventFd(efd) => efd.write(1),
                    Notify::Watch(tx) => { let _ = tx.send(true); }
                }
            }
            _ => {
                let _ = self.notify.send(true);
            }
        }
    }

    /// Swaps the current queue with an empty one so we only lock for a pointer swap
    #[inline]
    pub(crate) fn swap(&self, mut swap: Vec<SendPacket>) -> Vec<SendPacket> {
        swap.clear();
        std::mem::replace(&mut self.packets.lock(), swap)
    }
}

cfg_select! {
    target_os = "linux" => {
        impl Clone for Notify {
            fn clone(&self) -> Self {
                match self {
                    Self::EventFd(w) => Self::EventFd(w.clone()),
                    Self::Watch(tx) => Self::Watch(tx.clone()),
                }
            }
        }
    }
    _ => {}
}

pub struct SendPacket {
    /// The destination address of the packet
    pub destination: std::net::SocketAddr,
    /// The packet data being sent
    pub data: bytes::Bytes,
    /// The asn info for the sender, used for metrics
    pub asn_info: Option<crate::net::maxmind_db::MetricsIpNetEntry>,
}
