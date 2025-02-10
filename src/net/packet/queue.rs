use std::sync::Arc;

pub type PacketQueue = (PacketQueueSender, PacketQueueReceiver);

pub fn queue(capacity: usize) -> std::io::Result<PacketQueue> {
    let (notify, rx) = packet_queue()?;

    Ok((
        PacketQueueSender {
            packets: Arc::new(parking_lot::Mutex::new(Vec::with_capacity(capacity))),
            notify,
        },
        rx,
    ))
}

/// A simple packet queue that signals when a packet is pushed
///
/// For io_uring this notifies an eventfd that will be processed on the next
/// completion loop
#[derive(Clone)]
pub struct PacketQueueSender {
    packets: Arc<parking_lot::Mutex<Vec<SendPacket>>>,
    notify: PacketQueueNotifier,
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
        push(&self.notify);
    }

    /// Called to inform the I/O loop this sender is tied to to still process
    /// packets, but not to create new sessions
    #[inline]
    pub(crate) fn disable_new_sessions(&self) {
        shutdown_receiver(&self.notify)
    }

    /// Swaps the current queue with an empty one so we only lock for a pointer swap
    #[inline]
    pub(crate) fn swap(&self, mut swap: Vec<SendPacket>) -> Vec<SendPacket> {
        swap.clear();
        std::mem::replace(&mut self.packets.lock(), swap)
    }
}

pub struct SendPacket {
    /// The destination address of the packet
    pub destination: socket2::SockAddr,
    /// The packet data being sent
    pub data: crate::collections::FrozenPoolBuffer,
    /// The asn info for the sender, used for metrics
    pub asn_info: Option<crate::net::maxmind_db::MetricsIpNetEntry>,
}

cfg_if::cfg_if! {
    if #[cfg(target_os = "linux")] {
        pub type PacketQueueReceiver = crate::net::io_uring::EventFd;
        type PacketQueueNotifier = crate::net::io_uring::EventFdWriter;

        fn packet_queue() -> std::io::Result<(PacketQueueNotifier, PacketQueueReceiver)> {
            let rx = crate::net::io_uring::EventFd::new()?;
            Ok((rx.writer(), rx))
        }

        #[inline]
        fn push(notify: &PacketQueueNotifier) {
            notify.write(1);
        }

        pub(crate) const SHUTDOWN_TOKEN: u64 = 0xdeadbeef;

        #[inline]
        fn shutdown_receiver(notify: &PacketQueueNotifier) {
            notify.write(SHUTDOWN_TOKEN);
        }
    } else {
        pub type PacketQueueReceiver = tokio::sync::watch::Receiver<bool>;
        type PacketQueueNotifier = tokio::sync::watch::Sender<bool>;

        fn packet_queue() -> std::io::Result<(PacketQueueNotifier, PacketQueueReceiver)> {
            Ok(tokio::sync::watch::channel(true))
        }

        #[inline]
        fn push(notify: &PacketQueueNotifier) {
            let _ = notify.send(true);
        }

        #[inline]
        fn shutdown_receiver(notify: &PacketQueueNotifier) {
            let _ = notify.send(false);
        }
    }
}
