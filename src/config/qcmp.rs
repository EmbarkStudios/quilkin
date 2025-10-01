use std::sync::{
    Arc,
    atomic::{self, Ordering::Relaxed},
};

pub const DEFAULT_QCMP_PORT: u16 = 7600;

#[derive(Clone, Debug)]
pub struct QcmpPort {
    port: Arc<atomic::AtomicU16>,
    tx: tokio::sync::broadcast::Sender<u16>,
}

impl QcmpPort {
    pub fn new(port: u16) -> Self {
        Self {
            port: Arc::new(atomic::AtomicU16::new(port)),
            tx: tokio::sync::broadcast::channel(1).0,
        }
    }

    #[inline]
    pub fn store(&self, port: u16) {
        let old_port = self.port.swap(port, Relaxed);
        if old_port != port {
            let _ = self.tx.send(port);
        }
    }

    #[inline]
    pub fn load(&self) -> u16 {
        self.port.load(Relaxed)
    }

    pub fn subscribe(&self) -> tokio::sync::broadcast::Receiver<u16> {
        self.tx.subscribe()
    }
}

impl typemap_rev::TypeMapKey for QcmpPort {
    type Value = QcmpPort;
}

impl super::DynamicConfig {
    pub fn qcmp_port(&self) -> Option<&QcmpPort> {
        self.typemap.get::<QcmpPort>()
    }
}

impl Default for QcmpPort {
    fn default() -> Self {
        Self::new(DEFAULT_QCMP_PORT)
    }
}

impl PartialEq for QcmpPort {
    fn eq(&self, other: &Self) -> bool {
        self.port.load(Relaxed) == other.port.load(Relaxed)
    }
}
