use std::sync::Arc;

pub use quilkin_types::IcaoCode;

#[derive(Clone)]
#[cfg_attr(test, derive(Debug))]
pub struct NotifyingIcaoCode {
    icao: Arc<parking_lot::Mutex<IcaoCode>>,
    channel: tokio::sync::broadcast::Sender<()>,
}

impl Default for NotifyingIcaoCode {
    fn default() -> Self {
        Self {
            icao: Default::default(),
            channel: tokio::sync::broadcast::channel(1).0,
        }
    }
}

impl NotifyingIcaoCode {
    pub fn new(icao: IcaoCode) -> Self {
        Self {
            icao: Arc::new(parking_lot::Mutex::new(icao)),
            channel: tokio::sync::broadcast::channel(1).0,
        }
    }

    #[inline]
    pub fn store(&self, icao: IcaoCode) {
        {
            let mut cur = self.icao.lock();
            if *cur == icao {
                return;
            }

            *cur = icao;
        }

        let _ = self.channel.send(());
    }

    #[inline]
    pub fn load(&self) -> IcaoCode {
        *self.icao.lock()
    }

    #[inline]
    pub fn subscribe(&self) -> tokio::sync::broadcast::Receiver<()> {
        self.channel.subscribe()
    }
}
