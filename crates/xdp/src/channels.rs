//! Queries and updates a NIC's queue channel count via the kernel's ethtool
//! netlink interface (`ethtool -l`/`ethtool -L`).

use crate::NicIndex;

/// Queue channel counts reported by `ETHTOOL_MSG_CHANNELS_GET` (`ethtool -l`).
#[derive(Debug, Default, Clone, Copy)]
pub struct Channels {
    combined_max: u32,
    rx_count: u32,
    tx_count: u32,
    combined_count: u32,
}

impl Channels {
    /// Queues actually in use, whichever reporting style the driver uses.
    pub fn current(&self) -> u32 {
        self.rx_count.max(self.tx_count).max(self.combined_count)
    }

    /// Some drivers report separate RX/TX queues, others a single combined count.
    pub fn uses_combined(&self) -> bool {
        self.combined_max > 0
    }

    fn from_nlas(nlas: Vec<ethtool::EthtoolAttr>) -> Self {
        let mut channels = Self::default();
        for nla in nlas {
            let ethtool::EthtoolAttr::Channel(attr) = nla else {
                continue;
            };
            match attr {
                ethtool::EthtoolChannelAttr::CombinedMax(v) => channels.combined_max = v,
                ethtool::EthtoolChannelAttr::RxCount(v) => channels.rx_count = v,
                ethtool::EthtoolChannelAttr::TxCount(v) => channels.tx_count = v,
                ethtool::EthtoolChannelAttr::CombinedCount(v) => channels.combined_count = v,
                _ => {}
            }
        }
        channels
    }
}

/// Queries `iface`'s current queue channels (`ethtool -l`).
async fn get_channels(iface: &str) -> Result<Channels, ethtool::EthtoolError> {
    use futures::TryStreamExt as _;

    let (connection, mut handle, _) =
        ethtool::new_connection().map_err(|error| ethtool::EthtoolError::Bug(error.to_string()))?;
    tokio::spawn(connection);

    let mut stream = handle.channel().get(Some(iface)).execute().await;
    let mut channels = Channels::default();
    while let Some(msg) = stream.try_next().await? {
        channels = Channels::from_nlas(msg.payload.nlas);
    }
    Ok(channels)
}

/// Sets `iface`'s queue channels to `count`, using combined-style if
/// `uses_combined`, else separate rx/tx (`ethtool -L`).
async fn set_channel_count(
    iface: &str,
    uses_combined: bool,
    count: u32,
) -> Result<(), ethtool::EthtoolError> {
    let (connection, mut handle, _) =
        ethtool::new_connection().map_err(|error| ethtool::EthtoolError::Bug(error.to_string()))?;
    tokio::spawn(connection);

    let request = handle.channel().set(iface);
    if uses_combined {
        request.combined_count(count).execute().await
    } else {
        request.rx_count(count).tx_count(count).execute().await
    }
}

/// Bridges an async ethtool call into this sync-facing API; needs a
/// multi-threaded runtime, same as the rest of XDP setup.
fn block_on<F: std::future::Future>(fut: F) -> F::Output {
    tokio::task::block_in_place(|| tokio::runtime::Handle::current().block_on(fut))
}

/// Resolves `nic`'s interface name as UTF-8, the form the `ethtool` crate needs.
fn iface_name(nic: NicIndex) -> std::io::Result<String> {
    let name = nic
        .name()
        .map_err(|_err| std::io::Error::from(std::io::ErrorKind::NotFound))?;
    name.as_str().map(str::to_owned).ok_or_else(|| {
        std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "interface name is not utf-8",
        )
    })
}

/// Queries `nic`'s current queue channels (`ethtool -l`); the single source
/// of truth for queue count and reporting style.
pub fn query_channels(nic: NicIndex) -> std::io::Result<Channels> {
    let name = iface_name(nic)?;
    block_on(get_channels(&name)).map_err(std::io::Error::other)
}

/// Reduces `nic`'s configured queue count to `count` (`ethtool -L`).
pub fn shrink_queue_count(nic: NicIndex, uses_combined: bool, count: u32) -> std::io::Result<()> {
    let name = iface_name(nic)?;
    block_on(set_channel_count(&name, uses_combined, count)).map_err(std::io::Error::other)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn channels_current_takes_whichever_style_is_populated() {
        let separate = Channels {
            combined_max: 0,
            rx_count: 4,
            tx_count: 4,
            combined_count: 0,
        };
        assert_eq!(separate.current(), 4);
        assert!(!separate.uses_combined());

        let combined = Channels {
            combined_max: 8,
            rx_count: 0,
            tx_count: 0,
            combined_count: 2,
        };
        assert_eq!(combined.current(), 2);
        assert!(combined.uses_combined());
    }

    #[test]
    fn channels_from_nlas_parses_relevant_attrs_and_ignores_others() {
        let nlas = vec![
            ethtool::EthtoolAttr::Channel(ethtool::EthtoolChannelAttr::CombinedMax(4)),
            ethtool::EthtoolAttr::Channel(ethtool::EthtoolChannelAttr::RxCount(2)),
            ethtool::EthtoolAttr::Channel(ethtool::EthtoolChannelAttr::TxCount(2)),
            ethtool::EthtoolAttr::Channel(ethtool::EthtoolChannelAttr::OtherCount(0)),
        ];

        let channels = Channels::from_nlas(nlas);
        assert_eq!(channels.combined_max, 4);
        assert_eq!(channels.rx_count, 2);
        assert_eq!(channels.tx_count, 2);
        assert_eq!(channels.combined_count, 0);
    }
}
