pub use corro_api_types as api;
pub use corro_types as types;
pub use tripwire::Tripwire;

pub mod codec;
pub mod db;
pub mod gossip;
pub mod metrics;
pub mod persistent;
pub mod pubsub;
pub mod schema;
pub mod server;

pub use camino::{Utf8Path as Path, Utf8PathBuf as PathBuf};

pub type Peer = std::net::SocketAddrV6;
pub use smallvec::SmallVec;

#[inline]
pub fn ip_to_peer(ip: std::net::IpAddr) -> Peer {
    match ip {
        std::net::IpAddr::V4(v4) => Peer::new(v4.to_ipv6_mapped(), 0, 0, 0),
        std::net::IpAddr::V6(v6) => Peer::new(v6, 0, 0, 0),
    }
}

#[derive(Clone)]
pub struct Clock(pub std::sync::Arc<uhlc::HLC>);

impl Clock {
    #[inline]
    pub fn update_with_timestamp(
        &self,
        actor_id: types::actor::ActorId,
        ts: types::broadcast::Timestamp,
    ) -> Result<(), String> {
        // The try_into().unwrap() is an unfortunate necessity, the uhlc lib only provides TryFrom implementations,
        // but the conversion literally cannot fail, it would only fail if corrosion actorid increased in size beyond
        // its current 16 bytes
        self.0.update_with_timestamp(&uhlc::Timestamp::new(
            ts.to_ntp64(),
            actor_id.try_into().unwrap(),
        ))
    }

    #[inline]
    pub fn new_timestamp(&self) -> types::broadcast::Timestamp {
        self.0.new_timestamp().into()
    }
}
