pub use corro_api_types as api;
pub use corro_types as types;
pub use tripwire::Tripwire;

pub mod agent;
pub mod codec;
pub mod db;
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
