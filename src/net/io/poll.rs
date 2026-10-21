pub mod tokio;

/// The largest datagram payload the poll backend accepts
///
/// A buffer holds nothing but the payload, unlike the completion backend, so
/// this is the full [`crate::net::packet::RECV_BUFFER_LEN`].
pub const MAX_PACKET_LEN: usize = crate::net::packet::RECV_BUFFER_LEN;
