use crate::net::endpoint::metadata::Value;
use bytes::Bytes;

enum QuicHeaderType {
    Long,
    Short,
}

impl QuicHeaderType {
    /// Derives header type from first byte of contents
    /// <https://datatracker.ietf.org/doc/html/rfc9000#name-packet-formats>
    #[inline]
    fn derive(contents: &[u8]) -> Option<Self> {
        // First bit discriminates the type.
        // Other bits are of no consequence for DCID extraction.
        let msb = (contents.first()? & 0b10000000u8) != 0;
        match msb {
            true => Some(QuicHeaderType::Long),
            false => Some(QuicHeaderType::Short),
        }
    }
}

/// Capture Destination Connection ID from QUIC packet.
#[derive(Debug, Eq, PartialEq, serde::Deserialize, schemars::JsonSchema, serde::Serialize)]
pub struct QuicDcid {
    /// The number of bytes to capture.
    pub size: u32,
}

impl super::CaptureStrategy for QuicDcid {
    fn capture(&self, contents: &[u8]) -> Option<(Value, isize)> {
        let capture_offset = match QuicHeaderType::derive(contents) {
            // Capture only big enough DCID from Long packets.
            Some(QuicHeaderType::Long) if (*contents.get(5)? as u32 >= self.size) => 6_usize,
            Some(QuicHeaderType::Short) => 1_usize,
            _ => return None,
        };

        let capture_end = capture_offset + self.size as usize;

        if capture_end <= contents.len() {
            Some((
                // NOTE: trimming DCID to first self.size bytes if it's longer.
                Value::Bytes(Bytes::copy_from_slice(
                    &contents[capture_offset..capture_end],
                )),
                0,
            ))
        } else {
            None
        }
    }
}
