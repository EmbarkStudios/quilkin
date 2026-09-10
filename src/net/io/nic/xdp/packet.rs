use quilkin_xdp::xdp::{Packet, packet::net_types::UdpHeaders};

/// Wrapper around the actual packet buffer and the UDP metadata it parsed to
/// so that we can satisify the filter traits
pub struct PacketWrapper {
    pub(super) buffer: Packet,
    pub(super) headers: UdpHeaders,
    /// A modification a filter requested that couldn't be applied, the packet is
    /// dropped rather than forwarded partially modified
    pub(super) failure: Option<&'static str>,
}

impl PacketWrapper {
    #[inline]
    pub(super) fn new(buffer: Packet, headers: UdpHeaders) -> Self {
        Self {
            buffer,
            headers,
            failure: None,
        }
    }

    /// Records a modification that couldn't be applied, keeping the first
    #[inline]
    fn fail(&mut self, failure: &'static str) {
        self.failure.get_or_insert(failure);
    }

    /// Shrinks the data payload, recording `failure` if that would move the tail
    /// into the headers
    #[inline]
    fn trim(&mut self, length: usize, failure: &'static str) {
        if length > self.headers.data_length() || self.buffer.adjust_tail(-(length as i32)).is_err()
        {
            self.fail(failure);
            return;
        }

        self.headers.data.end -= length;
    }
}

impl crate::filters::Packet for PacketWrapper {
    #[inline]
    fn as_slice(&self) -> &[u8] {
        &self.buffer[self.headers.data.start..self.headers.data.end]
    }

    #[inline]
    fn len(&self) -> usize {
        self.headers.data_length()
    }
}

impl crate::filters::PacketMut for PacketWrapper {
    #[inline]
    fn extend_head(&mut self, bytes: &[u8]) {
        if self.buffer.insert(self.headers.data.start, bytes).is_err() {
            self.fail("filter::extend head");
            return;
        }

        self.headers.data.end += bytes.len();
    }

    #[inline]
    fn extend_tail(&mut self, bytes: &[u8]) {
        if self.buffer.append(bytes).is_err() {
            self.fail("filter::extend tail");
            return;
        }

        self.headers.data.end += bytes.len();
    }

    #[inline]
    fn remove_head(&mut self, length: usize) {
        if length == 0 {
            return;
        }

        if length > self.headers.data_length() || self.headers.data.end > self.buffer.len() {
            self.fail("filter::remove head");
            return;
        }

        // Shift the payload down over the removed bytes, the headers are rewritten
        // before the packet is sent
        self.buffer.copy_within(
            self.headers.data.start + length..self.headers.data.end,
            self.headers.data.start,
        );
        self.trim(length, "filter::remove head");
    }

    #[inline]
    fn remove_tail(&mut self, length: usize) {
        self.trim(length, "filter::remove tail");
    }

    // Only used in the io-uring/reference implementations
    fn freeze(self) -> bytes::Bytes {
        unreachable!();
    }
}
