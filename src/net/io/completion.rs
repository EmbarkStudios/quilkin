pub use quilkin_uring::{eventfd, ring};
pub mod io_uring;

impl crate::net::PacketMut for ring::RingBuffer<'_> {
    fn extend_head(&mut self, mut bytes: &[u8]) {
        // If the head is already above the base and has enough space we can
        // just shift it down and copy over the bytes
        if self.head >= bytes.len() {
            // SAFETY: we ensure the copy stays within bounds
            unsafe {
                self.head -= bytes.len();

                std::ptr::copy_nonoverlapping(
                    bytes.as_ptr(),
                    self.buf.as_mut_ptr().byte_add(self.head),
                    bytes.len(),
                );
            }
        } else {
            // SAFETY: we ensure the copy stays within bounds
            unsafe {
                let start = if self.head > 0 {
                    let start = self.head;
                    std::ptr::copy_nonoverlapping(bytes.as_ptr(), self.buf.as_mut_ptr(), start);
                    bytes = &bytes[start..];
                    self.head = 0;
                    start
                } else {
                    0
                };

                let copy = bytes.len().min(self.buf.len() - start);
                let shift = (self.tail - start)
                    .min(bytes.len())
                    .min(self.buf.len() - copy);

                if shift > 0 {
                    std::ptr::copy(
                        self.buf.as_ptr().byte_add(start),
                        self.buf.as_mut_ptr().byte_add(start + copy),
                        shift,
                    );
                }

                std::ptr::copy_nonoverlapping(
                    bytes.as_ptr(),
                    self.buf.as_mut_ptr().byte_add(start),
                    copy,
                );

                self.tail = (self.tail + copy).min(self.buf.len());
            }
        }
    }

    #[inline]
    fn extend_tail(&mut self, bytes: &[u8]) {
        // SAFETY: we ensure the copy stays within bounds
        unsafe {
            let max = (self.buf.len() - self.tail).min(bytes.len());
            std::ptr::copy_nonoverlapping(
                bytes.as_ptr(),
                self.buf.as_mut_ptr().byte_add(self.tail),
                max,
            );
            self.tail += max;
        }
    }

    #[inline]
    fn remove_head(&mut self, length: usize) {
        self.head = (self.head + length).min(self.buf.len());
        self.tail = self.tail.max(self.head);
    }

    #[inline]
    fn remove_tail(&mut self, length: usize) {
        self.tail = self.tail.saturating_sub(length);
        self.head = self.head.min(self.tail);
    }

    #[inline]
    fn freeze(self) -> bytes::Bytes {
        bytes::Bytes::copy_from_slice(&self.buf[self.head..self.tail])
    }
}

impl crate::net::Packet for ring::RingBuffer<'_> {
    #[inline]
    fn as_slice(&self) -> &[u8] {
        &self.buf[self.head..self.tail]
    }

    #[inline]
    fn is_empty(&self) -> bool {
        self.tail - self.head == 0
    }

    #[inline]
    fn len(&self) -> usize {
        self.tail - self.head
    }
}

#[cfg(test)]
mod ring_buffer {
    use super::ring::*;
    use crate::net::{Packet as _, PacketMut as _};

    use std::net::SocketAddr;

    const V4: usize = std::mem::size_of::<libc::sockaddr_in>();
    const V6: usize = std::mem::size_of::<libc::sockaddr_in6>();

    #[inline]
    fn namelen(addr: &SocketAddr) -> u32 {
        if addr.is_ipv4() { V4 as u32 } else { V6 as u32 }
    }

    #[inline]
    fn construct(
        storage: &mut [u8; 2048],
        addr: SocketAddr,
        fill: u8,
        count: usize,
    ) -> RingBuffer<'_> {
        let namelen = namelen(&addr);

        let mut cursor = 0;

        // SAFETY: just a test, chill
        unsafe {
            {
                let out = &mut *storage
                    .as_mut_ptr()
                    .byte_add(cursor)
                    .cast::<io_uring_recvmsg_out>();
                out.name = namelen;
                out.control = 0;
                out.payload = count as u32;
                out.flags = 0;

                cursor += std::mem::size_of::<io_uring_recvmsg_out>();
            }

            match addr {
                SocketAddr::V4(v4) => {
                    let sa = &mut *storage
                        .as_mut_ptr()
                        .byte_add(cursor)
                        .cast::<libc::sockaddr_in>();
                    sa.sin_family = libc::AF_INET as _;
                    sa.sin_addr.s_addr = v4.ip().to_bits().to_be();
                    sa.sin_port = v4.port().to_be();
                    sa.sin_zero = [0; 8];

                    // If the msghdr name length is greater than the space needed by the address, the kernel will 0 fill the
                    // remainder
                    std::ptr::write_bytes(storage.as_mut_ptr().byte_add(cursor + V4), 0, V6 - V4);
                }
                SocketAddr::V6(v6) => {
                    let sa = &mut *storage
                        .as_mut_ptr()
                        .byte_add(cursor)
                        .cast::<libc::sockaddr_in6>();
                    sa.sin6_family = libc::AF_INET6 as _;
                    sa.sin6_addr.s6_addr = v6.ip().octets();
                    sa.sin6_port = v6.port().to_be();
                    sa.sin6_flowinfo = v6.flowinfo();
                    sa.sin6_scope_id = v6.scope_id();
                }
            }

            cursor += V6;

            std::ptr::write_bytes(storage.as_mut_ptr().byte_add(cursor), fill, count);
        }

        RingBuffer {
            buf: storage,
            head: 0,
            tail: 0,
            buf_id: 0,
        }
    }

    #[inline]
    fn finalize(
        storage: &mut [u8; 2048],
        addr: SocketAddr,
        fill: u8,
        count: usize,
    ) -> (RingBuffer<'_>, SocketAddr) {
        let mut rb = construct(storage, addr, fill, count);
        let addr = rb
            .extract(
                (RECV_OUT + V6 + count) as u32,
                &libc::msghdr {
                    msg_namelen: V6 as _,
                    // SAFETY: POD
                    ..unsafe { std::mem::zeroed() }
                },
            )
            .unwrap();
        (rb, addr)
    }

    const V4_ADDR: SocketAddr = SocketAddr::V4(std::net::SocketAddrV4::new(
        std::net::Ipv4Addr::from_bits(0xaabbccdd),
        7890,
    ));
    const V6_ADDR: SocketAddr = SocketAddr::V6(std::net::SocketAddrV6::new(
        std::net::Ipv6Addr::from_bits(0xffaabbccddeeff),
        20899,
        1,
        2,
    ));

    /// Tests we can extract valid ipv4 and ipv6 source addresses
    #[test]
    fn extracts() {
        // v4
        {
            let mut storage = [0u8; 2048];
            let (rb, addr) = finalize(&mut storage, V4_ADDR, 0x67, 20);
            assert_eq!(V4_ADDR, addr);
            assert_eq!(&rb[..], &[0x67; 20]);
        }

        // v6
        {
            let mut storage = [0u8; 2048];
            let (rb, addr) = finalize(&mut storage, V6_ADDR, 0x89, 35);
            assert_eq!(V6_ADDR, addr);
            assert_eq!(&rb[..], &[0x89; 35]);
        }
    }

    /// Tests tail manipulations
    #[test]
    fn tail() {
        // Ensure we can extend the tail
        {
            let mut storage = [0u8; 2048];
            let (mut rb, _) = finalize(&mut storage, V4_ADDR, 0x11, 800);
            rb.extend_tail(&[0x22; 88]);

            assert_eq!(&rb[..800], &[0x11; 800]);
            assert_eq!(&rb[800..], &[0x22; 88]);
        }

        // Ensure we can truncate
        {
            let mut storage = [0u8; 2048];
            let (mut rb, _) = finalize(&mut storage, V4_ADDR, 0x11, 800);
            rb.extend_tail(&[0x22; 88]);
            rb.remove_tail(800);
            assert_eq!(&rb[..88], &[0x11; 88]);

            // Truncating more than the size of the actual buffer should be fine, we just saturate
            rb.remove_tail(100);
            assert!(rb.is_empty());

            rb.extend_tail(&[0x33; 23]);
            assert_eq!(rb.len(), 23);

            rb.remove_tail(100);
            assert!(rb.is_empty());
        }
    }

    /// Tests head manipulation
    #[test]
    fn head() {
        // Ensure we can extend the head within the bounds of the prefix
        {
            let mut storage = [0u8; 2048];
            let (mut rb, _) = finalize(&mut storage, V4_ADDR, 0x23, 12);
            assert_eq!(rb.len(), 12);
            rb.extend_head(&[0xee; RECV_OUT + V6 - 1]);
            rb.extend_head(&[0xfe; 1]);
            assert_eq!(&rb[..1], &[0xfe; 1]);
            assert_eq!(&rb[1..RECV_OUT + V6], &[0xee; RECV_OUT + V6 - 1]);
            assert_eq!(&rb[RECV_OUT + V6..], &[0x23; 12]);
        }

        // Ensure we can extend the head outside the bounds of the prefix
        {
            let mut storage = [0u8; 2048];
            let (mut rb, _) = finalize(&mut storage, V4_ADDR, 0x23, 12);
            assert_eq!(rb.len(), 12);
            rb.extend_head(&[0x32; 100]);
            assert_eq!(&rb[..100], &[0x32; 100]);
            assert_eq!(&rb[100..], &[0x23; 12]);
            rb.extend_head(&[0xde; 1024]);
            assert_eq!(&rb[..1024], &[0xde; 1024]);
            assert_eq!(&rb[1024..1024 + 100], &[0x32; 100]);
            assert_eq!(&rb[1024 + 100..], &[0x23; 12]);

            // Ensure we can displace the entire buffer
            rb.extend_head(&[0xf1; 2048]);
            assert_eq!(&rb[..], &[0xf1; 2048]);

            // ... even if it wildy too large
            rb.extend_head(&[0x80; 3000]);
            assert_eq!(&rb[..], &[0x80; 2048]);
        }

        // Ensure we can truncate
        {
            let mut storage = [0u8; 2048];
            let (mut rb, _) = finalize(&mut storage, V4_ADDR, 0x11, 800);
            rb.remove_head(1000);
            assert!(rb.is_empty());

            rb.remove_tail(100);
            assert!(rb.is_empty());

            rb.extend_head(&[0x33; 844]);
            assert_eq!(&rb[..], &[0x33; 844]);

            rb.remove_head(1000);
            rb.extend_head(&[0xdd; 4]);
            assert_eq!(&rb[..], &[0xdd; 4]);
        }
    }
}
