use std::{
    mem, net,
    sync::atomic::{AtomicU16, Ordering},
};

pub(super) struct Mmap {
    pub(super) buf: *mut u8,
    len: usize,
}

impl Mmap {
    fn anonymous(len: usize) -> eyre::Result<Self> {
        unsafe {
            let mmap = libc::mmap(
                std::ptr::null_mut(),
                len,
                libc::PROT_READ | libc::PROT_WRITE,
                libc::MAP_SHARED | libc::MAP_POPULATE | libc::MAP_ANONYMOUS,
                -1,
                0,
            );
            if mmap == libc::MAP_FAILED {
                return Err(std::io::Error::last_os_error().into());
            }

            Ok(Self {
                buf: mmap.cast(),
                len,
            })
        }
    }
}

impl Drop for Mmap {
    fn drop(&mut self) {
        unsafe {
            libc::munmap(self.buf.cast(), self.len);
        }
    }
}

/// A ring buffer of buffers that can be filled with data
pub struct BufferRing {
    /// The start address of the ring entries
    ring: *mut io_uring_buf,
    /// The start address of where the actual data buffers are stored
    buffers: *mut u8,
    tail: &'static AtomicU16,
    /// The length of each buffer in the ring
    length: usize,
    /// The capacity of the ring
    pub(super) count: u16,
    /// The mask to determine the offset within the ring regardless of the index
    mask: u16,
    /// The backing mmap
    pub(super) mmap: Mmap,
}

unsafe impl Send for BufferRing {}

#[inline]
const fn ring_size(count: u16, length: usize) -> usize {
    (count as usize) * (mem::size_of::<io_uring_buf>() + length)
}

impl BufferRing {
    pub fn new(count: u16, length: u16) -> eyre::Result<Self> {
        eyre::ensure!(
            count.is_power_of_two() && length.is_power_of_two(),
            "count and length must be powers of 2"
        );

        let length = length as usize;

        let size = ring_size(count, length);
        let mmap = Mmap::anonymous(size)?;

        unsafe {
            let ring = mmap.buf.cast();
            let buffers = mmap
                .buf
                .byte_add(count as usize * mem::size_of::<io_uring_buf>())
                .cast();
            let tail = mmap
                .buf
                .byte_add(mem::offset_of!(io_uring_buf, tail))
                .cast();

            let this = Self {
                mmap,
                ring,
                buffers,
                tail: AtomicU16::from_ptr(tail),
                mask: count - 1,
                length,
                count,
            };

            // Mark all buffers in the ring as available for I/O
            {
                let count = count as usize;
                let ring = std::slice::from_raw_parts_mut(this.ring, count);
                let buf_base = this.buffers as u64;
                let alen = length as u64;
                let len = alen as u32;

                for (i, rb) in ring.iter_mut().enumerate() {
                    rb.addr = buf_base + i as u64 * alen;
                    rb.bid = i as u16;
                    rb.len = len;
                }

                this.tail.store(this.count, Ordering::Release);
            }

            Ok(this)
        }
    }

    /// Gets a buffer from the ring
    #[inline]
    pub fn dequeue(&self, id: u16) -> RingBuffer<'_> {
        unsafe {
            RingBuffer {
                buf: std::slice::from_raw_parts_mut(
                    self.buffers.byte_add(id as usize * self.length),
                    self.length,
                ),
                head: 0,
                tail: 0,
                buf_id: id,
            }
        }
    }

    #[inline]
    pub fn enqueue(&self) -> BufferRingEnqueuer<'_> {
        BufferRingEnqueuer {
            inner: self,
            tail: self.tail.load(Ordering::Relaxed),
        }
    }
}

pub struct BufferRingEnqueuer<'br> {
    inner: &'br BufferRing,
    tail: u16,
}

impl<'br> BufferRingEnqueuer<'br> {
    /// Returns the specified buffer id to the ring
    #[inline]
    pub fn enqueue_by_id(&mut self, id: u16) {
        unsafe {
            let next = &mut *self.inner.ring.add((self.tail & self.inner.mask) as usize);
            next.addr = self.inner.buffers.byte_add(id as usize * self.inner.length) as u64;
            next.bid = id;
        }

        self.tail = self.tail.wrapping_add(1);
    }
}

impl Drop for BufferRingEnqueuer<'_> {
    fn drop(&mut self) {
        self.inner.tail.store(self.tail, Ordering::Release);
    }
}

pub struct RingBuffer<'ring> {
    buf: &'ring mut [u8],
    head: usize,
    tail: usize,
    buf_id: u16,
}

const RECV_OUT: usize = std::mem::size_of::<io_uring_recvmsg_out>();

impl RingBuffer<'_> {
    #[inline]
    pub fn extract(&mut self, len: u32, hdr: &libc::msghdr) -> eyre::Result<net::SocketAddr> {
        eyre::ensure!(
            RECV_OUT < len as usize,
            "not enough space for io_uring_recvmsg_out"
        );

        unsafe {
            let out = self
                .buf
                .as_ptr()
                .cast::<io_uring_recvmsg_out>()
                .read_unaligned();

            eyre::ensure!(
                RECV_OUT as u32 + hdr.msg_namelen + hdr.msg_controllen as u32 + out.payload <= len,
                "insufficient space required for address and payload"
            );

            // First 2 bytes are the address family
            let family = self.buf[RECV_OUT] as u16 | (self.buf[RECV_OUT + 1] as u16) << 8;

            let addr = match family {
                2 /*libc::AF_INET*/ => {
                    eyre::ensure!(out.name == std::mem::size_of::<libc::sockaddr_in>() as u32, "invalid amount of bytes for ipv4 socket address");

                    let ipv4 = self.buf.as_ptr().byte_add(RECV_OUT).cast::<libc::sockaddr_in>().read_unaligned();

                    net::SocketAddr::V4(net::SocketAddrV4::new(net::Ipv4Addr::from_bits(u32::from_be(ipv4.sin_addr.s_addr)), u16::from_be(ipv4.sin_port)))
                }
                10 /*libc::AF_INET6*/ => {
                    eyre::ensure!(out.name == std::mem::size_of::<libc::sockaddr_in6>() as u32, "invalid amount of bytes for ipv6 socket address");

                    let ipv6 = self.buf.as_ptr().byte_add(RECV_OUT).cast::<libc::sockaddr_in6>().read_unaligned();

                    net::SocketAddr::V6(net::SocketAddrV6::new(net::Ipv6Addr::from_octets(ipv6.sin6_addr.s6_addr), u16::from_be(ipv6.sin6_port), ipv6.sin6_flowinfo, ipv6.sin6_scope_id))
                }
                _ => eyre::bail!("unknown socket address family"),
            };

            self.head = RECV_OUT + hdr.msg_namelen as usize;
            self.tail = self.head + out.payload as usize;

            Ok(addr)
        }
    }

    /// The identifier for this buffer within its owning ring
    #[inline]
    pub fn id(&self) -> u16 {
        self.buf_id
    }
}

impl crate::net::PacketMut for RingBuffer<'_> {
    fn extend_head(&mut self, mut bytes: &[u8]) {
        // If the head is already above the base and has enough space we can
        // just shift it down and copy over the bytes
        if self.head >= bytes.len() {
            unsafe {
                self.head -= bytes.len();

                std::ptr::copy_nonoverlapping(
                    bytes.as_ptr(),
                    self.buf.as_mut_ptr().byte_add(self.head),
                    bytes.len(),
                );
            }
        } else {
            // Otherwise we need to shift up the current bytes before prepending
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

impl crate::net::Packet for RingBuffer<'_> {
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

impl std::ops::Deref for RingBuffer<'_> {
    type Target = [u8];

    fn deref(&self) -> &Self::Target {
        &self.buf[self.head..self.tail]
    }
}

/// An entry in the ring buffer
///
/// <https://github.com/axboe/liburing/blob/075438e0b3f94d0b797f7c938dd69718e1a0b7c6/src/include/liburing/io_uring.h#L812>
#[repr(C)]
pub(crate) struct io_uring_buf {
    /// The base address of the buffer
    pub addr: u64,
    /// The length of the buffer
    pub len: u32,
    /// The buffer id (index)
    pub bid: u16,
    /// This is `resv`, but is really the location of the atomic tail pointer
    pub tail: u16,
}

/// The description of the layout of the rest of the buffer
#[repr(C)]
pub(crate) struct io_uring_recvmsg_out {
    /// The length of the socket address, if the address is shorter than the length
    /// specified in the msghdr for the op, the remainder will be zero filled
    pub name: u32,
    /// The lnegth of the control payload, we don't use this so it should always be 0
    pub control: u32,
    /// The length of the actual payload sent
    pub payload: u32,
    pub flags: u32,
}

#[cfg(test)]
mod ring_buffer {
    use crate::net::{Packet as _, PacketMut as _};

    use super::*;
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
