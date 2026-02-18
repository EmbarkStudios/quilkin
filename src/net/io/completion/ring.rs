use std::{
    io, mem, net, ptr,
    sync::atomic::{AtomicU16, Ordering},
};

struct Mmap {
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
    /// The group id of this ring buffer used to distinguish it from other buffer groups
    pub group_id: u16,
    /// The backing mmap
    pub(super) mmap: Mmap,
}

#[inline]
const fn ring_size(count: u16, length: usize) -> usize {
    (count as usize) * (mem::size_of::<io_uring_buf>() + length)
}

impl BufferRing {
    pub fn new(count: u16, length: u16, group_id: u16) -> eyre::Result<Self> {
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
                group_id,
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

    // /// Register this ring buffer with the io_uring context
    // #[inline]
    // pub fn register(&self, fd: std::os::fd::RawFd) -> Result<(), skur_libc::error::Error> {
    //     let reg = crate::kernel::io_uring_buf_reg {
    //         ring_addr: self.buffers as u64,
    //         ring_entries: self.count as _,
    //         bgid: self.group_id,
    //         ..Default::default()
    //     };

    //     crate::kernel::io_uring_register(fd, crate::kernel::RegisterOp::RegisterRing(&reg))
    //         .map(|_| ())
    // }

    // #[inline]
    // pub fn unregister(&self, fd: std::os::fd::RawFd) -> Result<(), skur_libc::error::Error> {
    //     let reg = crate::kernel::io_uring_buf_reg {
    //         bgid: self.group_id,
    //         ..Default::default()
    //     };

    //     crate::kernel::io_uring_register(fd, crate::kernel::RegisterOp::UnregisterRing(&reg))
    //         .map(|_| ())
    // }

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
                group_id: self.group_id,
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
    /// Returns the buffer to the ring, allowing it to be used for I/O
    #[inline]
    pub fn enqueue(&mut self, buf: RingBuffer<'br>) {
        debug_assert_eq!(buf.group_id, self.inner.group_id);

        unsafe {
            let next = &mut *self.inner.ring.add((self.tail & self.inner.mask) as usize);
            next.addr = buf.buf.as_ptr() as u64;

            #[cfg(debug_assertions)]
            {
                assert_eq!(
                    buf.buf_id,
                    (buf.buf.as_ptr().offset_from_unsigned(self.inner.buffers) / self.inner.length)
                        as u16
                );
            }

            next.bid = buf.buf_id;
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
    group_id: u16,
    buf_id: u16,
}

impl RingBuffer<'_> {
    #[inline]
    pub fn extract(&mut self, len: u32, hdr: &libc::msghdr) -> eyre::Result<net::SocketAddr> {
        const RECV_OUT: usize = std::mem::size_of::<io_uring_recvmsg_out>();
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
            let family = self.buf[0] as u16 | (self.buf[1] as u16) << 8;

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

    // #[inline]
    // pub fn read<T: plain::Plain>(&self, offset: usize) -> Option<T> {
    //     if offset + std::mem::size_of::<T>() <= self.buf.len() {
    //         unsafe {
    //             Some(std::ptr::read_unaligned(
    //                 self.buf.as_ptr().byte_add(offset).cast(),
    //             ))
    //         }
    //     } else {
    //         None
    //     }
    // }
}

impl crate::net::PacketMut for RingBuffer<'_> {
    fn extend_head(&mut self, mut bytes: &[u8]) {
        assert!(bytes.len() < self.buf.len());

        // If the head is already above the base and has enough space we can
        // just shift it down copy the bytes
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

                let shift = (self.tail - start)
                    .min(bytes.len())
                    .min(self.buf.len() - self.tail);
                std::ptr::copy(
                    self.buf.as_ptr().byte_add(start),
                    self.buf.as_mut_ptr().byte_add(start + shift),
                    shift,
                );
                std::ptr::copy_nonoverlapping(
                    bytes[..shift].as_ptr(),
                    self.buf.as_mut_ptr().byte_add(start),
                    shift,
                );

                self.tail += shift;
            }
        }
    }

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

    fn remove_head(&mut self, length: usize) {
        self.head = (self.head + length).min(self.buf.len());
        self.tail = self.tail.max(self.head);
    }

    fn remove_tail(&mut self, length: usize) {
        self.tail = self.tail.saturating_sub(length);
        self.head = self.head.min(self.tail);
    }

    type FrozenPacket = Self;

    fn freeze(self) -> Self::FrozenPacket {
        self
    }
}

impl crate::net::Packet for RingBuffer<'_> {
    fn as_slice(&self) -> &[u8] {
        &self.buf[self.head..self.tail]
    }

    fn is_empty(&self) -> bool {
        self.head == self.tail
    }

    fn len(&self) -> usize {
        self.tail - self.head
    }
}

// impl std::ops::Deref for RingBuffer<'_> {
//     type Target = [u8];

//     fn deref(&self) -> &Self::Target {
//         self.buf
//     }
// }

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
