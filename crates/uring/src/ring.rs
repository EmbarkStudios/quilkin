use std::{
    io, mem, net,
    sync::atomic::{AtomicU16, Ordering},
};

pub struct Mmap {
    pub buf: *mut u8,
    len: usize,
}

impl Mmap {
    fn anonymous(len: usize) -> io::Result<Self> {
        // SAFETY: syscall, we check errors
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
                return Err(io::Error::last_os_error());
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
        // SAFETY: syscall, the inputs are valid
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
    pub count: u16,
    /// The mask to determine the offset within the ring regardless of the index
    mask: u16,
    /// The backing mmap
    pub mmap: Mmap,
}

// SAFETY: the pointers live as long as the owned mmap
unsafe impl Send for BufferRing {}

#[inline]
const fn ring_size(count: u16, length: usize) -> usize {
    (count as usize) * (mem::size_of::<io_uring_buf>() + length)
}

impl BufferRing {
    pub fn new(count: u16, length: u16) -> io::Result<Self> {
        if !count.is_power_of_two() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "count must be a power of 2",
            ));
        } else if !length.is_power_of_two() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "length must be a power of 2",
            ));
        }

        let length = length as usize;

        let size = ring_size(count, length);
        let mmap = Mmap::anonymous(size)?;

        // SAFETY: we've sized the mmap appopriately
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
        // SAFETY: the backing mmap lives as long as Self
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

    #[cfg(debug_assertions)]
    pub fn len(&self, id: u16) -> u16 {
        let tail = self.tail.load(Ordering::Relaxed) & self.mask;
        if tail > id {
            tail - id
        } else {
            (self.count - id).wrapping_add(tail).saturating_sub(1)
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
        // SAFETY: the backing mmap lives as long as the BufferRing itself
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
    pub buf: &'ring mut [u8],
    pub head: usize,
    pub tail: usize,
    pub buf_id: u16,
}

pub const RECV_OUT: usize = std::mem::size_of::<io_uring_recvmsg_out>();

impl RingBuffer<'_> {
    #[inline]
    pub fn extract(&mut self, len: u32, hdr: &libc::msghdr) -> eyre::Result<net::SocketAddr> {
        eyre::ensure!(
            RECV_OUT < len as usize,
            "not enough space for io_uring_recvmsg_out"
        );

        // SAFETY: we ensure we don't read outside of the bounds
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

            let addr = match family as i32 {
                libc::AF_INET => {
                    eyre::ensure!(
                        out.name == std::mem::size_of::<libc::sockaddr_in>() as u32,
                        "invalid amount of bytes for ipv4 socket address"
                    );

                    let ipv4 = self
                        .buf
                        .as_ptr()
                        .byte_add(RECV_OUT)
                        .cast::<libc::sockaddr_in>()
                        .read_unaligned();

                    net::SocketAddr::V4(net::SocketAddrV4::new(
                        net::Ipv4Addr::from_bits(u32::from_be(ipv4.sin_addr.s_addr)),
                        u16::from_be(ipv4.sin_port),
                    ))
                }
                libc::AF_INET6 => {
                    eyre::ensure!(
                        out.name == std::mem::size_of::<libc::sockaddr_in6>() as u32,
                        "invalid amount of bytes for ipv6 socket address"
                    );

                    let ipv6 = self
                        .buf
                        .as_ptr()
                        .byte_add(RECV_OUT)
                        .cast::<libc::sockaddr_in6>()
                        .read_unaligned();

                    net::SocketAddr::V6(net::SocketAddrV6::new(
                        net::Ipv6Addr::from_octets(ipv6.sin6_addr.s6_addr),
                        u16::from_be(ipv6.sin6_port),
                        ipv6.sin6_flowinfo,
                        ipv6.sin6_scope_id,
                    ))
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
pub struct io_uring_recvmsg_out {
    /// The length of the socket address, if the address is shorter than the length
    /// specified in the msghdr for the op, the remainder will be zero filled
    pub name: u32,
    /// The lnegth of the control payload, we don't use this so it should always be 0
    pub control: u32,
    /// The length of the actual payload sent
    pub payload: u32,
    pub flags: u32,
}
