use io_uring::squeue::Entry;
use std::os::fd::{AsRawFd, FromRawFd};

/// A simple wrapper around [eventfd](https://man7.org/linux/man-pages/man2/eventfd.2.html)
///
/// We use eventfd to signal to io uring loops from async tasks, it is essentially
/// the equivalent of a signalling 64 bit cross-process atomic
pub struct EventFd {
    fd: std::os::fd::OwnedFd,
    val: u64,
}

impl EventFd {
    #[inline]
    pub(crate) fn new() -> std::io::Result<Self> {
        // SAFETY: We have no invariants to uphold, but we do need to check the
        // return value
        let fd = unsafe { libc::eventfd(0, 0) };

        // This can fail for various reasons mostly around resource limits, if
        // this is hit there is either something really wrong (OOM, too many file
        // descriptors), or resource limits were externally placed that were too strict
        if fd == -1 {
            return Err(std::io::Error::last_os_error());
        }

        Ok(Self {
            // SAFETY: we've validated the file descriptor
            fd: unsafe { std::os::fd::OwnedFd::from_raw_fd(fd) },
            val: 0,
        })
    }

    #[inline]
    pub(crate) fn writer(&self) -> EventFdWriter {
        EventFdWriter {
            fd: self.fd.as_raw_fd(),
        }
    }

    /// Constructs an io-uring entry to read (ie wait) on this eventfd
    #[inline]
    pub(crate) fn io_uring_entry(&mut self) -> Entry {
        io_uring::opcode::Read::new(self.fd(), (&mut self.val as *mut u64).cast(), 8).build()
    }

    #[inline]
    pub(crate) fn fd(&self) -> io_uring::types::Fd {
        io_uring::types::Fd(self.fd.as_raw_fd())
    }
}

#[derive(Clone)]
pub(crate) struct EventFdWriter {
    fd: i32,
}

impl EventFdWriter {
    #[inline]
    pub(crate) fn write(&self, val: u64) {
        // SAFETY: we have a valid descriptor, and most of the errors that apply
        // to the general write call that eventfd_write wraps are not applicable
        //
        // Note that while the docs state eventfd_write is glibc, it is implemented
        // on musl as well, but really is just a write with 8 bytes
        unsafe {
            libc::eventfd_write(self.fd, val);
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;

    /// This is just a sanity check that eventfd, which we use to notify the io-uring
    /// loop of events from async tasks, functions as we need to, namely that
    /// an event posted before the I/O request is submitted to the I/O loop still
    /// triggers the completion of the I/O request
    #[test]
    #[cfg(target_os = "linux")]
    #[allow(clippy::undocumented_unsafe_blocks)]
    fn eventfd_works_as_expected() {
        let mut event = EventFd::new().unwrap();
        let event_writer = event.writer();

        // Write even before we create the loop
        event_writer.write(1);

        let mut ring = io_uring::IoUring::new(2).unwrap();
        let (submitter, mut sq, mut cq) = ring.split();

        unsafe {
            sq.push(&event.io_uring_entry().user_data(1)).unwrap();
        }

        sq.sync();

        loop {
            match submitter.submit_and_wait(1) {
                Ok(_) => {}
                Err(ref err) if err.raw_os_error() == Some(libc::EBUSY) => {}
                Err(error) => {
                    panic!("oh no {error}");
                }
            }
            cq.sync();

            for cqe in &mut cq {
                assert_eq!(cqe.result(), 8);

                match cqe.user_data() {
                    // This was written before the loop started, but now write to the event
                    // before queuing up the next read
                    1 => {
                        assert_eq!(event.val, 1);
                        event_writer.write(9999);

                        unsafe {
                            sq.push(&event.io_uring_entry().user_data(2)).unwrap();
                        }
                    }
                    2 => {
                        assert_eq!(event.val, 9999);
                        return;
                    }
                    _ => unreachable!(),
                }
            }

            sq.sync();
        }
    }
}
