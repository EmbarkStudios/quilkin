pub mod eventfd;
pub mod io_uring;
pub mod ring;

// io-uring keeps many things private which is incredibly tedious
pub mod ops {
    /// <https://man.archlinux.org/man/io_uring_enter.2.en#IORING_OP_READV>
    pub const IORING_OP_READV: u8 = 1;
    /// <https://man.archlinux.org/man/io_uring_enter.2.en#IORING_OP_RECVMSG>
    pub const IORING_OP_RECVMSG: u8 = 10;
    /// <https://man.archlinux.org/man/io_uring_enter.2.en#IORING_OP_READ>
    pub const IORING_OP_READ: u8 = 22;
    /// <https://man.archlinux.org/man/io_uring_enter.2.en#IORING_OP_SEND_ZC>
    pub const IORING_OP_SEND_ZC: u8 = 47;
    /// <https://man.archlinux.org/man/io_uring_enter.2.en#IORING_OP_READ_MULTISHOT>
    pub const IORING_OP_READ_MULTISHOT: u8 = 49;
}

pub mod flags {
    pub type Enum = u32;

    /// If set, the upper 16 bits of the flags field carries the buffer ID that was chosen for this request.
    ///
    /// The request must have been issued with `IOSQE_BUFFER_SELECT` set, and used with a request type that supports
    /// buffer selection. Additionally, buffers must have been provided upfront either via the `IORING_OP_PROVIDE_BUFFERS`
    /// or the `IORING_REGISTER_PBUF_RING` methods.
    pub const IORING_CQE_F_BUFFER: Enum = 1 << 0;
    /// If set, the application should expect more completions from the request.
    ///
    /// This is used for requests that can generate multiple completions, such as multi-shot requests, receive, or accept.
    pub const IORING_CQE_F_MORE: Enum = 1 << 1;
    /// Set for notification CQEs.
    ///
    /// Can be used to distinct them from sends.
    pub const IORING_CQE_F_NOTIF: Enum = 1 << 3;
}
