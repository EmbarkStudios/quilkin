use std::fmt;

/// Error codes that can be sent as the close/reset for an HTTP/3 stream
///
/// These are just integers, so they are just a subset of HTTP status codes
#[repr(u16)]
pub enum ErrorCode {
    Unknown = 0,
    /// Success
    Ok = 200,
    /// The client request was malformed
    BadRequest = 400,
    /// There was an error deserializing or otherwise handling a handshake
    BadHandshake = 402,
    /// A length prefixed piece frame could not be read because the length could
    /// not be read, or the frame could not be read before the end of the stream
    LengthRequired = 411,
    /// The size of a frame was too large
    PayloadTooLarge = 413,
    /// The size of a frame was too small
    PayloadInsufficient = 414,
    /// The client closed/aborted the connection before the server could send a
    /// response
    ClientClosed = 499,
    /// Internal server error
    InternalServerError = 500,
    /// The version of the client is not supported by the server
    VersionNotSupported = 505,
}

impl fmt::Display for ErrorCode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Unknown => f.write_str("0: unknown"),
            Self::Ok => f.write_str("200: ok"),
            Self::BadRequest => f.write_str("400: bad request"),
            Self::BadHandshake => f.write_str("402: bad handshake"),
            Self::LengthRequired => f.write_str("411: length required"),
            Self::PayloadTooLarge => f.write_str("413: payload too large"),
            Self::PayloadInsufficient => f.write_str("414: payload insufficient"),
            Self::ClientClosed => f.write_str("499: client closed"),
            Self::InternalServerError => f.write_str("500: internal server error"),
            Self::VersionNotSupported => f.write_str("505: version not supported"),
        }
    }
}

impl From<ErrorCode> for quinn::VarInt {
    fn from(value: ErrorCode) -> Self {
        Self::from_u32(value as u32)
    }
}

impl From<quinn::VarInt> for ErrorCode {
    fn from(value: quinn::VarInt) -> Self {
        match value.into_inner() {
            200 => Self::Ok,
            402 => Self::BadHandshake,
            411 => Self::LengthRequired,
            413 => Self::PayloadTooLarge,
            414 => Self::PayloadInsufficient,
            499 => Self::ClientClosed,
            500 => Self::InternalServerError,
            505 => Self::VersionNotSupported,
            _ => Self::Unknown,
        }
    }
}
