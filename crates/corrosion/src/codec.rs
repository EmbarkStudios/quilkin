use bytes::{BufMut, Bytes, BytesMut};

pub const LENGTH_MARKER: &[u8] = &[0xfe, 0xed];

pub enum LengthPrefixError {
    /// The length of the buffer is too long for a u16 prefix
    TooLong(usize),
    /// The expected marker for the length prefix was wrong, indicating programmer
    /// error by not reserving the length prefix
    InvalidMarker,
}

impl std::fmt::Debug for LengthPrefixError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::TooLong(len) => write!(f, "{len} is too large for a u16 prefix"),
            Self::InvalidMarker => f.write_str("buffer did not have a length prefix reserved"),
        }
    }
}

/// Helper that length prefixes the buffer when it is frozen
pub struct PrefixedBuf {
    inner: BytesMut,
}

impl PrefixedBuf {
    #[inline]
    #[allow(clippy::new_without_default)]
    pub fn new() -> Self {
        Self::with_capacity(0)
    }

    #[inline]
    pub fn with_capacity(capacity: usize) -> Self {
        let mut inner = BytesMut::with_capacity(capacity + 2);
        reserve_length_prefix(&mut inner);
        Self { inner }
    }

    /// Gets the length of the buffer, minus the length prefix
    #[inline]
    pub fn len(&self) -> usize {
        self.inner.len() - 2
    }

    /// Checks if the buffer is empty
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.inner.len() <= 2
    }

    /// The maximum amount of bytes that can be appended to this buffer before it
    /// will be split
    #[inline]
    pub fn max_remainder(&self) -> usize {
        (u16::MAX as usize).saturating_sub(self.len())
    }

    /// Extends the buffer with the provided slice.
    ///
    /// Unlike [`BytesMut::extend_from_slice`], this will return a frozen buffer
    /// if the slice would extend the current buffer past the capacity of a `u16::MAX`
    #[inline]
    #[must_use]
    #[allow(clippy::checked_conversions)]
    pub fn extend_from_slice(&mut self, buf: &[u8]) -> Option<Bytes> {
        assert!(buf.len() <= u16::MAX as usize);

        if self.len() + buf.len() < u16::MAX as usize {
            self.inner.extend_from_slice(buf);
            return None;
        }

        let fb = self.freeze();
        self.inner.extend_from_slice(buf);
        fb
    }

    /// Extends the buffer with the provided slice, splitting the current buffer
    /// if adding the slice would exceed the maximum
    #[inline]
    #[must_use]
    pub fn extend_capped(&mut self, buf: &[u8], max: u16) -> Option<Bytes> {
        let rb = if self.len() + buf.len() > max as usize {
            self.freeze()
        } else {
            None
        };

        self.inner.extend_from_slice(buf);
        rb
    }

    /// Serializes the specified object to JSON and appends it to the end of the buffer and returns it
    ///
    /// This function is a helper and is meant to be used to emit buffers with a single item in it,
    /// that then may be coalesced with other items
    #[inline]
    pub fn write_json<T: serde::Serialize>(&mut self, obj: &T) -> std::io::Result<Bytes> {
        {
            // Write into a stack buffer, we should never have single objects that exceed this (generous)
            // capacity
            const CAPACITY: usize = 8 * 1024;
            let mut cursor = std::io::Cursor::new([0u8; CAPACITY]);
            serde_json::to_writer(&mut cursor, &obj)?;
            let len = cursor.position() as usize;

            if cursor.position() as usize > self.max_remainder() {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::OutOfMemory,
                    format!(
                        "serialized object of {len} was too large to fit in the remaining capacity of {}",
                        self.max_remainder()
                    ),
                ));
            }

            let buf = cursor.into_inner();
            let res = self.extend_from_slice(&buf[..len]);
            debug_assert!(res.is_none());
        }

        Ok(self
            .freeze()
            .expect("unreachable, we've just serialized an object"))
    }

    #[inline]
    #[must_use]
    pub fn freeze(&mut self) -> Option<Bytes> {
        if self.is_empty() {
            return None;
        }

        update_length_prefix(&mut self.inner).expect("unreachable");
        let buf = self.inner.split().freeze();
        reserve_length_prefix(&mut self.inner);
        Some(buf)
    }
}

#[inline]
pub fn reserve_length_prefix(buf: &mut BytesMut) {
    buf.extend_from_slice(LENGTH_MARKER);
}

/// Writes the length of the buffer into the first 2 bytes
///
/// Asserts if the length of the buffer is > 64k
#[inline]
pub fn update_length_prefix(buf: &mut BytesMut) -> Result<(), LengthPrefixError> {
    if buf.len() - 2 > u16::MAX as usize {
        return Err(LengthPrefixError::TooLong(buf.len() - 2));
    }

    let len = (buf.len() - 2) as u16;

    let len_slice = buf.get_mut(0..2).unwrap();

    if len_slice != LENGTH_MARKER {
        return Err(LengthPrefixError::InvalidMarker);
    }

    len_slice[0] = len as u8;
    len_slice[1] = (len >> 8) as u8;
    Ok(())
}

#[inline]
pub fn write_length_prefixed_jsonb<T: serde::Serialize>(item: &T) -> std::io::Result<BytesMut> {
    let mut buf = bytes::BytesMut::new();

    reserve_length_prefix(&mut buf);

    {
        let mut w = buf.writer();
        serde_json::to_writer(&mut w, item)?;
        buf = w.into_inner();
    }

    update_length_prefix(&mut buf).map_err(|_e| {
        std::io::Error::new(std::io::ErrorKind::OutOfMemory, "serialized JSON was > 64k")
    })?;
    Ok(buf)
}

#[inline]
pub fn write_length_prefixed(bytes: &[u8]) -> Result<BytesMut, LengthPrefixError> {
    let mut buf = bytes::BytesMut::with_capacity(bytes.len() + 2);

    reserve_length_prefix(&mut buf);
    buf.extend_from_slice(bytes);
    update_length_prefix(&mut buf)?;

    Ok(buf)
}

#[derive(thiserror::Error, Debug)]
pub enum LengthReadError {
    #[error(transparent)]
    ReadExact(#[from] quinn::ReadExactError),
    #[error(transparent)]
    Read(#[from] quinn::ReadError),
    #[error("stream ended")]
    StreamEnded,
    #[error(
        "expected a chunk of JSON length {} but received {}",
        expected,
        received
    )]
    LengthMismatch { expected: usize, received: usize },
    #[error(transparent)]
    Json(#[from] serde_json::Error),
}

use crate::persistent::ErrorCode as Ec;

impl<'s> From<&'s LengthReadError> for Ec {
    fn from(value: &'s LengthReadError) -> Self {
        match value {
            LengthReadError::Read(_)
            | LengthReadError::ReadExact(quinn::ReadExactError::ReadError(_))
            | LengthReadError::StreamEnded => Ec::ClientClosed,
            LengthReadError::ReadExact(quinn::ReadExactError::FinishedEarly(_)) => {
                Ec::LengthRequired
            }
            LengthReadError::LengthMismatch { expected, received } => {
                if received > expected {
                    Ec::PayloadTooLarge
                } else {
                    Ec::PayloadInsufficient
                }
            }
            LengthReadError::Json(_) => Ec::BadRequest,
        }
    }
}

#[inline]
pub async fn read_length_prefixed(
    recv: &mut quinn::RecvStream,
) -> Result<bytes::Bytes, LengthReadError> {
    let mut len = [0u8; 2];
    recv.read_exact(&mut len).await?;
    let len = u16::from_le_bytes(len) as usize;

    let Some(chunk) = recv.read_chunk(len, true).await? else {
        return Err(LengthReadError::StreamEnded);
    };

    if chunk.bytes.len() != len {
        return Err(LengthReadError::LengthMismatch {
            expected: len,
            received: chunk.bytes.len(),
        });
    }

    Ok(chunk.bytes)
}

#[inline]
pub async fn read_length_prefixed_jsonb<T: serde::de::DeserializeOwned>(
    recv: &mut quinn::RecvStream,
) -> Result<T, LengthReadError> {
    let bytes = read_length_prefixed(recv).await?;
    Ok(serde_json::from_slice(&bytes)?)
}

// #[inline]
// pub fn explicit_size<const N: usize>(buf: &[u8]) -> Result<[u8; N], Error> {
//     if buf.len() < N {
//         return Err(Error::InsufficientLength {
//             length: buf.len(),
//             expected: N,
//         });
//     }

//     // For now we won't care about the length being larger than what we want
//     let mut es = [0u8; N];
//     es.copy_from_slice(&buf[..N]);
//     Ok(es)
// }
