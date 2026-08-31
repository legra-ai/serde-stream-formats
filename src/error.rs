//! Typed failures for streaming encode and incremental decode.

use std::{
    fmt,
    io,
};

/// A failure while decoding or encoding a Serde document.
#[derive(Debug)]
pub enum FormatError {
    /// The input is not a valid document in the named format.
    MalformedDocument {
        /// The format's display name.
        format: &'static str,
        /// Decoder detail (position information where the decoder
        /// provides it).
        detail: String,
    },
    /// The input stream ended the transfer because a configured size
    /// limit was exceeded (see [`crate::PayloadLimitExceeded`]).
    PayloadTooLarge,
    /// The underlying reader failed before the document ended.
    Read {
        /// Human-readable I/O detail.
        detail: String,
    },
    /// The value failed to encode in the named format.
    Encoding {
        /// The format's display name.
        format: &'static str,
        /// Encoder detail.
        detail: String,
    },
}

impl fmt::Display for FormatError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::MalformedDocument { format, detail } => {
                write!(f, "malformed {format} document: {detail}")
            }
            Self::PayloadTooLarge => f.write_str("payload exceeded the configured size limit"),
            Self::Read { detail } => write!(f, "body read failed: {detail}"),
            Self::Encoding { format, detail } => {
                write!(f, "{format} encoding failed: {detail}")
            }
        }
    }
}

impl std::error::Error for FormatError {}

/// The sentinel a size-limiting reader wraps into [`io::Error`] so the
/// decoder can distinguish "too large" from an ordinary read failure.
///
/// A transport adapter that enforces a body limit should surface the
/// limit hit as `io::Error::other(PayloadLimitExceeded)`; every
/// [`crate::DecodeFormat`] decoder then reports it as
/// [`FormatError::PayloadTooLarge`] instead of a generic read error.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PayloadLimitExceeded;

impl fmt::Display for PayloadLimitExceeded {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("payload exceeded the configured size limit")
    }
}

impl std::error::Error for PayloadLimitExceeded {}

impl FormatError {
    /// Classify a decoder-side I/O failure: the payload-limit sentinel
    /// becomes [`FormatError::PayloadTooLarge`]; everything else is a
    /// read failure.
    #[must_use]
    pub fn from_read_error(error: &io::Error) -> Self {
        if matches!(error.get_ref(), Some(source) if source.is::<PayloadLimitExceeded>()) {
            Self::PayloadTooLarge
        } else {
            Self::Read {
                detail: error.to_string(),
            }
        }
    }
}
