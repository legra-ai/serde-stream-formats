//! Incremental decoding of Serde documents from a byte reader.

use std::io::Read;

use serde::de::DeserializeOwned;

use crate::error::FormatError;

/// Scratch capacity for the Postcard incremental decoder.
const POSTCARD_SCRATCH_BYTES: usize = 16 * 1024;

/// A Serde wire format this crate can decode incrementally from a
/// reader, without ever owning the complete input.
///
/// Whole-document formats (YAML, TOML) are deliberately absent: their
/// available decoders require the full input in memory, which defeats
/// the bounded-memory contract. Reject them upstream with a typed
/// unsupported-media-type failure instead.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DecodeFormat {
    /// `application/json`.
    Json,
    /// `application/vnd.msgpack`, self-describing (named fields).
    MessagePack,
    /// `application/x-postcard` — not self-describing; callers must
    /// know the schema out of band.
    Postcard,
}

impl DecodeFormat {
    /// The format's canonical media type.
    #[must_use]
    pub const fn media_type(self) -> &'static str {
        match self {
            Self::Json => "application/json",
            Self::MessagePack => "application/vnd.msgpack",
            Self::Postcard => "application/x-postcard",
        }
    }

    /// The format's display name for error detail.
    #[must_use]
    pub const fn name(self) -> &'static str {
        match self {
            Self::Json => "JSON",
            Self::MessagePack => "MessagePack",
            Self::Postcard => "Postcard",
        }
    }

    /// Resolve a `Content-Type` value to a decodable format.
    ///
    /// Parameters (`; charset=…`) are ignored; matching is
    /// case-insensitive. Documented pre-registration aliases are
    /// accepted for `MessagePack`. `None` means the media type is not
    /// an incrementally decodable format.
    #[must_use]
    pub fn from_content_type(content_type: &str) -> Option<Self> {
        let media_type = content_type
            .split(';')
            .next()
            .unwrap_or_default()
            .trim()
            .to_ascii_lowercase();
        match media_type.as_str() {
            "application/json" => Some(Self::Json),
            "application/vnd.msgpack" | "application/msgpack" | "application/x-msgpack" => {
                Some(Self::MessagePack)
            }
            "application/x-postcard" => Some(Self::Postcard),
            _ => None,
        }
    }

    /// Decode a complete in-memory document.
    ///
    /// For inputs that are already bounded (a small control message, a
    /// test fixture). Streaming inputs go through
    /// [`DecodeFormat::decode_reader`].
    ///
    /// # Errors
    ///
    /// [`FormatError::MalformedDocument`] with decoder position detail
    /// (where the decoder provides it).
    pub fn decode_slice<T: DeserializeOwned>(self, body: &[u8]) -> Result<T, FormatError> {
        match self {
            Self::Json => {
                serde_json::from_slice(body).map_err(|error| FormatError::MalformedDocument {
                    format: self.name(),
                    detail: format!("line {} column {}: {error}", error.line(), error.column()),
                })
            }
            Self::MessagePack => rmp_serde::from_slice(body).map_err(|error| self.malformed(error)),
            Self::Postcard => postcard::from_bytes(body).map_err(|error| self.malformed(error)),
        }
    }

    /// Decode a document incrementally from `reader`.
    ///
    /// The decoder pulls from the reader as it parses, so the caller
    /// never owns the complete input. Run it on a blocking-capable
    /// thread when the reader bridges an asynchronous source.
    ///
    /// A reader that enforces a size limit should surface the limit
    /// hit as `io::Error::other(`[`crate::PayloadLimitExceeded`]`)`;
    /// it is reported as [`FormatError::PayloadTooLarge`].
    ///
    /// # Errors
    ///
    /// [`FormatError::Read`] / [`FormatError::PayloadTooLarge`] when
    /// the reader fails, [`FormatError::MalformedDocument`] when the
    /// input is not a valid document in this format.
    pub fn decode_reader<T, R>(self, reader: R) -> Result<T, FormatError>
    where
        T: DeserializeOwned,
        R: Read,
    {
        match self {
            Self::Json => serde_json::from_reader(reader).map_err(|error| {
                if error.is_io() {
                    return FormatError::from_read_error(&error.into());
                }
                FormatError::MalformedDocument {
                    format: self.name(),
                    detail: error.to_string(),
                }
            }),
            Self::MessagePack => rmp_serde::from_read(reader).map_err(|error| match error {
                rmp_serde::decode::Error::InvalidMarkerRead(error)
                | rmp_serde::decode::Error::InvalidDataRead(error) => {
                    FormatError::from_read_error(&error)
                }
                error => self.malformed(error),
            }),
            Self::Postcard => {
                let mut scratch = [0_u8; POSTCARD_SCRATCH_BYTES];
                postcard::from_io((reader, &mut scratch))
                    .map(|(value, _)| value)
                    .map_err(|error| self.malformed(error))
            }
        }
    }

    /// A typed malformed-document failure carrying the decoder's detail.
    fn malformed(self, error: impl std::fmt::Display) -> FormatError {
        FormatError::MalformedDocument {
            format: self.name(),
            detail: error.to_string(),
        }
    }
}
