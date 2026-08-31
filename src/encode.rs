//! Bounded streaming encoding of Serde values.

use std::fmt;
use std::io::{
    self,
    Write as IoWrite,
};
use std::pin::Pin;
use std::task::{
    Context,
    Poll,
};

use bytes::Bytes;
use serde::Serialize;
use tokio::sync::mpsc;
use tokio_stream::Stream;
use tokio_stream::wrappers::ReceiverStream;

use crate::error::FormatError;

/// How many encoded chunks may sit between the serializer and the
/// consumer before the serializer backpressures.
const SERIALIZER_CHANNEL_CAPACITY: usize = 8;
/// The largest single chunk the serializer emits.
const MAX_SERIALIZER_CHUNK_BYTES: usize = 64 * 1024;

/// A Serde wire format this crate can encode as a bounded stream.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EncodeFormat {
    /// `application/json`.
    Json,
    /// `application/yaml` (RFC 9512).
    Yaml,
    /// `application/vnd.msgpack`, encoded with named fields so the
    /// output stays self-describing.
    MessagePack,
    /// `application/x-postcard`.
    Postcard,
}

impl EncodeFormat {
    /// The format's canonical media type.
    #[must_use]
    pub const fn media_type(self) -> &'static str {
        match self {
            Self::Json => "application/json",
            Self::Yaml => "application/yaml",
            Self::MessagePack => "application/vnd.msgpack",
            Self::Postcard => "application/x-postcard",
        }
    }

    /// The format's display name for error detail.
    #[must_use]
    pub const fn name(self) -> &'static str {
        match self {
            Self::Json => "JSON",
            Self::Yaml => "YAML",
            Self::MessagePack => "MessagePack",
            Self::Postcard => "Postcard",
        }
    }

    /// Resolve a media-type value to an encodable format.
    ///
    /// Parameters are ignored; matching is case-insensitive; documented
    /// pre-registration aliases are accepted for YAML and `MessagePack`.
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
            "application/yaml" | "application/x-yaml" | "text/yaml" => Some(Self::Yaml),
            "application/vnd.msgpack" | "application/msgpack" | "application/x-msgpack" => {
                Some(Self::MessagePack)
            }
            "application/x-postcard" => Some(Self::Postcard),
            _ => None,
        }
    }

    /// Encode a bounded value into one in-memory buffer.
    ///
    /// For payloads that must exist before headers are sent (an error
    /// envelope, a small control document). Ordinary payloads stream
    /// through [`EncodeFormat::encode_stream`].
    ///
    /// # Errors
    ///
    /// [`FormatError::Encoding`] when the value cannot be encoded in
    /// this format.
    pub fn encode_vec<T: Serialize>(self, value: &T) -> Result<Vec<u8>, FormatError> {
        match self {
            Self::Json => serde_json::to_vec(value).map_err(|error| self.unencodable(error)),
            Self::Yaml => serde_yaml2::to_string(value)
                .map(String::into_bytes)
                .map_err(|error| self.unencodable(error)),
            Self::MessagePack => {
                rmp_serde::to_vec_named(value).map_err(|error| self.unencodable(error))
            }
            Self::Postcard => postcard::to_allocvec(value).map_err(|error| self.unencodable(error)),
        }
    }

    /// Encode an owned value as an asynchronous, bounded chunk stream.
    ///
    /// The serializer runs on a blocking worker and hands chunks of at
    /// most 64 KiB through a bounded channel, so memory stays
    /// `O(channel × chunk)` regardless of the encoded size and a slow
    /// consumer backpressures the serializer instead of buffering. An
    /// encoding failure surfaces as a trailing `Err` item — the stream
    /// is never silently truncated.
    ///
    /// Must be called from within a Tokio runtime.
    pub fn encode_stream<T>(self, value: T) -> EncodedStream
    where
        T: Serialize + Send + 'static,
    {
        let (sender, receiver) = mpsc::channel(SERIALIZER_CHANNEL_CAPACITY);
        tokio::task::spawn_blocking(move || {
            let mut writer = ChannelWriter { sender };
            let result = match self {
                Self::Json => {
                    serde_json::to_writer(&mut writer, &value).map_err(|error| error.to_string())
                }
                Self::Yaml => {
                    let mut formatter = ChannelFormatter {
                        writer: &mut writer,
                    };
                    let mut serializer = serde_yaml2::ser::YamlSerializer::new(&mut formatter);
                    serializer.write(value).map_err(|error| error.to_string())
                }
                Self::MessagePack => value
                    .serialize(&mut rmp_serde::Serializer::new(&mut writer).with_struct_map())
                    .map_err(|error| error.to_string()),
                Self::Postcard => postcard::to_io(&value, &mut writer)
                    .map(|_| ())
                    .map_err(|error| error.to_string()),
            };
            if let Err(error) = result {
                let format_error = self.unencodable(error);
                let _ = writer.sender.blocking_send(Err(format_error));
            }
        });
        EncodedStream {
            inner: ReceiverStream::new(receiver),
        }
    }

    fn unencodable(self, error: impl std::fmt::Display) -> FormatError {
        FormatError::Encoding {
            format: self.name(),
            detail: error.to_string(),
        }
    }
}

/// The bounded chunk stream produced by [`EncodeFormat::encode_stream`].
#[derive(Debug)]
pub struct EncodedStream {
    inner: ReceiverStream<Result<Bytes, FormatError>>,
}

impl Stream for EncodedStream {
    type Item = Result<Bytes, FormatError>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        Pin::new(&mut self.inner).poll_next(cx)
    }
}

/// Bridges the synchronous serializer to the bounded chunk channel.
struct ChannelWriter {
    sender: mpsc::Sender<Result<Bytes, FormatError>>,
}

impl IoWrite for ChannelWriter {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        if bytes.is_empty() {
            return Ok(0);
        }
        for chunk in bytes.chunks(MAX_SERIALIZER_CHUNK_BYTES) {
            self.sender
                .blocking_send(Ok(Bytes::copy_from_slice(chunk)))
                .map_err(|_| io::Error::new(io::ErrorKind::BrokenPipe, "encoded stream dropped"))?;
        }
        Ok(bytes.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

/// Adapts the byte writer for serializers that write `str` output.
struct ChannelFormatter<'a> {
    writer: &'a mut ChannelWriter,
}

impl fmt::Write for ChannelFormatter<'_> {
    fn write_str(&mut self, value: &str) -> fmt::Result {
        self.writer
            .write_all(value.as_bytes())
            .map_err(|_| fmt::Error)
    }
}
