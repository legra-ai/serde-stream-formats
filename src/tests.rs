//! Round-trips, incremental decoding, limit sentinels, and failure
//! surfacing.

use std::io::{
    self,
    Read,
};

use serde::{
    Deserialize,
    Serialize,
};
use tokio_stream::StreamExt;

use crate::{
    DecodeFormat,
    EncodeFormat,
    FormatError,
    PayloadLimitExceeded,
};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct Document {
    name: String,
    values: Vec<u32>,
}

fn document() -> Document {
    Document {
        name: "streaming".to_owned(),
        values: (0..256).collect(),
    }
}

async fn collect(stream: crate::EncodedStream) -> Vec<u8> {
    let mut out = Vec::new();
    let mut stream = stream;
    while let Some(chunk) = stream.next().await {
        out.extend_from_slice(&chunk.expect("encoded chunk"));
    }
    out
}

#[tokio::test]
async fn json_stream_round_trips() {
    let bytes = collect(EncodeFormat::Json.encode_stream(document())).await;
    let back: Document = DecodeFormat::Json.decode_slice(&bytes).expect("decode");
    assert_eq!(back, document());
}

#[tokio::test]
async fn msgpack_stream_round_trips_with_named_fields() {
    let bytes = collect(EncodeFormat::MessagePack.encode_stream(document())).await;
    let back: Document = DecodeFormat::MessagePack
        .decode_slice(&bytes)
        .expect("decode");
    assert_eq!(back, document());
}

#[tokio::test]
async fn postcard_stream_round_trips() {
    let bytes = collect(EncodeFormat::Postcard.encode_stream(document())).await;
    let back: Document = DecodeFormat::Postcard.decode_slice(&bytes).expect("decode");
    assert_eq!(back, document());
}

#[tokio::test]
async fn yaml_encodes_but_is_not_a_decode_format() {
    let bytes = collect(EncodeFormat::Yaml.encode_stream(document())).await;
    let text = String::from_utf8(bytes).expect("YAML is UTF-8");
    let back: Document = serde_yaml2::from_str(&text).expect("YAML round-trip");
    assert_eq!(back, document());
    assert!(DecodeFormat::from_content_type("application/yaml").is_none());
}

/// A reader that hands out one byte at a time, proving the decoders
/// parse incrementally rather than requiring one contiguous read.
struct TrickleReader {
    bytes: Vec<u8>,
    position: usize,
}

impl Read for TrickleReader {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        if self.position >= self.bytes.len() || buf.is_empty() {
            return Ok(0);
        }
        buf[0] = self.bytes[self.position];
        self.position += 1;
        Ok(1)
    }
}

#[test]
fn json_decodes_from_a_trickling_reader() {
    let bytes = serde_json::to_vec(&document()).expect("encode");
    let reader = TrickleReader { bytes, position: 0 };
    let back: Document = DecodeFormat::Json.decode_reader(reader).expect("decode");
    assert_eq!(back, document());
}

#[test]
fn msgpack_decodes_from_a_trickling_reader() {
    let bytes = rmp_serde::to_vec_named(&document()).expect("encode");
    let reader = TrickleReader { bytes, position: 0 };
    let back: Document = DecodeFormat::MessagePack
        .decode_reader(reader)
        .expect("decode");
    assert_eq!(back, document());
}

/// A reader that fails with the payload-limit sentinel after a prefix.
struct LimitedReader {
    prefix: Vec<u8>,
    position: usize,
}

impl Read for LimitedReader {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        if self.position < self.prefix.len() && !buf.is_empty() {
            buf[0] = self.prefix[self.position];
            self.position += 1;
            return Ok(1);
        }
        Err(io::Error::other(PayloadLimitExceeded))
    }
}

#[test]
fn limit_sentinel_surfaces_as_payload_too_large() {
    let reader = LimitedReader {
        prefix: br#"{"name":"str"#.to_vec(),
        position: 0,
    };
    let err = DecodeFormat::Json
        .decode_reader::<Document, _>(reader)
        .expect_err("must fail");
    assert!(matches!(err, FormatError::PayloadTooLarge), "got {err}");
}

#[test]
fn plain_read_failures_stay_read_errors() {
    struct FailingReader;
    impl Read for FailingReader {
        fn read(&mut self, _buf: &mut [u8]) -> io::Result<usize> {
            Err(io::Error::other("connection reset"))
        }
    }
    let err = DecodeFormat::Json
        .decode_reader::<Document, _>(FailingReader)
        .expect_err("must fail");
    assert!(matches!(err, FormatError::Read { .. }), "got {err}");
}

#[test]
fn malformed_json_reports_position_detail() {
    let err = DecodeFormat::Json
        .decode_slice::<Document>(b"{\"name\": }")
        .expect_err("must fail");
    let FormatError::MalformedDocument { format, detail } = err else {
        panic!("wrong variant");
    };
    assert_eq!(format, "JSON");
    assert!(detail.contains("column"), "{detail}");
}

#[tokio::test]
async fn encode_failure_surfaces_as_a_trailing_error() {
    struct Unencodable;
    impl Serialize for Unencodable {
        fn serialize<S: serde::Serializer>(&self, _serializer: S) -> Result<S::Ok, S::Error> {
            Err(serde::ser::Error::custom("deliberately unencodable"))
        }
    }
    let mut stream = EncodeFormat::Json.encode_stream(Unencodable);
    let mut saw_error = false;
    while let Some(item) = stream.next().await {
        if let Err(err) = item {
            assert!(matches!(err, FormatError::Encoding { .. }), "got {err}");
            saw_error = true;
        }
    }
    assert!(saw_error, "the failure must reach the stream");
}

#[test]
fn content_type_resolution_ignores_parameters_and_case() {
    assert_eq!(
        DecodeFormat::from_content_type("Application/JSON; charset=utf-8"),
        Some(DecodeFormat::Json)
    );
    assert_eq!(
        DecodeFormat::from_content_type("application/x-msgpack"),
        Some(DecodeFormat::MessagePack)
    );
    assert_eq!(DecodeFormat::from_content_type("text/plain"), None);
    assert_eq!(
        EncodeFormat::from_content_type("text/yaml"),
        Some(EncodeFormat::Yaml)
    );
}
