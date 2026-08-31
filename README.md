# serde-stream-formats

[![Crates.io][crates-badge]][crates-url]
[![Documentation][docs-badge]][docs-url]
[![CI][ci-badge]][ci-url]
[![License][license-badge]][license-url]
[![Downloads][downloads-badge]][downloads-url]

Bounded-memory streaming encode and incremental decode for Serde wire
formats, with typed failures.

An HTTP boundary (or any byte-stream boundary) that serializes a response
into one `Vec<u8>` holds the whole document in memory and cannot start
sending until encoding finishes; a decoder that requires the complete
request body does the same on the way in. This crate provides the two
halves as a framework-free layer:

- **[`EncodeFormat`]** — JSON, YAML, `MessagePack`, Postcard. `encode_stream`
  runs the serializer on a blocking worker and yields chunks of at most
  64 KiB through a bounded channel: memory stays `O(channel × chunk)`
  for any payload size, a slow consumer backpressures the serializer, and
  an encoding failure arrives as a trailing `Err` item — never a silent
  truncation. `encode_vec` covers bounded documents that must exist before
  headers are sent (an error envelope).
- **[`DecodeFormat`]** — JSON, `MessagePack`, Postcard. `decode_reader`
  parses incrementally from any `Read`, so the caller never owns the
  complete input. Whole-document formats (YAML, TOML) are deliberately
  absent from the decode set — their decoders would require full
  materialization; reject them upstream with a typed 415 instead.
- **[`PayloadLimitExceeded`]** — the sentinel a size-limiting reader wraps
  into `io::Error` so a limit hit is reported as
  `FormatError::PayloadTooLarge`, distinct from an ordinary read failure.

```rust
use serde_stream_formats::{DecodeFormat, EncodeFormat};
use tokio_stream::StreamExt;

# #[derive(serde::Serialize, serde::Deserialize, PartialEq, Debug)]
# struct Point { x: u32, y: u32 }
# #[tokio::main]
# async fn main() -> Result<(), serde_stream_formats::FormatError> {
let mut stream = EncodeFormat::Json.encode_stream(Point { x: 3, y: 4 });
let mut encoded = Vec::new();
while let Some(chunk) = stream.next().await {
    encoded.extend_from_slice(&chunk?);
}
let back: Point = DecodeFormat::Json.decode_slice(&encoded)?;
assert_eq!(back, Point { x: 3, y: 4 });
# Ok(())
# }
```

Media-type resolution (`from_content_type`) ignores parameters, matches
case-insensitively, and accepts the documented pre-registration aliases
(`application/x-msgpack`, `text/yaml`, …).

`encode_stream` must run within a Tokio runtime; everything else is
runtime-free.

[`EncodeFormat`]: https://docs.rs/serde-stream-formats/latest/serde_stream_formats/enum.EncodeFormat.html
[`DecodeFormat`]: https://docs.rs/serde-stream-formats/latest/serde_stream_formats/enum.DecodeFormat.html
[`PayloadLimitExceeded`]: https://docs.rs/serde-stream-formats/latest/serde_stream_formats/struct.PayloadLimitExceeded.html

## License

Licensed under either of:

- Apache License, Version 2.0 ([`LICENSE-APACHE`](LICENSE-APACHE));
- MIT License ([`LICENSE-MIT`](LICENSE-MIT)).

## Links

[crates-badge]: https://img.shields.io/crates/v/serde-stream-formats.svg
[crates-url]: https://crates.io/crates/serde-stream-formats
[docs-badge]: https://docs.rs/serde-stream-formats/badge.svg
[docs-url]: https://docs.rs/serde-stream-formats
[ci-badge]: https://github.com/legra-ai/serde-stream-formats/actions/workflows/ci.yml/badge.svg
[ci-url]: https://github.com/legra-ai/serde-stream-formats/actions/workflows/ci.yml
[license-badge]: https://img.shields.io/crates/l/serde-stream-formats.svg
[license-url]: https://github.com/legra-ai/serde-stream-formats/blob/main/LICENSE-APACHE
[downloads-badge]: https://img.shields.io/crates/d/serde-stream-formats.svg
[downloads-url]: https://crates.io/crates/serde-stream-formats
