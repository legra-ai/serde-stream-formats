//! Public-API integration test: encode a value as a byte stream and decode
//! it back, from the shipped crate.

use serde_stream_formats::{
    DecodeFormat,
    EncodeFormat,
    FormatError,
};
use tokio_stream::StreamExt;

#[derive(serde::Serialize, serde::Deserialize, PartialEq, Debug)]
struct Point {
    x: u32,
    y: u32,
}

#[tokio::test]
async fn json_round_trips_through_the_stream_encoder() -> Result<(), FormatError> {
    let mut stream = EncodeFormat::Json.encode_stream(Point { x: 3, y: 4 });
    let mut encoded = Vec::new();
    while let Some(chunk) = stream.next().await {
        encoded.extend_from_slice(&chunk?);
    }
    let back: Point = DecodeFormat::Json.decode_slice(&encoded)?;
    assert_eq!(back, Point { x: 3, y: 4 });
    Ok(())
}
