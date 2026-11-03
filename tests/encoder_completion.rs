//! Worker failures must not become successful stream completion.

use serde::{
    Serialize,
    Serializer,
};
use serde_stream_formats::EncodeFormat;
use tokio_stream::StreamExt;

struct PanickingValue;

impl Serialize for PanickingValue {
    fn serialize<S: Serializer>(&self, _serializer: S) -> Result<S::Ok, S::Error> {
        panic!("serializer worker regression");
    }
}

#[tokio::test]
async fn worker_panic_is_reported_before_stream_completion() {
    for format in [
        EncodeFormat::Json,
        EncodeFormat::Yaml,
        EncodeFormat::MessagePack,
        EncodeFormat::Postcard,
    ] {
        let mut stream = format.encode_stream(PanickingValue);
        let result = stream.next().await;
        assert!(matches!(result, Some(Err(_))), "{format:?}: {result:?}");
        assert!(stream.next().await.is_none());
        assert!(stream.next().await.is_none());
    }
}
