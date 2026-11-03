//! Worker failures must not become successful stream completion.

use std::future::{
    Future,
    poll_fn,
};
use std::task::Poll;

use serde::{
    Serialize,
    Serializer,
};
use serde_stream_formats::EncodeFormat;
use tokio::sync::oneshot;
use tokio_stream::StreamExt;

struct PanickingValue;

impl Serialize for PanickingValue {
    fn serialize<S: Serializer>(&self, _serializer: S) -> Result<S::Ok, S::Error> {
        panic!("serializer worker regression");
    }
}

struct HeldCleanup {
    entered: Option<oneshot::Sender<()>>,
    release: Option<oneshot::Receiver<()>>,
}

impl Serialize for HeldCleanup {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_u8(7)
    }
}

impl Drop for HeldCleanup {
    fn drop(&mut self) {
        // This value is owned and dropped only by the existing blocking worker.
        self.entered.take().unwrap().send(()).unwrap();
        self.release.take().unwrap().blocking_recv().unwrap();
    }
}

#[tokio::test]
async fn successful_eof_waits_for_worker_cleanup() {
    let (entered, started) = oneshot::channel();
    let (release, released) = oneshot::channel();
    let mut stream = EncodeFormat::Json.encode_stream(HeldCleanup {
        entered: Some(entered),
        release: Some(released),
    });
    started.await.unwrap();
    assert_eq!(stream.next().await.unwrap().unwrap().as_ref(), b"7");
    let mut next = std::pin::pin!(stream.next());
    let waiting = poll_fn(|cx| Poll::Ready(next.as_mut().poll(cx).is_pending())).await;
    release.send(()).unwrap();
    assert!(
        waiting,
        "EOF must wait for the worker to finish dropping its value"
    );
    assert!(next.await.is_none());
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
