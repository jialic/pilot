use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

use futures::Stream;
use tokio::sync::mpsc;
use tokio_stream::wrappers::UnboundedReceiverStream;

use super::chunk::Chunk;
use super::produces::Produces;
use super::transport::Transport;

/// Buffer-based transport. Reusable — each request() creates a fresh channel.
/// Source reference is permanent, set once via set_source().
/// Also supports direct write/close for when a producer writes to it directly.
pub struct BufferTransport {
    source: std::sync::Mutex<Option<Arc<dyn Produces>>>,
    /// For direct write/close pattern (no source trigger)
    direct_tx: std::sync::Mutex<Option<mpsc::UnboundedSender<Chunk>>>,
    direct_rx: std::sync::Mutex<Option<mpsc::UnboundedReceiver<Chunk>>>,
}

impl std::fmt::Debug for BufferTransport {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BufferTransport").finish()
    }
}

impl BufferTransport {
    pub fn new() -> Arc<Self> {
        let (tx, rx) = mpsc::unbounded_channel();
        Arc::new(Self {
            source: std::sync::Mutex::new(None),
            direct_tx: std::sync::Mutex::new(Some(tx)),
            direct_rx: std::sync::Mutex::new(Some(rx)),
        })
    }

    pub fn set_source(&self, source: Arc<dyn Produces>) {
        *self.source.lock().unwrap() = Some(source);
    }
}

impl Transport for BufferTransport {
    fn request<'a>(
        &'a self,
        req: Chunk,
    ) -> Pin<Box<dyn Future<Output = Pin<Box<dyn Stream<Item = Chunk> + Send>>> + Send + 'a>> {
        Box::pin(async move {
            let source = self.source.lock().unwrap().clone();

            if let Some(source) = source {
                // Source-triggered: fresh channel per request
                let (tx, rx) = mpsc::unbounded_channel();
                let writer = Arc::new(ChannelWriter(std::sync::Mutex::new(Some(tx))));

                use crate::dag_v2::{SCHEMA_REQ, DATA};
                let result = match req.stream_type {
                    SCHEMA_REQ => source.handle_schema(writer.clone()).await,
                    DATA => source.handle_data(writer.clone()).await,
                    other => Err(crate::dag_v2::ProduceError::msg(
                        format!("transport: unknown request type {other}")
                    )),
                };
                if let Err(e) = result {
                    tracing::error!("transport source handle failed: {e}");
                }
                writer.close();

                Box::pin(UnboundedReceiverStream::new(rx)) as Pin<Box<dyn Stream<Item = Chunk> + Send>>
            } else {
                // No source — read from direct write channel
                let rx = self.direct_rx.lock().unwrap().take();
                match rx {
                    Some(rx) => Box::pin(UnboundedReceiverStream::new(rx)) as Pin<Box<dyn Stream<Item = Chunk> + Send>>,
                    None => Box::pin(futures::stream::empty()) as Pin<Box<dyn Stream<Item = Chunk> + Send>>,
                }
            }
        })
    }

    fn write(&self, chunk: Chunk) {
        if let Some(tx) = self.direct_tx.lock().unwrap().as_ref() {
            let _ = tx.send(chunk);
        }
    }

    fn close(&self) {
        self.direct_tx.lock().unwrap().take();
    }

    fn set_source(&self, source: Arc<dyn Produces>) {
        *self.source.lock().unwrap() = Some(source);
    }
}

/// Per-request writer that wraps a channel sender.
#[derive(Debug)]
struct ChannelWriter(std::sync::Mutex<Option<mpsc::UnboundedSender<Chunk>>>);

impl Transport for ChannelWriter {
    fn request<'a>(
        &'a self,
        _req: Chunk,
    ) -> Pin<Box<dyn Future<Output = Pin<Box<dyn Stream<Item = Chunk> + Send>>> + Send + 'a>> {
        panic!("ChannelWriter is write-only");
    }

    fn write(&self, chunk: Chunk) {
        if let Some(tx) = self.0.lock().unwrap().as_ref() {
            let _ = tx.send(chunk);
        }
    }

    fn close(&self) {
        self.0.lock().unwrap().take();
    }

    fn set_source(&self, _source: Arc<dyn Produces>) {
        panic!("ChannelWriter does not have a source");
    }
}
