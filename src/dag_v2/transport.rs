use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

use futures::Stream;

use super::chunk::Chunk;
use super::produces::Produces;

/// Transport is the wire between nodes.
///
/// Carries opaque byte chunks. Triggers upstream via request/response.
/// Does not know about Arrow, schemas, or data formats.
///
/// Two roles:
/// - Downstream calls `request()` to trigger upstream and get a response stream
/// - Upstream calls `write()` + `close()` inside `handle()` to send response chunks
pub trait Transport: Send + Sync {
    /// Send a request upstream, get a response stream.
    /// Internally triggers source.handle_schema/handle_data based on req type.
    fn request<'a>(
        &'a self,
        req: Chunk,
    ) -> Pin<Box<dyn Future<Output = Pin<Box<dyn Stream<Item = Chunk> + Send>>> + Send + 'a>>;

    /// Write a response chunk. Called by the source inside handle().
    fn write(&self, chunk: Chunk);

    /// Signal end of response. Called by the source after writing all chunks.
    fn close(&self);

    /// Set the upstream source. request() will trigger source.handle().
    fn set_source(&self, source: Arc<dyn Produces>);
}

/// Request upstream schema, parse and return as Arrow Schema.
pub async fn request_schema(transport: &dyn Transport) -> Result<arrow::datatypes::Schema, crate::dag_v2::ProduceError> {
    use futures::StreamExt;
    let mut stream = transport.request(Chunk::schema_req()).await;
    let chunk = stream.next().await
        .ok_or_else(|| crate::dag_v2::ProduceError::msg("no schema from upstream"))?;
    crate::dag_v2::schema::schema_from_bytes(&chunk.data)
        .map_err(|e| crate::dag_v2::ProduceError::msg(format!("deser schema: {e}")))
}

/// Request upstream data, collect all chunks into a single buffer.
pub async fn request_data(transport: &dyn Transport) -> Vec<u8> {
    use futures::StreamExt;
    let mut stream = transport.request(Chunk::data(vec![])).await;
    let mut buf = Vec::new();
    while let Some(chunk) = stream.next().await {
        buf.extend_from_slice(&chunk.data);
    }
    buf
}
