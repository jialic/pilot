use std::sync::Arc;

use futures::StreamExt;

use crate::dag_v2::{BufferTransport, Chunk, Produces, Transport, SCHEMA_RES};

use super::ScreenshotProducer;

async fn collect_output(output: Arc<BufferTransport>) -> Vec<Chunk> {
    let mut stream = output.request(Chunk::new(0, vec![])).await;
    let mut chunks = Vec::new();
    while let Some(chunk) = stream.next().await {
        chunks.push(chunk);
    }
    chunks
}

#[tokio::test]
async fn schema_is_provider_provider_id() {
    let producer = ScreenshotProducer::new(vec![]);

    let output = BufferTransport::new();
    producer.handle_schema(output.clone()).await.unwrap();
    let chunks = collect_output(output).await;

    assert_eq!(chunks[0].stream_type, SCHEMA_RES);
    let schema = crate::dag_v2::schema::schema_from_bytes(&chunks[0].data).unwrap();
    assert_eq!(schema.fields().len(), 2);
    assert_eq!(schema.field(0).name(), "provider");
    assert_eq!(schema.field(1).name(), "provider_id");
}
