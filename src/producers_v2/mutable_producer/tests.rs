use std::sync::Arc;

use futures::StreamExt;

use crate::dag_v2::{BufferTransport, Chunk, Produces, Transport, DATA, SCHEMA_RES};

use super::MutableProducer;

async fn collect_output(output: Arc<BufferTransport>) -> Vec<Chunk> {
    let mut stream = output.request(Chunk::new(0, vec![])).await;
    let mut chunks = Vec::new();
    while let Some(chunk) = stream.next().await {
        chunks.push(chunk);
    }
    chunks
}

#[tokio::test]
async fn set_and_read_data() {
    let producer = MutableProducer::new();
    producer.set_data(Chunk::data(b"hello".to_vec()));

    let output = BufferTransport::new();
    producer.handle_data(output.clone()).await.unwrap();
    let chunks = collect_output(output).await;

    assert_eq!(chunks.len(), 1);
    assert_eq!(chunks[0].stream_type, DATA);
    assert_eq!(chunks[0].data, b"hello");
}

#[tokio::test]
async fn set_and_read_schema() {
    let producer = MutableProducer::new();
    producer.set_schema(Chunk::schema_res(b"schema_bytes".to_vec()));

    let output = BufferTransport::new();
    producer.handle_schema(output.clone()).await.unwrap();
    let chunks = collect_output(output).await;

    assert_eq!(chunks.len(), 1);
    assert_eq!(chunks[0].stream_type, SCHEMA_RES);
    assert_eq!(chunks[0].data, b"schema_bytes");
}

#[tokio::test]
async fn repeated_updates() {
    let producer = MutableProducer::new();

    producer.set_data(Chunk::data(b"first".to_vec()));
    let output = BufferTransport::new();
    producer.handle_data(output.clone()).await.unwrap();
    let chunks = collect_output(output).await;
    assert_eq!(chunks[0].data, b"first");

    producer.set_data(Chunk::data(b"second".to_vec()));
    let output = BufferTransport::new();
    producer.handle_data(output.clone()).await.unwrap();
    let chunks = collect_output(output).await;
    assert_eq!(chunks[0].data, b"second");
}

#[tokio::test]
async fn no_data_errors() {
    let producer = MutableProducer::new();
    let output = BufferTransport::new();
    let result = producer.handle_data(output).await;
    assert!(result.is_err());
}

#[tokio::test]
async fn no_schema_errors() {
    let producer = MutableProducer::new();
    let output = BufferTransport::new();
    let result = producer.handle_schema(output).await;
    assert!(result.is_err());
}
