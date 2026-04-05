use std::sync::Arc;

use futures::StreamExt;

use crate::dag_v2::{BufferTransport, Chunk, Produces, Transport, DATA, SCHEMA_REQ, SCHEMA_RES};
use crate::producers_v2::ConstProducer;

use super::PrintProducer;

fn wire(producer: &dyn Produces, source: Arc<dyn Produces>) {
    let transport = BufferTransport::new();
    transport.set_source(source);
    producer.set_input(transport);
}

async fn collect_output(output: Arc<BufferTransport>) -> Vec<Chunk> {
    let mut stream = output.request(Chunk::new(0, vec![])).await;
    let mut chunks = Vec::new();
    while let Some(chunk) = stream.next().await {
        chunks.push(chunk);
    }
    chunks
}

#[tokio::test]
async fn passthrough_data() {
    let node = PrintProducer::new();
    wire(&node, Arc::new(ConstProducer::output("hello")));

    let output = BufferTransport::new();
    node.handle_data(output.clone()).await.unwrap();
    let chunks = collect_output(output).await;

    assert_eq!(chunks.len(), 1);
    assert_eq!(chunks[0].stream_type, DATA);
    let batches = crate::sql::ipc_to_batches(&chunks[0].data).unwrap();
    assert_eq!(batches[0].num_rows(), 1);
    let val = crate::sql::datafusion::arrow_string_value(
        batches[0].column_by_name("output").unwrap().as_ref(), 0,
    ).unwrap();
    assert_eq!(val, "hello");
}

#[tokio::test]
async fn passthrough_schema() {
    let node = PrintProducer::new();
    wire(&node, Arc::new(ConstProducer::output("")));

    let output = BufferTransport::new();
    node.handle_schema(output.clone()).await.unwrap();
    let chunks = collect_output(output).await;

    assert_eq!(chunks.len(), 1);
    assert_eq!(chunks[0].stream_type, SCHEMA_RES);
    let schema = crate::dag_v2::schema::schema_from_bytes(&chunks[0].data).unwrap();
    assert_eq!(schema.fields().len(), 1);
    assert_eq!(schema.field(0).name(), "output");
}

#[tokio::test]
async fn no_input_errors() {
    let node = PrintProducer::new();
    let output = BufferTransport::new();
    let result = node.handle_data(output).await;
    assert!(result.is_err());
}
