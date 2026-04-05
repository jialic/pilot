use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

use futures::StreamExt;

use crate::dag_v2::{BufferTransport, Chunk, Produces, Transport, DATA, SCHEMA_RES};
use crate::producers_v2::ConstProducer;

use super::ReadInputProducer;

/// Mock UserIO that returns a canned string.
struct MockIO(String);

impl crate::user_io::UserIO for MockIO {
    fn ask(
        &self,
        _question: String,
    ) -> Pin<Box<dyn Future<Output = Result<String, String>> + Send + '_>> {
        let val = self.0.clone();
        Box::pin(async move { Ok(val) })
    }
}

/// Mock UserIO that always returns an error.
struct FailIO;

impl crate::user_io::UserIO for FailIO {
    fn ask(
        &self,
        _question: String,
    ) -> Pin<Box<dyn Future<Output = Result<String, String>> + Send + '_>> {
        Box::pin(async move { Err("io failed".to_string()) })
    }
}

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
async fn schema_is_output() {
    let io: Arc<dyn crate::user_io::UserIO> = Arc::new(MockIO("hello".into()));
    let node = ReadInputProducer::new(io, "Enter value:".into(), None);
    wire(&node, Arc::new(ConstProducer::output("")));

    let output = BufferTransport::new();
    node.handle_schema(output.clone()).await.unwrap();
    let chunks = collect_output(output).await;

    assert_eq!(chunks[0].stream_type, SCHEMA_RES);
    let schema = crate::dag_v2::schema::schema_from_bytes(&chunks[0].data).unwrap();
    assert_eq!(schema.fields().len(), 1);
    assert_eq!(schema.field(0).name(), "output");
}

#[tokio::test]
async fn data_returns_user_input() {
    let io: Arc<dyn crate::user_io::UserIO> = Arc::new(MockIO("user_answer".into()));
    let node = ReadInputProducer::new(io, "prompt".into(), None);
    wire(&node, Arc::new(ConstProducer::output("")));

    let output = BufferTransport::new();
    node.handle_data(output.clone()).await.unwrap();
    let chunks = collect_output(output).await;

    assert_eq!(chunks[0].stream_type, DATA);
    let batches = crate::sql::ipc_to_batches(&chunks[0].data).unwrap();
    let val = crate::sql::datafusion::arrow_string_value(
        batches[0].column_by_name("output").unwrap().as_ref(),
        0,
    )
    .unwrap();
    assert_eq!(val, "user_answer");
}

#[tokio::test]
async fn post_output_sql_reshapes_schema() {
    let io: Arc<dyn crate::user_io::UserIO> = Arc::new(MockIO("hello".into()));
    let node = ReadInputProducer::new(
        io,
        "prompt".into(),
        Some("SELECT output AS greeting FROM input".into()),
    );
    wire(&node, Arc::new(ConstProducer::output("")));

    let output = BufferTransport::new();
    node.handle_schema(output.clone()).await.unwrap();
    let chunks = collect_output(output).await;

    let schema = crate::dag_v2::schema::schema_from_bytes(&chunks[0].data).unwrap();
    assert_eq!(schema.field(0).name(), "greeting");
}

#[tokio::test]
async fn post_output_sql_reshapes_data() {
    let io: Arc<dyn crate::user_io::UserIO> = Arc::new(MockIO("hello".into()));
    let node = ReadInputProducer::new(
        io,
        "prompt".into(),
        Some("SELECT output AS greeting FROM input".into()),
    );
    wire(&node, Arc::new(ConstProducer::output("")));

    let output = BufferTransport::new();
    node.handle_data(output.clone()).await.unwrap();
    let chunks = collect_output(output).await;

    let batches = crate::sql::ipc_to_batches(&chunks[0].data).unwrap();
    assert!(batches[0].column_by_name("greeting").is_some());
    let val = crate::sql::datafusion::arrow_string_value(
        batches[0].column_by_name("greeting").unwrap().as_ref(),
        0,
    )
    .unwrap();
    assert_eq!(val, "hello");
}

#[tokio::test]
async fn no_input_transport_still_works() {
    // ReadInputProducer ignores upstream data, so no input transport is fine for data.
    // But set_input must have been called for the pattern to be consistent.
    // Since it ignores input, handle_data should still work without input transport.
    let io: Arc<dyn crate::user_io::UserIO> = Arc::new(MockIO("works".into()));
    let node = ReadInputProducer::new(io, "prompt".into(), None);
    // Don't wire input — ReadInputProducer doesn't read upstream data.

    let output = BufferTransport::new();
    node.handle_data(output.clone()).await.unwrap();
    let chunks = collect_output(output).await;

    let batches = crate::sql::ipc_to_batches(&chunks[0].data).unwrap();
    let val = crate::sql::datafusion::arrow_string_value(
        batches[0].column_by_name("output").unwrap().as_ref(),
        0,
    )
    .unwrap();
    assert_eq!(val, "works");
}

#[tokio::test]
async fn io_error_propagates() {
    let io: Arc<dyn crate::user_io::UserIO> = Arc::new(FailIO);
    let node = ReadInputProducer::new(io, "prompt".into(), None);
    wire(&node, Arc::new(ConstProducer::output("")));

    let output = BufferTransport::new();
    let result = node.handle_data(output).await;
    assert!(result.is_err());
    assert!(result.unwrap_err().to_string().contains("io failed"));
}
