use std::sync::Arc;

use arrow::array::{Int64Array, RecordBatch, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use futures::StreamExt;

use crate::dag_v2::{BufferTransport, Chunk, Produces, Transport, SCHEMA_RES};
use crate::llm::testing::MockLlm;
use crate::producers_v2::ConstProducer;
use crate::tools::dispatcher::DefaultToolDispatcher;

use super::LlmCallProducer;

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

/// Create a ConstProducer with |ts, role, content| conversation data.
fn conversation_source(rows: &[(i64, &str, &str)]) -> Arc<dyn Produces> {
    let schema = Schema::new(vec![
        Field::new("ts", DataType::Int64, false),
        Field::new("role", DataType::Utf8, false),
        Field::new("content", DataType::Utf8, false),
    ]);

    let ts_vals: Vec<i64> = rows.iter().map(|(ts, _, _)| *ts).collect();
    let role_vals: Vec<&str> = rows.iter().map(|(_, r, _)| *r).collect();
    let content_vals: Vec<&str> = rows.iter().map(|(_, _, c)| *c).collect();

    let batch = RecordBatch::try_new(
        Arc::new(schema.clone()),
        vec![
            Arc::new(Int64Array::from(ts_vals)),
            Arc::new(StringArray::from(role_vals)),
            Arc::new(StringArray::from(content_vals)),
        ],
    )
    .unwrap();

    let data_bytes = crate::sql::batches_to_ipc(&schema, &[batch]).unwrap();
    let schema_bytes = crate::dag_v2::schema::schema_to_bytes(&schema).unwrap();
    Arc::new(ConstProducer::new(schema_bytes, data_bytes))
}

fn empty_dispatcher() -> DefaultToolDispatcher {
    DefaultToolDispatcher::new(vec![])
}

fn make_producer(llm: Arc<dyn crate::llm::LlmClient>) -> LlmCallProducer {
    LlmCallProducer::new(
        llm,
        None,
        "You are a test assistant.".into(),
        vec![],
        empty_dispatcher(),
        None,
        None,
        None,
    )
}

#[tokio::test]
async fn schema_returns_ts_role_content() {
    let llm = Arc::new(MockLlm::new(vec!["ok".into()]));
    let producer = make_producer(llm);
    wire(
        &producer,
        conversation_source(&[(1, "user", "hello")]),
    );

    let output = BufferTransport::new();
    producer.handle_schema(output.clone()).await.unwrap();
    let chunks = collect_output(output).await;

    assert_eq!(chunks[0].stream_type, SCHEMA_RES);
    let schema = crate::dag_v2::schema::schema_from_bytes(&chunks[0].data).unwrap();
    assert_eq!(schema.fields().len(), 3);
    assert_eq!(schema.field(0).name(), "ts");
    assert_eq!(schema.field(1).name(), "role");
    assert_eq!(schema.field(2).name(), "content");
}

#[tokio::test]
async fn schema_with_post_output_sql() {
    let llm = Arc::new(MockLlm::new(vec!["ok".into()]));
    let producer = LlmCallProducer::new(
        llm,
        None,
        "test".into(),
        vec![],
        empty_dispatcher(),
        None,
        Some("SELECT content AS output FROM input".into()),
        None,
    );
    wire(
        &producer,
        conversation_source(&[(1, "user", "hello")]),
    );

    let output = BufferTransport::new();
    producer.handle_schema(output.clone()).await.unwrap();
    let chunks = collect_output(output).await;

    let schema = crate::dag_v2::schema::schema_from_bytes(&chunks[0].data).unwrap();
    assert_eq!(schema.fields().len(), 1);
    assert_eq!(schema.field(0).name(), "output");
}

#[tokio::test]
async fn data_simple_call_returns_assistant_response() {
    let llm = Arc::new(MockLlm::new(vec!["Hello from LLM!".into()]));
    let producer = make_producer(llm);
    wire(
        &producer,
        conversation_source(&[(1, "user", "hi there")]),
    );

    let output = BufferTransport::new();
    producer.handle_data(output.clone()).await.unwrap();
    let chunks = collect_output(output).await;

    let batches = crate::sql::ipc_to_batches(&chunks[0].data).unwrap();
    assert!(!batches.is_empty());
    let batch = &batches[0];
    assert!(batch.num_rows() >= 1);

    // Last row should be the assistant response
    let last_idx = batch.num_rows() - 1;
    let role = crate::sql::datafusion::arrow_string_value(
        batch.column_by_name("role").unwrap().as_ref(),
        last_idx,
    )
    .unwrap();
    let content = crate::sql::datafusion::arrow_string_value(
        batch.column_by_name("content").unwrap().as_ref(),
        last_idx,
    )
    .unwrap();
    assert_eq!(role, "assistant");
    assert_eq!(content, "Hello from LLM!");
}

#[tokio::test]
async fn data_with_post_output_sql() {
    let llm = Arc::new(MockLlm::new(vec!["extracted output".into()]));
    let producer = LlmCallProducer::new(
        llm,
        None,
        "test".into(),
        vec![],
        empty_dispatcher(),
        None,
        Some("SELECT content AS output FROM input WHERE role = 'assistant'".into()),
        None,
    );
    wire(
        &producer,
        conversation_source(&[(1, "user", "hello")]),
    );

    let output = BufferTransport::new();
    producer.handle_data(output.clone()).await.unwrap();
    let chunks = collect_output(output).await;

    let batches = crate::sql::ipc_to_batches(&chunks[0].data).unwrap();
    let batch = &batches[0];
    assert_eq!(batch.num_rows(), 1);
    let val = crate::sql::datafusion::arrow_string_value(
        batch.column_by_name("output").unwrap().as_ref(),
        0,
    )
    .unwrap();
    assert_eq!(val, "extracted output");
}

#[tokio::test]
async fn no_input_errors() {
    let llm = Arc::new(MockLlm::new(vec!["ok".into()]));
    let producer = make_producer(llm);

    let output = BufferTransport::new();
    let result = producer.handle_data(output).await;
    assert!(result.is_err());
}

#[tokio::test]
async fn pre_input_sql_bad_column_errors() {
    let llm = Arc::new(MockLlm::new(vec![]));
    let producer = LlmCallProducer::new(
        llm, None, "test".into(), vec![], empty_dispatcher(),
        Some("SELECT nonexistent FROM input".into()),
        None, None,
    );
    wire(&producer, conversation_source(&[(1, "user", "hi")]));

    let output = BufferTransport::new();
    let result = producer.handle_schema(output).await;
    assert!(result.is_err());
    assert!(result.unwrap_err().to_string().contains("pre_input_sql"));
}

#[tokio::test]
async fn post_output_sql_bad_column_errors() {
    let llm = Arc::new(MockLlm::new(vec![]));
    let producer = LlmCallProducer::new(
        llm, None, "test".into(), vec![], empty_dispatcher(),
        None,
        Some("SELECT nonexistent FROM input".into()), None,
    );
    wire(&producer, conversation_source(&[(1, "user", "hi")]));

    let output = BufferTransport::new();
    let result = producer.handle_schema(output).await;
    assert!(result.is_err());
    assert!(result.unwrap_err().to_string().contains("post_output_sql"));
}

#[tokio::test]
async fn image_rows_skipped() {
    let llm = Arc::new(MockLlm::new(vec!["I see the screen".into()]));
    let producer = make_producer(llm);
    // Include image_storage row — should be skipped without error
    wire(&producer, conversation_source(&[
        (1, "user", "look at this"),
        (2, "image_storage", "blob_1"),
        (3, "user", "what do you see?"),
    ]));

    let output = BufferTransport::new();
    let result = producer.handle_data(output.clone()).await;
    assert!(result.is_ok());
    let chunks = collect_output(output).await;
    let batches = crate::sql::ipc_to_batches(&chunks[0].data).unwrap();
    assert!(batches[0].num_rows() >= 1);
}
