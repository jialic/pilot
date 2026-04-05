use std::sync::Arc;

use arrow::array::{Int64Array, RecordBatch, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use futures::StreamExt;

use crate::dag_v2::{BufferTransport, Chunk, Produces, Transport, SCHEMA_RES};
use crate::llm::testing::MockLlm;
use crate::producers_v2::ConstProducer;

use super::CompactProducer;

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

/// Create a source with wrong columns (no ts/role/content).
fn wrong_schema_source() -> Arc<dyn Produces> {
    Arc::new(ConstProducer::output("just a string"))
}

#[tokio::test]
async fn schema_validates_ts_role_content() {
    let llm = Arc::new(MockLlm::new(vec![]));
    let producer = CompactProducer::new(llm, 100, 0.5, None, "Summarize concisely.".into());
    wire(
        &producer,
        conversation_source(&[(1, "user", "hello")]),
    );

    let output = BufferTransport::new();
    producer.handle_schema(output.clone()).await.unwrap();
    let chunks = collect_output(output).await;

    assert_eq!(chunks[0].stream_type, SCHEMA_RES);
    let schema = crate::dag_v2::schema::schema_from_bytes(&chunks[0].data).unwrap();
    assert!(schema.column_with_name("ts").is_some());
    assert!(schema.column_with_name("role").is_some());
    assert!(schema.column_with_name("content").is_some());
}

#[tokio::test]
async fn schema_rejects_wrong_columns() {
    let llm = Arc::new(MockLlm::new(vec![]));
    let producer = CompactProducer::new(llm, 100, 0.5, None, "Summarize concisely.".into());
    wire(&producer, wrong_schema_source());

    let output = BufferTransport::new();
    let result = producer.handle_schema(output).await;
    assert!(result.is_err());
    let err_msg = result.unwrap_err().to_string();
    assert!(err_msg.contains("compact requires column"));
}

#[tokio::test]
async fn passthrough_under_threshold() {
    let llm = Arc::new(MockLlm::new(vec![])); // should not be called
    let producer = CompactProducer::new(llm, 1000, 0.5, None, "Summarize concisely.".into());
    wire(
        &producer,
        conversation_source(&[
            (1, "user", "hello"),
            (2, "assistant", "hi there"),
        ]),
    );

    let output = BufferTransport::new();
    producer.handle_data(output.clone()).await.unwrap();
    let chunks = collect_output(output).await;

    let batches = crate::sql::ipc_to_batches(&chunks[0].data).unwrap();
    let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(total_rows, 2); // unchanged
}

#[tokio::test]
async fn compaction_triggers_over_threshold() {
    // Use compact_ratio=1.0 to compact all-but-last rows into a summary.
    // 4 rows with enough words to exceed max_words=5.
    let llm = Arc::new(MockLlm::new(vec!["Summary of earlier conversation.".into()]));
    let producer = CompactProducer::new(llm, 5, 1.0, None, "Summarize concisely.".into());
    wire(
        &producer,
        conversation_source(&[
            (1, "user", "one two three four five six"),
            (2, "assistant", "seven eight nine ten eleven twelve"),
            (3, "user", "thirteen fourteen fifteen"),
            (4, "assistant", "sixteen seventeen eighteen"),
        ]),
    );

    let output = BufferTransport::new();
    producer.handle_data(output.clone()).await.unwrap();
    let chunks = collect_output(output).await;

    let batches = crate::sql::ipc_to_batches(&chunks[0].data).unwrap();
    let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    // compact_ratio=1.0 compacts all but last row: summary + 1 remaining = 2 rows
    assert!(total_rows < 4, "expected fewer rows after compaction, got {total_rows}");

    // First row should be the system summary
    let role = crate::sql::datafusion::arrow_string_value(
        batches[0].column_by_name("role").unwrap().as_ref(),
        0,
    )
    .unwrap();
    assert_eq!(role, "system");

    let content = crate::sql::datafusion::arrow_string_value(
        batches[0].column_by_name("content").unwrap().as_ref(),
        0,
    )
    .unwrap();
    assert!(content.contains("Summary of earlier conversation"));
}

#[tokio::test]
async fn single_row_never_compacts() {
    let llm = Arc::new(MockLlm::new(vec![])); // should not be called
    // max_words=1 so word count exceeds, but single row should still passthrough
    let producer = CompactProducer::new(llm, 1, 0.5, None, "Summarize concisely.".into());
    wire(
        &producer,
        conversation_source(&[(1, "user", "many words here that exceed the limit")]),
    );

    let output = BufferTransport::new();
    producer.handle_data(output.clone()).await.unwrap();
    let chunks = collect_output(output).await;

    let batches = crate::sql::ipc_to_batches(&chunks[0].data).unwrap();
    let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(total_rows, 1); // unchanged
}

#[tokio::test]
async fn no_input_errors() {
    let llm = Arc::new(MockLlm::new(vec![]));
    let producer = CompactProducer::new(llm, 100, 0.5, None, "Summarize concisely.".into());

    let output = BufferTransport::new();
    let result = producer.handle_data(output).await;
    assert!(result.is_err());
}

#[tokio::test]
async fn sorts_by_timestamp_before_compacting() {
    // Rows out of order — compact should sort by ts, compact oldest
    let llm = Arc::new(MockLlm::new(vec!["Summary.".into()]));
    let producer = CompactProducer::new(llm, 5, 0.9, None, "Summarize concisely.".into());
    wire(
        &producer,
        conversation_source(&[
            (3, "user", "third message here words"),
            (1, "user", "first message here words"),
            (2, "assistant", "second message here words"),
        ]),
    );

    let output = BufferTransport::new();
    producer.handle_data(output.clone()).await.unwrap();
    let chunks = collect_output(output).await;

    let batches = crate::sql::ipc_to_batches(&chunks[0].data).unwrap();
    // After compaction, first row should be system summary (ts=0)
    let ts_col = batches[0].column_by_name("ts").unwrap();
    let first_ts = ts_col.as_any().downcast_ref::<Int64Array>().unwrap().value(0);
    assert_eq!(first_ts, 0, "first row after compaction should be summary with ts=0");
}

#[tokio::test]
async fn compact_ratio_controls_how_much() {
    // 4 rows, ratio=0.5 → compact ~50% of words (first 2 rows), keep last 2
    let llm = Arc::new(MockLlm::new(vec!["Summary.".into()]));
    let producer = CompactProducer::new(llm, 3, 0.5, None, "Summarize concisely.".into());
    wire(
        &producer,
        conversation_source(&[
            (1, "user", "word1 word2"),
            (2, "assistant", "word3 word4"),
            (3, "user", "word5 word6"),
            (4, "assistant", "word7 word8"),
        ]),
    );

    let output = BufferTransport::new();
    producer.handle_data(output.clone()).await.unwrap();
    let chunks = collect_output(output).await;

    let batches = crate::sql::ipc_to_batches(&chunks[0].data).unwrap();
    let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    // 1 summary + remaining rows (should be fewer than original 4)
    assert!(total_rows < 4, "expected compaction, got {total_rows} rows");
    assert!(total_rows >= 2, "should keep at least summary + 1 row, got {total_rows}");
}

#[tokio::test]
async fn schema_passthrough_matches_input() {
    let llm = Arc::new(MockLlm::new(vec![]));
    let producer = CompactProducer::new(llm, 100, 0.5, None, "Summarize concisely.".into());
    wire(
        &producer,
        conversation_source(&[(1, "user", "hello")]),
    );

    let output = BufferTransport::new();
    producer.handle_schema(output.clone()).await.unwrap();
    let chunks = collect_output(output).await;

    let schema = crate::dag_v2::schema::schema_from_bytes(&chunks[0].data).unwrap();
    assert_eq!(schema.fields().len(), 3);
    assert_eq!(schema.field(0).name(), "ts");
    assert_eq!(schema.field(1).name(), "role");
    assert_eq!(schema.field(2).name(), "content");
}
