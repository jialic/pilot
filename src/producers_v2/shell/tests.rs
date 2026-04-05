use std::sync::Arc;

use arrow::array::{RecordBatch, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use futures::StreamExt;

use crate::dag_v2::{BufferTransport, Chunk, Produces, Transport, DATA, SCHEMA_RES};
use crate::producers_v2::ConstProducer;

use super::ShellProducer;

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

fn name_value_source(names: &[&str], values: &[&str]) -> Arc<dyn Produces> {
    let schema = Schema::new(vec![
        Field::new("name", DataType::Utf8, false),
        Field::new("value", DataType::Utf8, false),
    ]);
    let batch = RecordBatch::try_new(
        Arc::new(schema.clone()),
        vec![
            Arc::new(StringArray::from(names.to_vec())),
            Arc::new(StringArray::from(values.to_vec())),
        ],
    ).unwrap();

    let data_bytes = crate::sql::batches_to_ipc(&schema, &[batch]).unwrap();
    let schema_bytes = crate::dag_v2::schema::schema_to_bytes(&schema).unwrap();
    Arc::new(ConstProducer::new(schema_bytes, data_bytes))
}

#[tokio::test]
async fn simple_command() {
    let node = ShellProducer::new("echo hello".into(), None, None, None);
    wire(&node, Arc::new(ConstProducer::output("")));

    let output = BufferTransport::new();
    node.handle_data(output.clone()).await.unwrap();
    let chunks = collect_output(output).await;

    let batches = crate::sql::ipc_to_batches(&chunks[0].data).unwrap();
    let val = crate::sql::datafusion::arrow_string_value(
        batches[0].column_by_name("output").unwrap().as_ref(), 0,
    ).unwrap();
    assert_eq!(val.trim(), "hello");
}

#[tokio::test]
async fn schema_is_output() {
    let node = ShellProducer::new("echo hi".into(), None, None, None);
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
async fn template_command() {
    let node = ShellProducer::new("echo {{ msg }}".into(), None, None, None);
    wire(&node, name_value_source(&["msg"], &["hello_template"]));

    let output = BufferTransport::new();
    node.handle_data(output.clone()).await.unwrap();
    let chunks = collect_output(output).await;

    let batches = crate::sql::ipc_to_batches(&chunks[0].data).unwrap();
    let val = crate::sql::datafusion::arrow_string_value(
        batches[0].column_by_name("output").unwrap().as_ref(), 0,
    ).unwrap();
    assert!(val.contains("hello_template"));
}

#[tokio::test]
async fn template_schema_validates_name_value() {
    let node = ShellProducer::new("echo {{ x }}".into(), None, None, None);
    wire(&node, Arc::new(ConstProducer::output(""))); // |output|, not |name, value|

    let output = BufferTransport::new();
    let result = node.handle_schema(output).await;
    assert!(result.is_err());
}

#[tokio::test]
async fn post_output_sql() {
    let node = ShellProducer::new(
        "echo hello".into(), None, None,
        Some("SELECT output AS greeting FROM input".into()),
    );
    wire(&node, Arc::new(ConstProducer::output("")));

    // Schema reflects post_output_sql
    let output = BufferTransport::new();
    node.handle_schema(output.clone()).await.unwrap();
    let chunks = collect_output(output).await;
    let schema = crate::dag_v2::schema::schema_from_bytes(&chunks[0].data).unwrap();
    assert_eq!(schema.field(0).name(), "greeting");

    // Data also reshaped
    let output = BufferTransport::new();
    node.handle_data(output.clone()).await.unwrap();
    let chunks = collect_output(output).await;
    let batches = crate::sql::ipc_to_batches(&chunks[0].data).unwrap();
    assert!(batches[0].column_by_name("greeting").is_some());
}

#[tokio::test]
async fn timeout() {
    let node = ShellProducer::new("sleep 10".into(), None, Some(1), None);
    wire(&node, Arc::new(ConstProducer::output("")));

    let output = BufferTransport::new();
    let result = node.handle_data(output).await;
    assert!(result.is_err());
    assert!(result.unwrap_err().to_string().contains("timed out"));
}

#[tokio::test]
async fn no_input_errors() {
    let node = ShellProducer::new("echo hi".into(), None, None, None);
    let output = BufferTransport::new();
    let result = node.handle_data(output).await;
    assert!(result.is_err());
}

#[tokio::test]
async fn multi_line_command() {
    let node = ShellProducer::new("echo hello\necho world".into(), None, None, None);
    wire(&node, Arc::new(ConstProducer::output("")));

    let output = BufferTransport::new();
    node.handle_data(output.clone()).await.unwrap();
    let chunks = collect_output(output).await;

    let batches = crate::sql::ipc_to_batches(&chunks[0].data).unwrap();
    let val = crate::sql::datafusion::arrow_string_value(
        batches[0].column_by_name("output").unwrap().as_ref(), 0,
    ).unwrap();
    assert!(val.contains("hello"));
    assert!(val.contains("world"));
}

#[tokio::test]
async fn nonzero_exit_code_errors() {
    let node = ShellProducer::new("false".into(), None, None, None);
    wire(&node, Arc::new(ConstProducer::output("")));

    let output = BufferTransport::new();
    let result = node.handle_data(output).await;
    assert!(result.is_err());
    assert!(result.unwrap_err().to_string().contains("exited with"));
}

#[tokio::test]
async fn cwd_option() {
    let node = ShellProducer::new("pwd".into(), Some("/tmp".into()), None, None);
    wire(&node, Arc::new(ConstProducer::output("")));

    let output = BufferTransport::new();
    node.handle_data(output.clone()).await.unwrap();
    let chunks = collect_output(output).await;

    let batches = crate::sql::ipc_to_batches(&chunks[0].data).unwrap();
    let val = crate::sql::datafusion::arrow_string_value(
        batches[0].column_by_name("output").unwrap().as_ref(), 0,
    ).unwrap();
    assert!(val.contains("tmp") || val.contains("private/tmp"));
}

#[tokio::test]
async fn static_command_accepts_any_input_schema() {
    // No template vars — any input schema is fine
    let node = ShellProducer::new("echo hi".into(), None, None, None);
    wire(&node, name_value_source(&["x"], &["y"])); // |name, value| input, not |output|

    let output = BufferTransport::new();
    node.handle_schema(output.clone()).await.unwrap();
    let chunks = collect_output(output).await;
    let schema = crate::dag_v2::schema::schema_from_bytes(&chunks[0].data).unwrap();
    assert_eq!(schema.field(0).name(), "output");
}
