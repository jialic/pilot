use std::sync::Arc;

use arrow::array::{RecordBatch, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use futures::StreamExt;

use crate::dag_v2::{BufferTransport, Chunk, Produces, Transport};
use crate::producers_v2::ConstProducer;
use crate::producers_v2::pipeline::Pipeline;
use crate::producers_v2::transform::TransformNode;
use crate::workflow::SchemaField;

use super::TriggerProducer;

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

fn event_schema() -> Vec<SchemaField> {
    vec![
        SchemaField { name: "msg".into(), field_type: "string".into() },
    ]
}

/// Body that outputs |name, output| from event data.
fn name_output_body() -> Pipeline {
    Pipeline::from_producers(vec![
        Arc::new(TransformNode::new(
            "SELECT 'event' AS name, msg AS output FROM input".into(),
        )),
    ])
}

#[tokio::test]
async fn basic_json_lines() {
    // Command emits 2 JSON lines
    let trigger = TriggerProducer::new(
        r#"printf '{"msg":"hello"}\n{"msg":"world"}\n'"#.into(),
        None,
        None,
        event_schema(),
        None,
        name_output_body(),
    );
    wire(&trigger, Arc::new(ConstProducer::output("")));

    let output = BufferTransport::new();
    trigger.handle_data(output.clone()).await.unwrap();
    let chunks = collect_output(output).await;

    let batches = crate::sql::ipc_to_batches(&chunks[0].data).unwrap();
    let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(total_rows, 2);
}

#[tokio::test]
async fn skips_non_json_lines() {
    let trigger = TriggerProducer::new(
        r#"printf 'not json\n{"msg":"valid"}\nalso not json\n'"#.into(),
        None,
        None,
        event_schema(),
        None,
        name_output_body(),
    );
    wire(&trigger, Arc::new(ConstProducer::output("")));

    let output = BufferTransport::new();
    trigger.handle_data(output.clone()).await.unwrap();
    let chunks = collect_output(output).await;

    let batches = crate::sql::ipc_to_batches(&chunks[0].data).unwrap();
    let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(total_rows, 1);
}

#[tokio::test]
async fn jq_transforms_event() {
    let trigger = TriggerProducer::new(
        r#"printf '{"data":{"text":"hello"}}\n'"#.into(),
        None,
        Some(".data | {msg: .text}".into()),
        event_schema(),
        None,
        name_output_body(),
    );
    wire(&trigger, Arc::new(ConstProducer::output("")));

    let output = BufferTransport::new();
    trigger.handle_data(output.clone()).await.unwrap();
    let chunks = collect_output(output).await;

    let batches = crate::sql::ipc_to_batches(&chunks[0].data).unwrap();
    let val = crate::sql::datafusion::arrow_string_value(
        batches[0].column_by_name("output").unwrap().as_ref(), 0,
    ).unwrap();
    assert_eq!(val, "hello");
}

#[tokio::test]
async fn empty_command_output() {
    let trigger = TriggerProducer::new(
        "true".into(), // produces no output
        None,
        None,
        event_schema(),
        None,
        name_output_body(),
    );
    wire(&trigger, Arc::new(ConstProducer::output("")));

    let output = BufferTransport::new();
    trigger.handle_data(output.clone()).await.unwrap();
    let chunks = collect_output(output).await;

    let batches = crate::sql::ipc_to_batches(&chunks[0].data).unwrap();
    let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(total_rows, 0);
}

#[tokio::test]
async fn schema_inference() {
    let trigger = TriggerProducer::new(
        "true".into(),
        None,
        None,
        event_schema(),
        None,
        name_output_body(),
    );
    wire(&trigger, Arc::new(ConstProducer::output("")));

    let output = BufferTransport::new();
    trigger.handle_schema(output.clone()).await.unwrap();
    let chunks = collect_output(output).await;

    let schema = crate::dag_v2::schema::schema_from_bytes(&chunks[0].data).unwrap();
    assert_eq!(schema.fields().len(), 2);
    assert_eq!(schema.field(0).name(), "name");
    assert_eq!(schema.field(1).name(), "output");
}

#[tokio::test]
async fn post_output_sql() {
    let trigger = TriggerProducer::new(
        r#"printf '{"msg":"hello"}\n'"#.into(),
        None,
        None,
        event_schema(),
        Some("SELECT output || '!' AS result FROM input".into()),
        name_output_body(),
    );
    wire(&trigger, Arc::new(ConstProducer::output("")));

    let output = BufferTransport::new();
    trigger.handle_data(output.clone()).await.unwrap();
    let chunks = collect_output(output).await;

    let batches = crate::sql::ipc_to_batches(&chunks[0].data).unwrap();
    let val = crate::sql::datafusion::arrow_string_value(
        batches[0].column_by_name("result").unwrap().as_ref(), 0,
    ).unwrap();
    assert_eq!(val, "hello!");
}

#[tokio::test]
async fn empty_output_schema_errors() {
    let trigger = TriggerProducer::new(
        "true".into(),
        None,
        None,
        vec![], // empty — should error
        None,
        name_output_body(),
    );
    wire(&trigger, Arc::new(ConstProducer::output("")));

    let output = BufferTransport::new();
    let result = trigger.handle_schema(output).await;
    assert!(result.is_err());
    assert!(result.unwrap_err().to_string().contains("output_schema"));
}

#[tokio::test]
async fn template_command() {
    // Input with |name, value| for template rendering
    let schema = Schema::new(vec![
        Field::new("name", DataType::Utf8, false),
        Field::new("value", DataType::Utf8, false),
    ]);
    let batch = RecordBatch::try_new(
        Arc::new(schema.clone()),
        vec![
            Arc::new(StringArray::from(vec!["greeting"])),
            Arc::new(StringArray::from(vec!["hello"])),
        ],
    )
    .unwrap();
    let data_bytes = crate::sql::batches_to_ipc(&schema, &[batch]).unwrap();
    let schema_bytes = crate::dag_v2::schema::schema_to_bytes(&schema).unwrap();
    let source = Arc::new(ConstProducer::new(schema_bytes, data_bytes));

    let trigger = TriggerProducer::new(
        r#"printf '{"msg":"{{greeting}}"}\n'"#.into(),
        None,
        None,
        event_schema(),
        None,
        name_output_body(),
    );
    wire(&trigger, source);

    let output = BufferTransport::new();
    trigger.handle_data(output.clone()).await.unwrap();
    let chunks = collect_output(output).await;

    let batches = crate::sql::ipc_to_batches(&chunks[0].data).unwrap();
    let val = crate::sql::datafusion::arrow_string_value(
        batches[0].column_by_name("output").unwrap().as_ref(), 0,
    ).unwrap();
    assert_eq!(val, "hello");
}

#[tokio::test]
async fn no_input_errors() {
    let trigger = TriggerProducer::new(
        "true".into(),
        None,
        None,
        event_schema(),
        None,
        name_output_body(),
    );

    let output = BufferTransport::new();
    let result = trigger.handle_data(output).await;
    assert!(result.is_err());
}

#[tokio::test]
async fn pre_input_sql() {
    // pre_input_sql transforms input before template rendering
    let schema = Schema::new(vec![
        Field::new("raw", DataType::Utf8, false),
    ]);
    let batch = RecordBatch::try_new(
        Arc::new(schema.clone()),
        vec![Arc::new(StringArray::from(vec!["hi"]))],
    )
    .unwrap();
    let data_bytes = crate::sql::batches_to_ipc(&schema, &[batch]).unwrap();
    let schema_bytes = crate::dag_v2::schema::schema_to_bytes(&schema).unwrap();
    let source = Arc::new(ConstProducer::new(schema_bytes, data_bytes));

    let trigger = TriggerProducer::new(
        r#"printf '{"msg":"from_pre_input"}\n'"#.into(),
        Some("SELECT 'key' AS name, raw AS value FROM input".into()),
        None,
        event_schema(),
        None,
        name_output_body(),
    );
    wire(&trigger, source);

    let output = BufferTransport::new();
    trigger.handle_data(output.clone()).await.unwrap();
    let chunks = collect_output(output).await;

    let batches = crate::sql::ipc_to_batches(&chunks[0].data).unwrap();
    let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(total_rows, 1);
}

#[tokio::test]
async fn template_rejects_missing_name_column() {
    // Template vars but input has no |name, value| columns
    let trigger = TriggerProducer::new(
        "echo '{{greeting}}'".into(),
        None,
        None,
        event_schema(),
        None,
        name_output_body(),
    );
    wire(&trigger, Arc::new(ConstProducer::output("")));

    let output = BufferTransport::new();
    let result = trigger.handle_schema(output).await;
    assert!(result.is_err());
    let err = result.unwrap_err().to_string();
    assert!(err.contains("name") || err.contains("value"), "error: {err}");
}

#[tokio::test]
async fn multi_field_output_schema() {
    let schema = vec![
        SchemaField { name: "id".into(), field_type: "integer".into() },
        SchemaField { name: "text".into(), field_type: "string".into() },
    ];
    let body = Pipeline::from_producers(vec![
        Arc::new(TransformNode::new(
            "SELECT 'event' AS name, CAST(id AS VARCHAR) || ': ' || text AS output FROM input".into(),
        )),
    ]);
    let trigger = TriggerProducer::new(
        r#"printf '{"id":1,"text":"hello"}\n{"id":2,"text":"world"}\n'"#.into(),
        None,
        None,
        schema,
        None,
        body,
    );
    wire(&trigger, Arc::new(ConstProducer::output("")));

    let output = BufferTransport::new();
    trigger.handle_data(output.clone()).await.unwrap();
    let chunks = collect_output(output).await;

    let batches = crate::sql::ipc_to_batches(&chunks[0].data).unwrap();
    let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(total_rows, 2);
}

#[tokio::test]
async fn multi_step_body() {
    let body = Pipeline::from_producers(vec![
        Arc::new(TransformNode::new("SELECT msg AS intermediate FROM input".into())),
        Arc::new(TransformNode::new("SELECT 'evt' AS name, intermediate AS output FROM input".into())),
    ]);
    let trigger = TriggerProducer::new(
        r#"printf '{"msg":"hello"}\n'"#.into(),
        None,
        None,
        event_schema(),
        None,
        body,
    );
    wire(&trigger, Arc::new(ConstProducer::output("")));

    let output = BufferTransport::new();
    trigger.handle_data(output.clone()).await.unwrap();
    let chunks = collect_output(output).await;

    let batches = crate::sql::ipc_to_batches(&chunks[0].data).unwrap();
    assert_eq!(batches[0].num_rows(), 1);
    let val = crate::sql::datafusion::arrow_string_value(
        batches[0].column_by_name("output").unwrap().as_ref(), 0,
    ).unwrap();
    assert_eq!(val, "hello");
}
