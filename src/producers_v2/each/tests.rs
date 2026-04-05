use std::sync::Arc;

use arrow::array::{RecordBatch, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use futures::StreamExt;

use crate::dag_v2::{BufferTransport, Chunk, Produces, Transport};
use crate::producers_v2::ConstProducer;
use crate::producers_v2::pipeline::Pipeline;
use crate::producers_v2::transform::TransformNode;
use crate::workflow::SchemaField;

use super::EachProducer;

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

/// Create a ConstProducer with |a, b| columns.
fn two_col_source(col_a: &[&str], col_b: &[&str]) -> Arc<dyn Produces> {
    let schema = Schema::new(vec![
        Field::new("a", DataType::Utf8, false),
        Field::new("b", DataType::Utf8, false),
    ]);
    let batch = RecordBatch::try_new(
        Arc::new(schema.clone()),
        vec![
            Arc::new(StringArray::from(col_a.to_vec())),
            Arc::new(StringArray::from(col_b.to_vec())),
        ],
    )
    .unwrap();
    let data_bytes = crate::sql::batches_to_ipc(&schema, &[batch]).unwrap();
    let schema_bytes = crate::dag_v2::schema::schema_to_bytes(&schema).unwrap();
    Arc::new(ConstProducer::new(schema_bytes, data_bytes))
}

/// Body pipeline that outputs |name, output| from input row columns.
fn name_output_body() -> Pipeline {
    Pipeline::from_producers(vec![
        Arc::new(TransformNode::new(
            "SELECT a AS name, b AS output FROM input".into(),
        )),
    ])
}

/// Body pipeline that outputs |x, y| (custom schema).
fn custom_schema_body() -> Pipeline {
    Pipeline::from_producers(vec![
        Arc::new(TransformNode::new(
            "SELECT a AS x, b AS y FROM input".into(),
        )),
    ])
}

/// Body pipeline that outputs wrong schema |wrong_col|.
fn wrong_schema_body() -> Pipeline {
    Pipeline::from_producers(vec![
        Arc::new(TransformNode::new(
            "SELECT a AS wrong_col FROM input".into(),
        )),
    ])
}

#[tokio::test]
async fn basic_iteration() {
    let body = name_output_body();
    let each = EachProducer::new(None, None, vec![], body);
    wire(&each, two_col_source(&["x", "y", "z"], &["1", "2", "3"]));

    let output = BufferTransport::new();
    each.handle_data(output.clone()).await.unwrap();
    let chunks = collect_output(output).await;

    let batches = crate::sql::ipc_to_batches(&chunks[0].data).unwrap();
    let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(total_rows, 3);

    // Check schema is |name, output|
    let schema = batches[0].schema();
    assert!(schema.column_with_name("name").is_some());
    assert!(schema.column_with_name("output").is_some());
}

#[tokio::test]
async fn pre_input_sql_filters_rows() {
    let body = name_output_body();
    let each = EachProducer::new(
        Some("SELECT * FROM input WHERE a != 'y'".into()),
        None,
        vec![],
        body,
    );
    wire(&each, two_col_source(&["x", "y", "z"], &["1", "2", "3"]));

    let output = BufferTransport::new();
    each.handle_data(output.clone()).await.unwrap();
    let chunks = collect_output(output).await;

    let batches = crate::sql::ipc_to_batches(&chunks[0].data).unwrap();
    let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(total_rows, 2, "should skip row where a='y'");
}

#[tokio::test]
async fn post_output_sql_reshapes() {
    let body = name_output_body();
    let each = EachProducer::new(
        None,
        Some("SELECT name AS id, output AS result FROM input".into()),
        vec![],
        body,
    );
    wire(&each, two_col_source(&["x", "y"], &["1", "2"]));

    let output = BufferTransport::new();
    each.handle_data(output.clone()).await.unwrap();
    let chunks = collect_output(output).await;

    let batches = crate::sql::ipc_to_batches(&chunks[0].data).unwrap();
    assert_eq!(batches[0].schema().field(0).name(), "id");
    assert_eq!(batches[0].schema().field(1).name(), "result");
}

#[tokio::test]
async fn empty_input_produces_empty_output() {
    let body = name_output_body();
    let each = EachProducer::new(None, None, vec![], body);

    let schema = Schema::new(vec![
        Field::new("a", DataType::Utf8, false),
        Field::new("b", DataType::Utf8, false),
    ]);
    let empty = RecordBatch::new_empty(Arc::new(schema.clone()));
    let data_bytes = crate::sql::batches_to_ipc(&schema, &[empty]).unwrap();
    let schema_bytes = crate::dag_v2::schema::schema_to_bytes(&schema).unwrap();
    wire(&each, Arc::new(ConstProducer::new(schema_bytes, data_bytes)));

    let output = BufferTransport::new();
    each.handle_data(output.clone()).await.unwrap();
    let chunks = collect_output(output).await;

    let batches = crate::sql::ipc_to_batches(&chunks[0].data).unwrap();
    let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(total_rows, 0);

    // Schema is still |name, output|
    assert!(batches[0].schema().column_with_name("name").is_some());
    assert!(batches[0].schema().column_with_name("output").is_some());
}

#[tokio::test]
async fn schema_inference() {
    let body = name_output_body();
    let each = EachProducer::new(None, None, vec![], body);
    wire(&each, two_col_source(&["x"], &["1"]));

    let output = BufferTransport::new();
    each.handle_schema(output.clone()).await.unwrap();
    let chunks = collect_output(output).await;

    let schema = crate::dag_v2::schema::schema_from_bytes(&chunks[0].data).unwrap();
    assert_eq!(schema.fields().len(), 2);
    assert_eq!(schema.field(0).name(), "name");
    assert_eq!(schema.field(1).name(), "output");
}

#[tokio::test]
async fn schema_inference_with_post_output_sql() {
    let body = name_output_body();
    let each = EachProducer::new(
        None,
        Some("SELECT name AS id FROM input".into()),
        vec![],
        body,
    );
    wire(&each, two_col_source(&["x"], &["1"]));

    let output = BufferTransport::new();
    each.handle_schema(output.clone()).await.unwrap();
    let chunks = collect_output(output).await;

    let schema = crate::dag_v2::schema::schema_from_bytes(&chunks[0].data).unwrap();
    assert_eq!(schema.fields().len(), 1);
    assert_eq!(schema.field(0).name(), "id");
}

#[tokio::test]
async fn no_input_errors() {
    let body = name_output_body();
    let each = EachProducer::new(None, None, vec![], body);

    let output = BufferTransport::new();
    let result = each.handle_data(output).await;
    assert!(result.is_err());
}

#[tokio::test]
async fn custom_output_schema() {
    let body = custom_schema_body();
    let each = EachProducer::new(
        None,
        None,
        vec![
            SchemaField { name: "x".into(), field_type: "string".into() },
            SchemaField { name: "y".into(), field_type: "string".into() },
        ],
        body,
    );
    wire(&each, two_col_source(&["hello", "world"], &["1", "2"]));

    let output = BufferTransport::new();
    each.handle_data(output.clone()).await.unwrap();
    let chunks = collect_output(output).await;

    let batches = crate::sql::ipc_to_batches(&chunks[0].data).unwrap();
    let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(total_rows, 2);

    let schema = batches[0].schema();
    assert_eq!(schema.field(0).name(), "x");
    assert_eq!(schema.field(1).name(), "y");
}

#[tokio::test]
async fn static_schema_mismatch_errors() {
    // Body outputs |wrong_col| but locked schema is |name, output|
    let body = wrong_schema_body();
    let each = EachProducer::new(None, None, vec![], body);
    wire(&each, two_col_source(&["x"], &["1"]));

    let output = BufferTransport::new();
    let result = each.handle_schema(output).await;
    assert!(result.is_err());
    assert!(result.unwrap_err().to_string().contains("schema mismatch"));
}

#[tokio::test]
async fn custom_schema_mismatch_errors() {
    // Custom output_schema is |x, y| but body outputs |name, output|
    let body = name_output_body();
    let each = EachProducer::new(
        None,
        None,
        vec![
            SchemaField { name: "x".into(), field_type: "string".into() },
            SchemaField { name: "y".into(), field_type: "string".into() },
        ],
        body,
    );
    wire(&each, two_col_source(&["hello"], &["1"]));

    let output = BufferTransport::new();
    let result = each.handle_schema(output).await;
    assert!(result.is_err());
    assert!(result.unwrap_err().to_string().contains("schema mismatch"));
}

#[tokio::test]
async fn multi_step_body() {
    let body = Pipeline::from_producers(vec![
        Arc::new(TransformNode::new("SELECT a AS intermediate FROM input".into())),
        Arc::new(TransformNode::new("SELECT intermediate AS name, 'done' AS output FROM input".into())),
    ]);
    let each = EachProducer::new(None, None, vec![], body);
    wire(&each, two_col_source(&["x", "y"], &["1", "2"]));

    let output = BufferTransport::new();
    each.handle_data(output.clone()).await.unwrap();
    let chunks = collect_output(output).await;

    let batches = crate::sql::ipc_to_batches(&chunks[0].data).unwrap();
    let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(total_rows, 2);
    assert!(batches[0].schema().column_with_name("name").is_some());
    assert!(batches[0].schema().column_with_name("output").is_some());
}
