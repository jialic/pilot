use std::sync::Arc;

use arrow::array::{RecordBatch, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use futures::StreamExt;

use crate::dag_v2::{BufferTransport, Chunk, Produces, Transport};
use crate::producers_v2::ConstProducer;
use crate::producers_v2::pipeline::Pipeline;
use crate::producers_v2::transform::TransformNode;

use super::ParallelProducer;

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

fn simple_source(text: &str) -> Arc<dyn Produces> {
    Arc::new(ConstProducer::output(text))
}

/// Branch that outputs |name, output|.
fn name_output_branch(name_val: &str, value: &str) -> (String, Pipeline) {
    let pipeline = Pipeline::from_producers(vec![
        Arc::new(TransformNode::new(
            format!("SELECT '{name_val}' AS name, '{value}' AS output"),
        )),
    ]);
    (name_val.to_string(), pipeline)
}

/// Branch that outputs |x, y| (custom schema).
fn custom_branch(name: &str, x: &str, y: &str) -> (String, Pipeline) {
    let pipeline = Pipeline::from_producers(vec![
        Arc::new(TransformNode::new(
            format!("SELECT '{x}' AS x, '{y}' AS y"),
        )),
    ]);
    (name.to_string(), pipeline)
}

#[tokio::test]
async fn two_branches_merged_via_sql() {
    let parallel = ParallelProducer::new(
        None,
        "SELECT a.name AS a_name, a.output AS a_output, b.name AS b_name, b.output AS b_output FROM a, b".into(),
        vec![
            name_output_branch("a", "alpha"),
            name_output_branch("b", "beta"),
        ],
    );
    wire(&parallel, simple_source(""));

    let output = BufferTransport::new();
    parallel.handle_data(output.clone()).await.unwrap();
    let chunks = collect_output(output).await;

    let batches = crate::sql::ipc_to_batches(&chunks[0].data).unwrap();
    assert_eq!(batches[0].num_rows(), 1);
    let a_out = crate::sql::datafusion::arrow_string_value(
        batches[0].column_by_name("a_output").unwrap().as_ref(), 0,
    ).unwrap();
    assert_eq!(a_out, "alpha");
    let b_out = crate::sql::datafusion::arrow_string_value(
        batches[0].column_by_name("b_output").unwrap().as_ref(), 0,
    ).unwrap();
    assert_eq!(b_out, "beta");
}

#[tokio::test]
async fn schema_inference_from_merge_sql() {
    let parallel = ParallelProducer::new(
        None,
        "SELECT a.output AS result_a, b.output AS result_b FROM a, b".into(),
        vec![
            name_output_branch("a", "alpha"),
            name_output_branch("b", "beta"),
        ],
    );
    wire(&parallel, simple_source(""));

    let output = BufferTransport::new();
    parallel.handle_schema(output.clone()).await.unwrap();
    let chunks = collect_output(output).await;

    let schema = crate::dag_v2::schema::schema_from_bytes(&chunks[0].data).unwrap();
    assert_eq!(schema.fields().len(), 2);
    assert_eq!(schema.field(0).name(), "result_a");
    assert_eq!(schema.field(1).name(), "result_b");
}

#[tokio::test]
async fn pre_input_sql() {
    let schema = Schema::new(vec![
        Field::new("a", DataType::Utf8, false),
        Field::new("b", DataType::Utf8, false),
    ]);
    let batch = RecordBatch::try_new(
        Arc::new(schema.clone()),
        vec![
            Arc::new(StringArray::from(vec!["hello"])),
            Arc::new(StringArray::from(vec!["world"])),
        ],
    )
    .unwrap();
    let data_bytes = crate::sql::batches_to_ipc(&schema, &[batch]).unwrap();
    let schema_bytes = crate::dag_v2::schema::schema_to_bytes(&schema).unwrap();
    let source = Arc::new(ConstProducer::new(schema_bytes, data_bytes));

    // Branch reads "val" column produced by pre_input_sql
    let branch = Pipeline::from_producers(vec![
        Arc::new(TransformNode::new(
            "SELECT val AS result FROM input".into(),
        )),
    ]);

    let parallel = ParallelProducer::new(
        Some("SELECT a || ' ' || b AS val FROM input".into()),
        "SELECT result FROM branch_a".into(),
        vec![("branch_a".into(), branch)],
    );
    wire(&parallel, source);

    let output = BufferTransport::new();
    parallel.handle_data(output.clone()).await.unwrap();
    let chunks = collect_output(output).await;

    let batches = crate::sql::ipc_to_batches(&chunks[0].data).unwrap();
    let val = crate::sql::datafusion::arrow_string_value(
        batches[0].column_by_name("result").unwrap().as_ref(), 0,
    ).unwrap();
    assert_eq!(val, "hello world");
}

#[tokio::test]
async fn single_branch() {
    let parallel = ParallelProducer::new(
        None,
        "SELECT * FROM solo".into(),
        vec![name_output_branch("solo", "value")],
    );
    wire(&parallel, simple_source(""));

    let output = BufferTransport::new();
    parallel.handle_data(output.clone()).await.unwrap();
    let chunks = collect_output(output).await;

    let batches = crate::sql::ipc_to_batches(&chunks[0].data).unwrap();
    assert_eq!(batches[0].num_rows(), 1);
}

#[tokio::test]
async fn no_input_errors() {
    let parallel = ParallelProducer::new(
        None,
        "SELECT * FROM a".into(),
        vec![name_output_branch("a", "alpha")],
    );

    let output = BufferTransport::new();
    let result = parallel.handle_data(output).await;
    assert!(result.is_err());
}

#[tokio::test]
async fn multi_step_branch() {
    // Branch with 2 steps: transform -> transform
    let branch = Pipeline::from_producers(vec![
        Arc::new(TransformNode::new("SELECT 'step1' AS intermediate".into())),
        Arc::new(TransformNode::new("SELECT intermediate AS result FROM input".into())),
    ]);
    let parallel = ParallelProducer::new(
        None,
        "SELECT result FROM branch_a".into(),
        vec![("branch_a".into(), branch)],
    );
    wire(&parallel, simple_source(""));

    let output = BufferTransport::new();
    parallel.handle_data(output.clone()).await.unwrap();
    let chunks = collect_output(output).await;

    let batches = crate::sql::ipc_to_batches(&chunks[0].data).unwrap();
    assert_eq!(batches[0].num_rows(), 1);
    let val = crate::sql::datafusion::arrow_string_value(
        batches[0].column_by_name("result").unwrap().as_ref(), 0,
    ).unwrap();
    assert_eq!(val, "step1");
}
