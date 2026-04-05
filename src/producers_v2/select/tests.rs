use std::sync::Arc;

use futures::StreamExt;

use crate::dag_v2::{BufferTransport, Chunk, Produces, Transport};
use crate::producers_v2::ConstProducer;
use crate::producers_v2::pipeline::Pipeline;
use crate::producers_v2::transform::TransformNode;
use crate::workflow::SchemaField;

use super::{MatchMode, SelectProducer};

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

/// Condition that returns true/false via |output| column.
fn condition(val: &str) -> Pipeline {
    Pipeline::from_producers(vec![
        Arc::new(TransformNode::new(format!("SELECT '{val}' AS output"))),
    ])
}

/// Body that outputs |name, output|.
fn body(name: &str, value: &str) -> Pipeline {
    Pipeline::from_producers(vec![
        Arc::new(TransformNode::new(
            format!("SELECT '{name}' AS name, '{value}' AS output"),
        )),
    ])
}

fn make_branch(name: &str, cond_val: &str, body_val: &str) -> (String, Pipeline, Pipeline) {
    (name.into(), condition(cond_val), body(name, body_val))
}

#[tokio::test]
async fn match_first_stops_at_first_true() {
    let select = SelectProducer::new(
        MatchMode::First,
        None, None, None, None, vec![],
        vec![
            make_branch("a", "false", "alpha"),
            make_branch("b", "true", "beta"),
            make_branch("c", "true", "gamma"),
        ],
    );
    wire(&select, simple_source(""));

    let output = BufferTransport::new();
    select.handle_data(output.clone()).await.unwrap();
    let chunks = collect_output(output).await;

    let batches = crate::sql::ipc_to_batches(&chunks[0].data).unwrap();
    let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(total_rows, 1);

    let val = crate::sql::datafusion::arrow_string_value(
        batches[0].column_by_name("output").unwrap().as_ref(), 0,
    ).unwrap();
    assert_eq!(val, "beta");
}

#[tokio::test]
async fn match_all_runs_all_true() {
    let select = SelectProducer::new(
        MatchMode::All,
        None, None, None, None, vec![],
        vec![
            make_branch("a", "true", "alpha"),
            make_branch("b", "false", "beta"),
            make_branch("c", "true", "gamma"),
        ],
    );
    wire(&select, simple_source(""));

    let output = BufferTransport::new();
    select.handle_data(output.clone()).await.unwrap();
    let chunks = collect_output(output).await;

    let batches = crate::sql::ipc_to_batches(&chunks[0].data).unwrap();
    let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(total_rows, 2); // a and c matched
}

#[tokio::test]
async fn no_match_returns_empty() {
    let select = SelectProducer::new(
        MatchMode::First,
        None, None, None, None, vec![],
        vec![make_branch("a", "false", "never")],
    );
    wire(&select, simple_source(""));

    let output = BufferTransport::new();
    select.handle_data(output.clone()).await.unwrap();
    let chunks = collect_output(output).await;

    let batches = crate::sql::ipc_to_batches(&chunks[0].data).unwrap();
    let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(total_rows, 0);

    // Schema still correct
    assert!(batches[0].schema().column_with_name("name").is_some());
    assert!(batches[0].schema().column_with_name("output").is_some());
}

#[tokio::test]
async fn schema_inference() {
    let select = SelectProducer::new(
        MatchMode::First,
        None, None, None, None, vec![],
        vec![make_branch("a", "true", "alpha")],
    );
    wire(&select, simple_source(""));

    let output = BufferTransport::new();
    select.handle_schema(output.clone()).await.unwrap();
    let chunks = collect_output(output).await;

    let schema = crate::dag_v2::schema::schema_from_bytes(&chunks[0].data).unwrap();
    assert_eq!(schema.fields().len(), 2);
    assert_eq!(schema.field(0).name(), "name");
    assert_eq!(schema.field(1).name(), "output");
}

#[tokio::test]
async fn static_schema_mismatch_errors() {
    // Body outputs |wrong_col| but locked schema is |name, output|
    let wrong_body = Pipeline::from_producers(vec![
        Arc::new(TransformNode::new("SELECT 'bad' AS wrong_col".into())),
    ]);
    let select = SelectProducer::new(
        MatchMode::First,
        None, None, None, None, vec![],
        vec![("a".into(), condition("true"), wrong_body)],
    );
    wire(&select, simple_source(""));

    let output = BufferTransport::new();
    let result = select.handle_schema(output).await;
    assert!(result.is_err());
    assert!(result.unwrap_err().to_string().contains("schema mismatch"));
}

#[tokio::test]
async fn post_output_sql() {
    let select = SelectProducer::new(
        MatchMode::First,
        None,
        Some("SELECT output || '!' AS result FROM input".into()),
        None, None, vec![],
        vec![make_branch("a", "true", "alpha")],
    );
    wire(&select, simple_source(""));

    let output = BufferTransport::new();
    select.handle_data(output.clone()).await.unwrap();
    let chunks = collect_output(output).await;

    let batches = crate::sql::ipc_to_batches(&chunks[0].data).unwrap();
    let val = crate::sql::datafusion::arrow_string_value(
        batches[0].column_by_name("result").unwrap().as_ref(), 0,
    ).unwrap();
    assert_eq!(val, "alpha!");
}

#[tokio::test]
async fn custom_output_schema() {
    let custom_body = Pipeline::from_producers(vec![
        Arc::new(TransformNode::new("SELECT 'hello' AS x, 'world' AS y".into())),
    ]);
    let select = SelectProducer::new(
        MatchMode::First,
        None, None, None, None,
        vec![
            SchemaField { name: "x".into(), field_type: "string".into() },
            SchemaField { name: "y".into(), field_type: "string".into() },
        ],
        vec![("a".into(), condition("true"), custom_body)],
    );
    wire(&select, simple_source(""));

    let output = BufferTransport::new();
    select.handle_data(output.clone()).await.unwrap();
    let chunks = collect_output(output).await;

    let batches = crate::sql::ipc_to_batches(&chunks[0].data).unwrap();
    assert_eq!(batches[0].schema().field(0).name(), "x");
    assert_eq!(batches[0].schema().field(1).name(), "y");
}

#[tokio::test]
async fn no_input_errors() {
    let select = SelectProducer::new(
        MatchMode::First,
        None, None, None, None, vec![],
        vec![make_branch("a", "true", "alpha")],
    );

    let output = BufferTransport::new();
    let result = select.handle_data(output).await;
    assert!(result.is_err());
}

#[tokio::test]
async fn match_all_all_true() {
    let select = SelectProducer::new(
        MatchMode::All,
        None, None, None, None, vec![],
        vec![
            make_branch("a", "true", "alpha"),
            make_branch("b", "true", "beta"),
        ],
    );
    wire(&select, simple_source(""));

    let output = BufferTransport::new();
    select.handle_data(output.clone()).await.unwrap();
    let chunks = collect_output(output).await;

    let batches = crate::sql::ipc_to_batches(&chunks[0].data).unwrap();
    let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(total_rows, 2);
}

#[tokio::test]
async fn prepare_input_for_when() {
    // Condition reads "flag" column produced by prepare_input_for_when
    let cond = Pipeline::from_producers(vec![
        Arc::new(TransformNode::new("SELECT flag AS output FROM input".into())),
    ]);
    let select = SelectProducer::new(
        MatchMode::First,
        None, None,
        Some("SELECT 'true' AS flag FROM input".into()),
        None,
        vec![],
        vec![("a".into(), cond, body("a", "alpha"))],
    );
    wire(&select, simple_source(""));

    let output = BufferTransport::new();
    select.handle_data(output.clone()).await.unwrap();
    let chunks = collect_output(output).await;

    let batches = crate::sql::ipc_to_batches(&chunks[0].data).unwrap();
    let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(total_rows, 1);
}

#[tokio::test]
async fn prepare_input_for_body() {
    // Body reads "val" column produced by prepare_input_for_body
    let custom_body = Pipeline::from_producers(vec![
        Arc::new(TransformNode::new("SELECT 'b' AS name, val AS output FROM input".into())),
    ]);
    let select = SelectProducer::new(
        MatchMode::First,
        None, None, None,
        Some("SELECT 'injected' AS val FROM input".into()),
        vec![],
        vec![("b".into(), condition("true"), custom_body)],
    );
    wire(&select, simple_source(""));

    let output = BufferTransport::new();
    select.handle_data(output.clone()).await.unwrap();
    let chunks = collect_output(output).await;

    let batches = crate::sql::ipc_to_batches(&chunks[0].data).unwrap();
    let val = crate::sql::datafusion::arrow_string_value(
        batches[0].column_by_name("output").unwrap().as_ref(), 0,
    ).unwrap();
    assert_eq!(val, "injected");
}

#[tokio::test]
async fn condition_truthy_values() {
    // "0" is falsy, "yes" is truthy
    let select = SelectProducer::new(
        MatchMode::All,
        None, None, None, None, vec![],
        vec![
            make_branch("a", "0", "alpha"),       // falsy
            make_branch("b", "yes", "beta"),       // truthy
            make_branch("c", "", "gamma"),          // falsy (empty)
            make_branch("d", "anything", "delta"), // truthy
        ],
    );
    wire(&select, simple_source(""));

    let output = BufferTransport::new();
    select.handle_data(output.clone()).await.unwrap();
    let chunks = collect_output(output).await;

    let batches = crate::sql::ipc_to_batches(&chunks[0].data).unwrap();
    let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(total_rows, 2); // "yes" and "anything"
}
