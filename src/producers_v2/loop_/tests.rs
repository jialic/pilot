use std::sync::Arc;

use futures::StreamExt;

use crate::dag_v2::{BufferTransport, Chunk, Produces, Transport, DATA, SCHEMA_RES};
use crate::producers_v2::ConstProducer;
use crate::producers_v2::pipeline::Pipeline;
use crate::producers_v2::transform::TransformNode;

use super::LoopProducer;

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
async fn single_iteration() {
    // Body appends 'x', while says false immediately
    let body = Pipeline::from_producers(vec![
        Arc::new(TransformNode::new("SELECT output || 'x' AS output FROM input".into())),
    ]);
    let loop_node = LoopProducer::new(
        None,
        "SELECT 'false' AS __continue".into(),
        body,
    );
    wire(&loop_node, Arc::new(ConstProducer::output("hello")));

    let output = BufferTransport::new();
    loop_node.handle_data(output.clone()).await.unwrap();
    let chunks = collect_output(output).await;

    let batches = crate::sql::ipc_to_batches(&chunks[0].data).unwrap();
    let val = crate::sql::datafusion::arrow_string_value(
        batches[0].column_by_name("output").unwrap().as_ref(), 0,
    ).unwrap();
    assert_eq!(val, "hellox");
}

#[tokio::test]
async fn multiple_iterations() {
    // Body appends 'x', continue while length < 4
    let body = Pipeline::from_producers(vec![
        Arc::new(TransformNode::new("SELECT output || 'x' AS output FROM input".into())),
    ]);
    let loop_node = LoopProducer::new(
        None,
        "SELECT CASE WHEN LENGTH(output) < 4 THEN 'true' ELSE 'false' END AS __continue FROM input".into(),
        body,
    );
    wire(&loop_node, Arc::new(ConstProducer::output("")));

    let output = BufferTransport::new();
    loop_node.handle_data(output.clone()).await.unwrap();
    let chunks = collect_output(output).await;

    let batches = crate::sql::ipc_to_batches(&chunks[0].data).unwrap();
    let val = crate::sql::datafusion::arrow_string_value(
        batches[0].column_by_name("output").unwrap().as_ref(), 0,
    ).unwrap();
    assert_eq!(val, "xxxx");
}

#[tokio::test]
async fn pre_input_sql_transforms_schema() {
    // Pre-input converts |output| → |a, b|. Body preserves |a, b|.
    let body = Pipeline::from_producers(vec![
        Arc::new(TransformNode::new("SELECT a, b FROM input".into())),
    ]);
    let loop_node = LoopProducer::new(
        Some("SELECT 'x' AS a, 'y' AS b".into()),
        "SELECT 'false' AS __continue".into(),
        body,
    );
    wire(&loop_node, Arc::new(ConstProducer::output("")));

    // Schema
    let output = BufferTransport::new();
    loop_node.handle_schema(output.clone()).await.unwrap();
    let chunks = collect_output(output).await;
    let schema = crate::dag_v2::schema::schema_from_bytes(&chunks[0].data).unwrap();
    assert_eq!(schema.fields().len(), 2);
    assert_eq!(schema.field(0).name(), "a");
    assert_eq!(schema.field(1).name(), "b");

    // Data
    let output = BufferTransport::new();
    loop_node.handle_data(output.clone()).await.unwrap();
    let chunks = collect_output(output).await;
    let batches = crate::sql::ipc_to_batches(&chunks[0].data).unwrap();
    let a = crate::sql::datafusion::arrow_string_value(
        batches[0].column_by_name("a").unwrap().as_ref(), 0,
    ).unwrap();
    assert_eq!(a, "x");
}

#[tokio::test]
async fn schema_mismatch_errors() {
    // Body transforms |output| → |renamed|. Locked schema is |output|. Mismatch.
    let body = Pipeline::from_producers(vec![
        Arc::new(TransformNode::new("SELECT output AS renamed FROM input".into())),
    ]);
    let loop_node = LoopProducer::new(
        None,
        "SELECT 'false' AS __continue".into(),
        body,
    );
    wire(&loop_node, Arc::new(ConstProducer::output("")));

    let output = BufferTransport::new();
    let result = loop_node.handle_schema(output).await;
    assert!(result.is_err());
    assert!(result.unwrap_err().to_string().contains("schema mismatch"));
}

#[tokio::test]
async fn while_sql_missing_continue_errors() {
    let body = Pipeline::from_producers(vec![
        Arc::new(TransformNode::new("SELECT output FROM input".into())),
    ]);
    let loop_node = LoopProducer::new(
        None,
        "SELECT 'true' AS wrong_name".into(), // no __continue
        body,
    );
    wire(&loop_node, Arc::new(ConstProducer::output("")));

    let output = BufferTransport::new();
    let result = loop_node.handle_schema(output).await;
    assert!(result.is_err());
    assert!(result.unwrap_err().to_string().contains("__continue"));
}

#[tokio::test]
async fn no_input_errors() {
    let body = Pipeline::from_producers(vec![
        Arc::new(TransformNode::new("SELECT output FROM input".into())),
    ]);
    let loop_node = LoopProducer::new(None, "SELECT 'false' AS __continue".into(), body);

    let output = BufferTransport::new();
    let result = loop_node.handle_data(output).await;
    assert!(result.is_err());
}

#[tokio::test]
async fn while_sql_multiple_rows_errors() {
    // Body outputs 2 rows, while_sql doesn't aggregate → 2 rows → error
    let body = Pipeline::from_producers(vec![
        Arc::new(TransformNode::new(
            "SELECT 'a' AS output UNION ALL SELECT 'b' AS output".into(),
        )),
    ]);
    let loop_node = LoopProducer::new(
        None,
        "SELECT 'true' AS __continue FROM input".into(), // returns 2 rows
        body,
    );
    wire(&loop_node, Arc::new(ConstProducer::output("")));

    let output = BufferTransport::new();
    let result = loop_node.handle_data(output).await;
    assert!(result.is_err());
    assert!(result.unwrap_err().to_string().contains("exactly 1 row"));
}

#[tokio::test]
async fn nested_loop() {
    // Inner loop appends 'i' until length 3. Outer wraps it.
    let inner_body = Pipeline::from_producers(vec![
        Arc::new(TransformNode::new("SELECT output || 'i' AS output FROM input".into())),
    ]);
    let inner = Arc::new(LoopProducer::new(
        None,
        "SELECT CASE WHEN LENGTH(output) < 3 THEN 'true' ELSE 'false' END AS __continue FROM input".into(),
        inner_body,
    ));

    let outer_body = Pipeline::from_producers(vec![inner]);
    let outer = LoopProducer::new(
        None,
        "SELECT 'false' AS __continue".into(), // one outer iteration
        outer_body,
    );
    wire(&outer, Arc::new(ConstProducer::output("")));

    let output = BufferTransport::new();
    outer.handle_data(output.clone()).await.unwrap();
    let chunks = collect_output(output).await;
    let batches = crate::sql::ipc_to_batches(&chunks[0].data).unwrap();
    let val = crate::sql::datafusion::arrow_string_value(
        batches[0].column_by_name("output").unwrap().as_ref(), 0,
    ).unwrap();
    assert_eq!(val, "iii");
}

#[tokio::test]
async fn parallel_inside_loop() {
    // Parallel with passthrough accumulates rows via UNION ALL. Loop runs 3 times.
    use crate::producers_v2::parallel::ParallelProducer;
    use crate::producers_v2::passthrough::PassthroughProducer;

    let passthrough_pipeline = Pipeline::from_producers(vec![
        Arc::new(PassthroughProducer::new()),
    ]);
    let body_pipeline = Pipeline::from_producers(vec![
        Arc::new(TransformNode::new("SELECT 'new_row' AS output".into())),
    ]);

    let parallel = Arc::new(ParallelProducer::new(
        None,
        "SELECT output FROM input UNION ALL SELECT output FROM result".into(),
        vec![
            ("input".into(), passthrough_pipeline),
            ("result".into(), body_pipeline),
        ],
    ));

    let loop_body = Pipeline::from_producers(vec![parallel]);
    let loop_node = LoopProducer::new(
        None,
        "SELECT CASE WHEN COUNT(*) < 3 THEN 'true' ELSE 'false' END AS __continue FROM input".into(),
        loop_body,
    );
    wire(&loop_node, Arc::new(ConstProducer::output("start")));

    let output = BufferTransport::new();
    loop_node.handle_data(output.clone()).await.unwrap();
    let chunks = collect_output(output).await;
    let batches = crate::sql::ipc_to_batches(&chunks[0].data).unwrap();
    let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    // start + new_row + new_row = 3 rows -> stops
    assert_eq!(total_rows, 3);
}

#[tokio::test]
async fn schema_passthrough_no_pre_input() {
    let body = Pipeline::from_producers(vec![
        Arc::new(TransformNode::new("SELECT output FROM input".into())),
    ]);
    let loop_node = LoopProducer::new(None, "SELECT 'false' AS __continue".into(), body);
    wire(&loop_node, Arc::new(ConstProducer::output("")));

    let output = BufferTransport::new();
    loop_node.handle_schema(output.clone()).await.unwrap();
    let chunks = collect_output(output).await;
    let schema = crate::dag_v2::schema::schema_from_bytes(&chunks[0].data).unwrap();
    assert_eq!(schema.fields().len(), 1);
    assert_eq!(schema.field(0).name(), "output");
}

#[tokio::test]
async fn pre_input_sql_empty_rows() {
    // pre_input_sql produces 0 rows (WHERE false) — like chat.yaml's initial state.
    // Loop should still work: schema is known, body runs with empty input.
    let body = Pipeline::from_producers(vec![
        Arc::new(TransformNode::new(
            "SELECT CAST(1 AS BIGINT) AS ts, 'user' AS role, 'hello' AS content".into(),
        )),
    ]);
    let loop_node = LoopProducer::new(
        Some("SELECT CAST(0 AS BIGINT) AS ts, '' AS role, '' AS content FROM input WHERE false".into()),
        "SELECT 'false' AS __continue".into(),
        body,
    );
    wire(&loop_node, Arc::new(ConstProducer::output("")));

    let output = BufferTransport::new();
    loop_node.handle_data(output.clone()).await.unwrap();
    let chunks = collect_output(output).await;

    let batches = crate::sql::ipc_to_batches(&chunks[0].data).unwrap();
    let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(total_rows, 1);
    let role = crate::sql::datafusion::arrow_string_value(
        batches[0].column_by_name("role").unwrap().as_ref(), 0,
    ).unwrap();
    assert_eq!(role, "user");
}

#[tokio::test]
async fn pre_input_sql_empty_rows_schema() {
    // Schema inference should work even with WHERE false
    let body = Pipeline::from_producers(vec![
        Arc::new(TransformNode::new(
            "SELECT ts, role, content FROM input".into(),
        )),
    ]);
    let loop_node = LoopProducer::new(
        Some("SELECT CAST(0 AS BIGINT) AS ts, '' AS role, '' AS content FROM input WHERE false".into()),
        "SELECT 'false' AS __continue".into(),
        body,
    );
    wire(&loop_node, Arc::new(ConstProducer::output("")));

    let output = BufferTransport::new();
    loop_node.handle_schema(output.clone()).await.unwrap();
    let chunks = collect_output(output).await;

    let schema = crate::dag_v2::schema::schema_from_bytes(&chunks[0].data).unwrap();
    assert_eq!(schema.fields().len(), 3);
    assert_eq!(schema.field(0).name(), "ts");
    assert_eq!(schema.field(1).name(), "role");
    assert_eq!(schema.field(2).name(), "content");
}
