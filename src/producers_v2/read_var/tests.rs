use std::collections::HashMap;
use std::sync::Arc;

use futures::StreamExt;

use crate::dag_v2::{BufferTransport, Chunk, Produces, Transport, DATA, SCHEMA_RES};
use crate::producers_v2::ConstProducer;
use crate::workflow::VarDef;

use super::ReadVarProducer;

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

fn var_arg(name: &str, default: Option<&str>) -> VarDef {
    VarDef {
        name: name.to_string(),
        arg: Some(name.to_string()),
        env: None,
        value: None,
        default: default.map(|s| s.to_string()),
        description: None,
    }
}

fn var_env(name: &str, env: &str, default: Option<&str>) -> VarDef {
    VarDef {
        name: name.to_string(),
        arg: None,
        env: Some(env.to_string()),
        value: None,
        default: default.map(|s| s.to_string()),
        description: None,
    }
}

fn var_value(name: &str, value: &str) -> VarDef {
    VarDef {
        name: name.to_string(),
        arg: None,
        env: None,
        value: Some(value.to_string()),
        default: None,
        description: None,
    }
}

/// Helper: extract (name, value) rows from IPC bytes
fn read_rows(bytes: &[u8]) -> Vec<(String, String)> {
    let batches = crate::sql::ipc_to_batches(bytes).unwrap();
    let mut rows = Vec::new();
    for batch in &batches {
        let name_col = batch.column_by_name("name").unwrap();
        let value_col = batch.column_by_name("value").unwrap();
        for i in 0..batch.num_rows() {
            let name =
                crate::sql::datafusion::arrow_string_value(name_col.as_ref(), i).unwrap();
            let value =
                crate::sql::datafusion::arrow_string_value(value_col.as_ref(), i).unwrap();
            rows.push((name, value));
        }
    }
    rows
}

#[tokio::test]
async fn schema_is_name_value() {
    let node = ReadVarProducer::new(
        vec![var_value("x", "1")],
        HashMap::new(),
        None,
    );
    wire(&node, Arc::new(ConstProducer::output("")));

    let output = BufferTransport::new();
    node.handle_schema(output.clone()).await.unwrap();
    let chunks = collect_output(output).await;

    assert_eq!(chunks[0].stream_type, SCHEMA_RES);
    let schema = crate::dag_v2::schema::schema_from_bytes(&chunks[0].data).unwrap();
    assert_eq!(schema.fields().len(), 2);
    assert_eq!(schema.field(0).name(), "name");
    assert_eq!(schema.field(1).name(), "value");
}

#[tokio::test]
async fn reads_args() {
    let mut args = HashMap::new();
    args.insert("question".to_string(), vec!["what is 2+2".to_string()]);

    let node = ReadVarProducer::new(vec![var_arg("question", None)], args, None);
    wire(&node, Arc::new(ConstProducer::output("")));

    let output = BufferTransport::new();
    node.handle_data(output.clone()).await.unwrap();
    let chunks = collect_output(output).await;

    assert_eq!(chunks[0].stream_type, DATA);
    let rows = read_rows(&chunks[0].data);
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0], ("question".to_string(), "what is 2+2".to_string()));
}

#[tokio::test]
async fn applies_defaults() {
    let node = ReadVarProducer::new(
        vec![var_arg("model", Some("gpt-5-mini"))],
        HashMap::new(),
        None,
    );
    wire(&node, Arc::new(ConstProducer::output("")));

    let output = BufferTransport::new();
    node.handle_data(output.clone()).await.unwrap();
    let chunks = collect_output(output).await;

    let rows = read_rows(&chunks[0].data);
    assert_eq!(rows[0], ("model".to_string(), "gpt-5-mini".to_string()));
}

#[tokio::test]
async fn errors_on_missing_required_arg() {
    let node = ReadVarProducer::new(
        vec![var_arg("question", None)],
        HashMap::new(),
        None,
    );
    wire(&node, Arc::new(ConstProducer::output("")));

    let output = BufferTransport::new();
    let result = node.handle_data(output).await;
    assert!(result.is_err());
    assert!(result
        .unwrap_err()
        .to_string()
        .contains("missing required variable: question"));
}

#[tokio::test]
async fn post_output_sql_reshapes_schema() {
    let node = ReadVarProducer::new(
        vec![var_value("x", "1")],
        HashMap::new(),
        Some("SELECT value AS output FROM input".into()),
    );
    wire(&node, Arc::new(ConstProducer::output("")));

    let output = BufferTransport::new();
    node.handle_schema(output.clone()).await.unwrap();
    let chunks = collect_output(output).await;

    let schema = crate::dag_v2::schema::schema_from_bytes(&chunks[0].data).unwrap();
    assert_eq!(schema.fields().len(), 1);
    assert_eq!(schema.field(0).name(), "output");
}

#[tokio::test]
async fn post_output_sql_reshapes_data() {
    let mut args = HashMap::new();
    args.insert("question".to_string(), vec!["hello".to_string()]);

    let node = ReadVarProducer::new(
        vec![var_arg("question", None)],
        args,
        Some("SELECT value AS output FROM input WHERE name = 'question'".into()),
    );
    wire(&node, Arc::new(ConstProducer::output("")));

    let output = BufferTransport::new();
    node.handle_data(output.clone()).await.unwrap();
    let chunks = collect_output(output).await;

    let batches = crate::sql::ipc_to_batches(&chunks[0].data).unwrap();
    let val = crate::sql::datafusion::arrow_string_value(
        batches[0].column_by_name("output").unwrap().as_ref(),
        0,
    )
    .unwrap();
    assert_eq!(val, "hello");
}

#[tokio::test]
async fn cli_overrides_default() {
    let mut args = HashMap::new();
    args.insert("model".to_string(), vec!["claude-opus".to_string()]);

    let node = ReadVarProducer::new(
        vec![var_arg("model", Some("gpt-5-mini"))],
        args,
        None,
    );
    wire(&node, Arc::new(ConstProducer::output("")));

    let output = BufferTransport::new();
    node.handle_data(output.clone()).await.unwrap();
    let chunks = collect_output(output).await;

    let rows = read_rows(&chunks[0].data);
    assert_eq!(rows[0].1, "claude-opus");
}

#[tokio::test]
async fn multi_value_args() {
    let mut args = HashMap::new();
    args.insert(
        "model".to_string(),
        vec!["a".to_string(), "b".to_string(), "c".to_string()],
    );

    let node = ReadVarProducer::new(
        vec![var_arg("model", Some("default"))],
        args,
        None,
    );
    wire(&node, Arc::new(ConstProducer::output("")));

    let output = BufferTransport::new();
    node.handle_data(output.clone()).await.unwrap();
    let chunks = collect_output(output).await;

    let rows = read_rows(&chunks[0].data);
    assert_eq!(rows.len(), 3);
    assert_eq!(rows[0], ("model".to_string(), "a".to_string()));
    assert_eq!(rows[1], ("model".to_string(), "b".to_string()));
    assert_eq!(rows[2], ("model".to_string(), "c".to_string()));
}

#[tokio::test]
async fn static_value() {
    let node = ReadVarProducer::new(
        vec![var_value("version", "1.0")],
        HashMap::new(),
        None,
    );
    wire(&node, Arc::new(ConstProducer::output("")));

    let output = BufferTransport::new();
    node.handle_data(output.clone()).await.unwrap();
    let chunks = collect_output(output).await;

    let rows = read_rows(&chunks[0].data);
    assert_eq!(rows[0], ("version".to_string(), "1.0".to_string()));
}

#[tokio::test]
async fn env_var_with_default() {
    unsafe {
        std::env::remove_var("TEST_PILOT_V2_MISSING");
    }

    let node = ReadVarProducer::new(
        vec![var_env("token", "TEST_PILOT_V2_MISSING", Some("fallback"))],
        HashMap::new(),
        None,
    );
    wire(&node, Arc::new(ConstProducer::output("")));

    let output = BufferTransport::new();
    node.handle_data(output.clone()).await.unwrap();
    let chunks = collect_output(output).await;

    let rows = read_rows(&chunks[0].data);
    assert_eq!(rows[0], ("token".to_string(), "fallback".to_string()));
}

#[tokio::test]
async fn reads_from_env_var() {
    unsafe {
        std::env::set_var("TEST_PILOT_V2_TOKEN", "secret123");
    }

    let node = ReadVarProducer::new(
        vec![var_env("token", "TEST_PILOT_V2_TOKEN", None)],
        HashMap::new(),
        None,
    );
    wire(&node, Arc::new(ConstProducer::output("")));

    let output = BufferTransport::new();
    node.handle_data(output.clone()).await.unwrap();
    let chunks = collect_output(output).await;

    let rows = read_rows(&chunks[0].data);
    assert_eq!(rows[0], ("token".to_string(), "secret123".to_string()));

    unsafe {
        std::env::remove_var("TEST_PILOT_V2_TOKEN");
    }
}

#[tokio::test]
async fn variable_no_source_errors() {
    let node = ReadVarProducer::new(
        vec![VarDef {
            name: "broken".to_string(),
            arg: None,
            env: None,
            value: None,
            default: None,
            description: None,
        }],
        HashMap::new(),
        None,
    );
    wire(&node, Arc::new(ConstProducer::output("")));

    let output = BufferTransport::new();
    let result = node.handle_data(output).await;
    assert!(result.is_err());
    assert!(result
        .unwrap_err()
        .to_string()
        .contains("has no source"));
}

#[tokio::test]
async fn mixed_sources() {
    let mut args = HashMap::new();
    args.insert("question".to_string(), vec!["hello".to_string()]);

    unsafe {
        std::env::set_var("TEST_PILOT_V2_MIX", "from_env");
    }

    let node = ReadVarProducer::new(
        vec![
            var_arg("question", None),
            var_env("token", "TEST_PILOT_V2_MIX", None),
            var_value("version", "2.0"),
        ],
        args,
        None,
    );
    wire(&node, Arc::new(ConstProducer::output("")));

    let output = BufferTransport::new();
    node.handle_data(output.clone()).await.unwrap();
    let chunks = collect_output(output).await;

    let rows = read_rows(&chunks[0].data);
    assert_eq!(rows.len(), 3);
    assert_eq!(rows[0], ("question".to_string(), "hello".to_string()));
    assert_eq!(rows[1], ("token".to_string(), "from_env".to_string()));
    assert_eq!(rows[2], ("version".to_string(), "2.0".to_string()));

    unsafe {
        std::env::remove_var("TEST_PILOT_V2_MIX");
    }
}
