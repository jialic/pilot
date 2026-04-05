use std::collections::HashMap;
use std::sync::Arc;

use arrow::array::{RecordBatch, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use futures::StreamExt;

use crate::dag_v2::{BufferTransport, Chunk, Produces, Transport, DATA, SCHEMA_RES};
use crate::producers_v2::ConstProducer;
use crate::workflow::SchemaField;

use super::{HttpProducer, run_jq};

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

// --- JQ unit tests ---

#[test]
fn jq_basic() {
    let input = serde_json::json!({"output": "hello"});
    let results = run_jq("{name: .output}", input).unwrap();
    assert_eq!(results.len(), 1);
    assert_eq!(results[0]["name"], "hello");
}

#[test]
fn jq_array_iteration() {
    let input = serde_json::json!({"items": [{"id": 1}, {"id": 2}, {"id": 3}]});
    let results = run_jq(".items[] | {id: .id}", input).unwrap();
    assert_eq!(results.len(), 3);
    assert_eq!(results[0]["id"], 1);
    assert_eq!(results[2]["id"], 3);
}

#[test]
fn jq_compile_error() {
    let result = run_jq("invalid[[[", serde_json::json!({}));
    assert!(result.is_err());
}

#[test]
fn jq_empty_input() {
    let results = run_jq(".missing // \"default\"", serde_json::json!({})).unwrap();
    assert_eq!(results[0], "default");
}

// --- Arrow conversion tests ---

#[test]
fn arrow_row_to_json_basic() {
    let schema = Arc::new(Schema::new(vec![
        Field::new("name", DataType::Utf8, false),
        Field::new("value", DataType::Utf8, false),
    ]));
    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(StringArray::from(vec!["hello"])),
            Arc::new(StringArray::from(vec!["world"])),
        ],
    )
    .unwrap();

    let json = HttpProducer::arrow_row_to_json(&[batch]);
    assert_eq!(json["name"], "hello");
    assert_eq!(json["value"], "world");
}

#[test]
fn arrow_row_to_json_empty() {
    let json = HttpProducer::arrow_row_to_json(&[]);
    assert!(json.is_object());
    assert_eq!(json.as_object().unwrap().len(), 0);
}

#[test]
fn json_to_arrow_batch_string() {
    let schema = Schema::new(vec![
        Field::new("id", DataType::Utf8, true),
        Field::new("name", DataType::Utf8, true),
    ]);
    let results = vec![
        serde_json::json!({"id": "1", "name": "task1"}),
        serde_json::json!({"id": "2", "name": "task2"}),
    ];

    let batch = HttpProducer::json_to_arrow_batch(&results, &schema).unwrap();
    assert_eq!(batch.num_rows(), 2);
    assert_eq!(batch.num_columns(), 2);
}

#[test]
fn json_to_arrow_batch_mixed_types() {
    let schema = Schema::new(vec![
        Field::new("name", DataType::Utf8, true),
        Field::new("count", DataType::Int64, true),
        Field::new("score", DataType::Float64, true),
        Field::new("active", DataType::Boolean, true),
    ]);
    let results = vec![serde_json::json!({
        "name": "test",
        "count": 42,
        "score": 3.14,
        "active": true
    })];

    let batch = HttpProducer::json_to_arrow_batch(&results, &schema).unwrap();
    assert_eq!(batch.num_rows(), 1);
    assert_eq!(batch.num_columns(), 4);
}

#[test]
fn json_to_arrow_batch_empty() {
    let schema = Schema::new(vec![Field::new("id", DataType::Utf8, true)]);
    let batch = HttpProducer::json_to_arrow_batch(&[], &schema).unwrap();
    assert_eq!(batch.num_rows(), 0);
}

#[test]
fn json_to_arrow_batch_null_values() {
    let schema = Schema::new(vec![
        Field::new("name", DataType::Utf8, true),
        Field::new("count", DataType::Int64, true),
    ]);
    let results = vec![serde_json::json!({"name": null, "count": null})];
    let batch = HttpProducer::json_to_arrow_batch(&results, &schema).unwrap();
    assert_eq!(batch.num_rows(), 1);
}

#[test]
fn json_to_arrow_batch_missing_fields() {
    let schema = Schema::new(vec![
        Field::new("name", DataType::Utf8, true),
        Field::new("missing", DataType::Utf8, true),
    ]);
    let results = vec![serde_json::json!({"name": "test"})];
    let batch = HttpProducer::json_to_arrow_batch(&results, &schema).unwrap();
    assert_eq!(batch.num_rows(), 1);
}

// --- Schema tests ---

#[tokio::test]
async fn schema_from_declared_fields() {
    let node = HttpProducer::new(
        "http://example.com".into(),
        "GET".into(),
        HashMap::new(),
        None,
        None,
        None,
        Some(".".into()),
        vec![
            SchemaField { name: "id".into(), field_type: "string".into() },
            SchemaField { name: "count".into(), field_type: "integer".into() },
        ],
    );
    wire(&node, Arc::new(ConstProducer::output("")));

    let output = BufferTransport::new();
    node.handle_schema(output.clone()).await.unwrap();
    let chunks = collect_output(output).await;

    assert_eq!(chunks[0].stream_type, SCHEMA_RES);
    let schema = crate::dag_v2::schema::schema_from_bytes(&chunks[0].data).unwrap();
    assert_eq!(schema.fields().len(), 2);
    assert_eq!(schema.field(0).name(), "id");
    assert_eq!(schema.field(0).data_type(), &DataType::Utf8);
    assert_eq!(schema.field(1).name(), "count");
    assert_eq!(schema.field(1).data_type(), &DataType::Int64);
}

#[tokio::test]
async fn schema_requires_output_schema() {
    let node = HttpProducer::new(
        "http://example.com".into(),
        "GET".into(),
        HashMap::new(),
        None,
        None,
        None,
        None,
        vec![],
    );
    wire(&node, Arc::new(ConstProducer::output("")));

    let output = BufferTransport::new();
    let result = node.handle_schema(output).await;
    assert!(result.is_err());
}

#[tokio::test]
async fn schema_requires_response_jq() {
    let node = HttpProducer::new(
        "http://example.com".into(),
        "GET".into(),
        HashMap::new(),
        None,
        None,
        None,
        None, // no response_jq
        vec![SchemaField { name: "id".into(), field_type: "string".into() }],
    );
    wire(&node, Arc::new(ConstProducer::output("")));

    let output = BufferTransport::new();
    let result = node.handle_schema(output).await;
    assert!(result.is_err());
}

// --- Integration tests with wiremock ---

use wiremock::{MockServer, Mock, ResponseTemplate};
use wiremock::matchers::{method, path, header, body_string};

#[tokio::test]
async fn get_with_response_jq() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/tasks"))
        .respond_with(ResponseTemplate::new(200).set_body_string(
            r#"{"tasks":[{"id":"1","name":"task1"},{"id":"2","name":"task2"}]}"#,
        ))
        .mount(&server)
        .await;

    let node = HttpProducer::new(
        format!("{}/tasks", server.uri()),
        "GET".into(),
        HashMap::new(),
        None,
        None,
        None,
        Some(".tasks[] | {id: .id, name: .name}".into()),
        vec![
            SchemaField { name: "id".into(), field_type: "string".into() },
            SchemaField { name: "name".into(), field_type: "string".into() },
        ],
    );
    wire(&node, Arc::new(ConstProducer::output("")));

    let output = BufferTransport::new();
    node.handle_data(output.clone()).await.unwrap();
    let chunks = collect_output(output).await;

    assert_eq!(chunks[0].stream_type, DATA);
    let batches = crate::sql::ipc_to_batches(&chunks[0].data).unwrap();
    assert_eq!(batches[0].num_rows(), 2);

    let id_col = batches[0].column_by_name("id").unwrap();
    let name_col = batches[0].column_by_name("name").unwrap();
    assert_eq!(
        crate::sql::datafusion::arrow_string_value(id_col.as_ref(), 0).unwrap(),
        "1"
    );
    assert_eq!(
        crate::sql::datafusion::arrow_string_value(name_col.as_ref(), 1).unwrap(),
        "task2"
    );
}

#[tokio::test]
async fn post_with_request_jq() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/create"))
        .and(body_string(r#"{"task_name":"my task"}"#))
        .respond_with(ResponseTemplate::new(200).set_body_string(r#"{"id":"123"}"#))
        .mount(&server)
        .await;

    let input_schema = Schema::new(vec![Field::new("output", DataType::Utf8, false)]);
    let input_batch = RecordBatch::try_new(
        Arc::new(input_schema.clone()),
        vec![Arc::new(StringArray::from(vec!["my task"]))],
    )
    .unwrap();
    let data_bytes = crate::sql::batches_to_ipc(&input_schema, &[input_batch]).unwrap();
    let schema_bytes = crate::dag_v2::schema::schema_to_bytes(&input_schema).unwrap();
    let source = Arc::new(ConstProducer::new(schema_bytes, data_bytes));

    let node = HttpProducer::new(
        format!("{}/create", server.uri()),
        "POST".into(),
        HashMap::new(),
        None,
        None,
        Some("{task_name: .output}".into()),
        Some("{output: .id}".into()),
        vec![SchemaField { name: "output".into(), field_type: "string".into() }],
    );
    wire(&node, source);

    let output = BufferTransport::new();
    node.handle_data(output.clone()).await.unwrap();
    let chunks = collect_output(output).await;

    let batches = crate::sql::ipc_to_batches(&chunks[0].data).unwrap();
    let val = crate::sql::datafusion::arrow_string_value(
        batches[0].column_by_name("output").unwrap().as_ref(),
        0,
    )
    .unwrap();
    assert!(val.contains("123"));
}

#[tokio::test]
async fn custom_headers() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/auth"))
        .and(header("Authorization", "Bearer test123"))
        .and(header("X-Custom", "myvalue"))
        .respond_with(ResponseTemplate::new(200).set_body_string(r#"{"result":"ok"}"#))
        .mount(&server)
        .await;

    let mut headers = HashMap::new();
    headers.insert("Authorization".into(), "Bearer test123".into());
    headers.insert("X-Custom".into(), "myvalue".into());

    let node = HttpProducer::new(
        format!("{}/auth", server.uri()),
        "GET".into(),
        headers,
        None,
        None,
        None,
        Some("{result: .result}".into()),
        vec![SchemaField { name: "result".into(), field_type: "string".into() }],
    );
    wire(&node, Arc::new(ConstProducer::output("")));

    let output = BufferTransport::new();
    node.handle_data(output.clone()).await.unwrap();
    let chunks = collect_output(output).await;

    let batches = crate::sql::ipc_to_batches(&chunks[0].data).unwrap();
    assert_eq!(batches[0].num_rows(), 1);
}

#[tokio::test]
async fn error_status() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/fail"))
        .respond_with(ResponseTemplate::new(404).set_body_string("not found"))
        .mount(&server)
        .await;

    let node = HttpProducer::new(
        format!("{}/fail", server.uri()),
        "GET".into(),
        HashMap::new(),
        None,
        None,
        None,
        Some(".".into()),
        vec![SchemaField { name: "x".into(), field_type: "string".into() }],
    );
    wire(&node, Arc::new(ConstProducer::output("")));

    let output = BufferTransport::new();
    let result = node.handle_data(output).await;
    assert!(result.is_err());
    let err = result.unwrap_err().to_string();
    assert!(err.contains("404"), "error should contain status code: {err}");
}

#[tokio::test]
async fn timeout_errors() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/slow"))
        .respond_with(
            ResponseTemplate::new(200)
                .set_body_string("ok")
                .set_delay(std::time::Duration::from_secs(5)),
        )
        .mount(&server)
        .await;

    let node = HttpProducer::new(
        format!("{}/slow", server.uri()),
        "GET".into(),
        HashMap::new(),
        None,
        Some(1), // 1 second timeout
        None,
        Some(".".into()),
        vec![SchemaField { name: "x".into(), field_type: "string".into() }],
    );
    wire(&node, Arc::new(ConstProducer::output("")));

    let output = BufferTransport::new();
    let result = node.handle_data(output).await;
    assert!(result.is_err(), "should have failed due to timeout");
}

#[tokio::test]
async fn response_with_mixed_types() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/stats"))
        .respond_with(ResponseTemplate::new(200).set_body_string(
            r#"{"count": 42, "name": "test", "active": true, "score": 3.14}"#,
        ))
        .mount(&server)
        .await;

    let node = HttpProducer::new(
        format!("{}/stats", server.uri()),
        "GET".into(),
        HashMap::new(),
        None,
        None,
        None,
        Some("{count: .count, name: .name, active: .active, score: .score}".into()),
        vec![
            SchemaField { name: "count".into(), field_type: "integer".into() },
            SchemaField { name: "name".into(), field_type: "string".into() },
            SchemaField { name: "active".into(), field_type: "boolean".into() },
            SchemaField { name: "score".into(), field_type: "float".into() },
        ],
    );
    wire(&node, Arc::new(ConstProducer::output("")));

    let output = BufferTransport::new();
    node.handle_data(output.clone()).await.unwrap();
    let chunks = collect_output(output).await;

    let batches = crate::sql::ipc_to_batches(&chunks[0].data).unwrap();
    assert_eq!(batches[0].num_rows(), 1);
    assert_eq!(batches[0].schema().field(0).data_type(), &DataType::Int64);
    assert_eq!(batches[0].schema().field(1).data_type(), &DataType::Utf8);
    assert_eq!(batches[0].schema().field(2).data_type(), &DataType::Boolean);
    assert_eq!(batches[0].schema().field(3).data_type(), &DataType::Float64);
}

#[tokio::test]
async fn headers_jq_from_input() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/auth"))
        .and(header("Authorization", "Bearer secret_token"))
        .respond_with(ResponseTemplate::new(200).set_body_string(r#"{"ok":true}"#))
        .mount(&server)
        .await;

    // Input has a token column
    let input_schema = Schema::new(vec![Field::new("token", DataType::Utf8, false)]);
    let input_batch = RecordBatch::try_new(
        Arc::new(input_schema.clone()),
        vec![Arc::new(StringArray::from(vec!["secret_token"]))],
    ).unwrap();
    let data_bytes = crate::sql::batches_to_ipc(&input_schema, &[input_batch]).unwrap();
    let schema_bytes = crate::dag_v2::schema::schema_to_bytes(&input_schema).unwrap();
    let source = Arc::new(ConstProducer::new(schema_bytes, data_bytes));

    let node = HttpProducer::new(
        format!("{}/auth", server.uri()),
        "GET".into(),
        HashMap::new(),
        Some(r#"{Authorization: "Bearer \(.token)"}"#.into()), // headers_jq
        None, None,
        Some("{ok: .ok}".into()),
        vec![SchemaField { name: "ok".into(), field_type: "string".into() }],
    );
    wire(&node, source);

    let output = BufferTransport::new();
    node.handle_data(output.clone()).await.unwrap();
    let chunks = collect_output(output).await;
    let batches = crate::sql::ipc_to_batches(&chunks[0].data).unwrap();
    assert_eq!(batches[0].num_rows(), 1);
}

#[tokio::test]
async fn static_headers_merged_with_headers_jq() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/merge"))
        .and(header("X-Static", "from_config"))
        .and(header("X-Dynamic", "from_jq"))
        .respond_with(ResponseTemplate::new(200).set_body_string(r#"{"x":"y"}"#))
        .mount(&server)
        .await;

    let mut static_headers = HashMap::new();
    static_headers.insert("X-Static".into(), "from_config".into());
    static_headers.insert("X-Dynamic".into(), "will_be_overridden".into());

    let node = HttpProducer::new(
        format!("{}/merge", server.uri()),
        "GET".into(),
        static_headers,
        Some(r#"{"X-Dynamic": "from_jq"}"#.into()), // overrides static
        None, None,
        Some("{x: .x}".into()),
        vec![SchemaField { name: "x".into(), field_type: "string".into() }],
    );
    wire(&node, Arc::new(ConstProducer::output("")));

    let output = BufferTransport::new();
    node.handle_data(output.clone()).await.unwrap();
    let chunks = collect_output(output).await;
    let batches = crate::sql::ipc_to_batches(&chunks[0].data).unwrap();
    assert_eq!(batches[0].num_rows(), 1);
}

#[tokio::test]
async fn no_input_transport_works_for_get() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/data"))
        .respond_with(ResponseTemplate::new(200).set_body_string(r#"{"x":"y"}"#))
        .mount(&server)
        .await;

    let node = HttpProducer::new(
        format!("{}/data", server.uri()),
        "GET".into(),
        HashMap::new(),
        None,
        None,
        None,
        Some("{x: .x}".into()),
        vec![SchemaField { name: "x".into(), field_type: "string".into() }],
    );
    // No input wired -- GET doesn't need upstream data

    let output = BufferTransport::new();
    node.handle_data(output.clone()).await.unwrap();
    let chunks = collect_output(output).await;

    let batches = crate::sql::ipc_to_batches(&chunks[0].data).unwrap();
    let val = crate::sql::datafusion::arrow_string_value(
        batches[0].column_by_name("x").unwrap().as_ref(),
        0,
    )
    .unwrap();
    assert_eq!(val, "y");
}
