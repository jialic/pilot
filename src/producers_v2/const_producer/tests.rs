use std::sync::Arc;

use futures::StreamExt;

use crate::dag_v2::{BufferTransport, Chunk, Produces, Transport, DATA, SCHEMA_RES};

use super::ConstProducer;

async fn collect_output(output: Arc<BufferTransport>) -> Vec<Chunk> {
    let mut stream = output.request(Chunk::new(0, vec![])).await;
    let mut chunks = Vec::new();
    while let Some(chunk) = stream.next().await {
        chunks.push(chunk);
    }
    chunks
}

#[tokio::test]
async fn output_returns_schema() {
    let producer = ConstProducer::output("hello");
    let output = BufferTransport::new();
    producer.handle_schema(output.clone()).await.unwrap();
    let chunks = collect_output(output).await;

    assert_eq!(chunks.len(), 1);
    assert_eq!(chunks[0].stream_type, SCHEMA_RES);
    let schema = crate::dag_v2::schema::schema_from_bytes(&chunks[0].data).unwrap();
    assert_eq!(schema.fields().len(), 1);
    assert_eq!(schema.field(0).name(), "output");
}

#[tokio::test]
async fn output_returns_data() {
    let producer = ConstProducer::output("hello");
    let output = BufferTransport::new();
    producer.handle_data(output.clone()).await.unwrap();
    let chunks = collect_output(output).await;

    assert_eq!(chunks.len(), 1);
    assert_eq!(chunks[0].stream_type, DATA);
    let batches = crate::sql::ipc_to_batches(&chunks[0].data).unwrap();
    assert_eq!(batches[0].num_rows(), 1);
    let val = crate::sql::datafusion::arrow_string_value(
        batches[0].column_by_name("output").unwrap().as_ref(), 0,
    ).unwrap();
    assert_eq!(val, "hello");
}

#[tokio::test]
async fn new_from_raw_bytes() {
    let schema = arrow::datatypes::Schema::new(vec![
        arrow::datatypes::Field::new("x", arrow::datatypes::DataType::Utf8, false),
    ]);
    let batch = arrow::array::RecordBatch::try_new(
        Arc::new(schema.clone()),
        vec![Arc::new(arrow::array::StringArray::from(vec!["val"]))],
    ).unwrap();

    let schema_bytes = crate::dag_v2::schema::schema_to_bytes(&schema).unwrap();
    let data_bytes = crate::sql::batches_to_ipc(&schema, &[batch]).unwrap();
    let producer = ConstProducer::new(schema_bytes, data_bytes);

    let output = BufferTransport::new();
    producer.handle_schema(output.clone()).await.unwrap();
    let chunks = collect_output(output).await;
    let s = crate::dag_v2::schema::schema_from_bytes(&chunks[0].data).unwrap();
    assert_eq!(s.field(0).name(), "x");

    let output = BufferTransport::new();
    producer.handle_data(output.clone()).await.unwrap();
    let chunks = collect_output(output).await;
    let batches = crate::sql::ipc_to_batches(&chunks[0].data).unwrap();
    let val = crate::sql::datafusion::arrow_string_value(
        batches[0].column(0).as_ref(), 0,
    ).unwrap();
    assert_eq!(val, "val");
}

#[tokio::test]
async fn set_input_is_noop() {
    let producer = ConstProducer::output("");
    let transport = BufferTransport::new();
    producer.set_input(transport);
}
