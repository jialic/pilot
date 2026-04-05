use std::sync::Arc;

use arrow::array::RecordBatch;
use arrow::ipc::reader::StreamReader;
use arrow::ipc::writer::StreamWriter;
use datafusion::datasource::MemTable;
use datafusion::prelude::*;

/// Infer output schema from SQL + input schema without executing.
/// Registers a zero-row table with the input schema, plans the SQL,
/// and reads the output schema from the logical plan.
pub async fn infer_schema(
    input_schema: &arrow::datatypes::Schema,
    sql: &str,
) -> Result<arrow::datatypes::Schema, String> {
    let ctx = SessionContext::new();
    let table = MemTable::try_new(Arc::new(input_schema.clone()), vec![vec![]])
        .map_err(|e| format!("memtable: {e}"))?;
    ctx.register_table("input", Arc::new(table))
        .map_err(|e| format!("register: {e}"))?;
    let df = ctx.sql(sql).await.map_err(|e| format!("sql plan: {e}"))?;
    Ok(df.schema().as_arrow().clone())
}

/// Infer output schema from SQL + N named table schemas without executing.
/// Each entry in `tables` is (table_name, schema).
pub async fn infer_schema_tables(
    tables: &[(&str, &arrow::datatypes::Schema)],
    sql: &str,
) -> Result<arrow::datatypes::Schema, String> {
    let ctx = SessionContext::new();
    for (name, schema) in tables {
        let table = MemTable::try_new(Arc::new((*schema).clone()), vec![vec![]])
            .map_err(|e| format!("memtable {name}: {e}"))?;
        ctx.register_table(*name, Arc::new(table))
            .map_err(|e| format!("register {name}: {e}"))?;
    }
    let df = ctx.sql(sql).await.map_err(|e| format!("sql plan: {e}"))?;
    Ok(df.schema().as_arrow().clone())
}

/// Extract a string value from an Arrow array at the given index.
/// Casts any string-like type (Utf8, LargeUtf8, Utf8View) to Utf8 first,
/// so callers don't need to know the underlying representation.
pub fn arrow_string_value(array: &dyn arrow::array::Array, index: usize) -> Option<String> {
    let utf8 = arrow::compute::cast(array, &arrow::datatypes::DataType::Utf8).ok()?;
    let arr = utf8.as_any().downcast_ref::<arrow::array::StringArray>()?;
    Some(arr.value(index).to_string())
}

/// Read the first value of a named column from Arrow IPC bytes.
/// Returns None if the column doesn't exist, has no rows, or isn't string-like.
pub fn read_column_string(ipc_bytes: &[u8], column: &str) -> Option<String> {
    let batches = super::ipc_to_batches(ipc_bytes).ok()?;
    let batch = batches.first()?;
    if batch.num_rows() == 0 {
        return None;
    }
    let col = batch.column_by_name(column)?;
    arrow_string_value(col.as_ref(), 0)
}

/// Run SQL on Arrow IPC bytes using DataFusion.
/// Registers input as "input" table, runs SQL, returns result as IPC bytes.
pub async fn run_sql_on_ipc(
    ipc_bytes: &[u8],
    sql: &str,
) -> Result<Vec<u8>, Box<dyn std::error::Error + Send + Sync>> {
    // Deserialize IPC → RecordBatches
    let reader = StreamReader::try_new(std::io::Cursor::new(ipc_bytes), None)?;
    let schema = reader.schema();
    let batches: Vec<RecordBatch> = reader.collect::<Result<Vec<_>, _>>()?;

    // Register as "input" table
    let ctx = SessionContext::new();
    let table = MemTable::try_new(schema, vec![batches])?;
    ctx.register_table("input", Arc::new(table))?;

    // Plan and execute SQL
    let df = ctx.sql(sql).await?;
    let result_schema = Arc::new(df.schema().as_arrow().clone());
    let result_batches = df.collect().await?;

    // Serialize back to IPC
    let mut buf = Vec::new();
    let mut writer = StreamWriter::try_new(&mut buf, &result_schema)?;
    for batch in &result_batches {
        writer.write(batch)?;
    }
    writer.finish()?;

    Ok(buf)
}

/// Run SQL on N Arrow IPC tables using DataFusion.
/// Each entry in `tables` is (table_name, ipc_bytes).
/// Registers them as named tables, runs SQL, returns result as IPC bytes.
pub async fn run_sql_on_tables(
    tables: &[(&str, &[u8])],
    sql: &str,
) -> Result<Vec<u8>, Box<dyn std::error::Error + Send + Sync>> {
    let ctx = SessionContext::new();

    for (name, bytes) in tables {
        let reader = StreamReader::try_new(std::io::Cursor::new(*bytes), None)?;
        let schema = reader.schema();
        let batches: Vec<RecordBatch> = reader.collect::<Result<Vec<_>, _>>()?;
        let mem = MemTable::try_new(schema, vec![batches])?;
        ctx.register_table(*name, Arc::new(mem))?;
    }

    // Plan and execute SQL
    let df = ctx.sql(sql).await?;
    let result_schema = Arc::new(df.schema().as_arrow().clone());
    let result_batches = df.collect().await?;

    // Serialize back to IPC
    let mut buf = Vec::new();
    let mut writer = StreamWriter::try_new(&mut buf, &result_schema)?;
    for batch in &result_batches {
        writer.write(batch)?;
    }
    writer.finish()?;

    Ok(buf)
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::StringArray;
    use arrow::datatypes::{DataType, Field, Schema};

    fn make_ipc(schema: Arc<Schema>, batch: RecordBatch) -> Vec<u8> {
        let mut buf = Vec::new();
        let mut writer = StreamWriter::try_new(&mut buf, &schema).unwrap();
        writer.write(&batch).unwrap();
        writer.finish().unwrap();
        buf
    }

    #[tokio::test]
    async fn eval_condition_flow() {
        // Simulate: condition pipeline produces {output: "true"}
        // Then eval_condition reads it
        let schema = Arc::new(Schema::new(vec![
            Field::new("output", DataType::Utf8, false),
        ]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(StringArray::from(vec![""]))],
        ).unwrap();
        let input_ipc = make_ipc(schema, batch);

        // Step 1: TransformNode runs this SQL
        let cond_bytes = run_sql_on_ipc(&input_ipc, "SELECT 'true' AS output").await.unwrap();

        // Step 2: eval_condition runs this SQL on the condition output
        let eval_bytes = run_sql_on_ipc(&cond_bytes, "SELECT CAST(output AS VARCHAR) AS output FROM input").await.unwrap();
        let b2 = crate::sql::ipc_to_batches(&eval_bytes).unwrap();
        assert_eq!(b2[0].num_rows(), 1);
        let val = arrow_string_value(b2[0].column(0).as_ref(), 0).unwrap();
        assert_eq!(val, "true");
    }

    #[tokio::test]
    async fn select_without_from() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("output", DataType::Utf8, false),
        ]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(StringArray::from(vec![""]))],
        ).unwrap();
        let ipc = make_ipc(schema, batch);

        let result = run_sql_on_ipc(&ipc, "SELECT 'true' AS output").await.unwrap();
        let batches = crate::sql::ipc_to_batches(&result).unwrap();
        assert_eq!(batches.len(), 1);
        assert_eq!(batches[0].num_rows(), 1);
        let col = batches[0].column(0).as_any().downcast_ref::<StringArray>().unwrap();
        assert_eq!(col.value(0), "true");
    }

    #[tokio::test]
    async fn select_from_input() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("name", DataType::Utf8, false),
            Field::new("value", DataType::Utf8, false),
        ]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(StringArray::from(vec!["question"])),
                Arc::new(StringArray::from(vec!["hello"])),
            ],
        ).unwrap();
        let ipc = make_ipc(schema, batch);

        let result = run_sql_on_ipc(&ipc, "SELECT value AS content FROM input WHERE name = 'question'").await.unwrap();
        let batches = crate::sql::ipc_to_batches(&result).unwrap();
        assert_eq!(batches[0].num_rows(), 1);
        let col = batches[0].column(0).as_any().downcast_ref::<StringArray>().unwrap();
        assert_eq!(col.value(0), "hello");
    }
}
