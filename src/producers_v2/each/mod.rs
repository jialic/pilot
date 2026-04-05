#[cfg(test)]
mod tests;

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

use arrow::array::RecordBatch;
use arrow::datatypes::Schema;

use crate::dag_v2::{
    BufferTransport, Chunk, ProduceError, Produces, Transport,
    request_schema, request_data,
};
use crate::producers_v2::MutableProducer;
use crate::producers_v2::pipeline::Pipeline;
use crate::workflow::SchemaField;

/// Iterates input rows, runs body per row, concatenates results.
///
/// - `pre_input_sql` (optional): filters/transforms rows before iterating
/// - `post_output_sql` (optional): reshapes final concatenated output
/// - `output_schema` (optional): locked schema for body output. Defaults to |name, output|.
/// - Static + runtime validation: body output must match locked schema.
pub struct EachProducer {
    pre_input_sql: Option<String>,
    post_output_sql: Option<String>,
    locked_schema: Schema,
    body_input: Arc<MutableProducer>,
    body_output_transport: Arc<BufferTransport>,
    input: std::sync::Mutex<Option<Arc<dyn Transport>>>,
}

impl EachProducer {
    pub fn new(
        pre_input_sql: Option<String>,
        post_output_sql: Option<String>,
        output_schema: Vec<SchemaField>,
        body: Pipeline,
    ) -> Self {
        let locked_schema = crate::producers_v2::locked_schema_from(&output_schema);

        // Wire: body_input → transport → body.head
        let body_input = Arc::new(MutableProducer::new());
        let head_transport = BufferTransport::new();
        head_transport.set_source(body_input.clone());
        body.head().set_input(head_transport);

        // Wire: body.tail → body_output_transport
        let body_output_transport = BufferTransport::new();
        body_output_transport.set_source(body.tail().clone());

        Self {
            pre_input_sql,
            post_output_sql,
            locked_schema,
            body_input,
            body_output_transport,
            input: std::sync::Mutex::new(None),
        }
    }
}

impl Produces for EachProducer {
    fn handle_schema<'a>(
        &'a self,
        output: Arc<dyn Transport>,
    ) -> Pin<Box<dyn Future<Output = Result<(), ProduceError>> + Send + 'a>> {
        Box::pin(async move {
            let input = self.input.lock().unwrap().clone()
                .ok_or_else(|| ProduceError::msg("each: no input transport"))?;
            let upstream_schema = request_schema(input.as_ref()).await?;

            // Determine per-row schema (what body receives)
            let row_schema = match &self.pre_input_sql {
                Some(sql) => crate::sql::datafusion::infer_schema(&upstream_schema, sql)
                    .await
                    .map_err(|e| ProduceError::msg(format!("each pre_input_sql: {e}")))?,
                None => upstream_schema,
            };

            // Set body input schema → get body output schema
            let schema_bytes = crate::dag_v2::schema::schema_to_bytes(&row_schema)
                .map_err(|e| ProduceError::msg(format!("each: ser schema: {e}")))?;
            self.body_input.set_schema(Chunk::schema_res(schema_bytes));
            let body_output_schema = request_schema(self.body_output_transport.as_ref()).await?;

            // Static validation: body output must match locked schema
            crate::producers_v2::validate_schemas_match(
                &self.locked_schema, &body_output_schema, "each: body output",
            )?;

            // Output schema = locked schema (+ post_output_sql)
            let core = self.locked_schema.clone();
            let final_schema = match &self.post_output_sql {
                Some(sql) => crate::sql::datafusion::infer_schema(&core, sql)
                    .await
                    .map_err(|e| ProduceError::msg(format!("each post_output_sql: {e}")))?,
                None => core,
            };

            let out_bytes = crate::dag_v2::schema::schema_to_bytes(&final_schema)
                .map_err(|e| ProduceError::msg(format!("each: ser output schema: {e}")))?;
            output.write(Chunk::schema_res(out_bytes));
            output.close();
            Ok(())
        })
    }

    fn handle_data<'a>(
        &'a self,
        output: Arc<dyn Transport>,
    ) -> Pin<Box<dyn Future<Output = Result<(), ProduceError>> + Send + 'a>> {
        Box::pin(async move {
            let input = self.input.lock().unwrap().clone()
                .ok_or_else(|| ProduceError::msg("each: no input transport"))?;
            let input_bytes = request_data(input.as_ref()).await;

            // Apply pre_input_sql
            let input_bytes = match &self.pre_input_sql {
                Some(sql) => crate::sql::datafusion::run_sql_on_ipc(&input_bytes, sql)
                    .await
                    .map_err(|e| ProduceError::msg(format!("each pre_input_sql: {e}")))?,
                None => input_bytes,
            };

            let input_batches = crate::sql::ipc_to_batches(&input_bytes)
                .map_err(|e| ProduceError::msg(format!("each: deser input: {e}")))?;

            // Iterate rows, run body per row
            let mut all_batches: Vec<RecordBatch> = Vec::new();

            for batch in &input_batches {
                for row_idx in 0..batch.num_rows() {
                    let row = batch.slice(row_idx, 1);
                    let row_schema = row.schema();
                    let row_bytes = crate::sql::batches_to_ipc(&row_schema, &[row])
                        .map_err(|e| ProduceError::msg(format!("each: ser row: {e}")))?;

                    // Feed row to body
                    self.body_input.set_data(Chunk::data(row_bytes));
                    let body_bytes = request_data(self.body_output_transport.as_ref()).await;

                    let body_batches = crate::sql::ipc_to_batches(&body_bytes)
                        .map_err(|e| ProduceError::msg(format!("each: deser body output: {e}")))?;

                    // Runtime validation: body output must match locked schema
                    if let Some(b) = body_batches.first() {
                        crate::producers_v2::validate_schemas_match(
                            &self.locked_schema, &b.schema(), "each: runtime",
                        )?;
                    }

                    all_batches.extend(body_batches);
                }
            }

            // Build result — concatenate all batches
            let result_bytes = if all_batches.is_empty() {
                let empty = RecordBatch::new_empty(Arc::new(self.locked_schema.clone()));
                crate::sql::batches_to_ipc(&Arc::new(self.locked_schema.clone()), &[empty])
                    .map_err(|e| ProduceError::msg(format!("each: ser empty: {e}")))?
            } else {
                crate::sql::batches_to_ipc(&Arc::new(self.locked_schema.clone()), &all_batches)
                    .map_err(|e| ProduceError::msg(format!("each: ser result: {e}")))?
            };

            // Apply post_output_sql
            let result_bytes = match &self.post_output_sql {
                Some(sql) => crate::sql::datafusion::run_sql_on_ipc(&result_bytes, sql)
                    .await
                    .map_err(|e| ProduceError::msg(format!("each post_output_sql: {e}")))?,
                None => result_bytes,
            };

            output.write(Chunk::data(result_bytes));
            output.close();
            Ok(())
        })
    }

    fn set_input(&self, transport: Arc<dyn Transport>) {
        *self.input.lock().unwrap() = Some(transport);
    }
}
