#[cfg(test)]
mod tests;

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

use crate::dag_v2::{
    BufferTransport, Chunk, ProduceError, Produces, Transport,
    request_schema, request_data,
};
use crate::producers_v2::MutableProducer;
use crate::producers_v2::pipeline::Pipeline;

/// Do-while loop. Runs body, checks __continue from while_sql, repeats if true.
///
/// - `pre_input_sql` (optional): transforms initial input into locked schema
/// - `while_sql` (required): SQL on body output, must produce `__continue` column
/// - Body output feeds back as next iteration's input
/// - Locked schema: body output field names+types must match across iterations
pub struct LoopProducer {
    pre_input_sql: Option<String>,
    while_sql: String,
    body_input: Arc<MutableProducer>,
    body_output_transport: Arc<BufferTransport>,
    input: std::sync::Mutex<Option<Arc<dyn Transport>>>,
}

impl LoopProducer {
    pub fn new(pre_input_sql: Option<String>, while_sql: String, body: Pipeline) -> Self {
        // Wire body head input — permanent
        let body_input = Arc::new(MutableProducer::new());
        let body_head_transport = BufferTransport::new();
        body_head_transport.set_source(body_input.clone());
        body.head().set_input(body_head_transport);

        // Wire body output — permanent
        let body_output_transport = BufferTransport::new();
        body_output_transport.set_source(body.tail().clone());

        Self {
            pre_input_sql,
            while_sql,
            body_input,
            body_output_transport,
            input: std::sync::Mutex::new(None),
        }
    }
}

impl Produces for LoopProducer {
    fn handle_schema<'a>(
        &'a self,
        output: Arc<dyn Transport>,
    ) -> Pin<Box<dyn Future<Output = Result<(), ProduceError>> + Send + 'a>> {
        Box::pin(async move {
            let input = self.input.lock().unwrap().clone()
                .ok_or_else(|| ProduceError::msg("loop: no input transport"))?;

            let upstream_schema = request_schema(input.as_ref()).await?;

            // Determine locked schema
            let locked = match &self.pre_input_sql {
                Some(sql) => crate::sql::datafusion::infer_schema(&upstream_schema, sql)
                    .await
                    .map_err(|e| ProduceError::msg(format!("loop pre_input_sql: {e}")))?,
                None => upstream_schema,
            };

            // Set body input schema → get body output schema
            let schema_bytes = crate::dag_v2::schema::schema_to_bytes(&locked)
                .map_err(|e| ProduceError::msg(format!("loop: ser schema: {e}")))?;
            self.body_input.set_schema(Chunk::schema_res(schema_bytes));
            let body_output_schema = request_schema(self.body_output_transport.as_ref()).await?;

            // Validate locked schema invariant (names + types, ignore nullability)
            crate::producers_v2::validate_schemas_match(
                &locked, &body_output_schema, "loop body output",
            )?;

            // Validate while_sql produces __continue
            let while_schema = crate::sql::datafusion::infer_schema(&body_output_schema, &self.while_sql)
                .await
                .map_err(|e| ProduceError::msg(format!("loop while_sql: {e}")))?;
            if while_schema.column_with_name("__continue").is_none() {
                return Err(ProduceError::msg(
                    "loop while_sql must produce '__continue' column"
                ));
            }

            // Loop output = locked schema
            let result_bytes = crate::dag_v2::schema::schema_to_bytes(&locked)
                .map_err(|e| ProduceError::msg(format!("loop: ser output schema: {e}")))?;
            output.write(Chunk::schema_res(result_bytes));
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
                .ok_or_else(|| ProduceError::msg("loop: no input transport"))?;

            let prev_bytes = request_data(input.as_ref()).await;

            // Transform initial input with pre_input_sql
            let mut feedback = match &self.pre_input_sql {
                Some(sql) => crate::sql::datafusion::run_sql_on_ipc(&prev_bytes, sql)
                    .await
                    .map_err(|e| ProduceError::msg(format!("loop pre_input_sql: {e}")))?,
                None => prev_bytes,
            };

            // Compute locked schema for runtime validation
            let locked_schema = crate::sql::ipc_schema(&feedback)
                .map_err(|e| ProduceError::msg(format!("loop: read feedback schema: {e}")))?;

            // Do-while loop
            loop {
                // Run body
                self.body_input.set_data(Chunk::data(feedback.clone()));
                let body_bytes = request_data(self.body_output_transport.as_ref()).await;

                // Runtime schema check — every iteration
                {
                    let body_batches = crate::sql::ipc_to_batches(&body_bytes)
                        .map_err(|e| ProduceError::msg(format!("loop: deser body output: {e}")))?;
                    if let Some(body_s) = body_batches.first().map(|b| b.schema()) {
                        crate::producers_v2::validate_schemas_match(
                            &locked_schema, &body_s, "loop: runtime",
                        )?;
                    }
                }

                // Evaluate while_sql on body output
                let while_bytes = crate::sql::datafusion::run_sql_on_ipc(&body_bytes, &self.while_sql)
                    .await
                    .map_err(|e| ProduceError::msg(format!("loop while_sql: {e}")))?;

                // Validate single row
                let while_batches = crate::sql::ipc_to_batches(&while_bytes)
                    .map_err(|e| ProduceError::msg(format!("loop: while_sql deser: {e}")))?;
                let row_count: usize = while_batches.iter().map(|b| b.num_rows()).sum();
                if row_count != 1 {
                    return Err(ProduceError::msg(format!(
                        "loop: while_sql must return exactly 1 row, got {row_count}"
                    )));
                }

                let continue_val = crate::sql::datafusion::read_column_string(&while_bytes, "__continue")
                    .ok_or_else(|| ProduceError::msg("loop: while_sql missing __continue column"))?;

                if !crate::producers_v2::as_bool(&continue_val) {
                    // Done — output last body result
                    output.write(Chunk::data(body_bytes));
                    output.close();
                    return Ok(());
                }

                // Feed back for next iteration
                feedback = body_bytes;
            }
        })
    }

    fn set_input(&self, transport: Arc<dyn Transport>) {
        *self.input.lock().unwrap() = Some(transport);
    }
}
