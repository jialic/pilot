pub(crate) mod jq;
mod template;
#[cfg(test)]
mod tests;

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

use arrow::array::RecordBatch;

use crate::dag_v2::{
    BufferTransport, Chunk, ProduceError, Produces, Transport,
    request_schema, request_data,
};
use crate::producers_v2::MutableProducer;
use crate::producers_v2::pipeline::Pipeline;
use crate::workflow::SchemaField;

use jq::{build_output_schema, json_to_arrow_batch, run_jq};
use template::{build_template_context, has_template_vars, render_command};

/// Long-running subprocess that emits JSON lines on stdout.
/// Per JSON line: parse → jq → Arrow batch → run body → collect results.
///
/// - `output_schema` (required): declares the schema of each event.
/// - Body receives event data, outputs whatever it wants.
/// - Body output schema validated against itself (consistent across events).
pub struct TriggerProducer {
    command: String,
    pre_input_sql: Option<String>,
    jq_expr: Option<String>,
    output_schema: Vec<SchemaField>,
    post_output_sql: Option<String>,
    body_input: Arc<MutableProducer>,
    body_output_transport: Arc<BufferTransport>,
    input: std::sync::Mutex<Option<Arc<dyn Transport>>>,
}

impl TriggerProducer {
    pub fn new(
        command: String,
        pre_input_sql: Option<String>,
        jq_expr: Option<String>,
        output_schema: Vec<SchemaField>,
        post_output_sql: Option<String>,
        body: Pipeline,
    ) -> Self {
        // Wire: body_input → transport → body.head
        let body_input = Arc::new(MutableProducer::new());
        let head_transport = BufferTransport::new();
        head_transport.set_source(body_input.clone());
        body.head().set_input(head_transport);

        // Wire: body.tail → body_output_transport
        let body_output_transport = BufferTransport::new();
        body_output_transport.set_source(body.tail().clone());

        Self {
            command,
            pre_input_sql,
            jq_expr,
            output_schema,
            post_output_sql,
            body_input,
            body_output_transport,
            input: std::sync::Mutex::new(None),
        }
    }
}

impl Produces for TriggerProducer {
    fn handle_schema<'a>(
        &'a self,
        output: Arc<dyn Transport>,
    ) -> Pin<Box<dyn Future<Output = Result<(), ProduceError>> + Send + 'a>> {
        Box::pin(async move {
            // Validate output_schema is non-empty
            let event_schema = build_output_schema(&self.output_schema)
                .map_err(|e| ProduceError::msg(e))?;

            let input = self.input.lock().unwrap().clone()
                .ok_or_else(|| ProduceError::msg("trigger: no input transport"))?;
            let upstream_schema = request_schema(input.as_ref()).await?;

            // Validate template vars have |name, value| if needed
            if has_template_vars(&self.command) {
                let prepared = match &self.pre_input_sql {
                    Some(sql) => crate::sql::datafusion::infer_schema(&upstream_schema, sql)
                        .await
                        .map_err(|e| ProduceError::msg(format!("trigger pre_input_sql: {e}")))?,
                    None => upstream_schema,
                };
                if prepared.column_with_name("name").is_none() {
                    return Err(ProduceError::msg("trigger template requires 'name' column in input"));
                }
                if prepared.column_with_name("value").is_none() {
                    return Err(ProduceError::msg("trigger template requires 'value' column in input"));
                }
            }

            // Set body input schema (event schema) → get body output schema
            let schema_bytes = crate::dag_v2::schema::schema_to_bytes(&event_schema)
                .map_err(|e| ProduceError::msg(format!("trigger: ser event schema: {e}")))?;
            self.body_input.set_schema(Chunk::schema_res(schema_bytes));
            let body_output_schema = request_schema(self.body_output_transport.as_ref()).await?;

            // Output = body output schema (+ post_output_sql)
            let final_schema = match &self.post_output_sql {
                Some(sql) => crate::sql::datafusion::infer_schema(&body_output_schema, sql)
                    .await
                    .map_err(|e| ProduceError::msg(format!("trigger post_output_sql: {e}")))?,
                None => body_output_schema,
            };

            let out_bytes = crate::dag_v2::schema::schema_to_bytes(&final_schema)
                .map_err(|e| ProduceError::msg(format!("trigger: ser output schema: {e}")))?;
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
                .ok_or_else(|| ProduceError::msg("trigger: no input transport"))?;
            let input_bytes = request_data(input.as_ref()).await;

            // Apply pre_input_sql
            let input_bytes = match &self.pre_input_sql {
                Some(sql) => crate::sql::datafusion::run_sql_on_ipc(&input_bytes, sql)
                    .await
                    .map_err(|e| ProduceError::msg(format!("trigger pre_input_sql: {e}")))?,
                None => input_bytes,
            };

            let input_batches = crate::sql::ipc_to_batches(&input_bytes)
                .map_err(|e| ProduceError::msg(format!("trigger: deser input: {e}")))?;

            let event_schema = build_output_schema(&self.output_schema)
                .map_err(|e| ProduceError::msg(e))?;

            // Render command template
            let command = if has_template_vars(&self.command) {
                let template_ctx = build_template_context(&input_batches);
                render_command(&self.command, &template_ctx)?
            } else {
                self.command.clone()
            };

            // Spawn subprocess
            let tokens = shlex::split(&command)
                .ok_or_else(|| ProduceError::msg(format!("invalid command syntax: {command}")))?;
            if tokens.is_empty() {
                output.write(Chunk::data(vec![]));
                output.close();
                return Ok(());
            }

            let mut child = tokio::process::Command::new(&tokens[0]);
            if tokens.len() > 1 {
                child.args(&tokens[1..]);
            }
            let mut child = child
                .stdout(std::process::Stdio::piped())
                .stderr(std::process::Stdio::null())
                .stdin(std::process::Stdio::null())
                .spawn()
                .map_err(|e| ProduceError::msg(format!("trigger spawn: {e}")))?;

            let stdout = child.stdout.take()
                .ok_or_else(|| ProduceError::msg("trigger: no stdout"))?;

            let mut reader = tokio::io::BufReader::new(stdout);
            let mut all_batches: Vec<RecordBatch> = Vec::new();
            let mut reference_schema: Option<Arc<arrow::datatypes::Schema>> = None;

            use tokio::io::AsyncBufReadExt;
            let mut line = String::new();
            loop {
                line.clear();
                let bytes_read = reader.read_line(&mut line).await
                    .map_err(|e| ProduceError::msg(format!("trigger read: {e}")))?;
                if bytes_read == 0 {
                    break; // EOF
                }

                let trimmed = line.trim();
                if trimmed.is_empty() {
                    continue;
                }

                // Parse as JSON — skip non-JSON lines
                let json: serde_json::Value = match serde_json::from_str(trimmed) {
                    Ok(v) => v,
                    Err(_) => continue,
                };

                // Transform via jq
                let results = if let Some(ref jq_expr) = self.jq_expr {
                    run_jq(jq_expr, json)
                        .map_err(|e| ProduceError::msg(format!("trigger jq: {e}")))?
                } else {
                    vec![json]
                };

                // Convert to Arrow batch
                let event_batch = json_to_arrow_batch(&results, &event_schema)?;
                let event_schema_arc = event_batch.schema();
                let event_bytes = crate::sql::batches_to_ipc(&event_schema_arc, &[event_batch])
                    .map_err(|e| ProduceError::msg(format!("trigger: ser event: {e}")))?;

                // Feed event to body
                self.body_input.set_data(Chunk::data(event_bytes));
                let body_bytes = request_data(self.body_output_transport.as_ref()).await;

                let body_batches = crate::sql::ipc_to_batches(&body_bytes)
                    .map_err(|e| ProduceError::msg(format!("trigger: deser body output: {e}")))?;

                // Runtime validation: body output schema consistent across events
                if let Some(b) = body_batches.first() {
                    let body_schema = b.schema();
                    match &reference_schema {
                        None => reference_schema = Some(body_schema),
                        Some(ref_s) => {
                            crate::producers_v2::validate_schemas_match(
                                ref_s, &body_schema, "trigger: runtime",
                            )?;
                        }
                    }
                }

                all_batches.extend(body_batches);
            }

            let _ = child.wait().await;

            // Concatenate results
            let result_bytes = if all_batches.is_empty() {
                // No events — need to determine output schema via schema inference
                let body_schema = request_schema(self.body_output_transport.as_ref()).await
                    .unwrap_or_else(|_| event_schema.clone());
                let empty = RecordBatch::new_empty(Arc::new(body_schema.clone()));
                crate::sql::batches_to_ipc(&Arc::new(body_schema), &[empty])
                    .map_err(|e| ProduceError::msg(format!("trigger: ser empty: {e}")))?
            } else {
                let schema = reference_schema.unwrap();
                crate::sql::batches_to_ipc(&schema, &all_batches)
                    .map_err(|e| ProduceError::msg(format!("trigger: ser result: {e}")))?
            };

            // Apply post_output_sql
            let result_bytes = match &self.post_output_sql {
                Some(sql) => crate::sql::datafusion::run_sql_on_ipc(&result_bytes, sql)
                    .await
                    .map_err(|e| ProduceError::msg(format!("trigger post_output_sql: {e}")))?,
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
