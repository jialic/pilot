#[cfg(test)]
mod tests;

use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

use arrow::array::{RecordBatch, StringArray};
use arrow::datatypes::{DataType, Field, Schema};

use crate::dag_v2::{Chunk, ProduceError, Produces, Transport, request_schema, request_data};

fn output_schema() -> Schema {
    Schema::new(vec![Field::new("output", DataType::Utf8, false)])
}

fn has_template_vars(command: &str) -> bool {
    command.contains("{{")
}

/// Runs a shell command. stdout becomes |output|.
/// Supports minijinja templates from |name, value| input.
pub struct ShellProducer {
    command: String,
    cwd: Option<String>,
    timeout: Option<u64>,
    post_output_sql: Option<String>,
    input: std::sync::Mutex<Option<Arc<dyn Transport>>>,
}

impl ShellProducer {
    pub fn new(
        command: String,
        cwd: Option<String>,
        timeout: Option<u64>,
        post_output_sql: Option<String>,
    ) -> Self {
        Self {
            command,
            cwd,
            timeout,
            post_output_sql,
            input: std::sync::Mutex::new(None),
        }
    }

    fn build_template_context(batches: &[RecordBatch]) -> HashMap<String, String> {
        let mut ctx = HashMap::new();
        for batch in batches {
            let name_col = match batch.column_by_name("name") {
                Some(c) => c,
                None => continue,
            };
            let value_col = match batch.column_by_name("value") {
                Some(c) => c,
                None => continue,
            };
            for i in 0..batch.num_rows() {
                let name = crate::sql::datafusion::arrow_string_value(name_col.as_ref(), i)
                    .unwrap_or_default();
                let value = crate::sql::datafusion::arrow_string_value(value_col.as_ref(), i)
                    .unwrap_or_default();
                ctx.insert(name, value);
            }
        }
        ctx
    }

    fn render_command(&self, ctx: &HashMap<String, String>) -> Result<String, ProduceError> {
        let mut env = minijinja::Environment::new();
        env.add_template("cmd", &self.command)
            .map_err(|e| ProduceError::msg(format!("invalid command template: {e}")))?;
        let tmpl = env.get_template("cmd")
            .map_err(|e| ProduceError::msg(format!("template error: {e}")))?;
        tmpl.render(ctx)
            .map_err(|e| ProduceError::msg(format!("template render error: {e}")))
    }

    async fn execute_command(&self, command: &str) -> Result<String, ProduceError> {
        let cwd = self.cwd.as_ref()
            .map(|dir| crate::tools::file::expand_home(dir));

        let exec_all = async {
            let mut all_stdout = String::new();
            for line in command.lines().map(|l| l.trim()).filter(|l| !l.is_empty()) {
                let tokens = shlex::split(line)
                    .ok_or_else(|| ProduceError::msg(format!("invalid shell syntax: {line}")))?;
                if tokens.is_empty() {
                    continue;
                }

                let mut cmd = tokio::process::Command::new(&tokens[0]);
                if tokens.len() > 1 {
                    cmd.args(&tokens[1..]);
                }
                if let Some(ref dir) = cwd {
                    cmd.current_dir(dir);
                }

                let output = cmd.output().await
                    .map_err(|e| ProduceError::msg(format!("command failed: {e}")))?;
                let stdout = String::from_utf8_lossy(&output.stdout);
                let stderr = String::from_utf8_lossy(&output.stderr);

                if !output.status.success() {
                    let mut msg = format!("command exited with {}: {line}", output.status);
                    if !stderr.is_empty() {
                        msg.push_str(&format!("\n{}", stderr));
                    }
                    if !stdout.is_empty() {
                        msg.push_str(&format!("\n{}", stdout));
                    }
                    return Err(ProduceError::msg(msg));
                }

                all_stdout.push_str(&stdout);
            }
            Ok::<String, ProduceError>(all_stdout)
        };

        if let Some(secs) = self.timeout {
            tokio::time::timeout(std::time::Duration::from_secs(secs), exec_all)
                .await
                .map_err(|_| ProduceError::msg(format!("shell command timed out after {secs}s")))?
        } else {
            exec_all.await
        }
    }
}

impl Produces for ShellProducer {
    fn handle_schema<'a>(
        &'a self,
        output: Arc<dyn Transport>,
    ) -> Pin<Box<dyn Future<Output = Result<(), ProduceError>> + Send + 'a>> {
        Box::pin(async move {
            let input = self.input.lock().unwrap().clone()
                .ok_or_else(|| ProduceError::msg("shell: no input transport"))?;

            // Validate template input if needed
            if has_template_vars(&self.command) {
                let upstream = request_schema(input.as_ref()).await?;
                let prepared = crate::sql::datafusion::infer_schema(&upstream, "SELECT name, value FROM input")
                    .await
                    .map_err(|e| ProduceError::msg(format!("shell: template requires |name, value| input: {e}")))?;
                if prepared.column_with_name("name").is_none() || prepared.column_with_name("value").is_none() {
                    return Err(ProduceError::msg("shell: template requires |name, value| input"));
                }
            }

            // Output schema: |output| or post_output_sql applied
            let core = output_schema();
            let result_schema = match &self.post_output_sql {
                Some(sql) => crate::sql::datafusion::infer_schema(&core, sql)
                    .await
                    .map_err(|e| ProduceError::msg(format!("shell: post_output_sql: {e}")))?,
                None => core,
            };

            let schema_bytes = crate::dag_v2::schema::schema_to_bytes(&result_schema)
                .map_err(|e| ProduceError::msg(format!("shell: ser schema: {e}")))?;
            output.write(Chunk::schema_res(schema_bytes));
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
                .ok_or_else(|| ProduceError::msg("shell: no input transport"))?;

            let input_bytes = request_data(input.as_ref()).await;

            // Render command template if needed
            let command = if has_template_vars(&self.command) {
                let batches = crate::sql::ipc_to_batches(&input_bytes)
                    .map_err(|e| ProduceError::msg(format!("shell: deser input: {e}")))?;
                let template_ctx = Self::build_template_context(&batches);
                self.render_command(&template_ctx)?
            } else {
                self.command.clone()
            };

            // Execute
            let stdout = self.execute_command(&command).await?;

            // Build |output| batch
            let schema = Arc::new(output_schema());
            let batch = RecordBatch::try_new(
                schema.clone(),
                vec![Arc::new(StringArray::from(vec![stdout.as_str()]))],
            ).map_err(|e| ProduceError::msg(format!("shell: arrow batch: {e}")))?;

            let mut result_bytes = crate::sql::batches_to_ipc(&schema, &[batch])
                .map_err(|e| ProduceError::msg(format!("shell: ser: {e}")))?;

            // Apply post_output_sql
            if let Some(ref sql) = self.post_output_sql {
                result_bytes = crate::sql::datafusion::run_sql_on_ipc(&result_bytes, sql)
                    .await
                    .map_err(|e| ProduceError::msg(format!("shell: post_output_sql: {e}")))?;
            }

            output.write(Chunk::data(result_bytes));
            output.close();
            Ok(())
        })
    }

    fn set_input(&self, transport: Arc<dyn Transport>) {
        *self.input.lock().unwrap() = Some(transport);
    }
}
