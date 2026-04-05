#[cfg(test)]
mod tests;

use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

use arrow::array::{RecordBatch, StringArray};
use arrow::datatypes::{DataType, Field, Schema};

use crate::dag_v2::{Chunk, ProduceError, Produces, Transport};
use crate::workflow::VarDef;

fn output_schema() -> Schema {
    Schema::new(vec![
        Field::new("name", DataType::Utf8, false),
        Field::new("value", DataType::Utf8, false),
    ])
}

/// Reads variables from CLI args, env vars, or static values.
/// Output: Arrow table with |name, value| rows.
pub struct ReadVarProducer {
    vars: Vec<VarDef>,
    args: HashMap<String, Vec<String>>,
    post_output_sql: Option<String>,
    input: std::sync::Mutex<Option<Arc<dyn Transport>>>,
}

impl ReadVarProducer {
    pub fn new(
        vars: Vec<VarDef>,
        args: HashMap<String, Vec<String>>,
        post_output_sql: Option<String>,
    ) -> Self {
        Self {
            vars,
            args,
            post_output_sql,
            input: std::sync::Mutex::new(None),
        }
    }

    /// Resolve all vars to (name, value) pairs.
    fn resolve_vars(&self) -> Result<(Vec<String>, Vec<String>), ProduceError> {
        let mut names: Vec<String> = Vec::new();
        let mut values: Vec<String> = Vec::new();

        for var in &self.vars {
            if let Some(ref static_val) = var.value {
                // Static value
                names.push(var.name.clone());
                values.push(static_val.clone());
            } else if let Some(ref arg_name) = var.arg {
                // CLI arg -- may have multiple values
                if let Some(vals) = self.args.get(arg_name) {
                    for v in vals {
                        names.push(var.name.clone());
                        values.push(v.clone());
                    }
                } else if let Some(ref default) = var.default {
                    names.push(var.name.clone());
                    values.push(default.clone());
                } else {
                    return Err(ProduceError::msg(format!(
                        "missing required variable: {}",
                        var.name
                    )));
                }
            } else if let Some(ref env_name) = var.env {
                // Env var -- empty string if not set
                let val = std::env::var(env_name).unwrap_or_default();
                if !val.is_empty() {
                    names.push(var.name.clone());
                    values.push(val);
                } else if let Some(ref default) = var.default {
                    names.push(var.name.clone());
                    values.push(default.clone());
                } else {
                    names.push(var.name.clone());
                    values.push(String::new());
                }
            } else {
                return Err(ProduceError::msg(format!(
                    "variable {} has no source (arg/env/value)",
                    var.name
                )));
            }
        }

        Ok((names, values))
    }
}

impl Produces for ReadVarProducer {
    fn handle_schema<'a>(
        &'a self,
        output: Arc<dyn Transport>,
    ) -> Pin<Box<dyn Future<Output = Result<(), ProduceError>> + Send + 'a>> {
        Box::pin(async move {
            let core = output_schema();
            let result_schema = match &self.post_output_sql {
                Some(sql) => crate::sql::datafusion::infer_schema(&core, sql)
                    .await
                    .map_err(|e| ProduceError::msg(format!("read_var: post_output_sql: {e}")))?,
                None => core,
            };

            let schema_bytes = crate::dag_v2::schema::schema_to_bytes(&result_schema)
                .map_err(|e| ProduceError::msg(format!("read_var: ser schema: {e}")))?;
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
            let (names, values) = self.resolve_vars()?;

            let schema = Arc::new(output_schema());
            let batch = RecordBatch::try_new(
                schema.clone(),
                vec![
                    Arc::new(StringArray::from(names)),
                    Arc::new(StringArray::from(values)),
                ],
            )
            .map_err(|e| ProduceError::msg(format!("read_var: arrow batch: {e}")))?;

            let mut result_bytes = crate::sql::batches_to_ipc(&schema, &[batch])
                .map_err(|e| ProduceError::msg(format!("read_var: ser: {e}")))?;

            // Apply post_output_sql
            if let Some(ref sql) = self.post_output_sql {
                result_bytes = crate::sql::datafusion::run_sql_on_ipc(&result_bytes, sql)
                    .await
                    .map_err(|e| ProduceError::msg(format!("read_var: post_output_sql: {e}")))?;
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
