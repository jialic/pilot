#[cfg(test)]
mod tests;

use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

use arrow::array::{
    Array, BooleanArray, Float64Array, Int64Array, RecordBatch, StringArray,
};
use arrow::datatypes::{DataType, Field, Schema};

use crate::dag_v2::{Chunk, ProduceError, Produces, Transport, request_data};
use crate::workflow::SchemaField;

/// Compile and run a JQ expression on a JSON value.
/// Compiled per-invocation because jaq's Filter uses Rc (not Send+Sync).
fn run_jq(expr: &str, input: serde_json::Value) -> Result<Vec<serde_json::Value>, String> {
    use jaq_core::load::{Arena, File, Loader};
    use jaq_core::{Compiler, Ctx, RcIter};
    use jaq_json::Val;

    let program = File {
        code: expr,
        path: (),
    };
    let loader = Loader::new(jaq_std::defs().chain(jaq_json::defs()));
    let arena = Arena::default();

    let modules = loader
        .load(&arena, program)
        .map_err(|errs| format!("JQ parse error: {:?}", errs))?;
    let filter = Compiler::default()
        .with_funs(jaq_std::funs().chain(jaq_json::funs()))
        .compile(modules)
        .map_err(|errs| format!("JQ compile error: {:?}", errs))?;

    let val = Val::from(input);
    let inputs = RcIter::new(core::iter::empty());
    let out = filter.run((Ctx::new([], &inputs), val));

    let mut results = Vec::new();
    for item in out {
        match item {
            Ok(v) => {
                let json: serde_json::Value = v.into();
                results.push(json);
            }
            Err(e) => return Err(format!("JQ runtime error: {e}")),
        }
    }
    Ok(results)
}

/// Makes HTTP requests with JQ transforms for request/response.
pub struct HttpProducer {
    url: String,
    method: String,
    headers: HashMap<String, String>,
    headers_jq: Option<String>,
    timeout: Option<u64>,
    request_jq: Option<String>,
    response_jq: Option<String>,
    output_schema: Vec<SchemaField>,
    input: std::sync::Mutex<Option<Arc<dyn Transport>>>,
}

impl HttpProducer {
    pub fn new(
        url: String,
        method: String,
        headers: HashMap<String, String>,
        headers_jq: Option<String>,
        timeout: Option<u64>,
        request_jq: Option<String>,
        response_jq: Option<String>,
        output_schema: Vec<SchemaField>,
    ) -> Self {
        Self {
            url,
            method,
            headers,
            headers_jq,
            timeout,
            request_jq,
            response_jq,
            output_schema,
            input: std::sync::Mutex::new(None),
        }
    }

    /// Build Arrow Schema from declared output_schema fields.
    fn build_output_schema(&self) -> Result<Schema, ProduceError> {
        if self.output_schema.is_empty() {
            return Err(ProduceError::msg("http: output_schema is required"));
        }
        if self.response_jq.is_none() {
            return Err(ProduceError::msg("http: response_jq is required"));
        }
        let fields: Vec<Field> = self
            .output_schema
            .iter()
            .map(|f| {
                let dt = match f.field_type.as_str() {
                    "integer" => DataType::Int64,
                    "float" => DataType::Float64,
                    "boolean" => DataType::Boolean,
                    _ => DataType::Utf8,
                };
                Field::new(&f.name, dt, true)
            })
            .collect();
        Ok(Schema::new(fields))
    }

    /// Convert first Arrow row to a JSON object for JQ processing.
    fn arrow_row_to_json(input: &[RecordBatch]) -> serde_json::Value {
        let batch = match input.first() {
            Some(b) if b.num_rows() > 0 => b,
            _ => return serde_json::Value::Object(serde_json::Map::new()),
        };

        let mut obj = serde_json::Map::new();
        for (i, field) in batch.schema().fields().iter().enumerate() {
            let col = batch.column(i);
            let val = if let Some(s) =
                crate::sql::datafusion::arrow_string_value(col.as_ref(), 0)
            {
                // Try to parse as JSON value (number, bool, etc.)
                serde_json::from_str(&s).unwrap_or(serde_json::Value::String(s))
            } else {
                serde_json::Value::Null
            };
            obj.insert(field.name().clone(), val);
        }
        serde_json::Value::Object(obj)
    }

    /// Build Arrow RecordBatch from JQ output + declared schema.
    fn json_to_arrow_batch(
        results: &[serde_json::Value],
        schema: &Schema,
    ) -> Result<RecordBatch, ProduceError> {
        let mut columns: Vec<Arc<dyn Array>> = Vec::new();

        for field in schema.fields() {
            match field.data_type() {
                DataType::Utf8 => {
                    let vals: Vec<Option<String>> = results
                        .iter()
                        .map(|row| {
                            row.get(field.name()).and_then(|v| match v {
                                serde_json::Value::String(s) => Some(s.clone()),
                                serde_json::Value::Null => None,
                                other => Some(other.to_string()),
                            })
                        })
                        .collect();
                    columns.push(Arc::new(StringArray::from(vals)));
                }
                DataType::Int64 => {
                    let vals: Vec<Option<i64>> = results
                        .iter()
                        .map(|row| row.get(field.name()).and_then(|v| v.as_i64()))
                        .collect();
                    columns.push(Arc::new(Int64Array::from(vals)));
                }
                DataType::Float64 => {
                    let vals: Vec<Option<f64>> = results
                        .iter()
                        .map(|row| row.get(field.name()).and_then(|v| v.as_f64()))
                        .collect();
                    columns.push(Arc::new(Float64Array::from(vals)));
                }
                DataType::Boolean => {
                    let vals: Vec<Option<bool>> = results
                        .iter()
                        .map(|row| row.get(field.name()).and_then(|v| v.as_bool()))
                        .collect();
                    columns.push(Arc::new(BooleanArray::from(vals)));
                }
                _ => {
                    return Err(ProduceError::msg(format!(
                        "http: unsupported output type: {:?}",
                        field.data_type()
                    )));
                }
            }
        }

        RecordBatch::try_new(Arc::new(schema.clone()), columns)
            .map_err(|e| ProduceError::msg(format!("http: build output batch: {e}")))
    }
}

impl Produces for HttpProducer {
    fn handle_schema<'a>(
        &'a self,
        output: Arc<dyn Transport>,
    ) -> Pin<Box<dyn Future<Output = Result<(), ProduceError>> + Send + 'a>> {
        Box::pin(async move {
            let schema = self.build_output_schema()?;

            let schema_bytes = crate::dag_v2::schema::schema_to_bytes(&schema)
                .map_err(|e| ProduceError::msg(format!("http: ser schema: {e}")))?;
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
            // 1. Get input data (for request_jq / body / headers_jq)
            let input_batches: Vec<RecordBatch> = {
                let input_transport = self.input.lock().unwrap().clone();
                if let Some(transport) = input_transport {
                    let input_bytes = request_data(transport.as_ref()).await;
                    if input_bytes.is_empty() {
                        vec![]
                    } else {
                        crate::sql::ipc_to_batches(&input_bytes)
                            .map_err(|e| ProduceError::msg(format!("http: deser input: {e}")))?
                    }
                } else {
                    vec![]
                }
            };

            // 2. Build request body
            let body: Option<String> = if let Some(ref jq_expr) = self.request_jq {
                let input_json = Self::arrow_row_to_json(&input_batches);
                let results = run_jq(jq_expr, input_json)
                    .map_err(|e| ProduceError::msg(format!("http: request_jq: {e}")))?;
                results
                    .first()
                    .map(|v| serde_json::to_string(v).unwrap_or_default())
            } else {
                // For POST/PUT/PATCH without request_jq, use output column as body
                let needs_body = matches!(self.method.as_str(), "POST" | "PUT" | "PATCH");
                if needs_body {
                    input_batches
                        .first()
                        .and_then(|b| b.column_by_name("output"))
                        .and_then(|col| {
                            crate::sql::datafusion::arrow_string_value(col.as_ref(), 0)
                        })
                } else {
                    None
                }
            };

            // 3. Make HTTP request
            let client = reqwest::Client::new();
            let mut request = match self.method.as_str() {
                "GET" => client.get(&self.url),
                "POST" => client.post(&self.url),
                "PUT" => client.put(&self.url),
                "DELETE" => client.delete(&self.url),
                "PATCH" => client.patch(&self.url),
                other => {
                    return Err(ProduceError::msg(format!(
                        "http: unsupported method: {other}"
                    )));
                }
            };

            // Set headers -- static first, then dynamic from headers_jq (overrides)
            let mut merged_headers = self.headers.clone();
            if let Some(ref jq_expr) = self.headers_jq {
                let input_json = Self::arrow_row_to_json(&input_batches);
                let results = run_jq(jq_expr, input_json)
                    .map_err(|e| ProduceError::msg(format!("http: headers_jq: {e}")))?;
                if let Some(obj) = results.first().and_then(|v| v.as_object()) {
                    for (k, v) in obj {
                        if let Some(s) = v.as_str() {
                            merged_headers.insert(k.clone(), s.to_string());
                        }
                    }
                }
            }
            for (key, value) in &merged_headers {
                request = request.header(key.as_str(), value.as_str());
            }

            // Set timeout
            if let Some(secs) = self.timeout {
                request = request.timeout(std::time::Duration::from_secs(secs));
            }

            // Set body
            if let Some(body) = body {
                request = request
                    .header("Content-Type", "application/json")
                    .body(body);
            }

            // Send
            let response = request
                .send()
                .await
                .map_err(|e| ProduceError::msg(format!("http: request failed: {e}")))?;

            let status = response.status();
            let response_body = response
                .text()
                .await
                .map_err(|e| ProduceError::msg(format!("http: read response: {e}")))?;

            if !status.is_success() {
                return Err(ProduceError::msg(format!(
                    "HTTP {} {}: {}",
                    status.as_u16(),
                    status.canonical_reason().unwrap_or(""),
                    &response_body[..response_body.len().min(500)]
                )));
            }

            // 4. Transform response with JQ
            let jq_expr = self
                .response_jq
                .as_ref()
                .ok_or_else(|| ProduceError::msg("http: response_jq is required"))?;

            let response_json: serde_json::Value =
                serde_json::from_str(&response_body).map_err(|e| {
                    ProduceError::msg(format!("http: response is not valid JSON: {e}"))
                })?;

            let results = run_jq(jq_expr, response_json)
                .map_err(|e| ProduceError::msg(format!("http: response_jq: {e}")))?;

            let schema = self.build_output_schema()?;
            let batch = Self::json_to_arrow_batch(&results, &schema)?;

            let result_bytes =
                crate::sql::batches_to_ipc(&schema, &[batch])
                    .map_err(|e| ProduceError::msg(format!("http: ser: {e}")))?;

            output.write(Chunk::data(result_bytes));
            output.close();
            Ok(())
        })
    }

    fn set_input(&self, transport: Arc<dyn Transport>) {
        *self.input.lock().unwrap() = Some(transport);
    }
}
