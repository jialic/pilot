#[cfg(test)]
mod tests;

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

use arrow::array::{Int64Array, RecordBatch, StringArray};
use arrow::datatypes::{DataType, Field, Schema};

use crate::dag_v2::{Chunk, ProduceError, Produces, Transport, request_data, request_schema};
use crate::llm::{ChatMessage, LlmClient};
use crate::tools::dispatcher::{DefaultToolDispatcher, ToolDispatcher};
use crate::workflow::{FileRef, ToolDef};

fn conversation_schema() -> Schema {
    Schema::new(vec![
        Field::new("ts", DataType::Int64, false),
        Field::new("role", DataType::Utf8, false),
        Field::new("content", DataType::Utf8, false),
    ])
}

/// Runs an LLM call with optional tool loop.
///
/// Input: |ts, role, content| rows (the conversation so far).
/// Output: |ts, role, content| rows for all new messages from the tool loop
/// (tool calls, tool results, final assistant response).
///
/// Supports optional `pre_input_sql` to reshape input before reading rows
/// (defaults to "SELECT role, content FROM input ORDER BY ts"),
/// and optional `post_output_sql` to reshape output after building rows.
pub struct LlmCallProducer {
    llm: Arc<dyn LlmClient>,
    model_override: Option<String>,
    instruction: String,
    context_files: Vec<FileRef>,
    dispatcher: Box<dyn ToolDispatcher>,
    pre_input_sql: Option<String>,
    post_output_sql: Option<String>,
    listener: Option<Arc<dyn crate::events::Listener>>,
    input: std::sync::Mutex<Option<Arc<dyn Transport>>>,
}

impl LlmCallProducer {
    pub fn new(
        llm: Arc<dyn LlmClient>,
        model_override: Option<String>,
        instruction: String,
        context_files: Vec<FileRef>,
        dispatcher: DefaultToolDispatcher,
        pre_input_sql: Option<String>,
        post_output_sql: Option<String>,
        listener: Option<Arc<dyn crate::events::Listener>>,
    ) -> Self {
        Self {
            llm,
            model_override,
            instruction,
            context_files,
            dispatcher: Box::new(dispatcher),
            pre_input_sql,
            post_output_sql,
            listener,
            input: std::sync::Mutex::new(None),
        }
    }

    fn effective_pre_input_sql(&self) -> &str {
        match &self.pre_input_sql {
            Some(sql) => sql.as_str(),
            None => "SELECT role, content FROM input ORDER BY ts",
        }
    }

    fn build_system_prompt(&self) -> String {
        let mut sb = String::new();

        sb.push_str(&self.instruction);

        if !self.context_files.is_empty() {
            sb.push_str("\n\n");
            for file_ref in &self.context_files {
                match file_ref.read() {
                    Ok(data) => {
                        sb.push_str(&format!("--- {} ---\n{}\n", file_ref.path, data));
                    }
                    Err(err) => {
                        sb.push_str(&format!("--- {} (error: {}) ---\n", file_ref.path, err));
                    }
                }
            }
        }

        sb
    }
}

/// Build a tool dispatcher from YAML tool definitions.
pub fn build_tool_dispatcher(tool_defs: &[ToolDef], s3_config: &crate::tools::dispatcher::S3Config, llm: Option<std::sync::Arc<dyn crate::llm::LlmClient>>) -> DefaultToolDispatcher {
    crate::tools::dispatcher::build_tool_dispatcher(tool_defs, s3_config, llm)
}

impl Produces for LlmCallProducer {
    fn handle_schema<'a>(
        &'a self,
        output: Arc<dyn Transport>,
    ) -> Pin<Box<dyn Future<Output = Result<(), ProduceError>> + Send + 'a>> {
        Box::pin(async move {
            let input = self.input.lock().unwrap().clone()
                .ok_or_else(|| ProduceError::msg("llm_call: no input transport"))?;

            let upstream_schema = request_schema(input.as_ref()).await?;

            // Validate pre_input_sql against upstream schema
            let pre_sql = self.effective_pre_input_sql();
            crate::sql::datafusion::infer_schema(&upstream_schema, pre_sql)
                .await
                .map_err(|e| ProduceError::msg(format!("llm_call: pre_input_sql: {e}")))?;

            // Core output schema: |ts, role, content|
            let core = conversation_schema();

            // Apply post_output_sql if set
            let result_schema = match &self.post_output_sql {
                Some(sql) => crate::sql::datafusion::infer_schema(&core, sql)
                    .await
                    .map_err(|e| ProduceError::msg(format!("llm_call: post_output_sql: {e}")))?,
                None => core,
            };

            let schema_bytes = crate::dag_v2::schema::schema_to_bytes(&result_schema)
                .map_err(|e| ProduceError::msg(format!("llm_call: ser schema: {e}")))?;
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
                .ok_or_else(|| ProduceError::msg("llm_call: no input transport"))?;

            let input_bytes = request_data(input.as_ref()).await;

            // Apply pre_input_sql to reshape input
            let pre_sql = self.effective_pre_input_sql();
            let prepared_bytes = crate::sql::datafusion::run_sql_on_ipc(&input_bytes, pre_sql)
                .await
                .map_err(|e| ProduceError::msg(format!("llm_call: pre_input_sql: {e}")))?;

            let batches = crate::sql::ipc_to_batches(&prepared_bytes)
                .map_err(|e| ProduceError::msg(format!("llm_call: deser input: {e}")))?;

            // Extract (role, content) rows from prepared input
            let mut rows: Vec<(String, String)> = Vec::new();
            for batch in &batches {
                let role_col = batch.column_by_name("role")
                    .ok_or_else(|| ProduceError::msg("llm_call: missing role column"))?;
                let content_col = batch.column_by_name("content")
                    .ok_or_else(|| ProduceError::msg("llm_call: missing content column"))?;
                for i in 0..batch.num_rows() {
                    let role = crate::sql::datafusion::arrow_string_value(role_col.as_ref(), i)
                        .unwrap_or_default();
                    let content = crate::sql::datafusion::arrow_string_value(content_col.as_ref(), i)
                        .unwrap_or_default();
                    rows.push((role, content));
                }
            }

            // Build chat messages from rows
            let system_prompt = self.build_system_prompt();
            let mut messages: Vec<ChatMessage> = vec![
                ChatMessage::System { content: system_prompt, name: None },
            ];

            for (role, content) in &rows {
                let msg = match role.as_str() {
                    "system" => ChatMessage::System { content: content.clone(), name: None },
                    "user" => ChatMessage::User { content: content.clone(), name: None },
                    "assistant" => ChatMessage::Assistant {
                        content: Some(content.clone()),
                        tool_calls: None,
                    },
                    // Inline base64 image — sent directly to LLM as image content
                    "image" => ChatMessage::Image {
                        media_type: "image/webp".to_string(),
                        data: content.clone(),
                    },
                    // Skip v1 blob references (image_storage, etc.)
                    r if r.starts_with("image_") => continue,
                    _ => continue,
                };
                messages.push(msg);
            }

            let new_messages = crate::llm::run_tool_loop(
                self.llm.as_ref(),
                &*self.dispatcher,
                &mut messages,
                self.model_override.as_deref(),
                self.listener.as_deref(),
            )
            .await
            .map_err(|e| ProduceError::msg(format!("llm_call: tool loop: {e}")))?;

            // Build |ts, role, content| output from new messages
            let now_ms = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_millis() as i64;

            let mut ts_vals: Vec<i64> = Vec::new();
            let mut role_vals: Vec<String> = Vec::new();
            let mut content_vals: Vec<String> = Vec::new();

            for (i, (role, content)) in new_messages.iter().enumerate() {
                ts_vals.push(now_ms + i as i64);
                role_vals.push(role.clone());
                content_vals.push(content.clone());
            }

            let schema = Arc::new(conversation_schema());
            let batch = RecordBatch::try_new(
                schema.clone(),
                vec![
                    Arc::new(Int64Array::from(ts_vals)),
                    Arc::new(StringArray::from(
                        role_vals.iter().map(|s| s.as_str()).collect::<Vec<_>>(),
                    )),
                    Arc::new(StringArray::from(
                        content_vals.iter().map(|s| s.as_str()).collect::<Vec<_>>(),
                    )),
                ],
            )
            .map_err(|e| ProduceError::msg(format!("llm_call: arrow batch: {e}")))?;

            let mut result_bytes = crate::sql::batches_to_ipc(&schema, &[batch])
                .map_err(|e| ProduceError::msg(format!("llm_call: ser: {e}")))?;

            // Apply post_output_sql if set
            if let Some(ref sql) = self.post_output_sql {
                result_bytes = crate::sql::datafusion::run_sql_on_ipc(&result_bytes, sql)
                    .await
                    .map_err(|e| ProduceError::msg(format!("llm_call: post_output_sql: {e}")))?;
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
