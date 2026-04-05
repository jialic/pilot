#[cfg(test)]
mod tests;

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

use arrow::array::{Int64Array, RecordBatch, StringArray};
use arrow::datatypes::{DataType, Field, Schema};

use crate::dag_v2::{Chunk, ProduceError, Produces, Transport, request_data, request_schema};
use crate::llm::{ChatMessage, LlmClient};

fn conversation_schema() -> Schema {
    Schema::new(vec![
        Field::new("ts", DataType::Int64, false),
        Field::new("role", DataType::Utf8, false),
        Field::new("content", DataType::Utf8, false),
    ])
}

fn word_count(s: &str) -> usize {
    s.split_whitespace().count()
}

pub const COMPACT_RATIO_MIN: f64 = 0.1;
pub const COMPACT_RATIO_MAX: f64 = 1.0;

/// Compresses conversation history when it exceeds a word threshold.
///
/// Input: |ts, role, content| rows.
/// Output: |ts, role, content| rows — passthrough if under threshold,
/// otherwise oldest rows are summarized via LLM and replaced with a
/// single system summary row.
pub struct CompactProducer {
    llm: Arc<dyn LlmClient>,
    max_words: usize,
    compact_ratio: f64,
    model_override: Option<String>,
    instruction: String,
    input: std::sync::Mutex<Option<Arc<dyn Transport>>>,
}

impl CompactProducer {
    pub fn new(
        llm: Arc<dyn LlmClient>,
        max_words: usize,
        compact_ratio: f64,
        model_override: Option<String>,
        instruction: String,
    ) -> Self {
        Self {
            llm,
            max_words,
            compact_ratio: compact_ratio.clamp(COMPACT_RATIO_MIN, COMPACT_RATIO_MAX),
            model_override,
            instruction,
            input: std::sync::Mutex::new(None),
        }
    }
}

impl Produces for CompactProducer {
    fn handle_schema<'a>(
        &'a self,
        output: Arc<dyn Transport>,
    ) -> Pin<Box<dyn Future<Output = Result<(), ProduceError>> + Send + 'a>> {
        Box::pin(async move {
            let input = self.input.lock().unwrap().clone()
                .ok_or_else(|| ProduceError::msg("compact: no input transport"))?;

            let upstream_schema = request_schema(input.as_ref()).await?;

            // Validate input has ts, role, content columns
            let required = ["ts", "role", "content"];
            for col in &required {
                if upstream_schema.column_with_name(col).is_none() {
                    return Err(ProduceError::msg(format!(
                        "compact requires column '{}', input has {:?}",
                        col,
                        upstream_schema
                            .fields()
                            .iter()
                            .map(|f| f.name().as_str())
                            .collect::<Vec<_>>(),
                    )));
                }
            }

            // Output schema matches input — compact preserves schema
            let schema_bytes = crate::dag_v2::schema::schema_to_bytes(&upstream_schema)
                .map_err(|e| ProduceError::msg(format!("compact: ser schema: {e}")))?;
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
                .ok_or_else(|| ProduceError::msg("compact: no input transport"))?;

            let input_bytes = request_data(input.as_ref()).await;

            let batches = crate::sql::ipc_to_batches(&input_bytes)
                .map_err(|e| ProduceError::msg(format!("compact: deser: {e}")))?;

            // Collect all rows as (ts, role, content)
            let mut rows: Vec<(i64, String, String)> = Vec::new();
            for batch in &batches {
                let ts_col = batch
                    .column_by_name("ts")
                    .and_then(|c| c.as_any().downcast_ref::<Int64Array>())
                    .ok_or_else(|| ProduceError::msg("compact: missing ts column"))?;
                let role_col = batch
                    .column_by_name("role")
                    .ok_or_else(|| ProduceError::msg("compact: missing role column"))?;
                let content_col = batch
                    .column_by_name("content")
                    .ok_or_else(|| ProduceError::msg("compact: missing content column"))?;

                for i in 0..batch.num_rows() {
                    let ts = ts_col.value(i);
                    let role =
                        crate::sql::datafusion::arrow_string_value(role_col.as_ref(), i)
                            .unwrap_or_default();
                    let content =
                        crate::sql::datafusion::arrow_string_value(content_col.as_ref(), i)
                            .unwrap_or_default();
                    rows.push((ts, role, content));
                }
            }

            // Sort by timestamp to ensure chronological order
            rows.sort_by_key(|(ts, _, _)| *ts);

            // Check if compaction is needed
            let total_words: usize = rows.iter().map(|(_, _, c)| word_count(c)).sum();
            if total_words <= self.max_words || rows.len() <= 1 {
                // Passthrough: send original data unchanged
                output.write(Chunk::data(input_bytes));
                output.close();
                return Ok(());
            }

            // Select oldest rows up to compact_ratio of total words
            let compact_budget = (total_words as f64 * self.compact_ratio) as usize;
            let mut compact_count = 0;
            let mut words_so_far: usize = 0;
            for (_, _, content) in &rows {
                let tw = word_count(content);
                if words_so_far + tw > compact_budget && compact_count > 0 {
                    break;
                }
                words_so_far += tw;
                compact_count += 1;
            }
            // Ensure at least 1 row compacted, but never all rows
            compact_count = compact_count.max(1).min(rows.len() - 1);

            // Build conversation text for summarization
            let mut conversation = String::new();
            for (_, role, content) in &rows[..compact_count] {
                conversation.push_str(&format!("{}: {}\n\n", role, content));
            }

            // LLM summarization
            let messages = vec![
                ChatMessage::System {
                    content: self.instruction.clone(),
                    name: None,
                },
                ChatMessage::User {
                    content: conversation,
                    name: None,
                },
            ];

            let response = self
                .llm
                .chat(messages, None, None, self.model_override.as_deref())
                .await
                .map_err(|e| ProduceError::msg(format!("compact: LLM call failed: {e}")))?;

            let summary = response
                .content
                .unwrap_or_else(|| "Earlier conversation occurred.".to_string());

            // Build compacted table: summary row + remaining rows
            let mut ts_vals = vec![0i64];
            let mut role_vals = vec!["system".to_string()];
            let mut content_vals = vec![format!("Previous conversation summary:\n{}", summary)];

            for (ts, role, content) in &rows[compact_count..] {
                ts_vals.push(*ts);
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
            .map_err(|e| ProduceError::msg(format!("compact: build batch: {e}")))?;

            let result_bytes = crate::sql::batches_to_ipc(&schema, &[batch])
                .map_err(|e| ProduceError::msg(format!("compact: ser: {e}")))?;

            output.write(Chunk::data(result_bytes));
            output.close();
            Ok(())
        })
    }

    fn set_input(&self, transport: Arc<dyn Transport>) {
        *self.input.lock().unwrap() = Some(transport);
    }
}
