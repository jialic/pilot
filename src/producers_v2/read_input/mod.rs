#[cfg(test)]
mod tests;

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

use arrow::array::{RecordBatch, StringArray};
use arrow::datatypes::{DataType, Field, Schema};

use crate::dag_v2::{Chunk, ProduceError, Produces, Transport};
use crate::user_io::UserIO;

fn output_schema() -> Schema {
    Schema::new(vec![Field::new("output", DataType::Utf8, false)])
}

/// Prompts the user for input and returns their response as |output|.
/// No LLM call — displays the instruction as a prompt and reads stdin.
pub struct ReadInputProducer {
    io: Arc<dyn UserIO>,
    prompt: String,
    post_output_sql: Option<String>,
    input: std::sync::Mutex<Option<Arc<dyn Transport>>>,
}

impl ReadInputProducer {
    pub fn new(
        io: Arc<dyn UserIO>,
        prompt: String,
        post_output_sql: Option<String>,
    ) -> Self {
        Self {
            io,
            prompt,
            post_output_sql,
            input: std::sync::Mutex::new(None),
        }
    }
}

impl Produces for ReadInputProducer {
    fn handle_schema<'a>(
        &'a self,
        output: Arc<dyn Transport>,
    ) -> Pin<Box<dyn Future<Output = Result<(), ProduceError>> + Send + 'a>> {
        Box::pin(async move {
            let core = output_schema();
            let result_schema = match &self.post_output_sql {
                Some(sql) => crate::sql::datafusion::infer_schema(&core, sql)
                    .await
                    .map_err(|e| ProduceError::msg(format!("read_input: post_output_sql: {e}")))?,
                None => core,
            };

            let schema_bytes = crate::dag_v2::schema::schema_to_bytes(&result_schema)
                .map_err(|e| ProduceError::msg(format!("read_input: ser schema: {e}")))?;
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
            let answer = self
                .io
                .ask(self.prompt.clone())
                .await
                .map_err(|e| ProduceError::msg(format!("read_input: {e}")))?;

            let schema = Arc::new(output_schema());
            let batch = RecordBatch::try_new(
                schema.clone(),
                vec![Arc::new(StringArray::from(vec![answer.as_str()]))],
            )
            .map_err(|e| ProduceError::msg(format!("read_input: arrow batch: {e}")))?;

            let mut result_bytes = crate::sql::batches_to_ipc(&schema, &[batch])
                .map_err(|e| ProduceError::msg(format!("read_input: ser: {e}")))?;

            // Apply post_output_sql
            if let Some(ref sql) = self.post_output_sql {
                result_bytes = crate::sql::datafusion::run_sql_on_ipc(&result_bytes, sql)
                    .await
                    .map_err(|e| ProduceError::msg(format!("read_input: post_output_sql: {e}")))?;
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
