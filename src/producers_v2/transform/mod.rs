#[cfg(test)]
mod tests;

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

use crate::dag_v2::{Chunk, ProduceError, Produces, Transport, request_schema, request_data};

/// A node that transforms data using SQL.
/// Stateless — SQL string is immutable config set at construction.
pub struct TransformNode {
    sql: String,
    input: std::sync::Mutex<Option<Arc<dyn Transport>>>,
}

impl TransformNode {
    pub fn new(sql: String) -> Self {
        Self {
            sql,
            input: std::sync::Mutex::new(None),
        }
    }
}

impl Produces for TransformNode {
    fn handle_schema<'a>(
        &'a self,
        output: Arc<dyn Transport>,
    ) -> Pin<Box<dyn Future<Output = Result<(), ProduceError>> + Send + 'a>> {
        Box::pin(async move {
            let input = self.input.lock().unwrap().clone()
                .ok_or_else(|| ProduceError::msg("transform: no input transport"))?;

            let upstream_schema = request_schema(input.as_ref()).await?;
            let output_schema = crate::sql::datafusion::infer_schema(&upstream_schema, &self.sql)
                .await
                .map_err(|e| ProduceError::msg(format!("transform: schema inference: {e}")))?;

            let schema_bytes = crate::dag_v2::schema::schema_to_bytes(&output_schema)
                .map_err(|e| ProduceError::msg(format!("transform: ser schema: {e}")))?;
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
                .ok_or_else(|| ProduceError::msg("transform: no input transport"))?;

            let input_bytes = request_data(input.as_ref()).await;
            let result_bytes = crate::sql::datafusion::run_sql_on_ipc(&input_bytes, &self.sql)
                .await
                .map_err(|e| ProduceError::msg(format!("transform: sql: {e}")))?;

            output.write(Chunk::data(result_bytes));
            output.close();
            Ok(())
        })
    }

    fn set_input(&self, transport: Arc<dyn Transport>) {
        *self.input.lock().unwrap() = Some(transport);
    }
}
