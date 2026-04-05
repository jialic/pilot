#[cfg(test)]
mod tests;

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

use crate::dag_v2::{Chunk, ProduceError, Produces, Transport, request_schema, request_data};

/// Prints input to stdout and passes it through unchanged.
pub struct PrintProducer {
    input: std::sync::Mutex<Option<Arc<dyn Transport>>>,
}

impl PrintProducer {
    pub fn new() -> Self {
        Self {
            input: std::sync::Mutex::new(None),
        }
    }
}

impl Produces for PrintProducer {
    fn handle_schema<'a>(
        &'a self,
        output: Arc<dyn Transport>,
    ) -> Pin<Box<dyn Future<Output = Result<(), ProduceError>> + Send + 'a>> {
        Box::pin(async move {
            let input = self.input.lock().unwrap().clone()
                .ok_or_else(|| ProduceError::msg("print: no input transport"))?;

            let schema = request_schema(input.as_ref()).await?;
            let schema_bytes = crate::dag_v2::schema::schema_to_bytes(&schema)
                .map_err(|e| ProduceError::msg(format!("print: ser schema: {e}")))?;
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
                .ok_or_else(|| ProduceError::msg("print: no input transport"))?;

            let data = request_data(input.as_ref()).await;
            let display = crate::sql::cli::arrow_bytes_to_json(&data).unwrap_or_default();
            println!("{}", display);

            output.write(Chunk::data(data));
            output.close();
            Ok(())
        })
    }

    fn set_input(&self, transport: Arc<dyn Transport>) {
        *self.input.lock().unwrap() = Some(transport);
    }
}
