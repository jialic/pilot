use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

use crate::dag_v2::{
    Chunk, ProduceError, Produces, Transport,
    request_schema, request_data,
};

/// Simple producer that forwards input unchanged.
pub struct PassthroughProducer {
    input: std::sync::Mutex<Option<Arc<dyn Transport>>>,
}

impl PassthroughProducer {
    pub fn new() -> Self {
        Self {
            input: std::sync::Mutex::new(None),
        }
    }
}

impl Produces for PassthroughProducer {
    fn handle_schema<'a>(
        &'a self,
        output: Arc<dyn Transport>,
    ) -> Pin<Box<dyn Future<Output = Result<(), ProduceError>> + Send + 'a>> {
        Box::pin(async move {
            let input = self.input.lock().unwrap().clone()
                .ok_or_else(|| ProduceError::msg("passthrough: no input transport"))?;
            let upstream_schema = request_schema(input.as_ref()).await?;
            let schema_bytes = crate::dag_v2::schema::schema_to_bytes(&upstream_schema)
                .map_err(|e| ProduceError::msg(format!("passthrough: ser schema: {e}")))?;
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
                .ok_or_else(|| ProduceError::msg("passthrough: no input transport"))?;
            let input_bytes = request_data(input.as_ref()).await;
            output.write(Chunk::data(input_bytes));
            output.close();
            Ok(())
        })
    }

    fn set_input(&self, transport: Arc<dyn Transport>) {
        *self.input.lock().unwrap() = Some(transport);
    }
}
