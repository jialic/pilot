#[cfg(test)]
mod tests;

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

use crate::dag_v2::{Chunk, ProduceError, Produces, Transport};

/// A producer whose data and schema can be updated between calls.
/// Used by structural producers (parallel, loop, each, trigger) to feed
/// changing data to sub-pipelines without rewiring.
pub struct MutableProducer {
    schema: std::sync::Mutex<Option<Chunk>>,
    data: std::sync::Mutex<Option<Chunk>>,
}

impl MutableProducer {
    pub fn new() -> Self {
        Self {
            schema: std::sync::Mutex::new(None),
            data: std::sync::Mutex::new(None),
        }
    }

    /// Set the schema chunk to return on next handle_schema call.
    pub fn set_schema(&self, chunk: Chunk) {
        *self.schema.lock().unwrap() = Some(chunk);
    }

    /// Set the data chunk to return on next handle_data call.
    pub fn set_data(&self, chunk: Chunk) {
        *self.data.lock().unwrap() = Some(chunk);
    }
}

impl Produces for MutableProducer {
    fn handle_schema<'a>(
        &'a self,
        output: Arc<dyn Transport>,
    ) -> Pin<Box<dyn Future<Output = Result<(), ProduceError>> + Send + 'a>> {
        Box::pin(async move {
            let chunk = self.schema.lock().unwrap().take()
                .ok_or_else(|| ProduceError::msg("mutable_producer: no schema set"))?;
            output.write(chunk);
            output.close();
            Ok(())
        })
    }

    fn handle_data<'a>(
        &'a self,
        output: Arc<dyn Transport>,
    ) -> Pin<Box<dyn Future<Output = Result<(), ProduceError>> + Send + 'a>> {
        Box::pin(async move {
            let chunk = self.data.lock().unwrap().take()
                .ok_or_else(|| ProduceError::msg("mutable_producer: no data set"))?;
            output.write(chunk);
            output.close();
            Ok(())
        })
    }

    fn set_input(&self, _transport: Arc<dyn Transport>) {}
}
