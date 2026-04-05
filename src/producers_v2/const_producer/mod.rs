use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

use arrow::array::{RecordBatch, StringArray};
use arrow::datatypes::{DataType, Field, Schema};

use crate::dag_v2::{Chunk, ProduceError, Produces, Transport};

#[cfg(test)]
mod tests;

/// A source node that writes fixed data. No input transport.
pub struct ConstProducer {
    schema_bytes: Vec<u8>,
    data_bytes: Vec<u8>,
}

impl ConstProducer {
    pub fn new(schema_bytes: Vec<u8>, data_bytes: Vec<u8>) -> Self {
        Self { schema_bytes, data_bytes }
    }

    /// Create a source that outputs a single |output| column with the given value.
    pub fn output(s: impl Into<String>) -> Self {
        let s = s.into();
        let schema = Schema::new(vec![Field::new("output", DataType::Utf8, false)]);
        let batch = RecordBatch::try_new(
            Arc::new(schema.clone()),
            vec![Arc::new(StringArray::from(vec![s.as_str()]))],
        ).expect("failed to create batch");

        let data_bytes = crate::sql::batches_to_ipc(&schema, &[batch])
            .expect("failed to serialize");
        let schema_bytes = crate::dag_v2::schema::schema_to_bytes(&schema)
            .expect("failed to serialize schema");

        Self::new(schema_bytes, data_bytes)
    }
}

impl Produces for ConstProducer {
    fn handle_schema<'a>(
        &'a self,
        output: Arc<dyn Transport>,
    ) -> Pin<Box<dyn Future<Output = Result<(), ProduceError>> + Send + 'a>> {
        Box::pin(async move {
            output.write(Chunk::schema_res(self.schema_bytes.clone()));
            output.close();
            Ok(())
        })
    }

    fn handle_data<'a>(
        &'a self,
        output: Arc<dyn Transport>,
    ) -> Pin<Box<dyn Future<Output = Result<(), ProduceError>> + Send + 'a>> {
        Box::pin(async move {
            output.write(Chunk::data(self.data_bytes.clone()));
            output.close();
            Ok(())
        })
    }

    fn set_input(&self, _transport: Arc<dyn Transport>) {}
}
