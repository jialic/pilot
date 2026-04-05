#[cfg(test)]
mod tests;

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

use crate::dag_v2::{
    BufferTransport, Chunk, ProduceError, Produces, Transport,
    request_schema, request_data,
};
use crate::producers_v2::MutableProducer;
use crate::producers_v2::pipeline::Pipeline;

struct BranchWiring {
    name: String,
    input: Arc<MutableProducer>,
    output: Arc<BufferTransport>,
}

/// Runs multiple named branch pipelines concurrently, merges results via SQL.
///
/// - All branches receive the same input (optionally transformed by pre_input_sql).
/// - Each branch produces its own output table, registered by name.
/// - `merge_sql` joins all branch tables into the final output.
pub struct ParallelProducer {
    pre_input_sql: Option<String>,
    merge_sql: String,
    branches: Vec<BranchWiring>,
    input: std::sync::Mutex<Option<Arc<dyn Transport>>>,
}

impl ParallelProducer {
    pub fn new(
        pre_input_sql: Option<String>,
        merge_sql: String,
        branch_pipelines: Vec<(String, Pipeline)>,
    ) -> Self {
        let branches: Vec<BranchWiring> = branch_pipelines
            .into_iter()
            .map(|(name, pipeline)| {
                // Wire: branch_input -> transport -> pipeline.head
                let branch_input = Arc::new(MutableProducer::new());
                let head_transport = BufferTransport::new();
                head_transport.set_source(branch_input.clone());
                pipeline.head().set_input(head_transport);

                // Wire: pipeline.tail -> branch_output
                let branch_output = BufferTransport::new();
                branch_output.set_source(pipeline.tail().clone());

                BranchWiring {
                    name,
                    input: branch_input,
                    output: branch_output,
                }
            })
            .collect();

        Self {
            pre_input_sql,
            merge_sql,
            branches,
            input: std::sync::Mutex::new(None),
        }
    }
}

impl Produces for ParallelProducer {
    fn handle_schema<'a>(
        &'a self,
        output: Arc<dyn Transport>,
    ) -> Pin<Box<dyn Future<Output = Result<(), ProduceError>> + Send + 'a>> {
        Box::pin(async move {
            let input = self.input.lock().unwrap().clone()
                .ok_or_else(|| ProduceError::msg("parallel: no input transport"))?;
            let upstream_schema = request_schema(input.as_ref()).await?;

            let input_schema = match &self.pre_input_sql {
                Some(sql) => crate::sql::datafusion::infer_schema(&upstream_schema, sql)
                    .await
                    .map_err(|e| ProduceError::msg(format!("parallel pre_input_sql: {e}")))?,
                None => upstream_schema,
            };

            // Set all branch input schemas and request output schemas
            let schema_bytes = crate::dag_v2::schema::schema_to_bytes(&input_schema)
                .map_err(|e| ProduceError::msg(format!("parallel: ser schema: {e}")))?;

            let mut branch_schemas: Vec<(&str, arrow::datatypes::Schema)> = Vec::new();
            for branch in &self.branches {
                branch.input.set_schema(Chunk::schema_res(schema_bytes.clone()));
                let branch_schema = request_schema(branch.output.as_ref()).await?;
                branch_schemas.push((&branch.name, branch_schema));
            }

            // Build table refs for infer_schema_tables
            let table_refs: Vec<(&str, &arrow::datatypes::Schema)> = branch_schemas
                .iter()
                .map(|(name, schema)| (*name, schema))
                .collect();

            let final_schema = crate::sql::datafusion::infer_schema_tables(&table_refs, &self.merge_sql)
                .await
                .map_err(|e| ProduceError::msg(format!("parallel merge_sql: {e}")))?;

            let out_bytes = crate::dag_v2::schema::schema_to_bytes(&final_schema)
                .map_err(|e| ProduceError::msg(format!("parallel: ser output schema: {e}")))?;
            output.write(Chunk::schema_res(out_bytes));
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
                .ok_or_else(|| ProduceError::msg("parallel: no input transport"))?;
            let input_bytes = request_data(input.as_ref()).await;

            // Apply pre_input_sql
            let input_bytes = match &self.pre_input_sql {
                Some(sql) => crate::sql::datafusion::run_sql_on_ipc(&input_bytes, sql)
                    .await
                    .map_err(|e| ProduceError::msg(format!("parallel pre_input_sql: {e}")))?,
                None => input_bytes,
            };

            // Set all branch inputs
            for branch in &self.branches {
                branch.input.set_data(Chunk::data(input_bytes.clone()));
            }

            // Run all branches concurrently
            let futures: Vec<_> = self.branches.iter().map(|branch| {
                request_data(branch.output.as_ref())
            }).collect();
            let results = futures::future::join_all(futures).await;

            // Collect (name, bytes) pairs for run_sql_on_tables
            let branch_data: Vec<(&str, Vec<u8>)> = self.branches.iter()
                .zip(results.into_iter())
                .map(|(branch, bytes)| (branch.name.as_str(), bytes))
                .collect();

            let table_refs: Vec<(&str, &[u8])> = branch_data.iter()
                .map(|(name, bytes)| (*name, bytes.as_slice()))
                .collect();

            let result_bytes = crate::sql::datafusion::run_sql_on_tables(&table_refs, &self.merge_sql)
                .await
                .map_err(|e| ProduceError::msg(format!("parallel merge_sql: {e}")))?;

            output.write(Chunk::data(result_bytes));
            output.close();
            Ok(())
        })
    }

    fn set_input(&self, transport: Arc<dyn Transport>) {
        *self.input.lock().unwrap() = Some(transport);
    }
}
