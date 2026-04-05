#[cfg(test)]
mod tests;

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

use arrow::array::RecordBatch;
use arrow::datatypes::Schema;

use crate::dag_v2::{
    BufferTransport, Chunk, ProduceError, Produces, Transport,
    request_schema, request_data,
};
use crate::producers_v2::as_bool;
use crate::producers_v2::MutableProducer;
use crate::producers_v2::pipeline::Pipeline;
use crate::workflow::SchemaField;

#[derive(Clone, Debug)]
pub enum MatchMode {
    First,
    All,
}

struct SelectBranchWiring {
    #[allow(dead_code)]
    name: String,
    cond_input: Arc<MutableProducer>,
    cond_output: Arc<BufferTransport>,
    body_input: Arc<MutableProducer>,
    body_output: Arc<BufferTransport>,
}

/// Conditional branching. Evaluates conditions on each branch, runs matching bodies.
///
/// - `match_mode=First`: sequential conditions, stop at first true, run that body
/// - `match_mode=All`: evaluate all conditions (concurrently), run all matching bodies
/// - `output_schema` (optional): locked schema. Defaults to |name, output|.
/// - Static + runtime validation: all branch body outputs must match locked schema.
pub struct SelectProducer {
    match_mode: MatchMode,
    pre_input_sql: Option<String>,
    post_output_sql: Option<String>,
    prepare_input_for_when: Option<String>,
    prepare_input_for_body: Option<String>,
    locked_schema: Schema,
    branches: Vec<SelectBranchWiring>,
    input: std::sync::Mutex<Option<Arc<dyn Transport>>>,
}

impl SelectProducer {
    pub fn new(
        match_mode: MatchMode,
        pre_input_sql: Option<String>,
        post_output_sql: Option<String>,
        prepare_input_for_when: Option<String>,
        prepare_input_for_body: Option<String>,
        output_schema: Vec<SchemaField>,
        branch_defs: Vec<(String, Pipeline, Pipeline)>,
    ) -> Self {
        let locked_schema = crate::producers_v2::locked_schema_from(&output_schema);

        let branches: Vec<SelectBranchWiring> = branch_defs
            .into_iter()
            .map(|(name, cond_pipeline, body_pipeline)| {
                // Wire condition: cond_input → transport → cond.head, cond.tail → cond_output
                let cond_input = Arc::new(MutableProducer::new());
                let cond_head_transport = BufferTransport::new();
                cond_head_transport.set_source(cond_input.clone());
                cond_pipeline.head().set_input(cond_head_transport);
                let cond_output = BufferTransport::new();
                cond_output.set_source(cond_pipeline.tail().clone());

                // Wire body: body_input → transport → body.head, body.tail → body_output
                let body_input = Arc::new(MutableProducer::new());
                let body_head_transport = BufferTransport::new();
                body_head_transport.set_source(body_input.clone());
                body_pipeline.head().set_input(body_head_transport);
                let body_output = BufferTransport::new();
                body_output.set_source(body_pipeline.tail().clone());

                SelectBranchWiring {
                    name,
                    cond_input,
                    cond_output,
                    body_input,
                    body_output,
                }
            })
            .collect();

        Self {
            match_mode,
            pre_input_sql,
            post_output_sql,
            prepare_input_for_when,
            prepare_input_for_body,
            locked_schema,
            branches,
            input: std::sync::Mutex::new(None),
        }
    }
}

/// Evaluate condition output as boolean: extract `output` column, interpret via as_bool.
async fn eval_condition(cond_bytes: &[u8]) -> Result<bool, ProduceError> {
    let result_bytes = crate::sql::datafusion::run_sql_on_ipc(
        cond_bytes,
        "SELECT CAST(output AS VARCHAR) AS output FROM input",
    )
    .await
    .map_err(|e| ProduceError::msg(format!("eval condition: {e}")))?;

    let batches = crate::sql::ipc_to_batches(&result_bytes)
        .map_err(|e| ProduceError::msg(format!("eval condition ipc: {e}")))?;

    let text = if let Some(batch) = batches.first() {
        if batch.num_rows() > 0 {
            crate::sql::datafusion::arrow_string_value(batch.column(0).as_ref(), 0)
                .unwrap_or_default()
        } else {
            String::new()
        }
    } else {
        String::new()
    };
    Ok(as_bool(&text))
}

impl Produces for SelectProducer {
    fn handle_schema<'a>(
        &'a self,
        output: Arc<dyn Transport>,
    ) -> Pin<Box<dyn Future<Output = Result<(), ProduceError>> + Send + 'a>> {
        Box::pin(async move {
            let input = self.input.lock().unwrap().clone()
                .ok_or_else(|| ProduceError::msg("select: no input transport"))?;
            let upstream_schema = request_schema(input.as_ref()).await?;

            let input_schema = match &self.pre_input_sql {
                Some(sql) => crate::sql::datafusion::infer_schema(&upstream_schema, sql)
                    .await
                    .map_err(|e| ProduceError::msg(format!("select pre_input_sql: {e}")))?,
                None => upstream_schema,
            };

            let schema_bytes = crate::dag_v2::schema::schema_to_bytes(&input_schema)
                .map_err(|e| ProduceError::msg(format!("select: ser schema: {e}")))?;

            // Compute condition input schema
            let when_schema = match &self.prepare_input_for_when {
                Some(sql) => {
                    let s = crate::sql::datafusion::infer_schema(&input_schema, sql)
                        .await
                        .map_err(|e| ProduceError::msg(format!("select prepare_input_for_when: {e}")))?;
                    crate::dag_v2::schema::schema_to_bytes(&s)
                        .map_err(|e| ProduceError::msg(format!("select: ser when schema: {e}")))?
                }
                None => schema_bytes.clone(),
            };

            // Compute body input schema
            let body_schema_bytes = match &self.prepare_input_for_body {
                Some(sql) => {
                    let s = crate::sql::datafusion::infer_schema(&input_schema, sql)
                        .await
                        .map_err(|e| ProduceError::msg(format!("select prepare_input_for_body: {e}")))?;
                    crate::dag_v2::schema::schema_to_bytes(&s)
                        .map_err(|e| ProduceError::msg(format!("select: ser body schema: {e}")))?
                }
                None => schema_bytes.clone(),
            };

            // Validate each branch
            for (i, branch) in self.branches.iter().enumerate() {
                // Condition schema (don't validate output — just ensure it resolves)
                branch.cond_input.set_schema(Chunk::schema_res(when_schema.clone()));
                let _cond_schema = request_schema(branch.cond_output.as_ref()).await?;

                // Body schema — validate against locked schema
                branch.body_input.set_schema(Chunk::schema_res(body_schema_bytes.clone()));
                let body_out_schema = request_schema(branch.body_output.as_ref()).await?;
                crate::producers_v2::validate_schemas_match(
                    &self.locked_schema,
                    &body_out_schema,
                    &format!("select: branch {i} body"),
                )?;
            }

            // Output schema = locked schema (+ post_output_sql)
            let core = self.locked_schema.clone();
            let final_schema = match &self.post_output_sql {
                Some(sql) => crate::sql::datafusion::infer_schema(&core, sql)
                    .await
                    .map_err(|e| ProduceError::msg(format!("select post_output_sql: {e}")))?,
                None => core,
            };

            let out_bytes = crate::dag_v2::schema::schema_to_bytes(&final_schema)
                .map_err(|e| ProduceError::msg(format!("select: ser output schema: {e}")))?;
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
                .ok_or_else(|| ProduceError::msg("select: no input transport"))?;
            let input_bytes = request_data(input.as_ref()).await;

            // Apply pre_input_sql
            let input_bytes = match &self.pre_input_sql {
                Some(sql) => crate::sql::datafusion::run_sql_on_ipc(&input_bytes, sql)
                    .await
                    .map_err(|e| ProduceError::msg(format!("select pre_input_sql: {e}")))?,
                None => input_bytes,
            };

            // Compute when_input and body_input
            let when_input = match &self.prepare_input_for_when {
                Some(sql) => crate::sql::datafusion::run_sql_on_ipc(&input_bytes, sql)
                    .await
                    .map_err(|e| ProduceError::msg(format!("select prepare_input_for_when: {e}")))?,
                None => input_bytes.clone(),
            };

            let body_input_data = match &self.prepare_input_for_body {
                Some(sql) => crate::sql::datafusion::run_sql_on_ipc(&input_bytes, sql)
                    .await
                    .map_err(|e| ProduceError::msg(format!("select prepare_input_for_body: {e}")))?,
                None => input_bytes.clone(),
            };

            // Evaluate conditions → find matching branch indices
            let matching: Vec<usize> = match self.match_mode {
                MatchMode::First => {
                    let mut result = Vec::new();
                    for (i, branch) in self.branches.iter().enumerate() {
                        branch.cond_input.set_data(Chunk::data(when_input.clone()));
                        let cond_bytes = request_data(branch.cond_output.as_ref()).await;
                        if eval_condition(&cond_bytes).await? {
                            result.push(i);
                            break;
                        }
                    }
                    result
                }
                MatchMode::All => {
                    // Set all condition inputs first
                    for branch in &self.branches {
                        branch.cond_input.set_data(Chunk::data(when_input.clone()));
                    }
                    // Evaluate all concurrently
                    let futures: Vec<_> = self.branches.iter().map(|branch| {
                        request_data(branch.cond_output.as_ref())
                    }).collect();
                    let cond_results = futures::future::join_all(futures).await;

                    let mut result = Vec::new();
                    for (i, cond_bytes) in cond_results.into_iter().enumerate() {
                        if eval_condition(&cond_bytes).await? {
                            result.push(i);
                        }
                    }
                    result
                }
            };

            // Run matching bodies
            let mut all_batches: Vec<RecordBatch> = Vec::new();

            if !matching.is_empty() {
                // Set body inputs for matching branches
                for &idx in &matching {
                    self.branches[idx].body_input.set_data(Chunk::data(body_input_data.clone()));
                }

                // Run matching bodies concurrently
                let futures: Vec<_> = matching.iter().map(|&idx| {
                    request_data(self.branches[idx].body_output.as_ref())
                }).collect();
                let body_results = futures::future::join_all(futures).await;

                for (j, body_bytes) in body_results.into_iter().enumerate() {
                    let batches = crate::sql::ipc_to_batches(&body_bytes)
                        .map_err(|e| ProduceError::msg(format!(
                            "select: deser branch {} body: {e}", matching[j]
                        )))?;

                    // Runtime validation
                    if let Some(b) = batches.first() {
                        crate::producers_v2::validate_schemas_match(
                            &self.locked_schema, &b.schema(),
                            &format!("select: runtime branch {}", matching[j]),
                        )?;
                    }

                    all_batches.extend(batches);
                }
            }

            // Concatenate results
            let result_bytes = if all_batches.is_empty() {
                let empty = RecordBatch::new_empty(Arc::new(self.locked_schema.clone()));
                crate::sql::batches_to_ipc(&Arc::new(self.locked_schema.clone()), &[empty])
                    .map_err(|e| ProduceError::msg(format!("select: ser empty: {e}")))?
            } else {
                crate::sql::batches_to_ipc(&Arc::new(self.locked_schema.clone()), &all_batches)
                    .map_err(|e| ProduceError::msg(format!("select: ser result: {e}")))?
            };

            // Apply post_output_sql
            let result_bytes = match &self.post_output_sql {
                Some(sql) => crate::sql::datafusion::run_sql_on_ipc(&result_bytes, sql)
                    .await
                    .map_err(|e| ProduceError::msg(format!("select post_output_sql: {e}")))?,
                None => result_bytes,
            };

            output.write(Chunk::data(result_bytes));
            output.close();
            Ok(())
        })
    }

    fn set_input(&self, transport: Arc<dyn Transport>) {
        *self.input.lock().unwrap() = Some(transport);
    }
}
