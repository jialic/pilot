#[cfg(test)]
mod tests;

use std::sync::Arc;

use crate::dag_v2::{BufferTransport, ProduceError, request_data};

use super::ConstProducer;
use super::pipeline::Pipeline;

/// Run a v2 pipeline with the given initial input string.
/// Returns the raw output bytes (Arrow IPC).
///
/// The first node receives |output: Utf8| with the input string as its value.
/// Most first nodes ignore this (shell, read_input, transform with constants).
/// If a loop is the first node, use pre_input_sql to reshape into the desired
/// locked schema (e.g. `SELECT ... FROM input WHERE false` for an empty table).
pub async fn run(pipeline: &Pipeline, input: &str) -> Result<Vec<u8>, ProduceError> {
    let initial = Arc::new(ConstProducer::output(input));
    let head_transport = BufferTransport::new();
    head_transport.set_source(initial);
    pipeline.head().set_input(head_transport);

    // Request data from tail
    let tail_transport = BufferTransport::new();
    tail_transport.set_source(pipeline.tail().clone());
    let output = request_data(tail_transport.as_ref()).await;

    Ok(output)
}
