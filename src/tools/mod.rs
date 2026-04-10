pub mod abort;
pub mod approve;
pub mod ask_user;
pub mod dispatcher;
pub mod file;
pub mod http;
pub mod input;
pub mod migrate;
pub mod s3;
pub mod shell;
pub mod submit_fields;
pub mod validate_yaml;
pub mod write_workflow;

use crate::llm::ToolDefinition;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum ToolError {
    #[error("tool execution failed: {0}")]
    ExecutionFailed(String),

    #[error("workflow aborted by user")]
    WorkflowAborted,
}

/// Tool defines a capability the LLM can call.
/// Each tool has one or more OpenAI function definitions (JSON schema) and an async executor.
/// Tools like file/s3 return multiple flat definitions (file_read, file_write, etc.)
/// so LLMs don't struggle with nested schemas.
pub trait Tool: Send + Sync {
    /// The tool's name (e.g. "ask_user", "file").
    fn name(&self) -> &str;

    /// Human-readable description of what this tool does.
    fn description(&self) -> &str;

    /// Returns tool definitions for LLM consumption. Most tools return one.
    /// Complex tools (file, s3) return multiple flat definitions.
    fn definitions(&self) -> Vec<ToolDefinition>;

    /// Returns the combined tool definition for CLI display.
    /// Default: first from definitions(). File/S3 override with the full schema.
    fn cli_definition(&self) -> ToolDefinition {
        self.definitions().into_iter().next().unwrap()
    }

    /// Execute the tool. `name` is the LLM-facing tool name (e.g. "file_write").
    /// Returns Ok(result_string) on success.
    /// Returns Ok(error_message) for soft failures the model should retry.
    /// Returns Err(ToolError) for hard failures that stop the workflow.
    fn execute<'a>(
        &'a self,
        name: &'a str,
        arguments: &'a str,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<String, ToolError>> + Send + 'a>>;
}
