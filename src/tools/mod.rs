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
/// Each tool has an OpenAI function definition (JSON schema) and an async executor.
pub trait Tool: Send + Sync {
    /// The tool's name (e.g. "ask_user", "edit_file").
    fn name(&self) -> &str;

    /// Human-readable description of what this tool does.
    fn description(&self) -> &str;

    /// Returns the OpenAI function tool definition for this tool.
    fn definition(&self) -> ToolDefinition;

    /// Execute the tool with the given JSON arguments string.
    /// Returns Ok(result_string) on success.
    /// Returns Ok(error_message) for soft failures the model should retry.
    /// Returns Err(ToolError) for hard failures that stop the workflow.
    fn execute<'a>(
        &'a self,
        arguments: &'a str,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<String, ToolError>> + Send + 'a>>;
}
