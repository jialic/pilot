use crate::llm::ToolDefinition;
use serde_json::json;

use super::{Tool, ToolError};

/// AbortTool is called by the model when the user rejects a proposal.
/// Returns ToolError::WorkflowAborted to stop the workflow.
pub struct AbortTool;

impl Tool for AbortTool {
    fn name(&self) -> &str {
        "abort"
    }
    fn description(&self) -> &str {
        "Abort the workflow"
    }

    fn definition(&self) -> ToolDefinition {
        ToolDefinition::new(json!({
            "type": "function",
            "function": {
                "name": "abort",
                "description": "Call this if the user rejects or declines to proceed.",
                "parameters": {
                    "type": "object",
                    "properties": {}
                }
            }
        }))
    }

    fn execute<'a>(
        &'a self,
        _arguments: &'a str,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<String, ToolError>> + Send + 'a>>
    {
        Box::pin(async move { Err(ToolError::WorkflowAborted) })
    }
}
