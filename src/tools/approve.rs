use crate::llm::ToolDefinition;
use serde_json::json;

use super::{Tool, ToolError};

/// ApproveTool is called by the model when the user approves a proposal.
pub struct ApproveTool;

impl Tool for ApproveTool {
    fn name(&self) -> &str {
        "approve"
    }
    fn description(&self) -> &str {
        "Approve the current step and proceed"
    }

    fn definitions(&self) -> Vec<ToolDefinition> {
        vec![ToolDefinition::new(json!({
            "type": "function",
            "function": {
                "name": "approve",
                "description": "Call this if the user approves or accepts the proposal.",
                "parameters": {
                    "type": "object",
                    "properties": {}
                }
            }
        }))]
    }

    fn execute<'a>(
        &'a self,
        _name: &'a str,
        _arguments: &'a str,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<String, ToolError>> + Send + 'a>>
    {
        Box::pin(async move { Ok("approved".into()) })
    }
}
