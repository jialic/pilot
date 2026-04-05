use crate::llm::ToolDefinition;
use serde_json::json;

use super::{Tool, ToolError};
use crate::workflow;

/// Validates YAML content as a pilot workflow.
/// Parses via serde_yaml and runs the same validation as load_workflow.
/// Returns "valid" or the specific error message — does NOT write anything.
pub struct ValidateYamlTool;

impl ValidateYamlTool {
    pub fn new() -> Self {
        Self
    }
}

impl Tool for ValidateYamlTool {
    fn name(&self) -> &str {
        "validate_yaml"
    }
    fn description(&self) -> &str {
        "Validate YAML content as a pilot workflow without writing it"
    }

    fn definition(&self) -> ToolDefinition {
        ToolDefinition::new(json!({
            "type": "function",
            "function": {
                "name": "validate_yaml",
                "description": "Validate YAML content as a pilot workflow without writing it. Returns 'valid' or a specific error message.",
                "parameters": {
                    "type": "object",
                    "properties": {
                        "content": {
                            "type": "string",
                            "description": "The YAML content to validate"
                        }
                    },
                    "required": ["content"]
                }
            }
        }))
    }

    fn execute<'a>(
        &'a self,
        arguments: &'a str,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<String, ToolError>> + Send + 'a>>
    {
        Box::pin(async move {
            let args: serde_json::Value = serde_json::from_str(arguments)
                .map_err(|e| ToolError::ExecutionFailed(format!("invalid arguments: {e}")))?;

            let content = args["content"]
                .as_str()
                .ok_or_else(|| ToolError::ExecutionFailed("missing 'content' field".into()))?;

            match workflow::parse_and_validate(content) {
                Ok(_) => Ok("valid".to_string()),
                Err(e) => Ok(format!("invalid: {e}")),
            }
        })
    }
}
