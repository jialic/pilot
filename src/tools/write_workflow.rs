use std::path::PathBuf;

use crate::llm::ToolDefinition;
use serde_json::json;

use super::{Tool, ToolError};

/// Writes YAML content to the workflow file.
/// The file path is fixed at construction — the LLM only provides content.
pub struct WriteWorkflowTool {
    path: PathBuf,
}

impl WriteWorkflowTool {
    pub fn new(path: PathBuf) -> Self {
        Self { path }
    }
}

impl Tool for WriteWorkflowTool {
    fn name(&self) -> &str {
        "write_workflow"
    }
    fn description(&self) -> &str {
        "Write workflow YAML content to the file"
    }

    fn definitions(&self) -> Vec<ToolDefinition> {
        vec![ToolDefinition::new(json!({
            "type": "function",
            "function": {
                "name": "write_workflow",
                "description": "Write the workflow YAML content to the file. The file path is already set — just provide the YAML content.",
                "parameters": {
                    "type": "object",
                    "properties": {
                        "content": {
                            "type": "string",
                            "description": "The full YAML content to write to the workflow file"
                        }
                    },
                    "required": ["content"]
                }
            }
        }))]
    }

    fn execute<'a>(
        &'a self,
        _name: &'a str,
        arguments: &'a str,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<String, ToolError>> + Send + 'a>>
    {
        Box::pin(async move {
            let args: serde_json::Value = serde_json::from_str(arguments)
                .map_err(|e| ToolError::ExecutionFailed(format!("invalid arguments: {e}")))?;

            let content = args["content"]
                .as_str()
                .ok_or_else(|| ToolError::ExecutionFailed("missing 'content' field".into()))?;

            // Validate before writing
            if let Err(e) = crate::workflow::parse_and_validate(content) {
                return Err(ToolError::ExecutionFailed(format!("invalid workflow YAML: {e}")));
            }

            std::fs::write(&self.path, content)
                .map_err(|e| ToolError::ExecutionFailed(format!("failed to write file: {e}")))?;

            Ok(format!("Written to {}", self.path.display()))
        })
    }
}
