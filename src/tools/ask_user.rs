use crate::llm::ToolDefinition;
use serde_json::json;

use super::{Tool, ToolError};

/// AskUserTool provides the function definition for asking the user a question.
/// The executor is intentionally unimplemented — nodes wrap this tool with their
/// own callback (TUI, API, etc.) to handle user interaction.
pub struct AskUserTool;

impl Tool for AskUserTool {
    fn name(&self) -> &str {
        "ask_user"
    }
    fn description(&self) -> &str {
        "Ask the user a question and wait for their response"
    }

    fn definition(&self) -> ToolDefinition {
        ToolDefinition::new(json!({
            "type": "function",
            "function": {
                "name": "ask_user",
                "description": "Ask the user a question and wait for their response",
                "parameters": {
                    "type": "object",
                    "properties": {
                        "question": {
                            "type": "string",
                            "description": "The question to ask the user"
                        }
                    },
                    "required": ["question"]
                }
            }
        }))
    }

    fn execute<'a>(
        &'a self,
        _arguments: &'a str,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<String, ToolError>> + Send + 'a>>
    {
        Box::pin(async move {
            Err(ToolError::ExecutionFailed(
                "ask_user must be wrapped by the node with a user interaction callback".into(),
            ))
        })
    }
}
