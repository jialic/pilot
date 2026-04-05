use crate::llm::ToolDefinition;
use serde_json::json;

use super::{Tool, ToolError};

/// ShellTool runs a command via direct exec (no sh -c).
/// When `allowed` patterns are set, validates the command against them.
pub struct ShellTool {
    allowed: Vec<regex::Regex>,
}

impl ShellTool {
    /// Create with no restrictions (empty allowed = nothing allowed).
    pub fn new(allowed_patterns: Vec<String>) -> Result<Self, String> {
        let mut regexes = Vec::new();
        for pattern in &allowed_patterns {
            let re = regex::Regex::new(pattern)
                .map_err(|e| format!("invalid shell allowed pattern '{pattern}': {e}"))?;
            regexes.push(re);
        }
        Ok(Self { allowed: regexes })
    }

    fn is_allowed(&self, command: &str) -> bool {
        self.allowed.iter().any(|re| re.is_match(command))
    }
}

impl Tool for ShellTool {
    fn name(&self) -> &str {
        "shell"
    }
    fn description(&self) -> &str {
        "Run a command and return its output"
    }

    fn definition(&self) -> ToolDefinition {
        ToolDefinition::new(json!({
            "type": "function",
            "function": {
                "name": "shell",
                "description": "Run a command. Returns stdout and stderr combined. Use this to inspect files, run tests, check system state, or execute any allowed command.",
                "parameters": {
                    "type": "object",
                    "properties": {
                        "command": {
                            "type": "string",
                            "description": "The command to execute"
                        }
                    },
                    "required": ["command"]
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

            let command = args["command"]
                .as_str()
                .ok_or_else(|| ToolError::ExecutionFailed("missing 'command' field".into()))?;

            // Validate against allowed patterns
            if !self.is_allowed(command) {
                return Err(ToolError::ExecutionFailed(
                    format!("command not allowed: {command}")
                ));
            }

            let tokens = shlex::split(command)
                .ok_or_else(|| ToolError::ExecutionFailed(format!("invalid command syntax: {command}")))?;
            if tokens.is_empty() {
                return Ok("(no command)".into());
            }

            let output = tokio::process::Command::new(&tokens[0])
                .args(&tokens[1..])
                .output()
                .await
                .map_err(|e| {
                    ToolError::ExecutionFailed(format!("failed to execute command: {e}"))
                })?;

            let stdout = String::from_utf8_lossy(&output.stdout);
            let stderr = String::from_utf8_lossy(&output.stderr);

            let mut result = String::new();
            if !stdout.is_empty() {
                result.push_str(&stdout);
            }
            if !stderr.is_empty() {
                if !result.is_empty() {
                    result.push('\n');
                }
                result.push_str("[stderr] ");
                result.push_str(&stderr);
            }

            if !output.status.success() {
                result.push_str(&format!(
                    "\n[exit code: {}]",
                    output.status.code().unwrap_or(-1)
                ));
            }

            if result.is_empty() {
                result.push_str("(no output)");
            }

            Ok(result)
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn allowed_matches_simple_command() {
        let tool = ShellTool::new(vec!["^echo ".into()]).unwrap();
        assert!(tool.is_allowed("echo hello"));
        assert!(!tool.is_allowed("rm -rf /"));
    }

    #[test]
    fn allowed_matches_subcommand() {
        let tool = ShellTool::new(vec!["^git (status|diff|log)".into()]).unwrap();
        assert!(tool.is_allowed("git status --short"));
        assert!(tool.is_allowed("git diff HEAD"));
        assert!(tool.is_allowed("git log --oneline"));
        assert!(!tool.is_allowed("git push origin main"));
        assert!(!tool.is_allowed("git reset --hard"));
    }

    #[test]
    fn allowed_multiple_patterns() {
        let tool = ShellTool::new(vec![
            "^curl ".into(),
            "^jq ".into(),
        ]).unwrap();
        assert!(tool.is_allowed("curl -s https://api.com"));
        assert!(tool.is_allowed("jq '.name' file.json"));
        assert!(!tool.is_allowed("rm file"));
    }

    #[test]
    fn empty_allowed_rejects_everything() {
        let tool = ShellTool::new(vec![]).unwrap();
        assert!(!tool.is_allowed("echo hello"));
        assert!(!tool.is_allowed("ls"));
    }

    #[test]
    fn invalid_regex_returns_error() {
        let result = ShellTool::new(vec!["[invalid".into()]);
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn execute_allowed_command() {
        let tool = ShellTool::new(vec!["^echo ".into()]).unwrap();

        let result = tool.execute(r#"{"command": "echo hello"}"#).await;
        assert!(result.is_ok());
        assert!(result.unwrap().contains("hello"));
    }

    #[tokio::test]
    async fn execute_rejected_command() {
        let tool = ShellTool::new(vec!["^echo ".into()]).unwrap();

        let result = tool.execute(r#"{"command": "rm -rf /"}"#).await;
        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(err.contains("not allowed"), "error should mention not allowed: {err}");
    }
}
