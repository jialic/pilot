use std::collections::HashMap;

use crate::llm::ToolDefinition;
use serde_json::json;

use super::{Tool, ToolError};

/// SubmitFieldsTool validates and returns collected field values.
/// Fields with defaults are auto-filled if omitted by the model.
pub struct SubmitFieldsTool {
    field_names: Vec<String>,
    defaults: HashMap<String, String>,
}

impl SubmitFieldsTool {
    pub fn new(field_names: Vec<String>, defaults: HashMap<String, String>) -> Self {
        Self {
            field_names,
            defaults,
        }
    }
}

impl Tool for SubmitFieldsTool {
    fn name(&self) -> &str {
        "submit_fields"
    }
    fn description(&self) -> &str {
        "Submit collected field values"
    }

    fn definitions(&self) -> Vec<ToolDefinition> {
        let mut properties = serde_json::Map::new();
        for name in &self.field_names {
            properties.insert(
                name.clone(),
                json!({
                    "type": "string",
                    "description": format!("Value for {}", name)
                }),
            );
        }

        // Fields without defaults are required
        let required: Vec<&str> = self
            .field_names
            .iter()
            .filter(|name| !self.defaults.contains_key(name.as_str()))
            .map(|s| s.as_str())
            .collect();

        vec![ToolDefinition::new(json!({
            "type": "function",
            "function": {
                "name": "submit_fields",
                "description": "Submit the collected field values. Call this when all required information has been gathered from the user.",
                "parameters": {
                    "type": "object",
                    "properties": properties,
                    "required": required
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
            let mut values: HashMap<String, String> =
                serde_json::from_str(arguments).map_err(|e| {
                    ToolError::ExecutionFailed(format!("invalid submit_fields arguments: {e}"))
                })?;

            // Apply defaults for missing fields
            for (name, default) in &self.defaults {
                values
                    .entry(name.clone())
                    .or_insert_with(|| default.clone());
            }

            // Validate all fields present
            let missing: Vec<&str> = self
                .field_names
                .iter()
                .filter(|name| values.get(name.as_str()).is_none_or(|v| v.is_empty()))
                .map(|s| s.as_str())
                .collect();

            if !missing.is_empty() {
                return Ok(format!(
                    "missing required fields: {}. Call ask_user to get them.",
                    missing.join(", ")
                ));
            }

            // Return as structured JSON
            let result = serde_json::to_string(&values).map_err(|e| {
                ToolError::ExecutionFailed(format!("failed to serialize fields: {e}"))
            })?;
            Ok(result)
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn applies_defaults() {
        let tool = SubmitFieldsTool::new(
            vec!["host".into(), "port".into(), "user".into()],
            HashMap::from([("port".into(), "22".into()), ("user".into(), "root".into())]),
        );

        let result = tool.execute("submit_fields", r#"{"host": "192.168.1.1"}"#).await.unwrap();
        let values: HashMap<String, String> = serde_json::from_str(&result).unwrap();
        assert_eq!(values["host"], "192.168.1.1");
        assert_eq!(values["port"], "22");
        assert_eq!(values["user"], "root");
    }

    #[tokio::test]
    async fn rejects_missing_required() {
        let tool = SubmitFieldsTool::new(
            vec!["host".into(), "port".into()],
            HashMap::from([("port".into(), "22".into())]),
        );

        let result = tool.execute("submit_fields", r#"{}"#).await.unwrap();
        assert!(result.contains("missing required fields: host"));
    }
}
