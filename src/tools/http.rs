use crate::llm::ToolDefinition;
use serde_json::json;

use super::{Tool, ToolError};

/// HttpTool makes HTTP requests. LLM provides URL, method, headers, body.
pub struct HttpTool;

impl Tool for HttpTool {
    fn name(&self) -> &str {
        "http"
    }

    fn description(&self) -> &str {
        "Make an HTTP request and return the response body"
    }

    fn definition(&self) -> ToolDefinition {
        ToolDefinition::new(json!({
            "type": "function",
            "function": {
                "name": "http",
                "description": "Make an HTTP request. Returns the response body. Non-2xx status returns an error with the status code and body.",
                "parameters": {
                    "type": "object",
                    "properties": {
                        "url": {
                            "type": "string",
                            "description": "Full URL to request"
                        },
                        "method": {
                            "type": "string",
                            "enum": ["GET", "POST", "PUT", "DELETE", "PATCH"],
                            "description": "HTTP method"
                        },
                        "headers": {
                            "type": "object",
                            "description": "HTTP headers as key-value pairs",
                            "additionalProperties": { "type": "string" }
                        },
                        "body": {
                            "type": "string",
                            "description": "Request body (typically JSON string)"
                        }
                    },
                    "required": ["url", "method"]
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

            let url = args["url"]
                .as_str()
                .ok_or_else(|| ToolError::ExecutionFailed("missing 'url' field".into()))?;

            let method = args["method"]
                .as_str()
                .ok_or_else(|| ToolError::ExecutionFailed("missing 'method' field".into()))?;

            let client = reqwest::Client::new();
            let mut request = match method {
                "GET" => client.get(url),
                "POST" => client.post(url),
                "PUT" => client.put(url),
                "DELETE" => client.delete(url),
                "PATCH" => client.patch(url),
                _ => return Err(ToolError::ExecutionFailed(format!("unsupported method: {method}"))),
            };

            // Add headers
            if let Some(headers) = args["headers"].as_object() {
                for (key, value) in headers {
                    if let Some(val) = value.as_str() {
                        request = request.header(key.as_str(), val);
                    }
                }
            }

            // Add body
            if let Some(body) = args["body"].as_str() {
                request = request
                    .header("Content-Type", "application/json")
                    .body(body.to_string());
            }

            let response = request
                .send()
                .await
                .map_err(|e| ToolError::ExecutionFailed(format!("request failed: {e}")))?;

            let status = response.status();
            let body = response
                .text()
                .await
                .map_err(|e| ToolError::ExecutionFailed(format!("read response: {e}")))?;

            if !status.is_success() {
                return Ok(format!("[HTTP {status}] {body}"));
            }

            Ok(body)
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn tool_definition_valid() {
        let tool = HttpTool;
        let def = tool.definition();
        assert_eq!(def.0["function"]["name"], "http");
    }

    #[tokio::test]
    async fn missing_url_errors() {
        let tool = HttpTool;

        let result = tool.execute(r#"{"method": "GET"}"#).await;
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("url"));
    }

    #[tokio::test]
    async fn missing_method_errors() {
        let tool = HttpTool;

        let result = tool.execute(r#"{"url": "https://example.com"}"#).await;
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("method"));
    }

    #[tokio::test]
    async fn unsupported_method_errors() {
        let tool = HttpTool;

        let result = tool.execute(r#"{"url": "https://example.com", "method": "TRACE"}"#).await;
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("unsupported"));
    }
}
