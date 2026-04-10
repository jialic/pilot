use super::{ChatMessage, ChatResponse, FunctionCall, LlmError, ToolCall, ToolDefinition};

pub struct AnthropicClient {
    client: reqwest::Client,
    api_key: String,
}

impl AnthropicClient {
    pub fn new(api_key: String) -> Self {
        Self {
            client: reqwest::Client::new(),
            api_key,
        }
    }

    /// Build Anthropic Messages API request body from our types.
    /// Key differences from OpenAI:
    /// - system is top-level, not in messages
    /// - tools use input_schema, not parameters
    /// - tool_choice: {"type": "any"} = required
    /// - tool results are user messages with tool_result content blocks
    /// - max_tokens is required
    fn build_body(
        model: &str,
        messages: &[ChatMessage],
        tools: Option<&[ToolDefinition]>,
        tool_choice: Option<&str>,
    ) -> serde_json::Value {
        let model = model.strip_prefix("anthropic/").unwrap_or(model);

        let mut system_parts: Vec<String> = Vec::new();
        let mut api_messages: Vec<serde_json::Value> = Vec::new();
        let mut pending_images: Vec<serde_json::Value> = Vec::new();

        for msg in messages {
            match msg {
                ChatMessage::System { content, .. } => {
                    system_parts.push(content.clone());
                }
                ChatMessage::User { content, .. } => {
                    if pending_images.is_empty() {
                        api_messages.push(serde_json::json!({
                            "role": "user",
                            "content": content,
                        }));
                    } else {
                        let mut parts = pending_images.drain(..).collect::<Vec<_>>();
                        parts.push(serde_json::json!({"type": "text", "text": content}));
                        api_messages.push(serde_json::json!({"role": "user", "content": parts}));
                    }
                }
                ChatMessage::Image { media_type, data } => {
                    pending_images.push(serde_json::json!({
                        "type": "image",
                        "source": {
                            "type": "base64",
                            "media_type": media_type,
                            "data": data,
                        }
                    }));
                }
                ChatMessage::Assistant { content, tool_calls } => {
                    if let Some(tcs) = tool_calls {
                        let mut content_parts: Vec<serde_json::Value> = Vec::new();
                        if let Some(text) = content {
                            if !text.is_empty() {
                                content_parts.push(serde_json::json!({"type": "text", "text": text}));
                            }
                        }
                        for tc in tcs {
                            let args: serde_json::Value = serde_json::from_str(&tc.function.arguments)
                                .unwrap_or(serde_json::json!({}));
                            content_parts.push(serde_json::json!({
                                "type": "tool_use",
                                "id": tc.id,
                                "name": tc.function.name,
                                "input": args,
                            }));
                        }
                        api_messages.push(serde_json::json!({
                            "role": "assistant",
                            "content": content_parts,
                        }));
                    } else {
                        api_messages.push(serde_json::json!({
                            "role": "assistant",
                            "content": content.clone().unwrap_or_default(),
                        }));
                    }
                }
                ChatMessage::Tool { content, tool_call_id } => {
                    // Anthropic: tool results are user messages with tool_result blocks
                    api_messages.push(serde_json::json!({
                        "role": "user",
                        "content": [{
                            "type": "tool_result",
                            "tool_use_id": tool_call_id,
                            "content": content,
                        }],
                    }));
                }
            }
        }

        if !pending_images.is_empty() {
            api_messages.push(serde_json::json!({"role": "user", "content": pending_images}));
        }

        let mut body = serde_json::json!({
            "model": model,
            "max_tokens": 4096,
            "messages": api_messages,
        });

        if !system_parts.is_empty() {
            body["system"] = serde_json::json!(system_parts.join("\n\n"));
        }

        // Convert tool definitions: OpenAI's "parameters" → Anthropic's "input_schema"
        if let Some(tools) = tools {
            let anthropic_tools: Vec<serde_json::Value> = tools.iter().map(|t| {
                let openai = &t.0;
                serde_json::json!({
                    "name": openai["function"]["name"],
                    "description": openai["function"]["description"],
                    "input_schema": openai["function"]["parameters"],
                })
            }).collect();
            body["tools"] = serde_json::json!(anthropic_tools);
        }

        if let Some(choice) = tool_choice {
            match choice {
                "required" => body["tool_choice"] = serde_json::json!({"type": "any"}),
                "auto" => body["tool_choice"] = serde_json::json!({"type": "auto"}),
                "none" => body["tool_choice"] = serde_json::json!({"type": "none"}),
                _ => body["tool_choice"] = serde_json::json!({"type": choice}),
            }
        }

        body
    }

    pub async fn call(
        &self,
        messages: &[ChatMessage],
        tools: Option<&[ToolDefinition]>,
        tool_choice: Option<&str>,
        model: &str,
    ) -> Result<ChatResponse, LlmError> {
        let body = Self::build_body(model, messages, tools, tool_choice);

        tracing::debug!("Anthropic REQUEST:\n{}", serde_json::to_string_pretty(&body).unwrap_or_default());

        const MAX_RETRIES: u32 = 5;
        let mut retries = 0;

        let (status, text) = loop {
            let response = self.client
                .post("https://api.anthropic.com/v1/messages")
                .header("x-api-key", &self.api_key)
                .header("anthropic-version", "2023-06-01")
                .header("Content-Type", "application/json")
                .json(&body)
                .send()
                .await
                .map_err(|e| LlmError::RequestFailed(e.to_string()))?;

            let status = response.status();

            if matches!(status.as_u16(), 424 | 429 | 500 | 502 | 503 | 529) && retries < MAX_RETRIES {
                retries += 1;
                let retry_after = response.headers()
                    .get("retry-after")
                    .and_then(|v| v.to_str().ok())
                    .and_then(|s| s.parse::<u64>().ok())
                    .unwrap_or(5);
                let jitter = 1 + rand::random::<u64>() % 5;
                let wait = std::time::Duration::from_secs(retry_after + jitter);
                tracing::warn!(
                    "Anthropic {status}, retry {retries}/{MAX_RETRIES} in {wait:?}",
                );
                tokio::time::sleep(wait).await;
                continue;
            }

            let text = response.text().await
                .map_err(|e| LlmError::RequestFailed(e.to_string()))?;

            break (status, text);
        };

        if !status.is_success() {
            return Err(LlmError::RequestFailed(format!("HTTP {status}: {text}")));
        }

        tracing::debug!("Anthropic RESPONSE:\n{text}");
        Self::parse_response(&text)
    }

    fn parse_response(text: &str) -> Result<ChatResponse, LlmError> {
        let resp: serde_json::Value = serde_json::from_str(text)
            .map_err(|e| LlmError::ParseError(format!("{e}\n{text}")))?;

        let content_blocks = resp["content"]
            .as_array()
            .ok_or_else(|| LlmError::ParseError("no content in response".into()))?;

        let mut text_parts: Vec<String> = Vec::new();
        let mut tool_calls: Vec<ToolCall> = Vec::new();

        for block in content_blocks {
            match block["type"].as_str() {
                Some("text") => {
                    if let Some(t) = block["text"].as_str() {
                        text_parts.push(t.to_string());
                    }
                }
                Some("tool_use") => {
                    tool_calls.push(ToolCall {
                        id: block["id"].as_str().unwrap_or("").to_string(),
                        call_type: "function".to_string(),
                        function: FunctionCall {
                            name: block["name"].as_str().unwrap_or("").to_string(),
                            arguments: serde_json::to_string(&block["input"]).unwrap_or_default(),
                        },
                    });
                }
                _ => {}
            }
        }

        let content = if text_parts.is_empty() { None } else { Some(text_parts.join("")) };
        let tool_calls = if tool_calls.is_empty() { None } else { Some(tool_calls) };

        Ok(ChatResponse { content, tool_calls })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn strips_model_prefix() {
        let messages = vec![ChatMessage::User { content: "hi".into(), name: None }];
        let body = AnthropicClient::build_body("anthropic/claude-haiku-4-5", &messages, None, None);
        assert_eq!(body["model"], "claude-haiku-4-5");
    }

    #[test]
    fn system_is_top_level() {
        let messages = vec![
            ChatMessage::System { content: "be helpful".into(), name: None },
            ChatMessage::User { content: "hi".into(), name: None },
        ];
        let body = AnthropicClient::build_body("claude-haiku-4-5", &messages, None, None);
        assert_eq!(body["system"], "be helpful");
        let msgs = body["messages"].as_array().unwrap();
        assert_eq!(msgs.len(), 1);
        assert_eq!(msgs[0]["role"], "user");
    }

    #[test]
    fn tools_use_input_schema() {
        let messages = vec![ChatMessage::User { content: "hi".into(), name: None }];
        let tools = vec![ToolDefinition::new(serde_json::json!({
            "type": "function",
            "function": {
                "name": "test",
                "description": "a test",
                "parameters": {"type": "object", "properties": {}}
            }
        }))];
        let body = AnthropicClient::build_body("claude-haiku-4-5", &messages, Some(&tools), None);
        let api_tools = body["tools"].as_array().unwrap();
        assert_eq!(api_tools[0]["name"], "test");
        assert!(api_tools[0].get("input_schema").is_some());
        assert!(api_tools[0].get("parameters").is_none());
    }

    #[test]
    fn tool_choice_required_becomes_any() {
        let messages = vec![ChatMessage::User { content: "hi".into(), name: None }];
        let body = AnthropicClient::build_body("claude-haiku-4-5", &messages, None, Some("required"));
        assert_eq!(body["tool_choice"]["type"], "any");
    }

    #[test]
    fn tool_result_is_user_message() {
        let messages = vec![
            ChatMessage::Tool { content: "result".into(), tool_call_id: "tc_1".into() },
        ];
        let body = AnthropicClient::build_body("claude-haiku-4-5", &messages, None, None);
        let msgs = body["messages"].as_array().unwrap();
        assert_eq!(msgs[0]["role"], "user");
        assert_eq!(msgs[0]["content"][0]["type"], "tool_result");
        assert_eq!(msgs[0]["content"][0]["tool_use_id"], "tc_1");
    }

    #[test]
    fn parse_text_response() {
        let text = r#"{"content":[{"type":"text","text":"Hello!"}],"model":"claude-haiku-4-5","stop_reason":"end_turn"}"#;
        let resp = AnthropicClient::parse_response(text).unwrap();
        assert_eq!(resp.content.unwrap(), "Hello!");
        assert!(resp.tool_calls.is_none());
    }

    #[test]
    fn parse_tool_use_response() {
        let text = r#"{"content":[{"type":"tool_use","id":"tc_1","name":"shell","input":{"command":"ls"}}],"model":"claude-haiku-4-5","stop_reason":"tool_use"}"#;
        let resp = AnthropicClient::parse_response(text).unwrap();
        assert!(resp.content.is_none());
        let tcs = resp.tool_calls.unwrap();
        assert_eq!(tcs[0].function.name, "shell");
        assert!(tcs[0].function.arguments.contains("ls"));
    }

    #[test]
    fn max_tokens_always_set() {
        let messages = vec![ChatMessage::User { content: "hi".into(), name: None }];
        let body = AnthropicClient::build_body("claude-haiku-4-5", &messages, None, None);
        assert_eq!(body["max_tokens"], 4096);
    }
}
