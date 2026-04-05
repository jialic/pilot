use super::{ChatMessage, ChatResponse, FunctionCall, LlmError, ToolCall, ToolDefinition};

pub struct GeminiClient {
    client: reqwest::Client,
    api_key: String,
}

impl GeminiClient {
    pub fn new(api_key: String) -> Self {
        Self {
            client: reqwest::Client::new(),
            api_key,
        }
    }

    fn build_body(
        messages: &[ChatMessage],
        tools: Option<&[ToolDefinition]>,
        tool_choice: Option<&str>,
    ) -> serde_json::Value {
        let mut system_parts: Vec<serde_json::Value> = Vec::new();
        let mut contents: Vec<serde_json::Value> = Vec::new();
        let mut pending_images: Vec<serde_json::Value> = Vec::new();

        for msg in messages {
            match msg {
                ChatMessage::System { content, .. } => {
                    system_parts.push(serde_json::json!({"text": content}));
                }
                ChatMessage::User { content, .. } => {
                    let mut parts = pending_images.drain(..).collect::<Vec<_>>();
                    parts.push(serde_json::json!({"text": content}));
                    contents.push(serde_json::json!({
                        "role": "user",
                        "parts": parts,
                    }));
                }
                ChatMessage::Image { media_type, data } => {
                    pending_images.push(serde_json::json!({
                        "inlineData": {
                            "mimeType": media_type,
                            "data": data,
                        }
                    }));
                }
                ChatMessage::Assistant { content, tool_calls } => {
                    let mut parts: Vec<serde_json::Value> = Vec::new();
                    if let Some(text) = content {
                        if !text.is_empty() {
                            parts.push(serde_json::json!({"text": text}));
                        }
                    }
                    if let Some(tcs) = tool_calls {
                        for tc in tcs {
                            let args: serde_json::Value = serde_json::from_str(&tc.function.arguments)
                                .unwrap_or(serde_json::json!({}));
                            parts.push(serde_json::json!({
                                "functionCall": {
                                    "name": tc.function.name,
                                    "args": args,
                                }
                            }));
                        }
                    }
                    if !parts.is_empty() {
                        contents.push(serde_json::json!({
                            "role": "model",
                            "parts": parts,
                        }));
                    }
                }
                ChatMessage::Tool { content, tool_call_id: _ } => {
                    // Gemini: function results are user messages with functionResponse parts
                    // Parse content as JSON if possible, otherwise wrap as string
                    let response_val: serde_json::Value = serde_json::from_str(content)
                        .unwrap_or(serde_json::json!({"result": content}));
                    // Need the function name — extract from the previous assistant message's tool call
                    // For simplicity, use a generic name; the actual name should be tracked
                    contents.push(serde_json::json!({
                        "role": "user",
                        "parts": [{
                            "functionResponse": {
                                "name": "tool_response",
                                "response": response_val,
                            }
                        }],
                    }));
                }
            }
        }

        if !pending_images.is_empty() {
            contents.push(serde_json::json!({"role": "user", "parts": pending_images}));
        }

        let mut body = serde_json::json!({
            "contents": contents,
        });

        if !system_parts.is_empty() {
            body["systemInstruction"] = serde_json::json!({"parts": system_parts});
        }

        // Convert tool definitions: OpenAI format → Gemini functionDeclarations
        if let Some(tools) = tools {
            let declarations: Vec<serde_json::Value> = tools.iter().map(|t| {
                let openai = &t.0;
                serde_json::json!({
                    "name": openai["function"]["name"],
                    "description": openai["function"]["description"],
                    "parameters": openai["function"]["parameters"],
                })
            }).collect();
            body["tools"] = serde_json::json!([{"functionDeclarations": declarations}]);
        }

        if let Some(choice) = tool_choice {
            let mode = match choice {
                "required" => "ANY",
                "auto" => "AUTO",
                "none" => "NONE",
                _ => "AUTO",
            };
            body["toolConfig"] = serde_json::json!({
                "functionCallingConfig": {"mode": mode}
            });
        }

        body
    }

    /// Extract model name from provider-prefixed string.
    /// "google-ai-studio/gemini-2.5-flash" → "gemini-2.5-flash"
    fn strip_model(model: &str) -> &str {
        model.strip_prefix("google-ai-studio/").unwrap_or(model)
    }

    pub async fn call(
        &self,
        messages: &[ChatMessage],
        tools: Option<&[ToolDefinition]>,
        tool_choice: Option<&str>,
        model: &str,
    ) -> Result<ChatResponse, LlmError> {
        let body = Self::build_body(messages, tools, tool_choice);
        let model_name = Self::strip_model(model);

        tracing::debug!("Gemini REQUEST:\n{}", serde_json::to_string_pretty(&body).unwrap_or_default());

        let url = format!(
            "https://generativelanguage.googleapis.com/v1beta/models/{}:generateContent?key={}",
            model_name, self.api_key
        );

        let response = self.client
            .post(&url)
            .header("Content-Type", "application/json")
            .json(&body)
            .send()
            .await
            .map_err(|e| LlmError::RequestFailed(e.to_string()))?;

        let status = response.status();
        let text = response.text().await
            .map_err(|e| LlmError::RequestFailed(e.to_string()))?;

        if !status.is_success() {
            return Err(LlmError::RequestFailed(format!("HTTP {status}: {text}")));
        }

        tracing::debug!("Gemini RESPONSE:\n{text}");
        Self::parse_response(&text)
    }

    fn parse_response(text: &str) -> Result<ChatResponse, LlmError> {
        let resp: serde_json::Value = serde_json::from_str(text)
            .map_err(|e| LlmError::ParseError(format!("{e}\n{text}")))?;

        let candidate = resp["candidates"]
            .as_array()
            .and_then(|c| c.first())
            .ok_or_else(|| LlmError::ParseError("no candidates in response".into()))?;

        let parts = candidate["content"]["parts"]
            .as_array()
            .ok_or_else(|| LlmError::ParseError("no parts in response".into()))?;

        let mut text_parts: Vec<String> = Vec::new();
        let mut tool_calls: Vec<ToolCall> = Vec::new();

        for part in parts {
            if let Some(t) = part["text"].as_str() {
                text_parts.push(t.to_string());
            }
            if let Some(fc) = part.get("functionCall") {
                tool_calls.push(ToolCall {
                    id: format!("gemini_{}", tool_calls.len()),
                    call_type: "function".to_string(),
                    function: FunctionCall {
                        name: fc["name"].as_str().unwrap_or("").to_string(),
                        arguments: serde_json::to_string(&fc["args"]).unwrap_or_default(),
                    },
                });
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
        assert_eq!(GeminiClient::strip_model("google-ai-studio/gemini-2.5-flash"), "gemini-2.5-flash");
        assert_eq!(GeminiClient::strip_model("gemini-2.5-flash"), "gemini-2.5-flash");
    }

    #[test]
    fn system_is_top_level() {
        let messages = vec![
            ChatMessage::System { content: "be helpful".into(), name: None },
            ChatMessage::User { content: "hi".into(), name: None },
        ];
        let body = GeminiClient::build_body(&messages, None, None);
        assert!(body.get("systemInstruction").is_some());
        assert_eq!(body["systemInstruction"]["parts"][0]["text"], "be helpful");
        // Contents should not contain system
        let contents = body["contents"].as_array().unwrap();
        assert_eq!(contents.len(), 1);
        assert_eq!(contents[0]["role"], "user");
    }

    #[test]
    fn roles_are_user_and_model() {
        let messages = vec![
            ChatMessage::User { content: "hi".into(), name: None },
            ChatMessage::Assistant { content: Some("hello".into()), tool_calls: None },
        ];
        let body = GeminiClient::build_body(&messages, None, None);
        let contents = body["contents"].as_array().unwrap();
        assert_eq!(contents[0]["role"], "user");
        assert_eq!(contents[1]["role"], "model");
    }

    #[test]
    fn messages_use_parts_array() {
        let messages = vec![ChatMessage::User { content: "hi".into(), name: None }];
        let body = GeminiClient::build_body(&messages, None, None);
        let parts = body["contents"][0]["parts"].as_array().unwrap();
        assert_eq!(parts[0]["text"], "hi");
    }

    #[test]
    fn tools_use_function_declarations() {
        let messages = vec![ChatMessage::User { content: "hi".into(), name: None }];
        let tools = vec![ToolDefinition::new(serde_json::json!({
            "type": "function",
            "function": {
                "name": "test",
                "description": "a test",
                "parameters": {"type": "object", "properties": {}}
            }
        }))];
        let body = GeminiClient::build_body(&messages, Some(&tools), None);
        let decls = body["tools"][0]["functionDeclarations"].as_array().unwrap();
        assert_eq!(decls[0]["name"], "test");
    }

    #[test]
    fn tool_choice_required_becomes_any() {
        let messages = vec![ChatMessage::User { content: "hi".into(), name: None }];
        let body = GeminiClient::build_body(&messages, None, Some("required"));
        assert_eq!(body["toolConfig"]["functionCallingConfig"]["mode"], "ANY");
    }

    #[test]
    fn parse_text_response() {
        let text = r#"{"candidates":[{"content":{"parts":[{"text":"Hello!"}],"role":"model"},"finishReason":"STOP"}]}"#;
        let resp = GeminiClient::parse_response(text).unwrap();
        assert_eq!(resp.content.unwrap(), "Hello!");
        assert!(resp.tool_calls.is_none());
    }

    #[test]
    fn parse_function_call_response() {
        let text = r#"{"candidates":[{"content":{"parts":[{"functionCall":{"name":"get_weather","args":{"location":"Boston"}}}],"role":"model"},"finishReason":"STOP"}]}"#;
        let resp = GeminiClient::parse_response(text).unwrap();
        assert!(resp.content.is_none());
        let tcs = resp.tool_calls.unwrap();
        assert_eq!(tcs[0].function.name, "get_weather");
        assert!(tcs[0].function.arguments.contains("Boston"));
    }

    #[test]
    fn no_model_in_body() {
        let messages = vec![ChatMessage::User { content: "hi".into(), name: None }];
        let body = GeminiClient::build_body(&messages, None, None);
        assert!(body.get("model").is_none(), "model should be in URL, not body");
    }
}
