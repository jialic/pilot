use super::{ChatMessage, ChatResponse, FunctionCall, LlmError, ToolCall, ToolDefinition};

pub struct OpenAIClient {
    client: reqwest::Client,
    api_key: String,
}

impl OpenAIClient {
    pub fn new(api_key: String) -> Self {
        Self {
            client: reqwest::Client::new(),
            api_key,
        }
    }

    pub(crate) fn build_body(
        model: &str,
        messages: &[ChatMessage],
        tools: Option<&[ToolDefinition]>,
        tool_choice: Option<&str>,
    ) -> serde_json::Value {
        let model = model.strip_prefix("openai/").unwrap_or(model);

        let mut api_messages: Vec<serde_json::Value> = Vec::new();
        // Collect pending images to attach to the next user message
        let mut pending_images: Vec<serde_json::Value> = Vec::new();

        for msg in messages {
            match msg {
                ChatMessage::System { content, .. } => {
                    api_messages.push(serde_json::json!({"role": "system", "content": content}));
                }
                ChatMessage::User { content, .. } => {
                    if pending_images.is_empty() {
                        api_messages.push(serde_json::json!({"role": "user", "content": content}));
                    } else {
                        // Attach pending images + text as multipart content
                        let mut parts = pending_images.drain(..).collect::<Vec<_>>();
                        parts.push(serde_json::json!({"type": "text", "text": content}));
                        api_messages.push(serde_json::json!({"role": "user", "content": parts}));
                    }
                }
                ChatMessage::Assistant { content, tool_calls } => {
                    let mut msg = serde_json::json!({"role": "assistant"});
                    if let Some(c) = content {
                        msg["content"] = serde_json::json!(c);
                    }
                    if let Some(tcs) = tool_calls {
                        let tc_json: Vec<serde_json::Value> = tcs.iter().map(|tc| {
                            serde_json::json!({
                                "id": tc.id,
                                "type": "function",
                                "function": {
                                    "name": tc.function.name,
                                    "arguments": tc.function.arguments,
                                }
                            })
                        }).collect();
                        msg["tool_calls"] = serde_json::json!(tc_json);
                    }
                    api_messages.push(msg);
                }
                ChatMessage::Tool { content, tool_call_id } => {
                    api_messages.push(serde_json::json!({
                        "role": "tool",
                        "content": content,
                        "tool_call_id": tool_call_id,
                    }));
                }
                ChatMessage::Image { media_type, data } => {
                    // Queue image to attach to the next user message
                    pending_images.push(serde_json::json!({
                        "type": "image_url",
                        "image_url": {
                            "url": format!("data:{media_type};base64,{data}"),
                        }
                    }));
                }
            }
        }

        // If there are pending images with no following user message, attach as standalone
        if !pending_images.is_empty() {
            api_messages.push(serde_json::json!({"role": "user", "content": pending_images}));
        }

        let mut body = serde_json::json!({
            "model": model,
            "messages": api_messages,
        });

        if let Some(tools) = tools {
            let tools_json: Vec<&serde_json::Value> = tools.iter().map(|t| &t.0).collect();
            body["tools"] = serde_json::json!(tools_json);
        }

        if let Some(choice) = tool_choice {
            body["tool_choice"] = serde_json::json!(choice);
        }

        body
    }

    pub async fn embed(&self, texts: &[String]) -> Result<Vec<Vec<f32>>, LlmError> {
        let body = serde_json::json!({
            "model": "text-embedding-3-small",
            "input": texts,
        });

        let response = self.client
            .post("https://api.openai.com/v1/embeddings")
            .header("Content-Type", "application/json")
            .header("Authorization", format!("Bearer {}", self.api_key))
            .json(&body)
            .send()
            .await
            .map_err(|e| LlmError::RequestFailed(e.to_string()))?;

        let status = response.status();
        let text = response.text().await
            .map_err(|e| LlmError::RequestFailed(e.to_string()))?;

        if !status.is_success() {
            return Err(LlmError::RequestFailed(format!("embedding HTTP {status}: {text}")));
        }

        let resp: serde_json::Value = serde_json::from_str(&text)
            .map_err(|e| LlmError::ParseError(format!("embedding parse: {e}")))?;

        let embeddings = resp["data"]
            .as_array()
            .ok_or_else(|| LlmError::ParseError("no data in embedding response".into()))?
            .iter()
            .map(|item| {
                item["embedding"]
                    .as_array()
                    .ok_or_else(|| LlmError::ParseError("no embedding vector".into()))
                    .map(|arr| {
                        arr.iter()
                            .filter_map(|v| v.as_f64().map(|f| f as f32))
                            .collect()
                    })
            })
            .collect::<Result<Vec<Vec<f32>>, _>>()?;

        Ok(embeddings)
    }

    pub async fn call(
        &self,
        messages: &[ChatMessage],
        tools: Option<&[ToolDefinition]>,
        tool_choice: Option<&str>,
        model: &str,
    ) -> Result<ChatResponse, LlmError> {
        openai_compatible_call(
            &self.client,
            "https://api.openai.com/v1/chat/completions",
            &self.api_key,
            messages, tools, tool_choice, model,
        ).await
    }
}

/// Send an OpenAI-compatible chat completion request.
/// Reused by any provider with an OpenAI-compatible API (OpenAI, xAI, etc.).
pub async fn openai_compatible_call(
    client: &reqwest::Client,
    url: &str,
    api_key: &str,
    messages: &[ChatMessage],
    tools: Option<&[ToolDefinition]>,
    tool_choice: Option<&str>,
    model: &str,
) -> Result<ChatResponse, LlmError> {
    let body = OpenAIClient::build_body(model, messages, tools, tool_choice);

    tracing::debug!("OpenAI-compat REQUEST to {url}:\n{}", serde_json::to_string_pretty(&body).unwrap_or_default());

    let response = client
        .post(url)
        .header("Content-Type", "application/json")
        .header("Authorization", format!("Bearer {api_key}"))
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

    tracing::debug!("OpenAI-compat RESPONSE:\n{text}");
    parse_openai_response(&text)
}

/// Parse OpenAI-format response JSON into ChatResponse.
pub fn parse_openai_response(text: &str) -> Result<ChatResponse, LlmError> {
    let resp: serde_json::Value = serde_json::from_str(text)
        .map_err(|e| LlmError::ParseError(format!("{e}\n{text}")))?;

    let choice = resp["choices"]
        .as_array()
        .and_then(|c| c.first())
        .ok_or_else(|| LlmError::ParseError("no choices in response".into()))?;

    let content = choice["message"]["content"].as_str().map(String::from);

    let tool_calls = choice["message"]["tool_calls"]
        .as_array()
        .map(|tcs| {
            tcs.iter()
                .filter_map(|tc| {
                    Some(ToolCall {
                        id: tc["id"].as_str()?.to_string(),
                        call_type: tc["type"].as_str().unwrap_or("function").to_string(),
                        function: FunctionCall {
                            name: tc["function"]["name"].as_str()?.to_string(),
                            arguments: tc["function"]["arguments"].as_str()?.to_string(),
                        },
                    })
                })
                .collect()
        });

    Ok(ChatResponse {
        content,
        tool_calls,
    })
}
