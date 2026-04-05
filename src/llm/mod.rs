mod anthropic;
mod gemini;
mod openai;
mod xai;
#[cfg(test)]
pub mod testing;

use std::future::Future;
use std::pin::Pin;

use serde::{Deserialize, Serialize};

use anthropic::AnthropicClient;
use gemini::GeminiClient;
use xai::XaiClient;
use openai::OpenAIClient;

// ---------------------------------------------------------------------------
// Public types
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "role", rename_all = "lowercase")]
pub enum ChatMessage {
    System {
        content: String,
        #[serde(skip_serializing_if = "Option::is_none")]
        name: Option<String>,
    },
    User {
        content: String,
        #[serde(skip_serializing_if = "Option::is_none")]
        name: Option<String>,
    },
    Assistant {
        #[serde(skip_serializing_if = "Option::is_none")]
        content: Option<String>,
        #[serde(skip_serializing_if = "Option::is_none")]
        tool_calls: Option<Vec<ToolCall>>,
    },
    Tool {
        content: String,
        tool_call_id: String,
    },
    /// Image content — attached to the next user message by provider clients.
    /// media_type: "image/webp", "image/png", etc.
    /// data: base64-encoded image bytes.
    #[serde(skip)]
    Image {
        media_type: String,
        data: String,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ToolCall {
    pub id: String,
    #[serde(rename = "type")]
    pub call_type: String,
    pub function: FunctionCall,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FunctionCall {
    pub name: String,
    pub arguments: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ToolDefinition(pub serde_json::Value);

impl ToolDefinition {
    pub fn new(value: serde_json::Value) -> Self {
        Self(value)
    }
}

#[derive(Debug, Clone)]
pub struct ChatResponse {
    pub content: Option<String>,
    pub tool_calls: Option<Vec<ToolCall>>,
}

#[derive(Debug, thiserror::Error)]
pub enum LlmError {
    #[error("LLM request failed: {0}")]
    RequestFailed(String),
    #[error("LLM response parse error: {0}")]
    ParseError(String),
}

// ---------------------------------------------------------------------------
// LlmClient trait
// ---------------------------------------------------------------------------

pub trait LlmClient: Send + Sync {
    fn chat<'a>(
        &'a self,
        messages: Vec<ChatMessage>,
        tools: Option<Vec<ToolDefinition>>,
        tool_choice: Option<&'a str>,
        model_override: Option<&'a str>,
    ) -> Pin<Box<dyn Future<Output = Result<ChatResponse, LlmError>> + Send + 'a>>;

    fn chat_with_tools<'a>(
        &'a self,
        messages: Vec<ChatMessage>,
        tools: Vec<ToolDefinition>,
        model_override: Option<&'a str>,
    ) -> Pin<Box<dyn Future<Output = Result<ChatResponse, LlmError>> + Send + 'a>>;

    fn embed<'a>(
        &'a self,
        texts: &'a [String],
    ) -> Pin<Box<dyn Future<Output = Result<Vec<Vec<f32>>, LlmError>> + Send + 'a>>;
}

// ---------------------------------------------------------------------------
// LLM — dispatches to per-provider clients
// ---------------------------------------------------------------------------

pub struct LLMConfig {
    pub openai_api_key: String,
    pub anthropic_api_key: String,
    pub gemini_api_key: String,
    pub xai_api_key: String,
    pub model: String,
}

pub struct LLM {
    openai: OpenAIClient,
    anthropic: AnthropicClient,
    gemini: GeminiClient,
    xai: XaiClient,
    pub model: String,
}

impl LLM {
    pub fn new(config: LLMConfig) -> Self {
        Self {
            openai: OpenAIClient::new(config.openai_api_key),
            anthropic: AnthropicClient::new(config.anthropic_api_key),
            gemini: GeminiClient::new(config.gemini_api_key),
            xai: XaiClient::new(config.xai_api_key),
            model: config.model,
        }
    }

    pub fn initial_messages(system_prompt: &str) -> Vec<ChatMessage> {
        vec![
            ChatMessage::System { content: system_prompt.to_string(), name: None },
            ChatMessage::User { content: "Execute the current step.".to_string(), name: None },
        ]
    }

    pub fn extract_tool_call(response: &ChatResponse) -> Option<(String, String, String)> {
        let tc = response.tool_calls.as_ref()?.first()?;
        Some((tc.function.name.clone(), tc.function.arguments.clone(), tc.id.clone()))
    }

    pub fn extract_question(arguments: &str) -> String {
        serde_json::from_str::<serde_json::Value>(arguments)
            .ok()
            .and_then(|v| v["question"].as_str().map(String::from))
            .unwrap_or_else(|| "Please provide input:".to_string())
    }

    pub fn tool_result_messages(
        tool_call_id: &str,
        tool_name: &str,
        arguments: &str,
        result: &str,
    ) -> Vec<ChatMessage> {
        vec![
            ChatMessage::Assistant {
                content: Some(String::new()),
                tool_calls: Some(vec![ToolCall {
                    id: tool_call_id.to_string(),
                    call_type: "function".to_string(),
                    function: FunctionCall {
                        name: tool_name.to_string(),
                        arguments: arguments.to_string(),
                    },
                }]),
            },
            ChatMessage::Tool {
                content: result.to_string(),
                tool_call_id: tool_call_id.to_string(),
            },
        ]
    }

    pub fn wrap_tool(tool: ToolDefinition) -> ToolDefinition {
        tool
    }

    fn is_openai(model: &str) -> bool {
        model.starts_with("openai/")
    }

    fn is_anthropic(model: &str) -> bool {
        model.starts_with("anthropic/")
    }

    fn is_gemini(model: &str) -> bool {
        model.starts_with("google-ai-studio/")
    }

    fn is_xai(model: &str) -> bool {
        model.starts_with("xai/")
    }
}

// ---------------------------------------------------------------------------
// Tool loop
// ---------------------------------------------------------------------------

/// Run the tool-calling loop. Returns all new messages as (role, content) pairs.
/// Includes tool calls (role="assistant"), tool results (role="tool"),
/// and the final response (role="assistant").
pub async fn run_tool_loop(
    llm: &dyn LlmClient,
    dispatcher: &dyn crate::tools::dispatcher::ToolDispatcher,
    messages: &mut Vec<ChatMessage>,
    model_override: Option<&str>,
    listener: Option<&dyn crate::events::Listener>,
) -> Result<Vec<(String, String)>, LlmError> {
    use crate::events::ToolEvent;

    let tool_defs: Vec<ToolDefinition> = dispatcher.definitions();
    let tools_param = if tool_defs.is_empty() {
        None
    } else {
        Some(tool_defs.clone())
    };

    let mut new_messages: Vec<(String, String)> = Vec::new();

    loop {
        if let Some(l) = listener {
            l.on_tool(&ToolEvent::LlmRequest);
        }

        let response = llm
            .chat(messages.clone(), tools_param.clone(), None, model_override)
            .await
            .map_err(|e| LlmError::RequestFailed(format!("LLM call failed: {e}")))?;

        if let Some(l) = listener {
            l.on_tool(&ToolEvent::LlmResponse);
        }

        if let Some((name, arguments, id)) = LLM::extract_tool_call(&response) {
            if let Some(l) = listener {
                l.on_tool(&ToolEvent::Called);
            }

            let result = match dispatcher.execute(&name, &arguments).await {
                Ok(output) => output,
                Err(crate::tools::ToolError::WorkflowAborted) => {
                    return Err(LlmError::RequestFailed("workflow aborted by user".into()));
                }
                Err(crate::tools::ToolError::ExecutionFailed(msg)) => {
                    if let Some(l) = listener {
                        l.on_tool(&ToolEvent::Error);
                    }
                    format!("ERROR: {msg}")
                }
            };

            if let Some(l) = listener {
                l.on_tool(&ToolEvent::Returned);
            }

            // Record tool call and result
            new_messages.push(("assistant".into(), format!("{{\"tool_call\": \"{name}\", \"arguments\": {arguments}}}")));
            new_messages.push(("tool".into(), result.clone()));

            messages.extend(LLM::tool_result_messages(&id, &name, &arguments, &result));
            continue;
        }

        let content = response.content.unwrap_or_default();
        new_messages.push(("assistant".into(), content));
        return Ok(new_messages);
    }
}

// ---------------------------------------------------------------------------
// LlmClient impl — dispatches to provider clients
// ---------------------------------------------------------------------------

impl LlmClient for LLM {
    fn chat<'a>(
        &'a self,
        messages: Vec<ChatMessage>,
        tools: Option<Vec<ToolDefinition>>,
        tool_choice: Option<&'a str>,
        model_override: Option<&'a str>,
    ) -> Pin<Box<dyn Future<Output = Result<ChatResponse, LlmError>> + Send + 'a>> {
        Box::pin(async move {
            let model = model_override.unwrap_or(&self.model);

            let dot_handle = tokio::spawn(async {
                loop {
                    tokio::time::sleep(std::time::Duration::from_secs(1)).await;
                    tracing::info!("·");
                }
            });

            let result = if Self::is_openai(model) {
                self.openai.call(&messages, tools.as_deref(), tool_choice, model).await
            } else if Self::is_anthropic(model) {
                self.anthropic.call(&messages, tools.as_deref(), tool_choice, model).await
            } else if Self::is_gemini(model) {
                self.gemini.call(&messages, tools.as_deref(), tool_choice, model).await
            } else if Self::is_xai(model) {
                self.xai.call(&messages, tools.as_deref(), tool_choice, model).await
            } else {
                Err(LlmError::RequestFailed(format!("unsupported provider for model: {model}. Supported: openai/*, anthropic/*, google-ai-studio/*, xai/*")))
            };

            dot_handle.abort();
            result
        })
    }

    fn chat_with_tools<'a>(
        &'a self,
        messages: Vec<ChatMessage>,
        tools: Vec<ToolDefinition>,
        model_override: Option<&'a str>,
    ) -> Pin<Box<dyn Future<Output = Result<ChatResponse, LlmError>> + Send + 'a>> {
        Box::pin(async move {
            self.chat(messages, Some(tools), Some("required"), model_override).await
        })
    }

    fn embed<'a>(
        &'a self,
        texts: &'a [String],
    ) -> Pin<Box<dyn Future<Output = Result<Vec<Vec<f32>>, LlmError>> + Send + 'a>> {
        Box::pin(async move {
            self.openai.embed(texts).await
        })
    }
}
