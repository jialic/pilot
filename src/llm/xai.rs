use super::{ChatMessage, ChatResponse, LlmError, ToolDefinition};
use super::openai::openai_compatible_call;

pub struct XaiClient {
    client: reqwest::Client,
    api_key: String,
}

impl XaiClient {
    pub fn new(api_key: String) -> Self {
        Self {
            client: reqwest::Client::new(),
            api_key,
        }
    }

    pub async fn call(
        &self,
        messages: &[ChatMessage],
        tools: Option<&[ToolDefinition]>,
        tool_choice: Option<&str>,
        model: &str,
    ) -> Result<ChatResponse, LlmError> {
        // Strip xai/ prefix for the API
        let model = model.strip_prefix("xai/").unwrap_or(model);
        openai_compatible_call(
            &self.client,
            "https://api.x.ai/v1/chat/completions",
            &self.api_key,
            messages, tools, tool_choice, model,
        ).await
    }
}
