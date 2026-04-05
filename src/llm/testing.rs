use super::*;
use std::sync::Mutex;

pub struct MockLlm {
    responses: Mutex<Vec<String>>,
}

impl MockLlm {
    pub fn new(responses: Vec<String>) -> Self {
        Self {
            responses: Mutex::new(responses),
        }
    }
}

impl LlmClient for MockLlm {
    fn chat<'a>(
        &'a self,
        _messages: Vec<ChatMessage>,
        _tools: Option<Vec<ToolDefinition>>,
        _tool_choice: Option<&'a str>,
        _model_override: Option<&'a str>,
    ) -> Pin<Box<dyn Future<Output = Result<ChatResponse, LlmError>> + Send + 'a>> {
        let content = {
            let mut responses = self.responses.lock().unwrap();
            if responses.is_empty() {
                "mock: no more responses".to_string()
            } else {
                responses.remove(0)
            }
        };

        Box::pin(async move {
            Ok(ChatResponse {
                content: Some(content),
                tool_calls: None,
            })
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
            // Return zero vectors for testing
            Ok(texts.iter().map(|_| vec![0.0; 256]).collect())
        })
    }
}

#[cfg(test)]
mod tests {
    use super::super::*;
    use super::super::openai::OpenAIClient;

    #[test]
    fn openai_no_tool_choice_when_not_passed() {
        let messages = vec![ChatMessage::User { content: "hi".into(), name: None }];
        let tools = vec![ToolDefinition::new(serde_json::json!({"type": "function", "function": {"name": "test"}}))];

        let body = OpenAIClient::build_body("model", &messages, Some(&tools), None);

        assert!(body.get("tools").is_some(), "tools should be present");
        assert!(body.get("tool_choice").is_none(), "tool_choice should NOT be set when not explicitly passed");
    }

    #[test]
    fn openai_tool_choice_set_when_passed() {
        let messages = vec![ChatMessage::User { content: "hi".into(), name: None }];
        let tools = vec![ToolDefinition::new(serde_json::json!({"type": "function", "function": {"name": "test"}}))];

        let body = OpenAIClient::build_body("model", &messages, Some(&tools), Some("required"));

        assert_eq!(body["tool_choice"], "required");
    }

    #[test]
    fn openai_image_attached_to_next_user_message() {
        let messages = vec![
            ChatMessage::Image { media_type: "image/png".into(), data: "abc123".into() },
            ChatMessage::User { content: "describe".into(), name: None },
        ];

        let body = OpenAIClient::build_body("model", &messages, None, None);
        let msgs = body["messages"].as_array().unwrap();
        assert_eq!(msgs.len(), 1);
        let content = msgs[0]["content"].as_array().unwrap();
        assert_eq!(content[0]["type"], "image_url");
        assert_eq!(content[1]["type"], "text");
        assert_eq!(content[1]["text"], "describe");
    }
}
