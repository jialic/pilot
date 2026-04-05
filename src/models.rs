pub const DEFAULT_MODEL: &str = "openai/gpt-5-mini";

pub const VALID_MODELS: &[&str] = &[
    "openai/gpt-5.4",
    "openai/gpt-5.2",
    "openai/o3",
    "openai/gpt-5.1-codex",
    "openai/gpt-5-mini",
    "openai/gpt-5-nano",
    "anthropic/claude-opus-4-6",
    "anthropic/claude-sonnet-4-6",
    "anthropic/claude-haiku-4-5",
    "google-ai-studio/gemini-2.5-pro",
    "google-ai-studio/gemini-2.5-flash",
    "deepseek/deepseek-reasoner",
    "xai/grok-4.20-0309-reasoning",
    "xai/grok-4.20-0309-non-reasoning",
    "xai/grok-4-1-fast-reasoning",
    "xai/grok-4-1-fast-non-reasoning",
];

pub fn is_valid_model(model: &str) -> bool {
    VALID_MODELS.contains(&model)
}
