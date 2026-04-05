use std::path::PathBuf;

use serde::{Deserialize, Serialize};

use crate::llm::LLMConfig;

#[derive(Debug, Default, Serialize, Deserialize)]
pub struct Config {
    #[serde(default)]
    pub openai_api_key: String,
    #[serde(default)]
    pub anthropic_api_key: String,
    #[serde(default)]
    pub gemini_api_key: String,
    #[serde(default)]
    pub xai_api_key: String,
    #[serde(default)]
    pub s3_endpoint: String,
    #[serde(default)]
    pub s3_access_key: String,
    #[serde(default)]
    pub s3_secret_key: String,
}

impl Config {
    pub fn to_llm_config(&self) -> LLMConfig {
        LLMConfig {
            openai_api_key: self.openai_api_key.clone(),
            anthropic_api_key: self.anthropic_api_key.clone(),
            gemini_api_key: self.gemini_api_key.clone(),
            xai_api_key: self.xai_api_key.clone(),
            model: String::new(), // runner picks from workflow or default
        }
    }
}

fn config_dir() -> PathBuf {
    let home = dirs_home().expect("could not determine home directory");
    home.join(".config").join("pilot")
}

fn config_path() -> PathBuf {
    config_dir().join("config.yaml")
}

fn dirs_home() -> Option<PathBuf> {
    #[cfg(unix)]
    {
        std::env::var_os("HOME").map(PathBuf::from)
    }
    #[cfg(windows)]
    {
        std::env::var_os("USERPROFILE").map(PathBuf::from)
    }
}

pub fn load_config() -> Result<Config, String> {
    let path = config_path();
    let data = std::fs::read_to_string(&path)
        .map_err(|e| format!("could not read config at {}: {e}", path.display()))?;
    serde_yaml::from_str(&data).map_err(|e| format!("could not parse config: {e}"))
}

pub fn save_config(config: &Config) -> Result<(), String> {
    let dir = config_dir();
    std::fs::create_dir_all(&dir).map_err(|e| format!("could not create config directory: {e}"))?;

    let data =
        serde_yaml::to_string(config).map_err(|e| format!("could not serialize config: {e}"))?;

    let path = config_path();
    std::fs::write(&path, data)
        .map_err(|e| format!("could not write config to {}: {e}", path.display()))?;

    Ok(())
}

pub fn mask_token(token: &str) -> String {
    if token.len() <= 4 {
        "****".to_string()
    } else {
        format!("****{}", &token[token.len() - 4..])
    }
}

pub const VALID_CONFIG_KEYS: &[&str] = &[
    "openai_api_key", "anthropic_api_key", "gemini_api_key", "xai_api_key",
    "s3_endpoint", "s3_access_key", "s3_secret_key",
];

pub fn set_config_key(config: &mut Config, key: &str, value: &str) -> Result<(), String> {
    match key {
        "openai_api_key" => config.openai_api_key = value.to_string(),
        "anthropic_api_key" => config.anthropic_api_key = value.to_string(),
        "gemini_api_key" => config.gemini_api_key = value.to_string(),
        "xai_api_key" => config.xai_api_key = value.to_string(),
        "s3_endpoint" => config.s3_endpoint = value.to_string(),
        "s3_access_key" => config.s3_access_key = value.to_string(),
        "s3_secret_key" => config.s3_secret_key = value.to_string(),
        _ => {
            return Err(format!(
                "unknown key: {key}. Valid keys: {}",
                VALID_CONFIG_KEYS.join(", ")
            ));
        }
    }
    Ok(())
}
