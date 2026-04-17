use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

use crate::llm::ToolDefinition;
use crate::workflow::ToolDef;

use super::ToolError;

/// ToolDispatcher provides scoped tool access for a node.
/// Each node gets its own dispatcher with only the tools it's allowed to use.
pub trait ToolDispatcher: Send + Sync {
    /// Tool definitions to send to the LLM.
    fn definitions(&self) -> Vec<ToolDefinition>;

    /// Execute a tool by name.
    fn execute<'a>(
        &'a self,
        name: &'a str,
        arguments: &'a str,
    ) -> Pin<Box<dyn Future<Output = Result<String, ToolError>> + Send + 'a>>;
}

/// Default dispatcher backed by a list of Tool instances.
/// Builds a lookup from LLM tool names to Tool instances, since one Tool
/// can expose multiple LLM-facing definitions (e.g. file → file_read, file_write, ...).
#[derive(Clone)]
pub struct DefaultToolDispatcher {
    tools: Vec<Arc<dyn super::Tool>>,
    /// LLM tool name → Tool instance (e.g. "file_write" → FileTool)
    lookup: HashMap<String, Arc<dyn super::Tool>>,
}

impl DefaultToolDispatcher {
    pub fn new(tools: Vec<Arc<dyn super::Tool>>) -> Self {
        let mut lookup = HashMap::new();
        for tool in &tools {
            for def in tool.definitions() {
                if let Some(name) = def.0["function"]["name"].as_str() {
                    lookup.insert(name.to_string(), tool.clone());
                }
            }
        }
        Self { tools, lookup }
    }

    /// Consume self, append additional tools, return new dispatcher.
    pub fn with(mut self, additional: Vec<Arc<dyn super::Tool>>) -> Self {
        for tool in &additional {
            for def in tool.definitions() {
                if let Some(name) = def.0["function"]["name"].as_str() {
                    self.lookup.insert(name.to_string(), tool.clone());
                }
            }
        }
        self.tools.extend(additional);
        self
    }
}

impl ToolDispatcher for DefaultToolDispatcher {
    fn definitions(&self) -> Vec<ToolDefinition> {
        self.tools.iter().flat_map(|t| t.definitions()).collect()
    }

    fn execute<'a>(
        &'a self,
        name: &'a str,
        arguments: &'a str,
    ) -> Pin<Box<dyn Future<Output = Result<String, ToolError>> + Send + 'a>> {
        Box::pin(async move {
            let tool = self
                .lookup
                .get(name)
                .ok_or_else(|| ToolError::ExecutionFailed(format!("unknown tool: {name}")))?;
            tool.execute(name, arguments).await
        })
    }
}

/// S3 config passed through from BuilderContext.
pub struct S3Config {
    pub endpoint: String,
    pub access_key: String,
    pub secret_key: String,
    pub yaml_path: String,
}

/// Build a tool dispatcher from YAML tool definitions.
pub fn build_tool_dispatcher(tool_defs: &[ToolDef], s3_config: &S3Config, llm: Option<Arc<dyn crate::llm::LlmClient>>) -> DefaultToolDispatcher {
    let tools: Vec<Arc<dyn super::Tool>> = tool_defs
        .iter()
        .map(|def| -> Arc<dyn super::Tool> {
            match def {
                ToolDef::AskUser => Arc::new(super::ask_user::AskUserTool),
                ToolDef::Approve => Arc::new(super::approve::ApproveTool),
                ToolDef::Abort => Arc::new(super::abort::AbortTool),
                ToolDef::SubmitFields => Arc::new(super::submit_fields::SubmitFieldsTool::new(vec![], std::collections::HashMap::new())),
                ToolDef::Shell { allowed } => Arc::new(
                    super::shell::ShellTool::new(allowed.clone())
                        .expect("invalid shell allowed pattern")
                ),
                ToolDef::File { read, write, semantic_index } => Arc::new(
                    super::file::FileTool::new(
                        read.clone(),
                        write.clone(),
                        *semantic_index,
                        llm.clone(),
                        &s3_config.yaml_path,
                    )
                    .expect("invalid file tool config")
                ),
                ToolDef::Http => Arc::new(super::http::HttpTool),
                ToolDef::Input => Arc::new(super::input::InputTool),
                ToolDef::S3 { config } => Arc::new(
                    super::s3::S3Tool::new(
                        &s3_config.endpoint,
                        &s3_config.access_key,
                        &s3_config.secret_key,
                        &config.bucket,
                        &config.read,
                        &config.write,
                        llm.clone(),
                        &s3_config.yaml_path,
                    )
                    .expect("S3 tool config error")
                ),
            }
        })
        .collect();
    DefaultToolDispatcher::new(tools)
}
