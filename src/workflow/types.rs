use serde::Deserialize;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum WorkflowError {
    #[error("could not read workflow: {0}")]
    ReadError(#[from] std::io::Error),

    #[error("could not parse workflow: {0}")]
    ParseError(#[from] serde_yaml::Error),

    #[error("workflow has no steps")]
    NoSteps,

    #[error("unknown model {0:?}")]
    UnknownModel(String),

    #[error("compact_ratio must be between {0} and {1}")]
    InvalidCompactRatio(f64, f64),

    #[error("invalid SQL: {0}")]
    InvalidSql(String),

    #[error("invalid tool config: {0}")]
    InvalidToolConfig(String),
}

/// Known tool names. Used by actions to specify which tools
/// are available. Validated at parse time — typos fail early.
#[derive(Debug, Clone, PartialEq, Deserialize, strum::EnumIter)]
#[serde(rename_all = "snake_case")]
pub enum ToolName {
    AskUser,
    Approve,
    Abort,
    Shell,
    File,
    Http,
    Input,
    SubmitFields,
    S3,
}

impl ToolName {
    pub fn name(&self) -> &str {
        match self {
            ToolName::AskUser => "ask_user",
            ToolName::Approve => "approve",
            ToolName::Abort => "abort",
            ToolName::Shell => "shell",
            ToolName::File => "file",
            ToolName::Http => "http",
            ToolName::Input => "input",
            ToolName::SubmitFields => "submit_fields",
            ToolName::S3 => "s3",
        }
    }

    pub fn description(&self) -> &str {
        match self {
            ToolName::AskUser => "Ask the user a question and wait for their response",
            ToolName::Approve => "Approve the current step and proceed",
            ToolName::Abort => "Abort the workflow",
            ToolName::Shell => "Run a command via direct exec (no shell). Requires 'allowed' regex patterns.",
            ToolName::File => "Read, list, find, and write files. Scoped by glob patterns for read and write paths. Paths must be absolute or ~/relative.",
            ToolName::Http => "Make an HTTP request. LLM provides URL, method, headers, body. Returns response body.",
            ToolName::Input => "[Experimental] Control keyboard and mouse. Click elements by ID, type text, press keys, scroll. macOS only.",
            ToolName::SubmitFields => "Submit collected field values",
            ToolName::S3 => "Read, write, list, and delete objects in S3-compatible storage. Scoped by glob patterns for read and write paths.",
        }
    }

    pub fn tool_attributes(&self) -> Vec<(&str, &str)> {
        match self {
            ToolName::Shell => vec![
                ("allowed", "Required. List of regex patterns. Command must match at least one. Empty list = nothing allowed."),
            ],
            ToolName::File => vec![
                ("read", "List of glob patterns for readable paths. Paths must be absolute or ~/relative. Empty list = no read access."),
                ("write", "List of glob patterns for writable paths. Paths must be absolute or ~/relative. Empty list = no write access."),
            ],
            ToolName::S3 => vec![
                ("bucket", "Required. S3 bucket name."),
                ("read", "List of glob patterns for readable keys. Empty = no read access."),
                ("write", "List of glob patterns for writable keys. Empty = no write access. Write implies read."),
            ],
            _ => vec![],
        }
    }

    /// Config keys (set via `pilot config set`) required at runtime.
    pub fn required_config(&self) -> Vec<(&str, &str)> {
        match self {
            ToolName::S3 => vec![
                ("s3_endpoint", "S3-compatible endpoint URL"),
                ("s3_access_key", "Access key ID"),
                ("s3_secret_key", "Secret access key"),
            ],
            _ => vec![],
        }
    }
}

/// Tool definition in YAML. Tagged by `name` field, like Action is tagged by `action`.
/// Each variant carries its own required config.
///
/// ```yaml
/// tools:
///   - name: ask_user
///   - name: shell
///     allowed: ["^git (status|diff)", "^curl "]
///   - name: file
///     read: ["~/projects/src/**"]
///     write: ["~/projects/output/**"]
/// ```
#[derive(Debug, Clone, PartialEq, Deserialize)]
#[serde(tag = "name", rename_all = "snake_case")]
pub enum ToolDef {
    AskUser,
    Approve,
    Abort,
    SubmitFields,
    Shell {
        allowed: Vec<String>,
    },
    File {
        read: Vec<String>,
        write: Vec<String>,
    },
    Http,
    Input,
    S3 {
        #[serde(flatten)]
        config: S3ToolConfig,
    },
}

/// Shared S3 tool configuration — used by both workflow ToolDef and ExposedToolDef.
#[derive(Debug, Clone, PartialEq, Deserialize)]
pub struct S3ToolConfig {
    pub bucket: String,
    #[serde(default)]
    pub read: Vec<String>,
    #[serde(default)]
    pub write: Vec<String>,
}

/// Tool definition for `pilot tool` CLI — exposed tools with scoped permissions.
/// One per YAML file in `.pilot/tools/`.
#[derive(Debug, Clone, PartialEq, Deserialize)]
#[serde(tag = "name", rename_all = "snake_case")]
pub enum ExposedToolDef {
    S3 {
        #[serde(flatten)]
        config: S3ToolConfig,
    },
}

/// A reference to a file. Holds the path, provides methods to read content.
/// Deserializes from a plain string in YAML.
#[derive(Debug, Clone, PartialEq, Deserialize)]
#[serde(from = "String")]
pub struct FileRef {
    pub path: String,
}

impl From<String> for FileRef {
    fn from(path: String) -> Self {
        Self { path }
    }
}

impl FileRef {
    /// Read the file content. Call at execution time, not parse time,
    /// because a previous step may have modified the file.
    pub fn read(&self) -> Result<String, std::io::Error> {
        let expanded = crate::tools::file::expand_home(&self.path);
        std::fs::read_to_string(expanded)
    }
}

/// Action-specific data. Each variant carries its own required attributes.
/// serde(tag = "action") means the YAML field "action" selects the variant,
/// and flatten on Step merges the variant's fields into the step object.
#[derive(Debug, Clone, PartialEq, Deserialize, strum::EnumDiscriminants)]
#[strum_discriminants(derive(strum::EnumIter))]
#[serde(tag = "action", rename_all = "snake_case")]
pub enum Action {
    ReadInput {
        #[serde(default)]
        prompt: String,
        #[serde(default)]
        name: Option<String>,
        #[serde(default)]
        context: Vec<FileRef>,
        #[serde(default)]
        post_output_sql: Option<String>,
    },
    ReadVar {
        #[serde(default)]
        name: Option<String>,
        vars: Vec<VarDef>,
        #[serde(default)]
        post_output_sql: Option<String>,
    },
    Llm {
        prompt: String,
        #[serde(default)]
        name: Option<String>,
        #[serde(default)]
        model: Option<String>,
        #[serde(default)]
        context: Vec<FileRef>,
        #[serde(default)]
        tools: Vec<ToolDef>,
        #[serde(default)]
        pre_input_sql: Option<String>,
        #[serde(default)]
        post_output_sql: Option<String>,
    },
    Parallel {
        #[serde(default)]
        name: Option<String>,
        branches: Vec<ParallelBranchDef>,
        #[serde(default)]
        pre_input_sql: Option<String>,
        merge_sql: String,
    },
    Select {
        #[serde(default)]
        name: Option<String>,
        #[serde(default = "default_match_mode", rename = "mode")]
        match_mode: String,
        #[serde(default)]
        pre_input_sql: Option<String>,
        #[serde(default)]
        post_output_sql: Option<String>,
        #[serde(default)]
        prepare_input_for_when: Option<String>,
        #[serde(default)]
        prepare_input_for_body: Option<String>,
        branches: Vec<SelectBranchDef>,
        #[serde(default)]
        output_schema: Vec<SchemaField>,
    },
    Http {
        url: String,
        #[serde(default = "default_http_method")]
        method: String,
        #[serde(default)]
        headers: std::collections::HashMap<String, String>,
        #[serde(default)]
        headers_jq: Option<String>,
        #[serde(default)]
        timeout: Option<u64>,
        #[serde(default)]
        request_jq: Option<String>,
        #[serde(default)]
        response_jq: Option<String>,
        #[serde(default)]
        output_schema: Vec<SchemaField>,
        #[serde(default)]
        name: Option<String>,
    },
    Shell {
        command: String,
        #[serde(default)]
        cwd: Option<String>,
        #[serde(default)]
        timeout: Option<u64>,
        #[serde(default)]
        name: Option<String>,
        #[serde(default)]
        post_output_sql: Option<String>,
    },
    Print {
        #[serde(default)]
        name: Option<String>,
    },
    Transform {
        sql: String,
        #[serde(default)]
        name: Option<String>,
    },
    Loop {
        #[serde(default)]
        name: Option<String>,
        #[serde(default)]
        pre_input_sql: Option<String>,
        body: Vec<Step>,
        while_sql: String,
    },
    Each {
        #[serde(default)]
        name: Option<String>,
        #[serde(default)]
        pre_input_sql: Option<String>,
        #[serde(default)]
        post_output_sql: Option<String>,
        body: Vec<Step>,
        #[serde(default)]
        output_schema: Vec<SchemaField>,
    },
    Trigger {
        command: String,
        #[serde(default)]
        name: Option<String>,
        #[serde(default)]
        pre_input_sql: Option<String>,
        #[serde(default)]
        jq: Option<String>,
        output_schema: Vec<SchemaField>,
        #[serde(default)]
        post_output_sql: Option<String>,
        body: Vec<Step>,
    },
    Screenshot {
        #[serde(default)]
        name: Option<String>,
        #[serde(default)]
        providers: Vec<String>,
    },
    Passthrough {
        #[serde(default)]
        name: Option<String>,
    },
    Compact {
        max_words: usize,
        #[serde(default = "default_compact_ratio")]
        compact_ratio: f64,
        #[serde(default)]
        model: Option<String>,
        prompt: String,
        #[serde(default)]
        name: Option<String>,
    },
}

fn default_compact_ratio() -> f64 {
    0.5
}

#[derive(Debug, Clone, PartialEq, Deserialize)]
pub struct ParallelBranchDef {
    pub name: String,
    pub body: Vec<Step>,
}

#[derive(Debug, Clone, PartialEq, Deserialize)]
pub struct SelectBranchDef {
    pub name: String,
    #[serde(rename = "when", deserialize_with = "deserialize_when")]
    pub when_steps: Vec<Step>,
    pub body: Vec<Step>,
}

/// Declares a column name and type for output schema.
#[derive(Debug, Clone, PartialEq, Deserialize)]
pub struct SchemaField {
    pub name: String,
    #[serde(rename = "type")]
    pub field_type: String,
}

fn default_http_method() -> String {
    "GET".to_string()
}


/// Accept `when` as either a SQL string (shorthand for a single transform step)
/// or a list of steps (full pipeline).
fn deserialize_when<'de, D>(deserializer: D) -> Result<Vec<Step>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    #[derive(Deserialize)]
    #[serde(untagged)]
    enum WhenDef {
        Sql(String),
        Steps(Vec<Step>),
    }

    match WhenDef::deserialize(deserializer)? {
        WhenDef::Sql(sql) => Ok(vec![Step {
            action: Action::Transform {
                name: None,
                sql,
            },
        }]),
        WhenDef::Steps(steps) => Ok(steps),
    }
}

pub(crate) fn default_match_mode() -> String {
    "first".to_string()
}

#[derive(Debug, Clone, PartialEq, Deserialize)]
pub struct VarDef {
    pub name: String,
    #[serde(default)]
    pub arg: Option<String>,
    #[serde(default)]
    pub env: Option<String>,
    #[serde(default)]
    pub value: Option<String>,
    #[serde(default)]
    pub default: Option<String>,
    #[serde(default)]
    pub description: Option<String>,
}

/// A step in a workflow. Action-specific attributes (target, tools, fields,
/// model, context, prompt, name, etc.) belong on the Action enum variants.
#[derive(Debug, Clone, PartialEq, Deserialize)]
pub struct Step {
    #[serde(flatten)]
    pub action: Action,
}

#[derive(Debug, Clone, Deserialize)]
pub struct Workflow {
    #[serde(default)]
    pub model: Option<String>,
    pub steps: Vec<Step>,
}
