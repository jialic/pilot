use strum::IntoEnumIterator;

use super::types::{Action, ActionDiscriminants};

#[derive(serde::Serialize)]
pub struct ActionSchema {
    pub name: String,
    pub description: String,
    pub input_schema: String,
    pub output_schema: String,
    pub flow: String,
    pub attributes: Vec<AttributeSchema>,
}

#[derive(serde::Serialize)]
pub struct AttributeSchema {
    pub name: String,
    pub description: String,
    pub value_type: String,
}

fn attr(name: &str, desc: &str, vtype: &str) -> AttributeSchema {
    AttributeSchema {
        name: name.into(),
        description: desc.into(),
        value_type: vtype.into(),
    }
}

impl ActionDiscriminants {
    pub fn schema_name(&self) -> &str {
        match self {
            Self::ReadInput => "read_input",
            Self::ReadVar => "read_var",
            Self::Llm => "llm",
            Self::Parallel => "parallel",
            Self::Select => "select",
            Self::Shell => "shell",
            Self::Print => "print",
            Self::Transform => "transform",
            Self::Loop => "loop",
            Self::Http => "http",
            Self::Each => "each",
            Self::Trigger => "trigger",
            Self::Screenshot => "screenshot",
            Self::Passthrough => "passthrough",
            Self::Compact => "compact",
        }
    }

    pub fn schema_description(&self) -> &str {
        match self {
            Self::ReadInput => "Read raw user input without LLM involvement",
            Self::ReadVar => "Read variables from CLI args, env vars, or static values. Outputs |name, value| table — feeds directly into shell/trigger templates.",
            Self::Llm => "LLM call with optional tool-calling loop. Input must be |ts, role, content| table — use pre_input_sql to convert from |output|: SELECT CAST(extract(epoch FROM now()) * 1000 AS BIGINT) AS ts, 'user' AS role, output AS content FROM input",
            Self::Parallel => "Run multiple named branch pipelines concurrently. Each branch has a name and body (list of steps). Results are merged via merge_sql, which references branch outputs as named tables.",
            Self::Select => "Conditional branching — evaluate when conditions, run matching branches. Each branch body must output |name, output|.",
            Self::Shell => "Run a shell command via sh -c. stdout becomes the output. Supports multi-line commands (use YAML | block). Supports Jinja2 templates {{ var }} — input must be |name, value| table where each row's 'name' becomes a template variable.",
            Self::Print => "Print the previous step's output and pass it through",
            Self::Transform => "Run SQL (DataFusion) to reshape data between steps",
            Self::Loop => "Do-while loop: runs body, checks condition, repeats while true",
            Self::Http => "Make an HTTP request. Supports JQ for request/response transformation and declared output schema.",
            Self::Each => "Iterate input rows. Runs body steps once per row. Body must output |name, output| — use post_output_sql on last body step.",
            Self::Trigger => "Long-running subprocess emitting JSON lines on stdout. Runs body steps per line. Body must output |name, output|. Command supports Jinja2 templates {{ var }} from |name, value| input.",
            Self::Screenshot => "[Experimental] Capture the primary monitor screenshot with OCR. macOS only.",
            Self::Passthrough => "Forward input unchanged",
            Self::Compact => "Compress conversation history when it exceeds a word threshold. Summarizes oldest rows via LLM and replaces with a single system summary row. Passthrough if under threshold.",
        }
    }

    pub fn schema_input(&self) -> &str {
        match self {
            Self::ReadInput => "(ignored)",
            Self::ReadVar => "(from CLI args, env vars, or static values)",
            Self::Llm => "`ts, role, content` (override with `pre_input_sql`)",
            Self::Transform => "any (referenced as `input` in SQL)",
            Self::Http => "any (used by request_jq)",
            Self::Parallel => "passed to all branches",
            Self::Select => "passed to matching branches",
            Self::Shell => "`name, value` (when command uses templates) or ignored",
            Self::Print => "any",
            Self::Loop => "any (body output must match this schema)",
            Self::Each => "any (each row becomes body input)",
            Self::Trigger => "`name, value` (when command uses templates) or any",
            Self::Screenshot => "any (ignored)",
            Self::Passthrough => "any",
            Self::Compact => "`ts, role, content` (conversation history)",
        }
    }

    pub fn schema_output(&self) -> &str {
        match self {
            Self::ReadInput => "`output`",
            Self::ReadVar => "`name, value` rows (or SQL result if post_output_sql set)",
            Self::Llm => "`ts, role, content` (all messages from this call including tool calls, tool results, and final response. Use post_output_sql to extract last response: SELECT content AS output FROM input WHERE role = 'assistant' ORDER BY ts DESC LIMIT 1)",
            Self::Transform => "SQL result",
            Self::Http => "declared `output_schema` (requires response_jq)",
            Self::Parallel => "merge_sql result (joining named branch tables)",
            Self::Select => "`name, output` (or SQL result)",
            Self::Shell => "`output`",
            Self::Print => "passthrough (unchanged)",
            Self::Loop => "last body output (locked schema)",
            Self::Each => "|name, output| (one row per iteration, or SQL result if post_output_sql set)",
            Self::Trigger => "|name, output| (one row per event, or SQL result if post_output_sql set)",
            Self::Screenshot => "`provider_id, provider` (one row per provider, always includes storage)",
            Self::Passthrough => "same as input",
            Self::Compact => "`ts, role, content` (compacted or passthrough)",
        }
    }

    pub fn schema_attributes(&self) -> Vec<AttributeSchema> {
        match self {
            Self::ReadInput => vec![
                attr("post_output_sql", "SQL to transform output after the step", "string"),
            ],
            Self::Print => vec![],
            Self::ReadVar => vec![
                attr("vars", "Variable definitions with source (arg/env/value)", "var_def[]"),
                attr("post_output_sql", "SQL to transform the name/value output", "string"),
            ],
            Self::Llm => vec![
                attr("model", "Model override for this step", "string"),
                attr("tools", "Tools the LLM can call (e.g., shell, ask_user)", "tool_name[]"),
                attr("pre_input_sql", "SQL to transform input before the LLM call. Overrides the default SELECT role, content FROM input ORDER BY ts", "string"),
                attr("post_output_sql", "SQL to transform output after the LLM call", "string"),
            ],
            Self::Parallel => vec![
                attr("branches", "Named branches to run in parallel. Each has name and body (list of steps).", "parallel_branch[]"),
                attr("pre_input_sql", "SQL to transform input before distributing to branches", "string"),
                attr("merge_sql", "SQL to merge branch outputs. Each branch output is a named table.", "string"),
            ],
            Self::Select => vec![
                attr("match_mode", "first (default) or all", "string"),
                attr("branches", "Branches with when condition and steps", "select_branch[]"),
                attr("pre_input_sql", "SQL to transform input before distributing to branches", "string"),
                attr("post_output_sql", "SQL to combine/transform branch results", "string"),
                attr("prepare_input_for_when", "SQL to transform input for when conditions", "string"),
                attr("prepare_input_for_body", "SQL to transform input for branch bodies", "string"),
            ],
            Self::Shell => vec![
                attr("command", "Shell command to execute. Supports multi-line (YAML |) and Jinja2 {{ var }} templates.", "string"),
                attr("cwd", "Working directory. Absolute or relative to ~. Default: inherits from process.", "string"),
                attr("timeout", "Command timeout in seconds. No timeout by default.", "integer"),
                attr("post_output_sql", "SQL to transform output after the command", "string"),
            ],
            Self::Transform => vec![
                attr("sql", "SQL query to run. Input table is 'input'.", "string"),
            ],
            Self::Loop => vec![
                attr("pre_input_sql", "SQL to transform initial input into locked schema. Determines the schema for all iterations.", "string"),
                attr("body", "Body steps to run each iteration. Output schema must match locked schema.", "step[]"),
                attr("while_sql", "SQL on body output, must produce __continue column. true continues, false stops.", "string"),
            ],
            Self::Http => vec![
                attr("url", "Endpoint URL", "string"),
                attr("method", "HTTP method (GET, POST, PUT, DELETE, PATCH). Default: GET", "string"),
                attr("headers", "Static HTTP headers as key-value pairs", "map"),
                attr("headers_jq", "JQ expression returning header object from pipeline input. Merged with static headers (overrides).", "string"),
                attr("timeout", "Request timeout in seconds. No timeout by default.", "integer"),
                attr("request_jq", "JQ expression to transform pipeline input into request body JSON", "string"),
                attr("response_jq", "JQ expression to transform response JSON into output rows", "string"),
                attr("output_schema", "Declared output column names and types", "schema_field[]"),
            ],
            Self::Each => vec![
                attr("pre_input_sql", "SQL to filter/reshape rows before iteration", "string"),
                attr("post_output_sql", "SQL to reshape the final output table", "string"),
                attr("body", "Body steps to execute per row", "step[]"),
            ],
            Self::Trigger => vec![
                attr("command", "Long-running shell command emitting JSON lines on stdout. Supports {{ }} templates.", "string"),
                attr("pre_input_sql", "SQL to reshape input for command template rendering", "string"),
                attr("jq", "JQ to transform each stdout JSON line", "string"),
                attr("output_schema", "Declared output column names and types per event", "schema_field[]"),
                attr("post_output_sql", "SQL to reshape the final output table", "string"),
                attr("body", "Body steps to execute per event. Must output |name, output|.", "step[]"),
            ],
            Self::Screenshot => vec![
                attr("providers", "Additional providers: 'file' saves PNG to /tmp. Default: storage only.", "string[]"),
            ],
            Self::Passthrough => vec![],
            Self::Compact => vec![
                attr("max_words", "Word count threshold for compaction", "integer"),
                attr("compact_ratio", "Fraction of words to compact (0.1-1.0, default 0.5)", "float"),
                attr("model", "Model for summarization LLM call", "string"),
            ],
        }
    }

    pub fn schema_flow(&self) -> &str {
        match self {
            Self::ReadInput => "\
prev output (ignored)
  → prompt user (`prompt`)
  → |output|
  → `post_output_sql` (optional: reshape output)",

            Self::ReadVar => "\
`vars` (resolve each: `arg` from CLI, `env` from environment, `value` static)
  → fallback to `default` if source is empty
  → |name, value| rows
  → `post_output_sql` (optional: reshape output)",

            Self::Llm => "\
prev output
  → `pre_input_sql` (optional: default SELECT role, content FROM input ORDER BY ts)
  → LLM call (`prompt` = system prompt, `model` optional, `tools` optional)
  → |output|
  → `post_output_sql` (optional: reshape output)",

            Self::Transform => "\
prev output
  → `sql` (prev table available as 'input')
  → SQL result columns",

            Self::Shell => "\
prev output
  → if `command` has {{ }}: require |name, value| input, render template
  → sh -c `command` in `cwd` (optional, default: inherit) with `timeout` (optional)
  → stdout → |output|
  → `post_output_sql` (optional: reshape output)",

            Self::Http => "\
prev output
  → `request_jq` (optional: build request body JSON from input)
  → HTTP `method` (default GET) to `url`
    headers: `headers` (optional static) + `headers_jq` (optional dynamic, overrides)
    timeout: `timeout` (optional)
  → response JSON
  → `response_jq` (optional: transform response)
  → `output_schema` (required: declare column names and types)
  → |declared columns|",

            Self::Print => "\
prev output
  → print to stdout
  → passthrough (unchanged)",

            Self::Parallel => "\
prev output
  → `pre_input_sql` (optional: reshape input for all branches)
  → run all named `branches` concurrently (each branch = name + body steps)
  → each branch output registered as a named table
  → `merge_sql` (join branch tables into final output)
  → SQL result columns",

            Self::Select => "\
prev output
  → `pre_input_sql` (optional: reshape input)
  → `prepare_input_for_when` (optional: reshape for condition eval)
  → evaluate each branch's `when` condition
  → `match_mode`: 'first' (default) runs first match, 'all' runs all matches
  → `prepare_input_for_body` (optional: reshape for branch body)
  → run matching `branches` `body`
  → |name, output| (one row per matched branch)
  → `post_output_sql` (optional: reshape/combine results)",

            Self::Loop => "\
prev output
  → `pre_input_sql` (optional: transform initial input into locked schema)
  → do:
      run `body` (body pipeline, output must match locked schema)
    while:
      `while_sql` on body output produces `__continue` = true
  → last body output (locked schema)",

            Self::Each => "\
prev output (table with N rows)
  → `pre_input_sql` (optional: filter/reshape rows before iteration)
  → for each row:
      → single-row table (same schema as input)
      → run `body` (body pipeline)
      → body must output |name, output|
  → concatenate all |name, output| rows
  → `post_output_sql` (optional: reshape final output)",

            Self::Trigger => "\
prev output
  → `pre_input_sql` (optional: reshape into |name, value| for templates)
  → render `command` template (if {{ }} present, requires |name, value|)
  → spawn subprocess (stdout piped, stdin closed)
  → read stdout line by line:
      → skip non-JSON lines
      → parse JSON
      → `jq` (optional: transform JSON)
      → `output_schema` (required: JSON → Arrow batch)
      → run `body` (body pipeline)
      → body must output |name, output|
  → concatenate all |name, output| rows
  → `post_output_sql` (optional: reshape final output)",

            Self::Screenshot => "\
prev output (ignored)
  → capture primary monitor → PNG bytes
  → store in blob storage (always)
  → `providers` (optional: 'file' saves PNG to /tmp)
  → |provider_id, provider| (one row per provider)",

            Self::Passthrough => "\
Input → Output (unchanged)",

            Self::Compact => "\
prev output (|ts, role, content|)
  → count total words across content values
  → if under `max_words`: passthrough unchanged
  → select oldest rows up to `compact_ratio` fraction
  → LLM summarizes selected rows (`model`)
  → replace selected rows with system summary row
  → |ts, role, content| (compacted)",
        }
    }
}

impl Action {
    pub fn all_schemas() -> Vec<ActionSchema> {
        ActionDiscriminants::iter()
            .map(|d| ActionSchema {
                name: d.schema_name().into(),
                description: d.schema_description().into(),
                input_schema: d.schema_input().into(),
                output_schema: d.schema_output().into(),
                flow: d.schema_flow().into(),
                attributes: d.schema_attributes(),
            })
            .collect()
    }

    pub fn name(&self) -> Option<&str> {
        match self {
            Action::Llm { name, .. } => name.as_deref(),
            Action::ReadInput { name, .. } => name.as_deref(),
            Action::ReadVar { name, .. } => name.as_deref(),
            Action::Shell { name, .. } => name.as_deref(),
            Action::Print { name, .. } => name.as_deref(),
            Action::Transform { name, .. } => name.as_deref(),
            Action::Parallel { name, .. } => name.as_deref(),
            Action::Select { name, .. } => name.as_deref(),
            Action::Loop { name, .. } => name.as_deref(),
            Action::Http { name, .. } => name.as_deref(),
            Action::Each { name, .. } => name.as_deref(),
            Action::Trigger { name, .. } => name.as_deref(),
            Action::Screenshot { name, .. } => name.as_deref(),
            Action::Passthrough { name, .. } => name.as_deref(),
            Action::Compact { name, .. } => name.as_deref(),
        }
    }

    pub fn display_name(&self) -> String {
        if let Some(name) = self.name() {
            return name.to_string();
        }
        match self {
            Action::ReadInput { .. } => "read_input".to_string(),
            Action::ReadVar { .. } => "read_var".to_string(),
            Action::Llm { model, .. } => match model {
                Some(m) => format!("llm ({m})"),
                None => "llm".to_string(),
            },
            Action::Shell { command, .. } => {
                let short = if command.len() > 30 { &command[..30] } else { command };
                format!("shell ({})", short)
            }
            Action::Print { .. } => "print".to_string(),
            Action::Transform { sql, .. } => {
                let short = if sql.len() > 30 { &sql[..30] } else { sql };
                format!("transform ({})", short)
            }
            Action::Parallel { .. } => "parallel".to_string(),
            Action::Select { .. } => "select".to_string(),
            Action::Loop { .. } => "loop".to_string(),
            Action::Http { url, .. } => {
                let short = if url.len() > 30 { &url[..30] } else { url };
                format!("http ({})", short)
            }
            Action::Each { .. } => "each".to_string(),
            Action::Trigger { command, .. } => {
                let short = if command.len() > 30 { &command[..30] } else { command };
                format!("trigger ({})", short)
            }
            Action::Screenshot { .. } => "screenshot".to_string(),
            Action::Passthrough { .. } => "passthrough".to_string(),
            Action::Compact { max_words, .. } => format!("compact ({}w)", max_words),
        }
    }
}
