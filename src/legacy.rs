//! Legacy workflow-era code — kept for reference.
//!
//! Pilot pivoted away from the YAML-workflow CLI surface to two things:
//!   - `pilot agent <file.md>` (see src/agent.rs) — prompt + scoped tools
//!   - `pilot tool <name>`    (see src/tool_cli.rs) — standalone CLI tools
//!
//! Both reuse the workflow runtime under the hood. What got removed from
//! the CLI is the `run` / `ls` / `explain` / `update` subcommands and the
//! thin helper for workflow resolution / workflow-only help text.
//!
//! Currently-active CLI-host adapters (StdinIO, TracingEvents) moved to
//! `src/cli_io.rs` because they're reused by the agent path. This module
//! retains only the pieces that are truly dead: the workflow-path resolver
//! and the original workflow-era help text.

#![allow(dead_code)]

use std::path::PathBuf;

use colored::Colorize;

// --------------------------------------------------------------------------
// Workflow resolution — was shared by the removed run / update subcommands
// --------------------------------------------------------------------------

/// Resolve a workflow name/path to a file path.
/// If arg is Some: try as file path, then as name via discovery.
/// If arg is None: show interactive fuzzy picker.
pub fn resolve_workflow(arg: Option<String>) -> PathBuf {
    use dialoguer::FuzzySelect;
    use pilot::discovery::{discover_workflows, tilde_path};

    match arg {
        Some(arg) => {
            let path = PathBuf::from(&arg);
            if path.exists() {
                return path;
            }

            let dirs = discover_workflows();
            for dir in &dirs {
                if dir.workflows.contains(&arg) {
                    return dir.abs_path.join(format!("{}.yaml", arg));
                }
            }
            for dir in &dirs {
                let yml_path = dir.abs_path.join(format!("{}.yml", arg));
                if yml_path.exists() {
                    return yml_path;
                }
            }

            eprintln!("{} workflow '{}' not found", "error:".red(), arg);
            std::process::exit(1);
        }
        None => {
            let dirs = discover_workflows();
            if dirs.is_empty() {
                eprintln!("{}", "No .pilot/ directories found.".dimmed());
                std::process::exit(1);
            }

            let mut items: Vec<(String, PathBuf)> = Vec::new();
            let max_name_len = dirs
                .iter()
                .flat_map(|d| d.workflows.iter())
                .map(|n| n.len())
                .max()
                .unwrap_or(0);

            for dir in &dirs {
                let source = if dir.is_cwd {
                    ".pilot/".to_string()
                } else {
                    tilde_path(&dir.abs_path).unwrap_or_else(|| dir.abs_path.display().to_string())
                };
                for name in &dir.workflows {
                    let display = format!(
                        "{:<width$}  {}",
                        name,
                        source.dimmed(),
                        width = max_name_len
                    );
                    let path = dir.abs_path.join(format!("{}.yaml", name));
                    items.push((display, path));
                }
            }

            if items.is_empty() {
                eprintln!("{}", "No workflows found.".dimmed());
                std::process::exit(1);
            }

            let display_items: Vec<&str> = items.iter().map(|(d, _)| d.as_str()).collect();
            let selection = FuzzySelect::new()
                .with_prompt("Select workflow")
                .items(&display_items)
                .interact_opt()
                .unwrap_or(None);

            match selection {
                Some(idx) => items[idx].1.clone(),
                None => std::process::exit(0),
            }
        }
    }
}

// --------------------------------------------------------------------------
// Original `pilot help` output — the workflow-era overview.
// --------------------------------------------------------------------------

pub const HELP_TEXT_LEGACY_WORKFLOW: &str = "\
OVERVIEW:
  Pilot is an AI scripting tool. Users define multi-step workflows in YAML.
  Each step has an action type that determines what it does. At runtime,
  pilot executes steps in order. Data flows between steps as tables with
  columns and rows.

DATA MODEL:
  Steps produce Arrow tables. Each step's output becomes the next step's
  input. Most steps output a table with a single 'output' column (string).
  Use 'transform' steps with SQL to reshape data between steps.
  'pre_input_sql' and 'post_output_sql' fields on steps transform data
  inline without a separate step.

ACTION TYPES:
  I/O:
    read_input   Prompt user for text input
    read_var     Read from CLI args, env vars, or static values
    print        Print output to stdout, pass through unchanged

  LLM:
    llm          LLM call with optional tool use (shell, file, etc.)

  Data:
    transform    Run SQL (DataFusion) to reshape tables
    http         HTTP request with JQ transformation and typed output
    shell        Run a shell command, stdout becomes output

  Control flow:
    loop         Do-while loop with SQL condition
    each         Iterate input rows, run body per row
    parallel     Run named branches concurrently, merge via SQL
    select       Conditional branching (evaluate when conditions)
    trigger      Long-running subprocess, run body per stdout JSON line
    passthrough  Forward input unchanged
    compact      Compress conversation history via LLM summarization

  Run 'pilot help actions' for detailed action reference (attributes, data flow).
  Run 'pilot help tools' for LLM tool reference (config, allowed patterns).
  Run 'pilot explain <workflow>' to understand a specific workflow.

YAML STRUCTURE:
  A workflow file has a top-level 'steps' array and optional 'model' field:

    steps:
      - action: read_input
        prompt: What do you want to do?
      - action: llm
        prompt: Help the user with their request
      - action: print

WORKFLOW DESIGN:
  Steps are goals, not API calls. Each step represents a mental phase of
  the task — \"see what reviewers said\", \"triage\", \"fix\", \"reply\" — not
  individual commands like \"gh api\" or \"git commit\".

  The LLM does the work. YAML declares what to achieve and what's allowed.
  Don't hardcode commands in shell steps — give the LLM tools (shell with
  allowed patterns, file with read/write globs) and let it figure out how.

  Data flows as text between LLM steps. One LLM's response is the next
  LLM's input context. Don't overthink structured schemas between steps.

  If a step is pure reasoning (triage, classification), it needs no tools.
  Only add tools when the LLM needs to interact with the outside world.

COMMON PATTERNS:
  LLM input: llm expects |ts, role, content| table. Convert from |output|:
    pre_input_sql: \"SELECT CAST(extract(epoch FROM now()) * 1000 AS BIGINT)
      AS ts, 'user' AS role, output AS content FROM input\"

  Shell/trigger templates: use Jinja2 syntax {{ var_name }}. Input must be
    |name, value| table — each row's 'name' becomes a template variable.
    read_var outputs |name, value| natively. Example:
      - action: read_var
        vars: [{name: url, arg: url}]
      - action: shell
        command: \"curl -s '{{ url }}'\"

  Structural node body contract: each and select default to
    |name, output| body output. Override with output_schema:
      output_schema:
        - name: status
          type: string

  LLM tools: tools are objects with 'name' and tool-specific config.
    Shell requires 'allowed' regex patterns:
      tools:
        - name: ask_user
        - name: shell
          allowed: [\"^git (status|diff)\", \"^curl \"]

WORKFLOW DISCOVERY:
  Workflows are YAML files in .pilot/ directories. Pilot searches the
  current directory and parents. Run 'pilot ls' to list available workflows.

EXPOSED TOOLS:
  Tools can be exposed as standalone CLI commands via .pilot/tools/<name>.yaml.
  Each file defines a single tool (s3 or file) with scoped permissions:

    # .pilot/tools/todos.yaml — S3-backed
    name: s3
    bucket: mine
    read: [\"*\"]
    write: [\"projects/pilot/*\"]

    # .pilot/tools/mynotes.yaml — local file-backed
    name: file
    read: [\"~/notes/**\"]
    write: [\"~/notes/**\"]
    semantic_index: true   # opt-in, enables `--operation search --query ...`

  Invoke with: pilot tool <name> --key value
  Supports dot notation for nesting: --edit.search \"old\" --edit.replace \"new\"
  Run 'pilot tool <name>' with no args to see parameters.
  Run 'pilot help tools' for tool types and required config.
  Discovery: searches .pilot/tools/ in current directory and parents.

EXAMPLE USE CASES:
  - Code review: parallel LLM analysis (security + architecture), synthesize
  - Data extraction: fetch APIs, LLM extracts structured data from responses
  - Monitoring: trigger watches a command, LLM evaluates each event
  - File processing: each iterates files, LLM processes individually
  - Interactive assistant: loop with parallel + passthrough for multi-turn conversation
  - DevOps automation: read_var for parameters, LLM orchestrates shell commands
  - Research: iterative search loop, LLM generates queries and synthesizes
  - Multi-model comparison: same prompt to multiple models via parallel";
