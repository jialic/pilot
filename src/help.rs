use colored::Colorize;
use strum::IntoEnumIterator;

use crate::workflow::{Action, ToolName};

/// New default help text — tool-broker focus.
///
/// Pilot pivoted away from YAML workflows (the workflow engine, runner,
/// and related subcommands are kept in the source for reference but are
/// no longer exposed via the CLI). Today pilot's job is to expose
/// permission-scoped tools that other AI assistants (Claude Code, etc.)
/// can invoke safely.
pub const HELP_TEXT: &str = "\
OVERVIEW:
  Pilot exposes permission-scoped tools that AI assistants (Claude Code,
  Cursor, MCP-aware agents) can invoke. You declare what a tool can
  read/write in a YAML file; pilot enforces the scope at runtime.

  Read the YAML, know the worst case.

DEFINE A TOOL:
  Each tool is a single YAML file in .pilot/tools/<name>.yaml. It
  names a tool type (s3, file, ...) and scopes it.

    # .pilot/tools/todos.yaml — S3-backed with scoped writes
    name: s3
    bucket: mine
    read:  [\"*\"]
    write: [\"projects/pilot/*\"]

    # .pilot/tools/notes.yaml — local files with semantic search
    name: file
    read:  [\"~/notes/**\"]
    write: [\"~/notes/**\"]
    semantic_index: true   # opt-in; enables --operation search --query ...

  Discovery: pilot searches .pilot/tools/ in the current directory and
  parents. Run 'pilot help tools' for tool types and required config.

INVOKE A TOOL:
  pilot tool <name>                      Show parameters for a tool
  pilot tool <name> --key value ...      Run the tool with arguments
    Dot notation for nesting: --edit.search \"old\" --edit.replace \"new\"

COMMANDS:
  pilot tool <name> [--key value ...]    Call an exposed tool
  pilot config set <key> <value>         Configure API keys / credentials
  pilot config show                      Show current config (masked)
  pilot test                             Verify API connections
  pilot help tools                       Reference for tool types
  pilot help models                      Supported models

NOTES:
  The YAML-workflow runtime (pilot run / pilot ls / pilot explain /
  pilot update) is no longer exposed. Smarter models + agent frameworks
  (Claude Code etc.) subsume the orchestration role; pilot's remaining
  value is the auditable permission layer for tool access.";

pub fn print_actions() {
    let schemas = Action::all_schemas();
    for schema in &schemas {
        println!("{}", schema.name.bold());
        println!("  {}", schema.description);
        println!();
        println!("  {}", "Flow:".dimmed());
        for line in schema.flow.lines() {
            println!("    {}", line);
        }
        println!();
        if !schema.attributes.is_empty() {
            println!("  {}", "Attributes:".dimmed());
            for attr in &schema.attributes {
                println!("    {} ({}): {}", attr.name.bold(), attr.value_type.dimmed(), attr.description);
            }
            println!();
        }
        println!("  {}: {}", "Input".dimmed(), schema.input_schema);
        println!("  {}: {}", "Output".dimmed(), schema.output_schema);
        println!();
    }

}

pub fn print_tools() {
    for tool in ToolName::iter() {
        println!("{}", tool.name().bold());
        println!("  {}", tool.description());
        let attrs = tool.tool_attributes();
        if !attrs.is_empty() {
            println!();
            println!("  {}", "Config:".dimmed());
            for (name, desc) in &attrs {
                println!("    {}: {}", name.bold(), desc);
            }
        }
        let req = tool.required_config();
        if !req.is_empty() {
            println!();
            println!("  {}", "Required config (pilot config set):".dimmed());
            for (key, desc) in &req {
                println!("    {}: {}", key.bold(), desc);
            }
        }
        if let Some(yaml) = tool.exposed_yaml() {
            println!();
            println!("  {}", "Exposed as CLI (.pilot/tools/<name>.yaml):".dimmed());
            for line in yaml.lines() {
                println!("    {}", line);
            }
        }
        println!();
    }
}
