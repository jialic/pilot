//! Agent = prompt file + scoped tools. The primary user-facing concept.
//!
//! An agent file is plain markdown with optional YAML frontmatter:
//!
//! ```markdown
//! ---
//! model: anthropic/claude-opus-4-7
//! tools:
//!   - notes                # string → reference .pilot/tools/notes.yaml
//!   - name: scratch        # object → inline ToolDef, same schema as tool YAML
//!     type: file
//!     read: ["/tmp/**"]
//!     write: ["/tmp/**"]
//! ---
//!
//! <prompt body>
//! ```
//!
//! The body becomes the LLM's system prompt; the tools list is passed to the
//! single-step `llm` action that runs the agent loop.
//!
//! Implementation reuses the workflow runtime (workflow + runner + llm action
//! with tool-calling loop) by synthesizing a minimal single-step workflow at
//! runtime. That's intentional — the workflow engine is the battle-tested
//! primitive; we just wrap it ergonomically.
//!
//! See also: `src/legacy.rs` for the workflow-era CLI glue kept for reference,
//! and `src/workflow/types.rs` for the underlying Workflow/Action/ToolDef
//! definitions.

use std::path::{Path, PathBuf};

use serde::Deserialize;

use crate::workflow::{Action, Step, ToolDef, Workflow};

/// A parsed agent file: structured frontmatter + the raw prompt body.
#[derive(Debug)]
pub struct AgentFile {
    pub frontmatter: AgentFrontmatter,
    pub prompt: String,
}

/// Agent frontmatter fields. Everything is optional — a file with no
/// frontmatter at all is a valid agent (prompt-only, no tools, default model).
#[derive(Debug, Default, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct AgentFrontmatter {
    /// Model override (format: "provider/model-name").
    pub model: Option<String>,
    /// Free-form description for the reader; ignored at runtime.
    pub description: Option<String>,
    /// Tools the agent can call. None = no tools. Empty vec = no tools.
    /// String entries resolve to `.pilot/tools/<name>.yaml`; object entries
    /// are parsed as an inline ToolDef using the same schema as a workflow
    /// `llm` step's `tools` field.
    pub tools: Option<Vec<ToolRef>>,
}

/// Either a reference by name (resolved from `.pilot/tools/`) or an inline
/// full ToolDef. Serde's untagged enum dispatches on shape: string vs. map.
#[derive(Debug, Deserialize)]
#[serde(untagged)]
pub enum ToolRef {
    Name(String),
    Inline(ToolDef),
}

/// Parse an agent file from disk.
pub fn parse_agent(path: &Path) -> Result<AgentFile, String> {
    let content = std::fs::read_to_string(path)
        .map_err(|e| format!("read {}: {e}", path.display()))?;
    parse_agent_str(&content)
}

/// Parse an agent file from a string. Splits on `---` frontmatter
/// delimiters; if no frontmatter is present the whole input is the prompt.
pub fn parse_agent_str(content: &str) -> Result<AgentFile, String> {
    // Normalize line endings just for the delimiter scan.
    let normalized = content.replace("\r\n", "\n");

    if let Some(rest) = normalized.strip_prefix("---\n") {
        if let Some(end_idx) = rest.find("\n---\n") {
            let (yaml, after) = rest.split_at(end_idx);
            // Skip past the closing "\n---\n".
            let body = &after[5..];
            let fm: AgentFrontmatter = serde_yaml::from_str(yaml)
                .map_err(|e| format!("parse frontmatter: {e}"))?;
            return Ok(AgentFile {
                frontmatter: fm,
                prompt: body.trim_start_matches('\n').to_string(),
            });
        }
        // Opening `---` with no close — treat as bad input rather than silently
        // swallowing, so the user notices the typo.
        return Err("frontmatter opened with `---` but no closing `---` found".to_string());
    }

    Ok(AgentFile {
        frontmatter: AgentFrontmatter::default(),
        prompt: content.to_string(),
    })
}

/// Resolve a tool refs list into a flat list of ToolDefs ready to pass to
/// the workflow `llm` action.
pub fn resolve_tools(refs: Option<Vec<ToolRef>>) -> Result<Vec<ToolDef>, String> {
    let Some(refs) = refs else { return Ok(vec![]) };
    refs.into_iter().map(resolve_one).collect()
}

fn resolve_one(r: ToolRef) -> Result<ToolDef, String> {
    match r {
        ToolRef::Inline(def) => Ok(def),
        ToolRef::Name(name) => {
            let path = find_tool_yaml(&name)?;
            let content = std::fs::read_to_string(&path)
                .map_err(|e| format!("read {}: {e}", path.display()))?;
            serde_yaml::from_str(&content)
                .map_err(|e| format!("parse {}: {e}", path.display()))
        }
    }
}

/// Walk up from cwd looking for `.pilot/tools/<name>.{yaml,yml}`.
/// Mirrors the same discovery rule the `pilot tool` CLI uses.
fn find_tool_yaml(name: &str) -> Result<PathBuf, String> {
    let mut dir =
        std::env::current_dir().map_err(|e| format!("cannot get cwd: {e}"))?;
    loop {
        for ext in ["yaml", "yml"] {
            let path = dir.join(".pilot").join("tools").join(format!("{name}.{ext}"));
            if path.exists() {
                return Ok(path);
            }
        }
        if !dir.pop() {
            break;
        }
    }
    Err(format!(
        "tool '{name}' not found in any .pilot/tools/ directory"
    ))
}

/// Build a single-step workflow equivalent to the agent. The synthesized
/// workflow has one `llm` step with the agent's prompt as the user message,
/// the resolved tools, and the frontmatter model.
///
/// Body-as-user convention: the markdown body describes the task, which
/// matches how agent invocations are normally shaped (system = identity /
/// persona, user = task). System prompt is empty for now — can be added
/// later via a frontmatter `system:` field if needed.
pub fn build_workflow(agent: AgentFile, resolved_tools: Vec<ToolDef>) -> Workflow {
    // The llm action expects input shaped as `|ts, role, content|`. Our
    // synthesized workflow has no prior step, so we seed it with the body
    // as the user turn. SQL requires escaping single quotes in the literal.
    let escaped = agent.prompt.replace('\'', "''");
    let seed_user_turn = format!(
        "SELECT \
         CAST(extract(epoch FROM now()) * 1000 AS BIGINT) AS ts, \
         'user' AS role, \
         '{}' AS content",
        escaped
    );

    // Extract the final assistant message as the workflow's output row, so
    // callers get the agent's answer (not the full conversation transcript).
    let extract_last = "SELECT content AS output \
                        FROM input \
                        WHERE role = 'assistant' \
                        ORDER BY ts DESC LIMIT 1"
        .to_string();

    Workflow {
        model: agent.frontmatter.model,
        steps: vec![Step {
            action: Action::Llm {
                // Empty system prompt — the agent body IS the user turn.
                prompt: String::new(),
                name: None,
                model: None,
                context: vec![],
                tools: resolved_tools,
                pre_input_sql: Some(seed_user_turn),
                post_output_sql: Some(extract_last),
            },
        }],
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_file_with_no_frontmatter() {
        let body = "just a plain prompt\nwith some lines\n";
        let a = parse_agent_str(body).unwrap();
        assert!(a.frontmatter.model.is_none());
        assert!(a.frontmatter.tools.is_none());
        assert_eq!(a.prompt, body);
    }

    #[test]
    fn parses_file_with_frontmatter() {
        let src = "---\nmodel: anthropic/claude-opus-4-7\n---\nHello.\n";
        let a = parse_agent_str(src).unwrap();
        assert_eq!(
            a.frontmatter.model.as_deref(),
            Some("anthropic/claude-opus-4-7")
        );
        assert_eq!(a.prompt, "Hello.\n");
    }

    #[test]
    fn parses_tools_mixed_name_and_inline() {
        let src = "---\ntools:\n  - notes\n  - name: file\n    read: [\"/tmp/**\"]\n    write: []\n---\nbody";
        let a = parse_agent_str(src).unwrap();
        let tools = a.frontmatter.tools.expect("tools present");
        assert_eq!(tools.len(), 2);
        match &tools[0] {
            ToolRef::Name(n) => assert_eq!(n, "notes"),
            _ => panic!("expected Name"),
        }
        match &tools[1] {
            ToolRef::Inline(ToolDef::File { read, write, .. }) => {
                assert_eq!(read, &vec!["/tmp/**".to_string()]);
                assert!(write.is_empty());
            }
            _ => panic!("expected File inline"),
        }
    }

    #[test]
    fn missing_closing_delimiter_is_error() {
        let src = "---\nmodel: x\n(no closing)";
        let err = parse_agent_str(src).unwrap_err();
        assert!(err.contains("closing"));
    }

    #[test]
    fn resolve_empty_gives_empty() {
        assert!(resolve_tools(None).unwrap().is_empty());
        assert!(resolve_tools(Some(vec![])).unwrap().is_empty());
    }
}
