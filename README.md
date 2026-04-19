# Pilot

Permission-scoped tools for AI assistants.

Declare what a tool can read and write in YAML. Pilot enforces the
scope at runtime. Claude Code, Cursor, or any other agent with shell
access can invoke your tools without being able to step outside the
boundaries you defined.

**Read the YAML, know the worst case.**

```yaml
# .pilot/tools/notes.yaml — local files scoped to one directory
name: file
read:  ["~/notes/**"]
write: ["~/notes/**"]
semantic_index: true   # opt-in; enables --operation search --query ...
```

```yaml
# .pilot/tools/todos.yaml — S3-backed, scoped writes
name: s3
bucket: mine
read:  ["*"]
write: ["projects/pilot/*"]
```

Invoke:

```
$ pilot tool notes --operation read --path ~/notes/today.md
$ pilot tool todos --operation edit --path projects/pilot/next.md \
                   --edit.search "TODO: ship" --edit.replace "DONE"
```

## Install

```
cargo install --git https://github.com/jialic/pilot
```

## Quick Start

Configure credentials as needed (semantic-index tools need an OpenAI
or Anthropic key for embeddings; S3 tools need bucket credentials):

```
pilot config set openai_api_key sk-...
pilot config show
```

Drop a tool definition at `.pilot/tools/<name>.yaml`:

```yaml
name: file
read:  ["~/code/**"]
write: []    # read-only
```

Invoke:

```
pilot tool <name>                      # show parameters for the tool
pilot tool <name> --key value ...      # run it
```

Dot notation for nested args: `--edit.search "old" --edit.replace "new"`.
Pilot searches `.pilot/tools/` in the current directory and parents,
so tools can be per-project, per-user (`~/.pilot/tools/`), or a mix.

## Agents

An agent is a markdown file. The body is the task; optional YAML
frontmatter declares which model runs it and which tools it can use.
Frontmatter tool entries use the same schema as `.pilot/tools/<name>.yaml` —
reference a tool by name or define one inline.

```markdown
---
model: anthropic/claude-opus-4-7
tools:
  - notes                       # reference .pilot/tools/notes.yaml
  - name: scratch                # inline definition
    type: file
    read:  ["/tmp/**"]
    write: ["/tmp/**"]
---

Read today's notes. Summarize the top 3 actionable items in under
200 words, markdown.
```

Run it:

```
pilot agent morning.md
```

No frontmatter required — a file with just a prompt body also works
(uses the configured default model, no tools).

## Why

LLM agents get more capable every month, and the blast radius grows
with them. Most frameworks bury tool permissions inside code that's
hard to audit. Pilot's bet is that the format matters more than the
framework: if your agent can shell out, scope the shell with a regex;
if it can touch files, scope the glob. The permission policy is the
first thing you see when you open the YAML — whether it's a tool file
or an agent file.

## Commands

```
pilot agent <file.md>                  Run an agent (prompt + scoped tools)
pilot tool <name> [--key value ...]    Call an exposed tool
pilot config set <key> <value>         Configure API keys / credentials
pilot config show                      Show current config (masked)
pilot test                             Verify API connections
pilot help                             Overview
pilot help tools                       Tool types + required config
pilot help models                      Supported models
```

## History

Pilot started as a YAML workflow engine — steps as goals, LLM as one
node, Arrow tables between steps, a full pipeline story. That runtime
is still here (`src/runner.rs`, `src/workflow/`, `src/dag_v2/`), now
used internally: `pilot agent <file.md>` synthesizes a single-step
workflow under the hood so agents reuse the battle-tested core.

The workflow-era CLI subcommands (`run` / `ls` / `explain` / `update`)
were removed from the user surface; their glue lives in
`src/legacy.rs` for reference. Smart models plus agent frameworks
(Claude Code et al.) absorbed explicit orchestration; the permission
layer — scoped tools — and the single-prompt agent runner are the
pieces that kept earning their keep.

## License

MIT
