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

## Why

LLM agents get more capable every month, and the blast radius grows
with them. Most frameworks bury tool permissions inside code that's
hard to audit. Pilot's bet is that the format matters more than the
framework: if your agent can shell out, scope the shell with a regex;
if it can touch files, scope the glob. The permission policy is the
first thing you see when you open the YAML.

## Commands

```
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
node, Arrow tables between steps, a full pipeline story. That code is
still in the repo for reference (`src/legacy.rs` for the CLI glue;
`src/runner.rs`, `src/workflow/`, `src/dag_v2/` for the engine) but
is no longer wired into the CLI. Smart models plus agent frameworks
(Claude Code et al.) absorbed the orchestration role; the permission
layer — scoped tools — is the part that kept earning its keep.

## License

MIT
