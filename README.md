# Pilot

AI workflows in YAML. Your prompts are the code.

- Prompts are the logic. Edit YAML, run, iterate — no rebuild, no deploy. No framework, no abstractions.
- Fearless workflows. Read the YAML, know the worst case. Each step declares what the LLM can do.
- Batteries included. Loops, branching, parallel, data transformation — all in YAML. Shell and HTTP when you need custom logic.

```yaml
steps:
  - action: read_input
    prompt: What do you want to research?
  - action: llm
    tools:
      - name: shell
        allowed: ["^curl ", "^jq "]
    prompt: Research this topic using web APIs. Summarize your findings.
  - action: llm
    prompt: Write a brief report based on the research above.
```

```
$ pilot run research.yaml
```

## Install

```
cargo install --git https://github.com/jialic/pilot
```

## Quick Start

Configure your API key:

```
pilot config set openai_api_key sk-...
```

Run an example:

```
pilot run examples/chat.yaml
```

## Examples

Coding agent with scoped permissions — you can see exactly what it can do:
```yaml
steps:
  - action: read_input
    prompt: What do you want me to do?
  # Loop: runs body, then checks while_sql. Body output is available as 'input'.
  # __continue column controls whether to loop. Stops when LLM outputs "done".
  - action: loop
    while_sql: "SELECT CASE WHEN output = 'done' THEN 'false' ELSE 'true' END AS __continue FROM input"
    body:
      - action: llm
        tools:
          - name: file
            read: ["src/**", "tests/**", "*.toml"]
            write: ["src/**", "tests/**"]
          - name: shell
            allowed: ["^cargo (build|test|check)", "^git diff"]
        prompt: |
          Read the relevant code, make the requested changes,
          and run tests to verify. Output "done" when finished.
```

More examples: [chat](examples/chat.yaml), [code review](examples/code-review.yaml), [multi-model comparison](examples/second-opinion.yaml), [each](examples/each.yaml), [parallel](examples/parallel.yaml), [select](examples/select.yaml), [trigger](examples/trigger.yaml)

## Learn More

```
pilot help             # overview
pilot help actions     # all action types
pilot help tools       # all LLM tools
pilot help models      # supported models
pilot explain <name>   # explain a workflow
```

## License

MIT
