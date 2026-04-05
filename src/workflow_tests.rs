use super::*;

#[test]
fn reject_no_steps() {
    let yaml = "steps: []";
    let wf: Workflow = serde_yaml::from_str(yaml).unwrap();
    let err = validate(&wf).unwrap_err();
    assert!(matches!(err, WorkflowError::NoSteps));
}

#[test]
fn reject_unknown_action() {
    let yaml = r#"
steps:
  - action: unknown_thing
    prompt: do something
"#;
    let result: Result<Workflow, _> = serde_yaml::from_str(yaml);
    assert!(result.is_err());
}

#[test]
fn reject_unknown_model() {
    let yaml = r#"
model: fake/model
steps:
  - action: read_input
    prompt: hello
"#;
    let wf: Workflow = serde_yaml::from_str(yaml).unwrap();
    let err = validate(&wf).unwrap_err();
    assert!(matches!(err, WorkflowError::UnknownModel(_)));
}

#[test]
fn accept_valid_model() {
    let yaml = r#"
model: openai/gpt-5-mini
steps:
  - action: read_input
    prompt: hello
"#;
    let wf: Workflow = serde_yaml::from_str(yaml).unwrap();
    assert!(validate(&wf).is_ok());
}

#[test]
fn parse_read_input() {
    let yaml = r#"
steps:
  - action: read_input
    prompt: What is your question?
"#;
    let wf: Workflow = serde_yaml::from_str(yaml).unwrap();
    assert!(validate(&wf).is_ok());
    let Action::ReadInput { ref prompt, .. } = wf.steps[0].action else { panic!("expected ReadInput") };
    assert_eq!(prompt, "What is your question?");
}

#[test]
fn parse_llm_with_model() {
    let yaml = r#"
steps:
  - action: llm
    prompt: test prompt
    model: openai/gpt-5-mini
"#;
    let wf: Workflow = serde_yaml::from_str(yaml).unwrap();
    assert!(validate(&wf).is_ok());
    if let Action::Llm { ref model, .. } = wf.steps[0].action {
        assert_eq!(model.as_deref(), Some("openai/gpt-5-mini"));
    } else {
        panic!("expected Llm action");
    }
}

#[test]
fn parse_llm_without_model() {
    let yaml = r#"
steps:
  - action: llm
    prompt: test prompt
"#;
    let wf: Workflow = serde_yaml::from_str(yaml).unwrap();
    assert!(validate(&wf).is_ok());
    if let Action::Llm { ref model, .. } = wf.steps[0].action {
        assert!(model.is_none());
    } else {
        panic!("expected Llm action");
    }
}

#[test]
fn reject_llm_with_invalid_model() {
    let yaml = r#"
steps:
  - action: llm
    prompt: test prompt
    model: fake/nope
"#;
    let wf: Workflow = serde_yaml::from_str(yaml).unwrap();
    let err = validate(&wf).unwrap_err();
    assert!(matches!(err, WorkflowError::UnknownModel(_)));
}

#[test]
fn parse_parallel() {
    let yaml = r#"
steps:
  - action: parallel
    merge_sql: "SELECT gpt.output AS gpt, claude.output AS claude FROM gpt, claude"
    branches:
      - name: gpt
        body:
          - action: llm
            prompt: test prompt
            model: openai/gpt-5-mini
            post_output_sql: "SELECT output FROM input"
      - name: claude
        body:
          - action: llm
            prompt: test prompt
            model: anthropic/claude-haiku-4-5
            post_output_sql: "SELECT output FROM input"
"#;
    let wf: Workflow = serde_yaml::from_str(yaml).unwrap();
    assert!(validate(&wf).is_ok());
    if let Action::Parallel { ref branches, ref merge_sql, .. } = wf.steps[0].action {
        assert_eq!(branches.len(), 2);
        assert_eq!(branches[0].name, "gpt");
        assert_eq!(branches[1].name, "claude");
        assert!(merge_sql.contains("gpt"));
    } else {
        panic!("expected Parallel action");
    }
}

#[test]
fn parse_passthrough() {
    let yaml = r#"
steps:
  - action: passthrough
"#;
    let wf: Workflow = serde_yaml::from_str(yaml).unwrap();
    assert!(validate(&wf).is_ok());
    assert!(matches!(wf.steps[0].action, Action::Passthrough { .. }));
}

#[test]
fn display_name_custom() {
    let step = Step {
        action: Action::Llm {
            prompt: String::new(),
            name: Some("My Custom Name".to_string()),
            model: Some("openai/gpt-5-mini".to_string()),
            context: vec![],
            tools: vec![],
            pre_input_sql: None,
            post_output_sql: None,
        },
    };
    assert_eq!(step.action.display_name(), "My Custom Name");
}

#[test]
fn display_name_generated() {
    let step = Step {
        action: Action::Llm {
            prompt: String::new(),
            name: None,
            model: Some("openai/gpt-5-mini".to_string()),
            context: vec![],
            tools: vec![],
            pre_input_sql: None,
            post_output_sql: None,
        },
    };
    assert_eq!(step.action.display_name(), "llm (openai/gpt-5-mini)");
}

#[test]
fn accept_read_input_without_prompt() {
    let yaml = r#"
steps:
  - action: read_input
"#;
    let wf: Workflow = serde_yaml::from_str(yaml).unwrap();
    assert!(validate(&wf).is_ok());
}

#[test]
fn reject_parallel_with_invalid_branch_model() {
    let yaml = r#"
steps:
  - action: parallel
    merge_sql: "SELECT * FROM branch"
    branches:
      - name: branch
        body:
          - action: llm
            prompt: test prompt
            model: fake/bad
"#;
    let wf: Workflow = serde_yaml::from_str(yaml).unwrap();
    let err = validate(&wf).unwrap_err();
    assert!(matches!(err, WorkflowError::UnknownModel(_)));
}

#[test]
fn all_schemas_covers_all_actions() {
    let schemas = Action::all_schemas();
    // EnumIter generates all variants — count matches Action enum variants
    // Removed Combine, added Passthrough — same count (15)
    assert_eq!(schemas.len(), 15);
}

#[test]
fn parse_loop_with_pre_input_sql() {
    let yaml = r#"
steps:
  - action: loop
    pre_input_sql: "SELECT CAST(0 AS BIGINT) AS ts, '' AS role, '' AS content FROM input WHERE false"
    body:
      - action: print
    while_sql: "SELECT 'false' AS __continue"
"#;
    let wf: Workflow = serde_yaml::from_str(yaml).unwrap();
    assert!(validate(&wf).is_ok());
    let Action::Loop { ref pre_input_sql, .. } = wf.steps[0].action else {
        panic!("expected Loop action");
    };
    assert!(pre_input_sql.is_some());
}

#[test]
fn parse_compact() {
    let yaml = r#"
steps:
  - action: compact
    prompt: summarize the conversation
    max_words: 10000
    compact_ratio: 0.7
    model: openai/gpt-5-mini
"#;
    let wf: Workflow = serde_yaml::from_str(yaml).unwrap();
    assert!(validate(&wf).is_ok());
    let Action::Compact { max_words, compact_ratio, ref model, .. } = wf.steps[0].action else {
        panic!("expected Compact action");
    };
    assert_eq!(max_words, 10000);
    assert!((compact_ratio - 0.7).abs() < f64::EPSILON);
    assert_eq!(model.as_deref(), Some("openai/gpt-5-mini"));
}
