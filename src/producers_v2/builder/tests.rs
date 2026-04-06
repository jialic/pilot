use std::collections::HashMap;
use std::sync::Arc;

use crate::llm::testing::MockLlm;
use crate::user_io::UserIO;
use crate::workflow::{Action, Step};

use super::{build_pipeline, BuilderContext};

struct MockIO;
impl UserIO for MockIO {
    fn ask<'a>(&'a self, _prompt: String) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<String, String>> + Send + 'a>> {
        Box::pin(async { Ok("mock".into()) })
    }
}

fn test_ctx() -> BuilderContext {
    BuilderContext {
        llm: Arc::new(MockLlm::new(vec![])),
        io: Arc::new(MockIO),
        args: HashMap::new(),
        s3_endpoint: String::new(),
        s3_access_key: String::new(),
        s3_secret_key: String::new(),
        yaml_path: String::new(),
        listener: Arc::new(crate::events::NoopListener),
    }
}

#[test]
fn build_transform() {
    let steps = vec![Step {
        action: Action::Transform { name: None, sql: "SELECT 1 AS x".into() },
    }];
    let pipeline = build_pipeline(&steps, &test_ctx()).unwrap();
    assert_eq!(pipeline.producers.len(), 1);
}

#[test]
fn build_shell_transform_print() {
    let steps = vec![
        Step { action: Action::Shell {
            name: None, command: "echo hello".into(),
            cwd: None, timeout: None, post_output_sql: None,
        }},
        Step { action: Action::Transform { name: None, sql: "SELECT output AS greeting FROM input".into() }},
        Step { action: Action::Print { name: None }},
    ];
    let pipeline = build_pipeline(&steps, &test_ctx()).unwrap();
    assert_eq!(pipeline.producers.len(), 3);
}

#[test]
fn build_read_input() {
    let steps = vec![Step {
        action: Action::ReadInput { name: None, prompt: "prompt".into(), context: vec![], post_output_sql: None },
    }];
    let pipeline = build_pipeline(&steps, &test_ctx()).unwrap();
    assert_eq!(pipeline.producers.len(), 1);
}

#[test]
fn build_llm() {
    let steps = vec![Step {
        action: Action::Llm {
            name: None, prompt: "test".into(), model: None,
            context: vec![], tools: vec![],
            pre_input_sql: None, post_output_sql: None,
        },
    }];
    let pipeline = build_pipeline(&steps, &test_ctx()).unwrap();
    assert_eq!(pipeline.producers.len(), 1);
}

#[test]
fn empty_steps_errors() {
    let result = build_pipeline(&[], &test_ctx());
    assert!(result.is_err());
}
