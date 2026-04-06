use std::collections::HashMap;
use std::sync::Arc;

use crate::llm::testing::MockLlm;
use crate::user_io::UserIO;
use crate::producers_v2::builder::{build_pipeline, BuilderContext};
use crate::workflow::{Action, Step, ParallelBranchDef};

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

#[tokio::test]
async fn shell_transform_print() {
    let steps = vec![
        Step { action: Action::Shell {
            name: None, command: "echo hello".into(),
            cwd: None, timeout: None, post_output_sql: None,
        }},
        Step { action: Action::Transform {
            name: None, sql: "SELECT output AS greeting FROM input".into(),
        }},
        Step { action: Action::Print { name: None }},
    ];

    let pipeline = build_pipeline(&steps, &test_ctx()).unwrap();
    let output = super::run(&pipeline, "").await.unwrap();

    let batches = crate::sql::ipc_to_batches(&output).unwrap();
    assert_eq!(batches[0].num_rows(), 1);
    let val = crate::sql::datafusion::arrow_string_value(
        batches[0].column_by_name("greeting").unwrap().as_ref(), 0,
    ).unwrap();
    assert_eq!(val.trim(), "hello");
}

#[tokio::test]
async fn transform_only() {
    let steps = vec![
        Step { action: Action::Transform {
            name: None, sql: "SELECT 'world' AS msg".into(),
        }},
    ];

    let pipeline = build_pipeline(&steps, &test_ctx()).unwrap();
    let output = super::run(&pipeline, "").await.unwrap();

    let batches = crate::sql::ipc_to_batches(&output).unwrap();
    let val = crate::sql::datafusion::arrow_string_value(
        batches[0].column_by_name("msg").unwrap().as_ref(), 0,
    ).unwrap();
    assert_eq!(val, "world");
}

#[tokio::test]
async fn parallel_in_pipeline() {
    let steps = vec![
        Step { action: Action::Parallel {
            name: None,
            pre_input_sql: None,
            merge_sql: "SELECT input.output AS original, result.output AS upper FROM input, result".into(),
            branches: vec![
                ParallelBranchDef {
                    name: "input".into(),
                    body: vec![
                        Step { action: Action::Passthrough { name: None } },
                    ],
                },
                ParallelBranchDef {
                    name: "result".into(),
                    body: vec![
                        Step { action: Action::Transform {
                            name: None, sql: "SELECT UPPER(output) AS output FROM input".into(),
                        }},
                    ],
                },
            ],
        }},
        Step { action: Action::Print { name: None }},
    ];

    let pipeline = build_pipeline(&steps, &test_ctx()).unwrap();
    let output = super::run(&pipeline, "hello").await.unwrap();

    let batches = crate::sql::ipc_to_batches(&output).unwrap();
    assert_eq!(batches[0].num_rows(), 1);
    let original = crate::sql::datafusion::arrow_string_value(
        batches[0].column_by_name("original").unwrap().as_ref(), 0,
    ).unwrap();
    let upper = crate::sql::datafusion::arrow_string_value(
        batches[0].column_by_name("upper").unwrap().as_ref(), 0,
    ).unwrap();
    assert_eq!(original, "hello");
    assert_eq!(upper, "HELLO");
}
