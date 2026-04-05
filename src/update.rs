use std::path::PathBuf;
use std::sync::Arc;

use crate::llm::{ChatMessage, LlmClient};
use crate::user_io::UserIO;

fn build_system_prompt(current_content: &str) -> String {
    use crate::workflow::{Action, ToolName};

    let template_src = include_str!("update_prompt.md.j2");
    let mut env = minijinja::Environment::new();
    env.add_template("update_prompt", template_src)
        .expect("invalid update_prompt template");

    let tmpl = env.get_template("update_prompt").unwrap();

    use strum::IntoEnumIterator;
    let tools: Vec<_> = ToolName::iter()
        .map(|t| {
            let attrs: Vec<_> = t
                .tool_attributes()
                .iter()
                .map(|(name, desc)| minijinja::context! { name => *name, description => *desc })
                .collect();
            minijinja::context! {
                name => t.name(),
                description => t.description(),
                attributes => attrs,
            }
        })
        .collect();

    let ctx = minijinja::context! {
        actions => Action::all_schemas(),
        tools => tools,
        models => crate::models::VALID_MODELS,
        has_content => !current_content.is_empty(),
        current_content => current_content,
    };

    tmpl.render(ctx)
        .expect("failed to render update_prompt template")
}

/// Run the `pilot update` interactive chat loop.
/// Reads the current workflow file, teaches the LLM the schema, and lets
/// the user edit the workflow via conversation.
pub async fn run_update(
    llm: Arc<dyn LlmClient>,
    workflow_path: PathBuf,
    io: Arc<dyn UserIO>,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let dispatcher = crate::tools::dispatcher::DefaultToolDispatcher::new(vec![
        Arc::new(crate::tools::write_workflow::WriteWorkflowTool::new(
            workflow_path.clone(),
        )),
        Arc::new(crate::tools::validate_yaml::ValidateYamlTool::new()),
    ]);

    let current_content = std::fs::read_to_string(&workflow_path).unwrap_or_default();
    let system_prompt = build_system_prompt(&current_content);

    let mut messages: Vec<ChatMessage> = vec![
        ChatMessage::System {
            content: system_prompt,
            name: None,
        },
        ChatMessage::User {
            content: "<system>Start the session.</system>".to_string(),
            name: None,
        },
    ];

    // Initial LLM call — let the model greet/summarize before asking for input
    let greeting_msgs = crate::llm::run_tool_loop(
        llm.as_ref(),
        &dispatcher,
        &mut messages,
        None,
        None,
    )
    .await
    .map_err(|e| format!("LLM call failed: {e}"))?;

    let greeting = greeting_msgs
        .last()
        .map(|(_, c)| c.clone())
        .unwrap_or_default();
    println!("{}", greeting);
    messages.push(ChatMessage::Assistant {
        content: Some(greeting),
        tool_calls: None,
    });

    // Chat loop — user input → tool loop → response
    loop {
        let user_input = io
            .ask("".to_string())
            .await
            .map_err(|e| format!("user input failed: {e}"))?;

        messages.push(ChatMessage::User {
            content: user_input,
            name: None,
        });

        let new_msgs = crate::llm::run_tool_loop(
            llm.as_ref(),
            &dispatcher,
            &mut messages,
            None,
            None,
        )
        .await
        .map_err(|e| format!("LLM call failed: {e}"))?;

        let content = new_msgs
            .last()
            .map(|(_, c)| c.clone())
            .unwrap_or_default();
        println!("{}", content);

        messages.push(ChatMessage::Assistant {
            content: Some(content),
            tool_calls: None,
        });
    }
}
