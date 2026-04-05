use crate::workflow::Action;

/// Build the system prompt for the explain command.
/// Includes pilot schema knowledge and the workflow YAML to explain.
pub fn build_explain_prompt(yaml_content: &str, workflow_name: &str) -> String {
    let template_src = include_str!("explain_prompt.md.j2");
    let mut env = minijinja::Environment::new();
    env.add_template("explain_prompt", template_src)
        .expect("invalid explain_prompt template");

    let tmpl = env.get_template("explain_prompt").unwrap();

    let ctx = minijinja::context! {
        actions => Action::all_schemas(),
        models => crate::models::VALID_MODELS,
        yaml_content => yaml_content,
        workflow_name => workflow_name,
    };

    tmpl.render(ctx)
        .expect("failed to render explain_prompt template")
}
