use super::types::*;
use crate::models;

pub(crate) fn validate(workflow: &Workflow) -> Result<(), WorkflowError> {
    if workflow.steps.is_empty() {
        return Err(WorkflowError::NoSteps);
    }

    if let Some(model) = &workflow.model {
        if !models::is_valid_model(model) {
            return Err(WorkflowError::UnknownModel(model.clone()));
        }
    }

    for (i, step) in workflow.steps.iter().enumerate() {
        validate_step(i, step)?;
    }

    Ok(())
}

fn validate_step(_index: usize, step: &Step) -> Result<(), WorkflowError> {
    // Validate model on Llm action
    if let Action::Llm {
        model: Some(ref m), ..
    } = step.action
    {
        if !models::is_valid_model(m) {
            return Err(WorkflowError::UnknownModel(m.clone()));
        }
    }

    // Validate tool configs
    if let Action::Llm { ref tools, .. } = step.action {
        validate_tools(tools)?;
    }

    // Validate parallel branches recursively
    if let Action::Parallel { ref branches, .. } = step.action {
        if branches.is_empty() {
            return Err(WorkflowError::NoSteps);
        }
        for branch in branches {
            for step in &branch.body {
                validate_step(0, step)?;
            }
        }
    }

    // Validate compact ratio
    if let Action::Compact { compact_ratio, .. } = &step.action {
        const COMPACT_RATIO_MIN: f64 = 0.1;
        const COMPACT_RATIO_MAX: f64 = 1.0;
        if *compact_ratio < COMPACT_RATIO_MIN || *compact_ratio > COMPACT_RATIO_MAX {
            return Err(WorkflowError::InvalidCompactRatio(
                COMPACT_RATIO_MIN,
                COMPACT_RATIO_MAX,
            ));
        }
    }

    // Validate loop body recursively
    if let Action::Loop {
        ref body,
        ..
    } = step.action
    {
        if body.is_empty() {
            return Err(WorkflowError::NoSteps);
        }
        for (si, s) in body.iter().enumerate() {
            validate_step(si, s)?;
        }
    }

    // Validate trigger body recursively
    if let Action::Trigger { ref body, .. } = step.action {
        if body.is_empty() {
            return Err(WorkflowError::NoSteps);
        }
        for (si, s) in body.iter().enumerate() {
            validate_step(si, s)?;
        }
    }

    // Validate each body recursively
    if let Action::Each { ref body, .. } = step.action {
        if body.is_empty() {
            return Err(WorkflowError::NoSteps);
        }
        for (si, s) in body.iter().enumerate() {
            validate_step(si, s)?;
        }
    }

    // Validate SQL syntax on actions that have sql fields
    validate_sql(&step.action)?;

    Ok(())
}

fn validate_tools(tools: &[ToolDef]) -> Result<(), WorkflowError> {
    for tool in tools {
        if let ToolDef::File { read, write } = tool {
            for path in read.iter().chain(write.iter()) {
                if !path.starts_with('/') && !path.starts_with("~/") {
                    return Err(WorkflowError::InvalidToolConfig(
                        format!("file tool path must be absolute or ~/relative, got: {path}"),
                    ));
                }
            }
        }
    }
    Ok(())
}

fn validate_sql(action: &Action) -> Result<(), WorkflowError> {
    let mut sqls: Vec<&str> = Vec::new();

    match action {
        Action::Transform { sql, .. } => sqls.push(sql.as_str()),
        Action::Llm { pre_input_sql, post_output_sql, .. } => {
            sqls.extend(pre_input_sql.as_deref());
            sqls.extend(post_output_sql.as_deref());
        }
        Action::ReadVar { post_output_sql, .. } => {
            sqls.extend(post_output_sql.as_deref());
        }
        Action::Shell { post_output_sql, .. } => {
            sqls.extend(post_output_sql.as_deref());
        }
        Action::ReadInput { post_output_sql, .. } => {
            sqls.extend(post_output_sql.as_deref());
        }
        Action::Parallel { pre_input_sql, merge_sql, .. } => {
            sqls.extend(pre_input_sql.as_deref());
            sqls.push(merge_sql.as_str());
        }
        Action::Select {
            pre_input_sql,
            post_output_sql,
            prepare_input_for_when,
            prepare_input_for_body,
            ..
        } => {
            sqls.extend(pre_input_sql.as_deref());
            sqls.extend(post_output_sql.as_deref());
            sqls.extend(prepare_input_for_when.as_deref());
            sqls.extend(prepare_input_for_body.as_deref());
        }
        Action::Each { pre_input_sql, post_output_sql, .. } => {
            sqls.extend(pre_input_sql.as_deref());
            sqls.extend(post_output_sql.as_deref());
        }
        Action::Trigger { pre_input_sql, post_output_sql, .. } => {
            sqls.extend(pre_input_sql.as_deref());
            sqls.extend(post_output_sql.as_deref());
        }
        Action::Loop { pre_input_sql, while_sql, .. } => {
            sqls.extend(pre_input_sql.as_deref());
            sqls.push(while_sql.as_str());
        }
        _ => {}
    };

    for sql in sqls {
        use sqlparser::dialect::GenericDialect;
        use sqlparser::parser::Parser;

        Parser::parse_sql(&GenericDialect {}, sql).map_err(|e| {
            WorkflowError::InvalidSql(format!("{e}"))
        })?;
    }

    Ok(())
}
