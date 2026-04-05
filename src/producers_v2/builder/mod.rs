use std::collections::HashMap;
use std::sync::Arc;

use crate::dag_v2::{ProduceError, Produces};
use crate::events::Listener;
use crate::llm::LlmClient;
use crate::user_io::UserIO;
use crate::workflow::{Action, Step};

use super::compact::CompactProducer;
use super::each::EachProducer;
use super::http::HttpProducer;
use super::llm_call::LlmCallProducer;
use super::loop_::LoopProducer;
use super::parallel::ParallelProducer;
use super::passthrough::PassthroughProducer;
use super::pipeline::Pipeline;
use super::print::PrintProducer;
use super::read_input::ReadInputProducer;
use super::screenshot::ScreenshotProducer;
use super::read_var::ReadVarProducer;
use super::select::{MatchMode, SelectProducer};
use super::shell::ShellProducer;
use super::transform::TransformNode;
use super::trigger::TriggerProducer;

/// Dependencies needed to build producers.
pub struct BuilderContext {
    pub llm: Arc<dyn LlmClient>,
    pub io: Arc<dyn UserIO>,
    pub args: HashMap<String, Vec<String>>,
    pub s3_endpoint: String,
    pub s3_access_key: String,
    pub s3_secret_key: String,
    pub listener: Arc<dyn Listener>,
}

/// Build a single producer from a workflow step.
fn build_producer(step: &Step, ctx: &BuilderContext) -> Result<Arc<dyn Produces>, ProduceError> {
    match &step.action {
        Action::Transform { sql, .. } => {
            Ok(Arc::new(TransformNode::new(sql.clone())))
        }
        Action::Print { .. } => {
            Ok(Arc::new(PrintProducer::new()))
        }
        Action::Shell { command, cwd, timeout, post_output_sql, .. } => {
            Ok(Arc::new(ShellProducer::new(
                command.clone(),
                cwd.clone(),
                *timeout,
                post_output_sql.clone(),
            )))
        }
        Action::ReadInput { prompt, post_output_sql, .. } => {
            Ok(Arc::new(ReadInputProducer::new(
                ctx.io.clone(),
                prompt.clone(),
                post_output_sql.clone(),
            )))
        }
        Action::ReadVar { vars, post_output_sql, .. } => {
            Ok(Arc::new(ReadVarProducer::new(
                vars.clone(),
                ctx.args.clone(),
                post_output_sql.clone(),
            )))
        }
        Action::Http { url, method, headers, headers_jq, timeout, request_jq, response_jq, output_schema, .. } => {
            Ok(Arc::new(HttpProducer::new(
                url.clone(),
                method.clone(),
                headers.clone(),
                headers_jq.clone(),
                *timeout,
                request_jq.clone(),
                response_jq.clone(),
                output_schema.clone(),
            )))
        }
        Action::Llm { prompt, model, tools, pre_input_sql, post_output_sql, context, .. } => {
            let s3_config = crate::tools::dispatcher::S3Config {
                endpoint: ctx.s3_endpoint.clone(),
                access_key: ctx.s3_access_key.clone(),
                secret_key: ctx.s3_secret_key.clone(),
            };
            let dispatcher = crate::tools::dispatcher::build_tool_dispatcher(tools, &s3_config, Some(ctx.llm.clone()));
            Ok(Arc::new(LlmCallProducer::new(
                ctx.llm.clone(),
                model.clone(),
                prompt.clone(),
                context.clone(),
                dispatcher,
                pre_input_sql.clone(),
                post_output_sql.clone(),
                Some(ctx.listener.clone()),
            )))
        }
        Action::Compact { max_words, compact_ratio, model, prompt, .. } => {
            Ok(Arc::new(CompactProducer::new(
                ctx.llm.clone(),
                *max_words,
                *compact_ratio,
                model.clone(),
                prompt.clone(),
            )))
        }
        Action::Loop { body: body_steps, while_sql, pre_input_sql, .. } => {
            let body = build_pipeline(body_steps, ctx)?;
            Ok(Arc::new(LoopProducer::new(pre_input_sql.clone(), while_sql.clone(), body)))
        }
        Action::Each { body: body_steps, pre_input_sql, post_output_sql, output_schema, .. } => {
            let body = build_pipeline(body_steps, ctx)?;
            Ok(Arc::new(EachProducer::new(
                pre_input_sql.clone(),
                post_output_sql.clone(),
                output_schema.clone(),
                body,
            )))
        }
        Action::Parallel { branches, pre_input_sql, merge_sql, .. } => {
            let branch_pipelines: Vec<(String, Pipeline)> = branches.iter()
                .map(|b| Ok((b.name.clone(), build_pipeline(&b.body, ctx)?)))
                .collect::<Result<_, ProduceError>>()?;
            Ok(Arc::new(ParallelProducer::new(
                pre_input_sql.clone(),
                merge_sql.clone(),
                branch_pipelines,
            )))
        }
        Action::Passthrough { .. } => {
            Ok(Arc::new(PassthroughProducer::new()))
        }
        Action::Select { branches, match_mode, pre_input_sql, post_output_sql,
                         prepare_input_for_when, prepare_input_for_body, output_schema, .. } => {
            let mode = match match_mode.as_str() {
                "all" => MatchMode::All,
                _ => MatchMode::First,
            };
            let wired_branches: Result<Vec<_>, _> = branches.iter().map(|b| {
                let cond = build_pipeline(&b.when_steps, ctx)?;
                let body = build_pipeline(&b.body, ctx)?;
                Ok((b.name.clone(), cond, body))
            }).collect();
            Ok(Arc::new(SelectProducer::new(
                mode,
                pre_input_sql.clone(),
                post_output_sql.clone(),
                prepare_input_for_when.clone(),
                prepare_input_for_body.clone(),
                output_schema.clone(),
                wired_branches?,
            )))
        }
        Action::Trigger { command, pre_input_sql, jq, output_schema, post_output_sql,
                          body: body_steps, .. } => {
            let body = build_pipeline(body_steps, ctx)?;
            Ok(Arc::new(TriggerProducer::new(
                command.clone(),
                pre_input_sql.clone(),
                jq.clone(),
                output_schema.clone(),
                post_output_sql.clone(),
                body,
            )))
        }
        Action::Screenshot { providers, .. } => {
            Ok(Arc::new(ScreenshotProducer::new(providers.clone())))
        }
    }
}

/// Build a wired pipeline from workflow steps.
pub fn build_pipeline(steps: &[Step], ctx: &BuilderContext) -> Result<Pipeline, ProduceError> {
    let producers: Result<Vec<_>, _> = steps.iter().map(|s| build_producer(s, ctx)).collect();
    let producers = producers?;
    if producers.is_empty() {
        return Err(ProduceError::msg("v2 builder: no steps"));
    }
    Ok(Pipeline::from_producers(producers))
}

#[cfg(test)]
mod tests;
