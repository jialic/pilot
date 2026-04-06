use crate::events::{RunnerEvents, StderrListener};
use crate::producers_v2::builder::{build_pipeline, BuilderContext};
use crate::producers_v2::runner::run;
use crate::workflow::Workflow;
use std::collections::HashMap;
use std::sync::Arc;

/// Runner builds a v2 pipeline from a workflow and executes it.
pub struct Runner {
    workflow: Workflow,
    events: Arc<dyn RunnerEvents>,
    ctx: BuilderContext,
}

impl Runner {
    pub fn new(
        llm_config: crate::llm::LLMConfig,
        cfg: &crate::config::Config,
        workflow: Workflow,
        io: Arc<dyn crate::user_io::UserIO>,
        events: Arc<dyn RunnerEvents>,
        args: HashMap<String, Vec<String>>,
        yaml_path: &str,
    ) -> Self {
        let mut llm_config = llm_config;
        llm_config.model = workflow.model.clone()
            .unwrap_or_else(|| crate::models::DEFAULT_MODEL.to_string());
        let llm = Arc::new(crate::llm::LLM::new(llm_config));
        let ctx = BuilderContext {
            llm, io, args,
            s3_endpoint: cfg.s3_endpoint.clone(),
            s3_access_key: cfg.s3_access_key.clone(),
            s3_secret_key: cfg.s3_secret_key.clone(),
            yaml_path: yaml_path.to_string(),
            listener: Arc::new(StderrListener),
        };
        Self { workflow, events, ctx }
    }

    /// Run the workflow. Returns the final step's output as Arrow IPC bytes.
    pub async fn run(&self) -> Result<Vec<u8>, Box<dyn std::error::Error + Send + Sync>> {
        let pipeline = build_pipeline(&self.workflow.steps, &self.ctx)
            .map_err(|e| Box::new(e) as Box<dyn std::error::Error + Send + Sync>)?;

        let result = run(&pipeline, "").await;

        match result {
            Ok(bytes) => {
                self.events.workflow_done();
                Ok(bytes)
            }
            Err(e) => {
                self.events.workflow_error(&e.to_string());
                Err(Box::new(e) as Box<dyn std::error::Error + Send + Sync>)
            }
        }
    }
}
