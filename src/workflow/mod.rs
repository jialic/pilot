mod schema;
mod types;
mod validation;

pub use schema::*;
pub use types::*;
// Re-export validate so tests (which use `super::*`) can call it directly
#[cfg(test)]
pub(crate) use validation::validate;

use std::path::Path;

pub fn load_workflow(path: &Path) -> Result<Workflow, WorkflowError> {
    let data = std::fs::read_to_string(path)?;
    parse_and_validate(&data)
}

/// Parse YAML string and validate as a workflow.
/// Used by load_workflow and by the validate_yaml tool.
pub fn parse_and_validate(yaml: &str) -> Result<Workflow, WorkflowError> {
    let workflow: Workflow = serde_yaml::from_str(yaml)?;
    validation::validate(&workflow)?;
    Ok(workflow)
}

#[cfg(test)]
#[path = "../workflow_tests.rs"]
mod tests;
