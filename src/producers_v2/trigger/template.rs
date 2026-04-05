use std::collections::HashMap;

use arrow::array::RecordBatch;

use crate::dag_v2::ProduceError;

pub(super) fn has_template_vars(command: &str) -> bool {
    command.contains("{{")
}

pub(super) fn build_template_context(input: &[RecordBatch]) -> HashMap<String, String> {
    let mut ctx = HashMap::new();
    for batch in input {
        let name_col = match batch.column_by_name("name") {
            Some(c) => c,
            None => continue,
        };
        let value_col = match batch.column_by_name("value") {
            Some(c) => c,
            None => continue,
        };
        for i in 0..batch.num_rows() {
            let name = crate::sql::datafusion::arrow_string_value(name_col.as_ref(), i)
                .unwrap_or_default();
            let value = crate::sql::datafusion::arrow_string_value(value_col.as_ref(), i)
                .unwrap_or_default();
            ctx.insert(name, value);
        }
    }
    ctx
}

pub(super) fn render_command(command: &str, ctx: &HashMap<String, String>) -> Result<String, ProduceError> {
    let mut env = minijinja::Environment::new();
    env.add_template("cmd", command)
        .map_err(|e| ProduceError::msg(format!("invalid command template: {e}")))?;
    let tmpl = env.get_template("cmd")
        .map_err(|e| ProduceError::msg(format!("template error: {e}")))?;
    tmpl.render(ctx)
        .map_err(|e| ProduceError::msg(format!("template render error: {e}")))
}
