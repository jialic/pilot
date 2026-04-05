use std::sync::Arc;

use arrow::array::{
    Array, BooleanArray, Float64Array, Int64Array, RecordBatch, StringArray,
};
use arrow::datatypes::{DataType, Field, Schema};

use crate::dag_v2::ProduceError;
use crate::workflow::SchemaField;

/// Compile and run a JQ expression on a JSON value.
pub(super) fn run_jq(expr: &str, input: serde_json::Value) -> Result<Vec<serde_json::Value>, String> {
    use jaq_core::load::{Arena, File, Loader};
    use jaq_core::{Compiler, Ctx, RcIter};
    use jaq_json::Val;

    let program = File {
        code: expr,
        path: (),
    };
    let loader = Loader::new(jaq_std::defs().chain(jaq_json::defs()));
    let arena = Arena::default();

    let modules = loader
        .load(&arena, program)
        .map_err(|errs| format!("JQ parse error: {:?}", errs))?;
    let filter = Compiler::default()
        .with_funs(jaq_std::funs().chain(jaq_json::funs()))
        .compile(modules)
        .map_err(|errs| format!("JQ compile error: {:?}", errs))?;

    let val = Val::from(input);
    let inputs = RcIter::new(core::iter::empty());
    let out = filter.run((Ctx::new([], &inputs), val));

    let mut results = Vec::new();
    for item in out {
        match item {
            Ok(v) => {
                let json: serde_json::Value = v.into();
                results.push(json);
            }
            Err(e) => return Err(format!("JQ runtime error: {e}")),
        }
    }
    Ok(results)
}

/// Build Arrow Schema from declared output_schema fields.
pub(super) fn build_output_schema(output_schema: &[SchemaField]) -> Result<Schema, String> {
    if output_schema.is_empty() {
        return Err("trigger requires output_schema".into());
    }
    let fields: Vec<Field> = output_schema
        .iter()
        .map(|f| {
            let dt = match f.field_type.as_str() {
                "integer" => DataType::Int64,
                "float" => DataType::Float64,
                "boolean" => DataType::Boolean,
                _ => DataType::Utf8,
            };
            Field::new(&f.name, dt, true)
        })
        .collect();
    Ok(Schema::new(fields))
}

/// Build Arrow RecordBatch from JQ output + declared schema.
pub(super) fn json_to_arrow_batch(
    results: &[serde_json::Value],
    schema: &Schema,
) -> Result<RecordBatch, ProduceError> {
    let mut columns: Vec<Arc<dyn Array>> = Vec::new();

    for field in schema.fields() {
        match field.data_type() {
            DataType::Utf8 => {
                let vals: Vec<Option<String>> = results
                    .iter()
                    .map(|row| {
                        row.get(field.name())
                            .and_then(|v| match v {
                                serde_json::Value::String(s) => Some(s.clone()),
                                serde_json::Value::Null => None,
                                other => Some(other.to_string()),
                            })
                    })
                    .collect();
                columns.push(Arc::new(StringArray::from(vals)));
            }
            DataType::Int64 => {
                let vals: Vec<Option<i64>> = results
                    .iter()
                    .map(|row| row.get(field.name()).and_then(|v| v.as_i64()))
                    .collect();
                columns.push(Arc::new(Int64Array::from(vals)));
            }
            DataType::Float64 => {
                let vals: Vec<Option<f64>> = results
                    .iter()
                    .map(|row| row.get(field.name()).and_then(|v| v.as_f64()))
                    .collect();
                columns.push(Arc::new(Float64Array::from(vals)));
            }
            DataType::Boolean => {
                let vals: Vec<Option<bool>> = results
                    .iter()
                    .map(|row| row.get(field.name()).and_then(|v| v.as_bool()))
                    .collect();
                columns.push(Arc::new(BooleanArray::from(vals)));
            }
            _ => {
                return Err(ProduceError::msg(format!(
                    "unsupported output type: {:?}",
                    field.data_type()
                )));
            }
        }
    }

    RecordBatch::try_new(Arc::new(schema.clone()), columns)
        .map_err(|e| ProduceError::msg(format!("build output batch: {e}")))
}
