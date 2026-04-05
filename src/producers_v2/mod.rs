pub mod builder;
pub mod compact;
pub mod const_producer;
pub mod each;
pub mod parallel;
pub mod passthrough;
pub mod screenshot;
pub mod select;
pub mod trigger;
pub mod http;
pub mod llm_call;
pub mod loop_;
pub mod mutable_producer;
pub mod pipeline;
pub mod print;
pub mod read_input;
pub mod read_var;
pub mod runner;
pub mod shell;
pub mod transform;

pub use const_producer::ConstProducer;
pub use mutable_producer::MutableProducer;

use arrow::datatypes::{DataType, Field, Schema};
use crate::dag_v2::ProduceError;
use crate::workflow::SchemaField;

/// Compare two Arrow schemas by field names and types, ignoring nullability.
/// Returns Ok(()) if they match, Err with description if they differ.
pub fn validate_schemas_match(
    expected: &Schema,
    actual: &Schema,
    context: &str,
) -> Result<(), ProduceError> {
    let expected_fields: Vec<_> = expected.fields().iter()
        .map(|f| (f.name().as_str(), f.data_type()))
        .collect();
    let actual_fields: Vec<_> = actual.fields().iter()
        .map(|f| (f.name().as_str(), f.data_type()))
        .collect();
    if expected_fields != actual_fields {
        return Err(ProduceError::msg(format!(
            "{}: schema mismatch — expected {:?}, got {:?}",
            context,
            expected_fields.iter().map(|(n, _)| *n).collect::<Vec<_>>(),
            actual_fields.iter().map(|(n, _)| *n).collect::<Vec<_>>(),
        )));
    }
    Ok(())
}

/// Interpret a string as boolean. Non-empty strings that aren't "false", "no", or "0" are true.
pub fn as_bool(s: &str) -> bool {
    let trimmed = s.trim().to_lowercase();
    !trimmed.is_empty() && trimmed != "false" && trimmed != "no" && trimmed != "0"
}

/// Build a locked schema from user-defined output_schema, or default to |name, output|.
pub fn locked_schema_from(output_schema: &[SchemaField]) -> Schema {
    if output_schema.is_empty() {
        Schema::new(vec![
            Field::new("name", DataType::Utf8, false),
            Field::new("output", DataType::Utf8, false),
        ])
    } else {
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
        Schema::new(fields)
    }
}
