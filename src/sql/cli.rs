//! CLI helpers: JSON ↔ Arrow conversions for display.

use std::sync::Arc;

use arrow::array::{ArrayRef, BooleanArray, Float64Array, Int64Array, RecordBatch, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::ipc::reader::StreamReader;
use arrow::ipc::writer::StreamWriter;

/// Convert JSON string to Arrow IPC bytes.
pub fn json_to_arrow_bytes(json_data: &str) -> Result<Vec<u8>, Box<dyn std::error::Error>> {
    let value: serde_json::Value = serde_json::from_str(json_data)?;

    let rows: Vec<&serde_json::Map<String, serde_json::Value>> = match &value {
        serde_json::Value::Object(obj) => vec![obj],
        serde_json::Value::Array(arr) => arr
            .iter()
            .map(|v| v.as_object().ok_or("array elements must be JSON objects"))
            .collect::<Result<Vec<_>, _>>()?,
        _ => return Err("expected JSON object or array of objects".into()),
    };

    if rows.is_empty() {
        return Err("empty input".into());
    }

    let first = rows[0];
    let fields: Vec<Field> = first
        .iter()
        .map(|(key, val)| {
            let dt = match val {
                serde_json::Value::Number(n) if n.is_i64() => DataType::Int64,
                serde_json::Value::Number(_) => DataType::Float64,
                serde_json::Value::Bool(_) => DataType::Boolean,
                _ => DataType::Utf8,
            };
            Field::new(key, dt, true)
        })
        .collect();
    let schema = Arc::new(Schema::new(fields.clone()));

    let mut arrays: Vec<ArrayRef> = Vec::new();
    for field in &fields {
        match field.data_type() {
            DataType::Utf8 => {
                let vals: Vec<Option<String>> = rows
                    .iter()
                    .map(|row| match row.get(field.name()) {
                        Some(serde_json::Value::String(s)) => Some(s.clone()),
                        Some(serde_json::Value::Null) | None => None,
                        Some(other) => Some(other.to_string()),
                    })
                    .collect();
                arrays.push(Arc::new(StringArray::from(vals)));
            }
            DataType::Int64 => {
                let vals: Vec<Option<i64>> = rows
                    .iter()
                    .map(|row| row.get(field.name()).and_then(|v| v.as_i64()))
                    .collect();
                arrays.push(Arc::new(Int64Array::from(vals)));
            }
            DataType::Float64 => {
                let vals: Vec<Option<f64>> = rows
                    .iter()
                    .map(|row| row.get(field.name()).and_then(|v| v.as_f64()))
                    .collect();
                arrays.push(Arc::new(Float64Array::from(vals)));
            }
            DataType::Boolean => {
                let vals: Vec<Option<bool>> = rows
                    .iter()
                    .map(|row| row.get(field.name()).and_then(|v| v.as_bool()))
                    .collect();
                arrays.push(Arc::new(BooleanArray::from(vals)));
            }
            _ => return Err(format!("unsupported type: {:?}", field.data_type()).into()),
        }
    }

    let batch = RecordBatch::try_new(schema.clone(), arrays)?;

    let mut buf = Vec::new();
    {
        let mut writer = StreamWriter::try_new(&mut buf, &schema)?;
        writer.write(&batch)?;
        writer.finish()?;
    }

    Ok(buf)
}

/// Deserialize Arrow IPC bytes to JSON string (for display/debugging).
pub fn arrow_bytes_to_json(arrow_bytes: &[u8]) -> Result<String, Box<dyn std::error::Error>> {
    let reader = StreamReader::try_new(std::io::Cursor::new(arrow_bytes), None)?;

    let batches: Vec<RecordBatch> = reader.collect::<Result<Vec<_>, _>>()?;
    let batch_refs: Vec<&RecordBatch> = batches.iter().collect();

    let buf = Vec::new();
    let mut writer = arrow::json::ArrayWriter::new(buf);
    writer.write_batches(&batch_refs)?;
    writer.finish()?;
    let json_bytes = writer.into_inner();

    let value: serde_json::Value = serde_json::from_slice(&json_bytes)?;
    let result = match &value {
        serde_json::Value::Array(arr) if arr.len() == 1 => arr[0].clone(),
        other => other.clone(),
    };

    Ok(serde_json::to_string_pretty(&result)?)
}
