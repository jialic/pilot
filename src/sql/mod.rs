pub mod cli;
pub mod datafusion;

use std::sync::Arc;

use arrow::array::RecordBatch;
use arrow::datatypes::Schema;
use arrow::ipc::reader::StreamReader;
use arrow::ipc::writer::StreamWriter;

/// Deserialize Arrow IPC bytes into RecordBatches.
pub fn ipc_to_batches(arrow_bytes: &[u8]) -> Result<Vec<RecordBatch>, Box<dyn std::error::Error>> {
    let reader = StreamReader::try_new(std::io::Cursor::new(arrow_bytes), None)?;
    let batches: Vec<RecordBatch> = reader.collect::<Result<Vec<_>, _>>()?;
    Ok(batches)
}

/// Read the schema from Arrow IPC bytes without consuming batches.
pub fn ipc_schema(arrow_bytes: &[u8]) -> Result<Arc<Schema>, Box<dyn std::error::Error>> {
    let reader = StreamReader::try_new(std::io::Cursor::new(arrow_bytes), None)?;
    Ok(reader.schema())
}

/// Serialize RecordBatches to Arrow IPC bytes.
pub fn batches_to_ipc(
    schema: &Schema,
    batches: &[RecordBatch],
) -> Result<Vec<u8>, Box<dyn std::error::Error>> {
    let mut buf = Vec::new();
    let mut writer = StreamWriter::try_new(&mut buf, schema)?;
    for batch in batches {
        writer.write(batch)?;
    }
    writer.finish()?;
    Ok(buf)
}

/// Serialize schema + each RecordBatch as individual IPC messages.
/// First message = schema, subsequent messages = one RecordBatch each.
pub fn batches_to_ipc_messages(
    schema: &Schema,
    batches: &[RecordBatch],
) -> Result<Vec<Vec<u8>>, Box<dyn std::error::Error>> {
    use arrow::ipc::writer::{IpcDataGenerator, IpcWriteOptions, DictionaryTracker, write_message};

    let data_gen = IpcDataGenerator::default();
    let opts = IpcWriteOptions::default();
    let mut dict_tracker = DictionaryTracker::new(false);
    let mut messages = Vec::new();

    // Schema message
    let schema_encoded = data_gen.schema_to_bytes_with_dictionary_tracker(
        schema, &mut dict_tracker, &opts,
    );
    let mut schema_buf = Vec::new();
    write_message(&mut schema_buf, schema_encoded, &opts)?;
    messages.push(schema_buf);

    // One message per RecordBatch
    for batch in batches {
        let (dict_msgs, batch_encoded) = data_gen.encode(
            batch, &mut dict_tracker, &opts, &mut Default::default(),
        )?;
        // Dictionary messages (if any) come before the batch
        for dict_msg in dict_msgs {
            let mut dict_buf = Vec::new();
            write_message(&mut dict_buf, dict_msg, &opts)?;
            messages.push(dict_buf);
        }
        let mut batch_buf = Vec::new();
        write_message(&mut batch_buf, batch_encoded, &opts)?;
        messages.push(batch_buf);
    }

    // EOS marker as final message
    messages.push(vec![0xFF, 0xFF, 0xFF, 0xFF, 0x00, 0x00, 0x00, 0x00]);

    Ok(messages)
}
