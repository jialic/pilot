use arrow::datatypes::Schema;
use arrow::ipc::reader::StreamReader;
use arrow::ipc::writer::StreamWriter;

/// Serialize an Arrow Schema to IPC bytes (zero-row stream).
pub fn schema_to_bytes(schema: &Schema) -> Result<Vec<u8>, Box<dyn std::error::Error>> {
    let mut buf = Vec::new();
    let mut writer = StreamWriter::try_new(&mut buf, schema)?;
    writer.finish()?;
    Ok(buf)
}

/// Deserialize Arrow Schema from IPC bytes.
pub fn schema_from_bytes(bytes: &[u8]) -> Result<Schema, Box<dyn std::error::Error>> {
    let reader = StreamReader::try_new(std::io::Cursor::new(bytes), None)?;
    Ok(reader.schema().as_ref().clone())
}
