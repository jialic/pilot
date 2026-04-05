/// Unit of data on the transport.
/// stream_type identifies what kind of data this is.
/// data is opaque bytes — the transport doesn't parse it.
#[derive(Debug, Clone)]
pub struct Chunk {
    pub stream_type: u16,
    pub data: Vec<u8>,
}

// Stream types
pub const SCHEMA_REQ: u16 = 0x01;
pub const SCHEMA_RES: u16 = 0x02;
pub const DATA: u16 = 0x03;
pub const BLOB: u16 = 0x04;

impl Chunk {
    pub fn new(stream_type: u16, data: Vec<u8>) -> Self {
        Self { stream_type, data }
    }

    pub fn schema_req() -> Self {
        Self::new(SCHEMA_REQ, Vec::new())
    }

    pub fn schema_res(data: Vec<u8>) -> Self {
        Self::new(SCHEMA_RES, data)
    }

    pub fn data(data: Vec<u8>) -> Self {
        Self::new(DATA, data)
    }

    pub fn blob(data: Vec<u8>) -> Self {
        Self::new(BLOB, data)
    }
}
