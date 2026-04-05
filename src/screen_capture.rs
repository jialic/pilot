use serde::{Deserialize, Serialize};

/// A screen capture with OCR text elements.
/// Serialized to JSON and stored in BlobStore as a single blob.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ScreenCapture {
    /// Image bytes, base64 encoded for JSON serialization.
    pub image_b64: String,
    /// Text elements detected by OCR with bounding boxes.
    pub text_elements: Vec<TextElement>,
    /// MIME type of the image (e.g. "image/webp", "image/png").
    #[serde(default = "default_media_type")]
    pub media_type: String,
}

fn default_media_type() -> String {
    "image/png".to_string()
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TextElement {
    pub text: String,
    pub x: i32,
    pub y: i32,
    pub width: i32,
    pub height: i32,
    pub confidence: f32,
}

impl ScreenCapture {
    pub fn to_bytes(&self) -> Vec<u8> {
        serde_json::to_vec(self).expect("ScreenCapture serialization failed")
    }

    pub fn from_bytes(bytes: &[u8]) -> Option<Self> {
        serde_json::from_slice(bytes).ok()
    }

    /// Generate a letter ID from index: 0=aaa, 1=aab, ..., 25=aaz, 26=aba, etc.
    fn id_from_index(i: usize) -> String {
        let a = (i / 676) % 26;
        let b = (i / 26) % 26;
        let c = i % 26;
        format!(
            "{}{}{}",
            (b'a' + a as u8) as char,
            (b'a' + b as u8) as char,
            (b'a' + c as u8) as char,
        )
    }

    /// Format text elements with letter IDs for the LLM.
    /// Returns (formatted string, id-to-element mapping).
    pub fn format_elements(&self) -> String {
        if self.text_elements.is_empty() {
            return "(no text elements detected)".to_string();
        }
        let mut out = String::from("Clickable text elements on screen:\n");
        let mut idx = 0;
        for el in &self.text_elements {
            if el.confidence < 0.5 {
                continue;
            }
            let id = Self::id_from_index(idx);
            out.push_str(&format!(
                "[{}] \"{}\" | pos:({},{}) size:({}x{}) conf:{:.1}\n",
                id, el.text, el.x, el.y, el.width, el.height, el.confidence
            ));
            idx += 1;
        }
        out
    }

    /// Get the element by letter ID. Returns the center coordinates in image space.
    pub fn element_center_by_id(&self, id: &str) -> Option<(i32, i32)> {
        let filtered: Vec<&TextElement> = self.text_elements
            .iter()
            .filter(|el| el.confidence >= 0.5)
            .collect();

        // Parse letter ID back to index
        let bytes = id.as_bytes();
        if bytes.len() != 3 {
            return None;
        }
        let a = (bytes[0] - b'a') as usize;
        let b = (bytes[1] - b'a') as usize;
        let c = (bytes[2] - b'a') as usize;
        let idx = a * 676 + b * 26 + c;

        let el = filtered.get(idx)?;
        Some((el.x + el.width / 2, el.y + el.height / 2))
    }
}
