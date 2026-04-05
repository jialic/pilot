#[cfg(test)]
mod tests;

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

use arrow::array::{RecordBatch, StringArray};
use arrow::datatypes::{DataType, Field, Schema};

use crate::dag_v2::{Chunk, ProduceError, Produces, Transport};
use crate::screen_capture::{ScreenCapture, TextElement};

fn output_schema() -> Schema {
    Schema::new(vec![
        Field::new("provider", DataType::Utf8, false),
        Field::new("provider_id", DataType::Utf8, false),
    ])
}

/// Captures the primary monitor, runs OCR, outputs inline base64 image + OCR text.
///
/// Output schema: `|provider, provider_id|`
/// - `provider="base64"`, `provider_id=<base64 webp image data>`
/// - `provider="ocr"`, `provider_id=<formatted OCR text elements>`
/// - `provider="file"`, `provider_id=<temp file path>` (if providers includes "file")
pub struct ScreenshotProducer {
    providers: Vec<String>,
    input: std::sync::Mutex<Option<Arc<dyn Transport>>>,
}

impl ScreenshotProducer {
    pub fn new(providers: Vec<String>) -> Self {
        Self {
            providers,
            input: std::sync::Mutex::new(None),
        }
    }

    /// Run OCR on a PNG file using macOS Vision framework.
    async fn run_ocr(png_path: &str) -> Result<Vec<TextElement>, ProduceError> {
        let swift_code = format!(r#"
import Vision
import AppKit

let url = URL(fileURLWithPath: "{png_path}")
guard let image = NSImage(contentsOf: url),
      let cgImage = image.cgImage(forProposedRect: nil, context: nil, hints: nil) else {{
    exit(1)
}}

let request = VNRecognizeTextRequest()
request.recognitionLevel = .accurate
let handler = VNImageRequestHandler(cgImage: cgImage)
try handler.perform([request])

guard let observations = request.results else {{ exit(1) }}
for obs in observations {{
    let box = obs.boundingBox
    let text = obs.topCandidates(1).first?.string ?? ""
    let conf = obs.confidence
    let x = Int(box.origin.x * Double(cgImage.width))
    let y = Int((1.0 - box.origin.y - box.height) * Double(cgImage.height))
    let w = Int(box.width * Double(cgImage.width))
    let h = Int(box.height * Double(cgImage.height))
    print("\(conf)\t\(x)\t\(y)\t\(w)\t\(h)\t\(text)")
}}
"#);

        let output = tokio::process::Command::new("swift")
            .arg("-e")
            .arg(&swift_code)
            .output()
            .await
            .map_err(|e| ProduceError::msg(format!("OCR failed to start: {e}")))?;

        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            tracing::warn!("OCR failed: {stderr}");
            return Ok(vec![]);
        }

        let stdout = String::from_utf8_lossy(&output.stdout);
        let elements: Vec<TextElement> = stdout
            .lines()
            .filter_map(|line| {
                let parts: Vec<&str> = line.splitn(6, '\t').collect();
                if parts.len() < 6 {
                    return None;
                }
                Some(TextElement {
                    confidence: parts[0].parse().unwrap_or(0.0),
                    x: parts[1].parse().unwrap_or(0),
                    y: parts[2].parse().unwrap_or(0),
                    width: parts[3].parse().unwrap_or(0),
                    height: parts[4].parse().unwrap_or(0),
                    text: parts[5].to_string(),
                })
            })
            .collect();

        tracing::info!("OCR detected {} text elements", elements.len());
        Ok(elements)
    }
}

impl Produces for ScreenshotProducer {
    fn handle_schema<'a>(
        &'a self,
        output: Arc<dyn Transport>,
    ) -> Pin<Box<dyn Future<Output = Result<(), ProduceError>> + Send + 'a>> {
        Box::pin(async move {
            let schema = output_schema();
            let out_bytes = crate::dag_v2::schema::schema_to_bytes(&schema)
                .map_err(|e| ProduceError::msg(format!("screenshot: ser schema: {e}")))?;
            output.write(Chunk::schema_res(out_bytes));
            output.close();
            Ok(())
        })
    }

    fn handle_data<'a>(
        &'a self,
        output: Arc<dyn Transport>,
    ) -> Pin<Box<dyn Future<Output = Result<(), ProduceError>> + Send + 'a>> {
        Box::pin(async move {
            // Capture primary monitor
            let monitors = xcap::Monitor::all()
                .map_err(|e| ProduceError::msg(format!("failed to list monitors: {e}")))?;
            let monitor = monitors
                .first()
                .ok_or_else(|| ProduceError::msg("no monitors found"))?;

            let image = monitor
                .capture_image()
                .map_err(|e| ProduceError::msg(format!("screenshot capture failed: {e}")))?;

            // Save to temp file for OCR
            let ts = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_millis();
            let tmp_path = format!("/tmp/pilot_screenshot_{ts}.png");
            image
                .save(&tmp_path)
                .map_err(|e| ProduceError::msg(format!("save temp screenshot: {e}")))?;

            // Run OCR
            let text_elements = Self::run_ocr(&tmp_path).await?;

            // Encode to WebP → base64
            let png_bytes = std::fs::read(&tmp_path)
                .map_err(|e| ProduceError::msg(format!("read screenshot: {e}")))?;
            let img = image::load_from_memory(&png_bytes)
                .map_err(|e| ProduceError::msg(format!("decode screenshot: {e}")))?;
            let mut webp_buf = std::io::Cursor::new(Vec::new());
            img.write_to(&mut webp_buf, image::ImageFormat::WebP)
                .map_err(|e| ProduceError::msg(format!("encode webp: {e}")))?;
            let webp_bytes = webp_buf.into_inner();
            tracing::info!(
                "screenshot: PNG {}KB → WebP {}KB",
                png_bytes.len() / 1024,
                webp_bytes.len() / 1024
            );

            use base64::Engine;
            let image_b64 = base64::engine::general_purpose::STANDARD.encode(&webp_bytes);

            // Format OCR elements
            let capture = ScreenCapture {
                image_b64: String::new(), // not used for format_elements
                text_elements,
                media_type: "image/webp".to_string(),
            };
            let ocr_text = capture.format_elements();

            // Build output rows
            let mut provider_vals = vec!["base64".to_string(), "ocr".to_string()];
            let mut provider_id_vals = vec![image_b64, ocr_text];

            for provider in &self.providers {
                if provider == "file" {
                    tracing::info!("screenshot saved to {tmp_path}");
                    provider_vals.push("file".to_string());
                    provider_id_vals.push(tmp_path.clone());
                }
            }

            // Clean up temp file if not using file provider
            if !self.providers.contains(&"file".to_string()) {
                let _ = std::fs::remove_file(&tmp_path);
            }

            let schema = Arc::new(output_schema());
            let batch = RecordBatch::try_new(
                schema.clone(),
                vec![
                    Arc::new(StringArray::from(
                        provider_vals.iter().map(|s| s.as_str()).collect::<Vec<_>>(),
                    )),
                    Arc::new(StringArray::from(
                        provider_id_vals.iter().map(|s| s.as_str()).collect::<Vec<_>>(),
                    )),
                ],
            )
            .map_err(|e| ProduceError::msg(format!("screenshot: arrow batch: {e}")))?;

            let result_bytes = crate::sql::batches_to_ipc(&schema, &[batch])
                .map_err(|e| ProduceError::msg(format!("screenshot: ser result: {e}")))?;

            output.write(Chunk::data(result_bytes));
            output.close();
            Ok(())
        })
    }

    fn set_input(&self, transport: Arc<dyn Transport>) {
        *self.input.lock().unwrap() = Some(transport);
    }
}
