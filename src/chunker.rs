/// A chunk of text with its source location.
pub struct Chunk {
    /// The chunk text.
    pub text: String,
    /// Byte offset in the original text where this chunk starts.
    pub offset: usize,
}

/// Split text into overlapping chunks.
///
/// - `chunk_size`: target size in characters per chunk
/// - `overlap`: number of overlapping characters between consecutive chunks
///
/// Small texts (below chunk_size) produce a single chunk.
/// Splits at char boundaries. Does not split on semantic boundaries.
pub fn chunk_text(text: &str, chunk_size: usize, overlap: usize) -> Vec<Chunk> {
    if text.is_empty() {
        return vec![];
    }

    if text.len() <= chunk_size {
        return vec![Chunk {
            text: text.to_string(),
            offset: 0,
        }];
    }

    let step = chunk_size - overlap;
    let mut chunks = Vec::new();
    let mut start = 0;

    while start < text.len() {
        let mut end = start + chunk_size;
        if end >= text.len() {
            end = text.len();
        } else {
            // Find a char boundary
            while !text.is_char_boundary(end) {
                end -= 1;
            }
        }

        chunks.push(Chunk {
            text: text[start..end].to_string(),
            offset: start,
        });

        if end >= text.len() {
            break;
        }

        start += step;
        // Ensure start is at a char boundary
        while start < text.len() && !text.is_char_boundary(start) {
            start += 1;
        }
    }

    chunks
}

/// Default chunk size in characters (~512 tokens for English markdown).
pub const DEFAULT_CHUNK_SIZE: usize = 2000;

/// Default overlap in characters (~50 tokens).
pub const DEFAULT_OVERLAP: usize = 200;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn empty_text() {
        let chunks = chunk_text("", DEFAULT_CHUNK_SIZE, DEFAULT_OVERLAP);
        assert!(chunks.is_empty());
    }

    #[test]
    fn small_text_single_chunk() {
        let text = "hello world";
        let chunks = chunk_text(text, DEFAULT_CHUNK_SIZE, DEFAULT_OVERLAP);
        assert_eq!(chunks.len(), 1);
        assert_eq!(chunks[0].text, "hello world");
        assert_eq!(chunks[0].offset, 0);
    }

    #[test]
    fn exact_chunk_size() {
        let text = "a".repeat(2000);
        let chunks = chunk_text(&text, 2000, 200);
        assert_eq!(chunks.len(), 1);
        assert_eq!(chunks[0].text.len(), 2000);
    }

    #[test]
    fn two_chunks_with_overlap() {
        let text = "a".repeat(3000);
        let chunks = chunk_text(&text, 2000, 200);
        assert_eq!(chunks.len(), 2);
        assert_eq!(chunks[0].text.len(), 2000);
        assert_eq!(chunks[0].offset, 0);
        assert_eq!(chunks[1].offset, 1800); // 2000 - 200 overlap
        assert_eq!(chunks[1].text.len(), 1200); // remaining
    }

    #[test]
    fn three_chunks() {
        let text = "a".repeat(5000);
        let chunks = chunk_text(&text, 2000, 200);
        assert_eq!(chunks.len(), 3);
        assert_eq!(chunks[0].offset, 0);
        assert_eq!(chunks[1].offset, 1800);
        assert_eq!(chunks[2].offset, 3600);
    }

    #[test]
    fn overlap_content_matches() {
        let text: String = (0..3000).map(|i| char::from(b'a' + (i % 26) as u8)).collect();
        let chunks = chunk_text(&text, 2000, 200);
        assert_eq!(chunks.len(), 2);
        // Last 200 chars of chunk 0 should equal first 200 chars of chunk 1
        let tail = &chunks[0].text[1800..];
        let head = &chunks[1].text[..200];
        assert_eq!(tail, head);
    }

    #[test]
    fn multibyte_chars() {
        // Each emoji is 4 bytes
        let text = "🎉".repeat(600); // 2400 bytes
        let chunks = chunk_text(&text, 2000, 200);
        // Should not panic on char boundaries
        for chunk in &chunks {
            // Verify valid UTF-8
            assert!(chunk.text.is_char_boundary(0));
            assert!(chunk.text.len() <= 2000);
        }
    }

    #[test]
    fn offsets_are_correct() {
        let text = "abcdefghij".repeat(500); // 5000 chars
        let chunks = chunk_text(&text, 2000, 200);
        for chunk in &chunks {
            assert_eq!(&text[chunk.offset..chunk.offset + chunk.text.len()], chunk.text);
        }
    }

    #[test]
    fn covers_full_text() {
        let text = "x".repeat(4999);
        let chunks = chunk_text(&text, 2000, 200);
        // Last chunk should reach the end
        let last = chunks.last().unwrap();
        assert_eq!(last.offset + last.text.len(), text.len());
    }

    #[test]
    fn zero_overlap() {
        let text = "a".repeat(4000);
        let chunks = chunk_text(&text, 2000, 0);
        assert_eq!(chunks.len(), 2);
        assert_eq!(chunks[0].offset, 0);
        assert_eq!(chunks[1].offset, 2000);
    }
}
