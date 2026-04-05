use std::sync::Arc;
use serde_json::json;

use crate::llm::{LlmClient, ToolDefinition};

use super::{Tool, ToolError};

/// S3-compatible object storage tool with read/write path scoping and semantic search.
pub struct S3Tool {
    bucket: Box<s3::Bucket>,
    read_patterns: Vec<glob::Pattern>,
    write_patterns: Vec<glob::Pattern>,
    unrestricted: bool,
    llm: Option<Arc<dyn LlmClient>>,
}

impl S3Tool {
    pub fn new(
        endpoint: &str,
        access_key: &str,
        secret_key: &str,
        bucket_name: &str,
        read_globs: &[String],
        write_globs: &[String],
        llm: Option<Arc<dyn LlmClient>>,
    ) -> Result<Self, String> {
        if endpoint.is_empty() || access_key.is_empty() || secret_key.is_empty() {
            return Err("S3 tool requires s3_endpoint, s3_access_key, s3_secret_key in pilot config".into());
        }

        let region = s3::Region::Custom {
            region: "auto".into(),
            endpoint: format!("https://{endpoint}"),
        };
        let credentials = s3::creds::Credentials::new(
            Some(access_key),
            Some(secret_key),
            None,
            None,
            None,
        )
        .map_err(|e| format!("S3 credentials error: {e}"))?;

        let bucket = s3::Bucket::new(bucket_name, region, credentials)
            .map_err(|e| format!("S3 bucket error: {e}"))?;

        let read_patterns = read_globs
            .iter()
            .map(|g| glob::Pattern::new(g).map_err(|e| format!("invalid read glob '{g}': {e}")))
            .collect::<Result<Vec<_>, _>>()?;

        let write_patterns = write_globs
            .iter()
            .map(|g| glob::Pattern::new(g).map_err(|e| format!("invalid write glob '{g}': {e}")))
            .collect::<Result<Vec<_>, _>>()?;

        let unrestricted = read_patterns.is_empty() && write_patterns.is_empty();

        Ok(Self {
            bucket,
            read_patterns,
            write_patterns,
            unrestricted,
            llm,
        })
    }

    fn is_readable(&self, key: &str) -> bool {
        if self.unrestricted { return true; }
        self.read_patterns.iter().any(|p| p.matches(key))
            || self.write_patterns.iter().any(|p| p.matches(key))
    }

    fn is_writable(&self, key: &str) -> bool {
        if self.unrestricted { return true; }
        self.write_patterns.iter().any(|p| p.matches(key))
    }

    fn read_scope(&self) -> String {
        let all: Vec<&str> = self.read_patterns.iter()
            .chain(self.write_patterns.iter())
            .map(|p| p.as_str())
            .collect();
        if all.is_empty() { "none".into() } else { all.join(", ") }
    }

    fn write_scope(&self) -> String {
        let globs: Vec<&str> = self.write_patterns.iter().map(|p| p.as_str()).collect();
        if globs.is_empty() { "none".into() } else { globs.join(", ") }
    }
}

impl Tool for S3Tool {
    fn name(&self) -> &str {
        "s3"
    }

    fn description(&self) -> &str {
        "Read, write, list, and delete objects in S3-compatible storage"
    }

    fn definition(&self) -> ToolDefinition {
        let scope_desc = if self.unrestricted {
            "Access: unrestricted.".to_string()
        } else {
            let read_globs: Vec<&str> = self.read_patterns.iter().map(|p| p.as_str()).collect();
            let write_globs: Vec<&str> = self.write_patterns.iter().map(|p| p.as_str()).collect();
            format!(
                "Allowed read paths: {}. Allowed write paths: {}.",
                if read_globs.is_empty() { "none".to_string() } else { read_globs.join(", ") + " (write paths are also readable)" },
                if write_globs.is_empty() { "none".to_string() } else { write_globs.join(", ") },
            )
        };

        ToolDefinition::new(json!({
            "type": "function",
            "function": {
                "name": "s3",
                "description": format!(
                    "S3 object storage. Bucket: {}.\n\n{scope_desc}\n\nOperations:\n- list: list objects. Optional 'prefix' to filter.\n- read: get object content by key.\n- write: use exactly one of 'overwrite', 'append', or 'edit'. overwrite: write full content (creates if missing). append: add to end (creates if missing). edit: search/replace text in existing object.\n- delete: remove an object.\n- search: semantic search across all readable objects. Returns top matches ranked by relevance.",
                    self.bucket.name,
                ),
                "parameters": {
                    "type": "object",
                    "properties": {
                        "operation": {
                            "type": "string",
                            "enum": ["list", "read", "write", "delete", "search"],
                            "description": "list | read | write | delete | search"
                        },
                        "key": {
                            "type": "string",
                            "description": "Object key (for read, write, delete)"
                        },
                        "prefix": {
                            "type": "string",
                            "description": "For list: filter objects by prefix"
                        },
                        "overwrite": {
                            "type": "object",
                            "description": "Write full object content. Creates if missing.",
                            "properties": {
                                "content": { "type": "string", "description": "Full object content" }
                            },
                            "required": ["content"]
                        },
                        "append": {
                            "type": "object",
                            "description": "Append to end of object. Creates if missing.",
                            "properties": {
                                "content": { "type": "string", "description": "Text to append" }
                            },
                            "required": ["content"]
                        },
                        "edit": {
                            "type": "object",
                            "description": "Find and replace text in an existing object.",
                            "properties": {
                                "search": { "type": "string", "description": "Exact text to find" },
                                "replace": { "type": "string", "description": "Text to replace it with" }
                            },
                            "required": ["search", "replace"]
                        },
                        "query": {
                            "type": "string",
                            "description": "For search: natural language query to find relevant objects"
                        }
                    },
                    "required": ["operation"]
                }
            }
        }))
    }

    fn execute<'a>(
        &'a self,
        arguments: &'a str,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<String, ToolError>> + Send + 'a>>
    {
        Box::pin(async move {
            let args: serde_json::Value = serde_json::from_str(arguments)
                .map_err(|e| ToolError::ExecutionFailed(format!("invalid arguments: {e}")))?;

            let operation = args["operation"]
                .as_str()
                .ok_or_else(|| ToolError::ExecutionFailed("missing 'operation' field".into()))?;

            match operation {
                "list" => {
                    let prefix = args["prefix"].as_str().unwrap_or("");

                    let results = self.bucket
                        .list(prefix.to_string(), None)
                        .await
                        .map_err(|e| ToolError::ExecutionFailed(format!("S3 list failed: {e}")))?;

                    let mut entries = Vec::new();
                    for result in &results {
                        for obj in &result.contents {
                            if self.is_readable(&obj.key) {
                                entries.push(format!("{} ({} bytes)", obj.key, obj.size));
                            }
                        }
                    }

                    if entries.is_empty() {
                        Ok("(no objects found)".into())
                    } else {
                        Ok(entries.join("\n"))
                    }
                }
                "read" => {
                    let key = args["key"].as_str()
                        .ok_or_else(|| ToolError::ExecutionFailed("read requires 'key'".into()))?;

                    if !self.is_readable(key) {
                        return Err(ToolError::ExecutionFailed(
                            format!("key '{key}' is not readable. Allowed read paths: {}", self.read_scope())
                        ));
                    }

                    let response = self.bucket
                        .get_object(key)
                        .await
                        .map_err(|e| ToolError::ExecutionFailed(format!("S3 read failed: {e}")))?;

                    if response.status_code() == 404 {
                        return Ok(format!("(object not found: {key})"));
                    }

                    let body = String::from_utf8_lossy(response.bytes()).to_string();
                    Ok(body)
                }
                "write" => {
                    let key = args["key"].as_str()
                        .ok_or_else(|| ToolError::ExecutionFailed("write requires 'key'".into()))?;

                    if !self.is_writable(key) {
                        return Err(ToolError::ExecutionFailed(
                            format!("key '{key}' is not writable. Allowed write paths: {}", self.write_scope())
                        ));
                    }

                    if let Some(obj) = args["overwrite"].as_object() {
                        let content = obj["content"].as_str()
                            .ok_or_else(|| ToolError::ExecutionFailed("overwrite requires 'content'".into()))?;
                        self.bucket
                            .put_object(key, content.as_bytes())
                            .await
                            .map_err(|e| ToolError::ExecutionFailed(format!("S3 write failed: {e}")))?;
                        Ok(format!("wrote {key}"))
                    } else if let Some(obj) = args["append"].as_object() {
                        let content = obj["content"].as_str()
                            .ok_or_else(|| ToolError::ExecutionFailed("append requires 'content'".into()))?;
                        // Read existing content, append, write back
                        let existing = match self.bucket.get_object(key).await {
                            Ok(resp) if resp.status_code() != 404 => {
                                String::from_utf8_lossy(resp.bytes()).to_string()
                            }
                            _ => String::new(),
                        };
                        let new_content = format!("{existing}{content}");
                        self.bucket
                            .put_object(key, new_content.as_bytes())
                            .await
                            .map_err(|e| ToolError::ExecutionFailed(format!("S3 write failed: {e}")))?;
                        Ok(format!("appended to {key}"))
                    } else if let Some(obj) = args["edit"].as_object() {
                        let search = obj["search"].as_str()
                            .ok_or_else(|| ToolError::ExecutionFailed("edit requires 'search'".into()))?;
                        let replace = obj["replace"].as_str()
                            .ok_or_else(|| ToolError::ExecutionFailed("edit requires 'replace'".into()))?;
                        // Read, search/replace, write back
                        let response = self.bucket
                            .get_object(key)
                            .await
                            .map_err(|e| ToolError::ExecutionFailed(format!("S3 read failed: {e}")))?;
                        if response.status_code() == 404 {
                            return Ok(format!("(object not found: {key})"));
                        }
                        let content = String::from_utf8_lossy(response.bytes()).to_string();
                        if !content.contains(search) {
                            return Ok(format!("search string not found in {key}. Read the object first, then retry with exact text."));
                        }
                        let new_content = content.replacen(search, replace, 1);
                        self.bucket
                            .put_object(key, new_content.as_bytes())
                            .await
                            .map_err(|e| ToolError::ExecutionFailed(format!("S3 write failed: {e}")))?;
                        Ok(format!("edited {key}"))
                    } else {
                        Ok("write requires one of: overwrite, append, or edit. Example: {\"operation\": \"write\", \"key\": \"file.md\", \"overwrite\": {\"content\": \"hello\"}}".into())
                    }
                }
                "delete" => {
                    let key = args["key"].as_str()
                        .ok_or_else(|| ToolError::ExecutionFailed("delete requires 'key'".into()))?;

                    if !self.is_writable(key) {
                        return Err(ToolError::ExecutionFailed(
                            format!("key '{key}' is not writable. Allowed write paths: {}", self.write_scope())
                        ));
                    }

                    self.bucket
                        .delete_object(key)
                        .await
                        .map_err(|e| ToolError::ExecutionFailed(format!("S3 delete failed: {e}")))?;

                    Ok(format!("deleted {key}"))
                }
                "search" => {
                    let query = args["query"].as_str()
                        .ok_or_else(|| ToolError::ExecutionFailed("search requires 'query'".into()))?;

                    let llm = self.llm.as_ref()
                        .ok_or_else(|| ToolError::ExecutionFailed("search requires LLM client for embeddings".into()))?;

                    // 1. List all readable objects
                    let results = self.bucket
                        .list(String::new(), None)
                        .await
                        .map_err(|e| ToolError::ExecutionFailed(format!("S3 list failed: {e}")))?;

                    let mut keys: Vec<String> = Vec::new();
                    for result in &results {
                        for obj in &result.contents {
                            if self.is_readable(&obj.key) && obj.key.ends_with(".md") {
                                keys.push(obj.key.clone());
                            }
                        }
                    }

                    if keys.is_empty() {
                        return Ok("(no searchable objects found)".into());
                    }

                    // 2. Read all file contents
                    let mut contents: Vec<String> = Vec::new();
                    for key in &keys {
                        let response = self.bucket
                            .get_object(key)
                            .await
                            .map_err(|e| ToolError::ExecutionFailed(format!("S3 read failed: {e}")))?;
                        let body = String::from_utf8_lossy(response.bytes()).to_string();
                        // Truncate large files for embedding
                        let truncated = if body.len() > 8000 {
                            // Find a char boundary near 8000
                            let mut end = 8000;
                            while !body.is_char_boundary(end) { end -= 1; }
                            body[..end].to_string()
                        } else { body };
                        contents.push(truncated);
                    }

                    // 3. Embed all files + query
                    let mut all_texts = contents.clone();
                    all_texts.push(query.to_string());

                    let embeddings = llm.embed(&all_texts)
                        .await
                        .map_err(|e| ToolError::ExecutionFailed(format!("embedding failed: {e}")))?;

                    if embeddings.is_empty() {
                        return Ok("(no embeddings returned)".into());
                    }

                    let dims = embeddings[0].len();
                    let query_embedding = embeddings.last()
                        .ok_or_else(|| ToolError::ExecutionFailed("no query embedding".into()))?;

                    // 4. Build usearch index in memory
                    use usearch::Index;
                    let index = Index::new(&usearch::IndexOptions {
                        dimensions: dims,
                        metric: usearch::MetricKind::Cos,
                        quantization: usearch::ScalarKind::F32,
                        ..Default::default()
                    }).map_err(|e| ToolError::ExecutionFailed(format!("index create: {e}")))?;

                    index.reserve(keys.len())
                        .map_err(|e| ToolError::ExecutionFailed(format!("index reserve: {e}")))?;

                    for (i, emb) in embeddings[..keys.len()].iter().enumerate() {
                        index.add(i as u64, emb)
                            .map_err(|e| ToolError::ExecutionFailed(format!("index add: {e}")))?;
                    }

                    // 5. Search
                    let results = index.search(query_embedding, 10)
                        .map_err(|e| ToolError::ExecutionFailed(format!("index search: {e}")))?;

                    let output: Vec<String> = results.keys.iter()
                        .zip(results.distances.iter())
                        .map(|(key, distance)| {
                            let i = *key as usize;
                            let k = &keys[i];
                            let similarity = 1.0 - distance;
                            format!("--- {k} (score: {similarity:.3}) ---\n{}\n", contents[i])
                        })
                        .collect();

                    Ok(output.join("\n"))
                }
                other => {
                    Ok(format!("unknown operation: {other}. Supported: list, read, write, delete, search"))
                }
            }
        })
    }
}

