use std::sync::Arc;
use serde_json::json;

use crate::llm::{LlmClient, ToolDefinition};

use super::{Tool, ToolError};

/// S3-compatible object storage tool with read/write path scoping and semantic search.
pub struct S3Tool {
    bucket: Box<s3::Bucket>,
    bucket_name: String,
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
            bucket_name: bucket_name.to_string(),
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

fn is_text_file(key: &str) -> bool {
    matches!(
        key.rsplit('.').next(),
        Some("md" | "txt" | "yaml" | "yml" | "json" | "toml" | "csv" | "xml" | "html" | "rst" | "org" | "log")
    )
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

                    // 1. List all readable objects with ETags
                    let results = self.bucket
                        .list(String::new(), None)
                        .await
                        .map_err(|e| ToolError::ExecutionFailed(format!("S3 list failed: {e}")))?;

                    struct S3Entry {
                        key: String,
                        etag: String,
                    }

                    let mut s3_entries: Vec<S3Entry> = Vec::new();
                    for result in &results {
                        for obj in &result.contents {
                            if self.is_readable(&obj.key) && is_text_file(&obj.key) {
                                s3_entries.push(S3Entry {
                                    key: obj.key.clone(),
                                    etag: obj.e_tag.clone().unwrap_or_default(),
                                });
                            }
                        }
                    }

                    if s3_entries.is_empty() {
                        return Ok("(no searchable objects found)".into());
                    }

                    // 2. Open or create file-based libSQL db
                    let home = std::env::var_os("HOME")
                        .ok_or_else(|| ToolError::ExecutionFailed("HOME not set".into()))?;
                    let cache_dir = std::path::PathBuf::from(home)
                        .join(".pilot")
                        .join("cache")
                        .join("s3");
                    std::fs::create_dir_all(&cache_dir)
                        .map_err(|e| ToolError::ExecutionFailed(format!("cache dir: {e}")))?;
                    let db_path = cache_dir.join(format!("{}.db", self.bucket_name));

                    let db = libsql::Builder::new_local(&db_path)
                        .build().await
                        .map_err(|e| ToolError::ExecutionFailed(format!("db open: {e}")))?;
                    let conn = db.connect()
                        .map_err(|e| ToolError::ExecutionFailed(format!("db connect: {e}")))?;

                    // 3. Ensure schema exists + check embedding dimension
                    conn.execute(
                        "CREATE TABLE IF NOT EXISTS meta (key TEXT PRIMARY KEY, value TEXT)",
                        (),
                    ).await.map_err(|e| ToolError::ExecutionFailed(format!("db meta: {e}")))?;

                    conn.execute(
                        "CREATE TABLE IF NOT EXISTS files (s3_key TEXT PRIMARY KEY, etag TEXT)",
                        (),
                    ).await.map_err(|e| ToolError::ExecutionFailed(format!("db files: {e}")))?;

                    // Get a single embedding to determine dimensions
                    let dim_probe = llm.embed(&[query.to_string()])
                        .await
                        .map_err(|e| ToolError::ExecutionFailed(format!("embedding failed: {e}")))?;
                    if dim_probe.is_empty() || dim_probe[0].is_empty() {
                        return Ok("(no embeddings returned)".into());
                    }
                    let dims = dim_probe[0].len();
                    if dims > 10000 {
                        return Err(ToolError::ExecutionFailed(format!("unexpected embedding dimensions: {dims}")));
                    }
                    let query_embedding = &dim_probe[0];

                    // Check if stored dimension matches; if not, rebuild
                    let mut stored_dims: Option<usize> = None;
                    let mut rows = conn.query("SELECT value FROM meta WHERE key = 'embedding_dim'", ())
                        .await.map_err(|e| ToolError::ExecutionFailed(format!("db meta read: {e}")))?;
                    if let Some(row) = rows.next().await
                        .map_err(|e| ToolError::ExecutionFailed(format!("db meta row: {e}")))? {
                        let v: String = row.get(0).map_err(|e| ToolError::ExecutionFailed(format!("meta get: {e}")))?;
                        stored_dims = v.parse().ok();
                    }

                    if stored_dims != Some(dims) {
                        // Dimension changed — drop and recreate chunks table
                        conn.execute("DROP TABLE IF EXISTS chunks", ()).await
                            .map_err(|e| ToolError::ExecutionFailed(format!("db drop: {e}")))?;
                        conn.execute("DELETE FROM files", ()).await
                            .map_err(|e| ToolError::ExecutionFailed(format!("db clear files: {e}")))?;
                        conn.execute(
                            "INSERT OR REPLACE INTO meta (key, value) VALUES ('embedding_dim', ?1)",
                            libsql::params![dims.to_string()],
                        ).await.map_err(|e| ToolError::ExecutionFailed(format!("db meta write: {e}")))?;
                    }

                    conn.execute(
                        &format!("CREATE TABLE IF NOT EXISTS chunks (id INTEGER PRIMARY KEY AUTOINCREMENT, s3_key TEXT, chunk_text TEXT, embedding F32_BLOB({dims}))"),
                        (),
                    ).await.map_err(|e| ToolError::ExecutionFailed(format!("db create chunks: {e}")))?;

                    // 4. Diff: compare S3 ETags against cached files
                    let mut cached: std::collections::HashMap<String, String> = std::collections::HashMap::new();
                    let mut rows = conn.query("SELECT s3_key, etag FROM files", ())
                        .await.map_err(|e| ToolError::ExecutionFailed(format!("db files read: {e}")))?;
                    while let Some(row) = rows.next().await
                        .map_err(|e| ToolError::ExecutionFailed(format!("db files row: {e}")))? {
                        let k: String = row.get(0).map_err(|e| ToolError::ExecutionFailed(format!("row: {e}")))?;
                        let e: String = row.get(1).map_err(|e| ToolError::ExecutionFailed(format!("row: {e}")))?;
                        cached.insert(k, e);
                    }

                    let s3_keys: std::collections::HashSet<&str> = s3_entries.iter().map(|e| e.key.as_str()).collect();

                    // Delete stale keys (removed from S3 or etag changed)
                    let mut stale_keys: Vec<String> = Vec::new();
                    for (cached_key, cached_etag) in &cached {
                        if !s3_keys.contains(cached_key.as_str()) {
                            stale_keys.push(cached_key.clone());
                        } else if let Some(entry) = s3_entries.iter().find(|e| &e.key == cached_key) {
                            if entry.etag != *cached_etag {
                                stale_keys.push(cached_key.clone());
                            }
                        }
                    }
                    for key in &stale_keys {
                        conn.execute("DELETE FROM chunks WHERE s3_key = ?1", libsql::params![key.clone()])
                            .await.map_err(|e| ToolError::ExecutionFailed(format!("db delete chunks: {e}")))?;
                        conn.execute("DELETE FROM files WHERE s3_key = ?1", libsql::params![key.clone()])
                            .await.map_err(|e| ToolError::ExecutionFailed(format!("db delete file: {e}")))?;
                    }

                    // Identify new keys (not in cache, or were stale and just deleted)
                    let new_entries: Vec<&S3Entry> = s3_entries.iter()
                        .filter(|e| !cached.contains_key(&e.key) || stale_keys.contains(&e.key))
                        .collect();

                    // 5. Download, chunk, and embed new/changed files
                    let fresh_count = s3_entries.len() - new_entries.len();
                    tracing::info!(
                        total = s3_entries.len(),
                        cached = fresh_count,
                        new = new_entries.len() - stale_keys.len(),
                        stale = stale_keys.len(),
                        "search index sync",
                    );

                    if !new_entries.is_empty() {
                        use crate::chunker::{chunk_text, DEFAULT_CHUNK_SIZE, DEFAULT_OVERLAP};

                        struct NewChunk {
                            s3_key: String,
                            text: String,
                        }

                        let mut new_chunks: Vec<NewChunk> = Vec::new();
                        for entry in &new_entries {
                            let response = self.bucket
                                .get_object(&entry.key)
                                .await
                                .map_err(|e| ToolError::ExecutionFailed(format!("S3 read failed: {e}")))?;
                            let body = String::from_utf8_lossy(response.bytes()).to_string();
                            let chunks = chunk_text(&body, DEFAULT_CHUNK_SIZE, DEFAULT_OVERLAP);
                            for chunk in chunks {
                                new_chunks.push(NewChunk { s3_key: entry.key.clone(), text: chunk.text });
                            }
                        }

                        if !new_chunks.is_empty() {
                            let texts: Vec<String> = new_chunks.iter().map(|c| c.text.clone()).collect();
                            let embeddings = llm.embed(&texts)
                                .await
                                .map_err(|e| ToolError::ExecutionFailed(format!("embedding failed: {e}")))?;

                            for (i, emb) in embeddings.iter().enumerate() {
                                let emb_json = serde_json::to_string(emb)
                                    .map_err(|e| ToolError::ExecutionFailed(format!("json: {e}")))?;
                                conn.execute(
                                    "INSERT INTO chunks (s3_key, chunk_text, embedding) VALUES (?1, ?2, vector32(?3))",
                                    libsql::params![new_chunks[i].s3_key.clone(), new_chunks[i].text.clone(), emb_json],
                                ).await.map_err(|e| ToolError::ExecutionFailed(format!("db insert chunk: {e}")))?;
                            }
                        }

                        // Update files table with new ETags
                        for entry in &new_entries {
                            conn.execute(
                                "INSERT OR REPLACE INTO files (s3_key, etag) VALUES (?1, ?2)",
                                libsql::params![entry.key.clone(), entry.etag.clone()],
                            ).await.map_err(|e| ToolError::ExecutionFailed(format!("db insert file: {e}")))?;
                        }
                    }

                    // 6. Query
                    let query_json = serde_json::to_string(query_embedding)
                        .map_err(|e| ToolError::ExecutionFailed(format!("json: {e}")))?;

                    let mut rows = conn.query(
                        "SELECT s3_key, chunk_text, vector_distance_cos(embedding, vector32(?1)) AS distance FROM chunks ORDER BY distance ASC LIMIT 10",
                        libsql::params![query_json],
                    ).await.map_err(|e| ToolError::ExecutionFailed(format!("db query: {e}")))?;

                    let mut output: Vec<serde_json::Value> = Vec::new();
                    while let Some(row) = rows.next().await
                        .map_err(|e| ToolError::ExecutionFailed(format!("db row: {e}")))? {
                        let s3_key: String = row.get(0).map_err(|e| ToolError::ExecutionFailed(format!("row get: {e}")))?;
                        let chunk_text: String = row.get(1).map_err(|e| ToolError::ExecutionFailed(format!("row get: {e}")))?;
                        let distance: f64 = row.get(2).map_err(|e| ToolError::ExecutionFailed(format!("row get: {e}")))?;
                        let similarity = 1.0 - distance;
                        output.push(serde_json::json!({
                            "path": s3_key,
                            "cosine_similarity": (similarity * 1000.0).round() / 1000.0,
                            "chunk": chunk_text,
                        }));
                    }

                    Ok(serde_json::to_string_pretty(&output)
                        .unwrap_or_else(|_| "[]".into()))
                }
                other => {
                    Ok(format!("unknown operation: {other}. Supported: list, read, write, delete, search"))
                }
            }
        })
    }
}

