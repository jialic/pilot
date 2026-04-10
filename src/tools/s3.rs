use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
use std::sync::Arc;
use serde_json::json;

use crate::llm::{LlmClient, ToolDefinition};

use super::{Tool, ToolError};

/// S3-compatible object storage tool with read/write path scoping and semantic search.
pub struct S3Tool {
    bucket: Box<s3::Bucket>,
    cache_key: String,
    read_patterns: Vec<glob::Pattern>,
    write_patterns: Vec<glob::Pattern>,
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
        yaml_path: &str,
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

        let mut hasher = DefaultHasher::new();
        yaml_path.hash(&mut hasher);
        let cache_key = format!("{}-{:x}", bucket_name, hasher.finish());

        Ok(Self {
            bucket,
            cache_key,
            read_patterns,
            write_patterns,
            llm,
        })
    }

    fn is_readable(&self, key: &str) -> bool {
        self.read_patterns.iter().any(|p| p.matches(key))
            || self.write_patterns.iter().any(|p| p.matches(key))
    }

    fn is_writable(&self, key: &str) -> bool {
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

struct S3Entry {
    key: String,
    etag: String,
}

struct IndexDiff {
    /// Keys to remove from the index (deleted from S3 or etag changed).
    stale: Vec<String>,
    /// Indices into the entries slice that need (re-)indexing.
    new: Vec<usize>,
}

/// Compare current S3 entries against cached (key → etag) map.
fn diff_index(entries: &[S3Entry], cached: &std::collections::HashMap<String, String>) -> IndexDiff {
    let s3_keys: std::collections::HashSet<&str> = entries.iter().map(|e| e.key.as_str()).collect();

    let mut stale: Vec<String> = Vec::new();
    for (cached_key, cached_etag) in cached {
        if !s3_keys.contains(cached_key.as_str()) {
            stale.push(cached_key.clone());
        } else if let Some(entry) = entries.iter().find(|e| &e.key == cached_key) {
            if entry.etag != *cached_etag {
                stale.push(cached_key.clone());
            }
        }
    }

    let stale_set: std::collections::HashSet<&str> = stale.iter().map(|s| s.as_str()).collect();
    let new: Vec<usize> = entries.iter().enumerate()
        .filter(|(_, e)| !cached.contains_key(&e.key) || stale_set.contains(e.key.as_str()))
        .map(|(i, _)| i)
        .collect();

    IndexDiff { stale, new }
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
        let scope_desc = {
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
                    "S3 object storage. Bucket: {}.\n\n{scope_desc}\n\nOperations:\n- list: list objects. Optional 'prefix' to filter.\n- read: get object content by key.\n- write: set operation to \"write\" AND provide exactly one of the overwrite/append/edit objects.\n- delete: remove an object.\n- search: semantic search across all readable objects. Returns top matches ranked by relevance.\n\nExamples:\n  Write: {{\"operation\": \"write\", \"key\": \"f.md\", \"overwrite\": {{\"content\": \"hello\"}}}}\n  Edit:  {{\"operation\": \"write\", \"key\": \"f.md\", \"edit\": {{\"search\": \"old\", \"replace\": \"new\"}}}}\n  WRONG: {{\"operation\": \"overwrite\", ...}} — overwrite is NOT an operation.",
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
                            "description": "Write mode: write full object content. Creates if missing.",
                            "properties": {
                                "content": { "type": "string", "description": "Full object content" }
                            },
                            "required": ["content"]
                        },
                        "append": {
                            "type": "object",
                            "description": "Write mode: append to end of object. Creates if missing.",
                            "properties": {
                                "content": { "type": "string", "description": "Text to append" }
                            },
                            "required": ["content"]
                        },
                        "edit": {
                            "type": "object",
                            "description": "Write mode: find and replace text in an existing object.",
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

                    // 2. Open or create file-based libSQL db
                    let home = std::env::var_os("HOME")
                        .ok_or_else(|| ToolError::ExecutionFailed("HOME not set".into()))?;
                    let cache_dir = std::path::PathBuf::from(home)
                        .join(".pilot")
                        .join("cache")
                        .join("s3");
                    std::fs::create_dir_all(&cache_dir)
                        .map_err(|e| ToolError::ExecutionFailed(format!("cache dir: {e}")))?;
                    let db_path = cache_dir.join(format!("{}.db", self.cache_key));

                    let db = libsql::Builder::new_local(&db_path)
                        .build().await
                        .map_err(|e| ToolError::ExecutionFailed(format!("db open: {e}")))?;
                    let conn = db.connect()
                        .map_err(|e| ToolError::ExecutionFailed(format!("db connect: {e}")))?;
                    conn.execute("PRAGMA foreign_keys = ON", ())
                        .await.map_err(|e| ToolError::ExecutionFailed(format!("pragma: {e}")))?;

                    // Run schema migrations
                    super::migrate::run_s3_migrations(&conn, &db_path)
                        .await
                        .map_err(|e| ToolError::ExecutionFailed(e))?;

                    // 3. Diff: compare S3 ETags against cached files
                    let mut cached: std::collections::HashMap<String, String> = std::collections::HashMap::new();
                    let mut rows = conn.query("SELECT s3_key, etag FROM files", ())
                        .await.map_err(|e| ToolError::ExecutionFailed(format!("db files read: {e}")))?;
                    while let Some(row) = rows.next().await
                        .map_err(|e| ToolError::ExecutionFailed(format!("db files row: {e}")))? {
                        let k: String = row.get(0).map_err(|e| ToolError::ExecutionFailed(format!("row: {e}")))?;
                        let e: String = row.get(1).map_err(|e| ToolError::ExecutionFailed(format!("row: {e}")))?;
                        cached.insert(k, e);
                    }

                    let diff = diff_index(&s3_entries, &cached);

                    for key in &diff.stale {
                        conn.execute("DELETE FROM files WHERE s3_key = ?1", libsql::params![key.clone()])
                            .await.map_err(|e| ToolError::ExecutionFailed(format!("db delete file: {e}")))?;
                    }

                    if s3_entries.is_empty() {
                        return Ok("(no searchable objects found)".into());
                    }

                    // 4. Download, chunk, and embed new/changed files
                    tracing::info!(
                        total = s3_entries.len(),
                        cached = s3_entries.len() - diff.new.len(),
                        new = diff.new.len() - diff.stale.len(),
                        stale = diff.stale.len(),
                        "search index sync",
                    );

                    if !diff.new.is_empty() {
                        use crate::chunker::{chunk_text, DEFAULT_CHUNK_SIZE, DEFAULT_OVERLAP};
                        use futures::stream::{self, StreamExt};

                        let tasks: Vec<_> = diff.new.iter().map(|&idx| {
                            (s3_entries[idx].key.clone(), s3_entries[idx].etag.clone())
                        }).collect();

                        let results: Vec<Result<(), ToolError>> = stream::iter(tasks.into_iter().map(|(key, etag)| {
                            let bucket = self.bucket.clone();
                            let llm = llm.clone();
                            let conn = conn.clone();
                            async move {
                                let response = bucket
                                    .get_object(&key)
                                    .await
                                    .map_err(|e| ToolError::ExecutionFailed(format!("S3 read failed: {e}")))?;
                                let body = String::from_utf8_lossy(response.bytes()).to_string();
                                let chunks = chunk_text(&body, DEFAULT_CHUNK_SIZE, DEFAULT_OVERLAP);

                                // Insert file with empty etag placeholder (satisfies FK for chunks)
                                conn.execute(
                                    "INSERT OR REPLACE INTO files (s3_key, etag) VALUES (?1, '')",
                                    libsql::params![key.clone()],
                                ).await.map_err(|e| ToolError::ExecutionFailed(format!("db insert file: {e}")))?;

                                if !chunks.is_empty() {
                                    let texts: Vec<String> = chunks.iter().map(|c| c.text.clone()).collect();
                                    let embeddings = llm.embed(&texts)
                                        .await
                                        .map_err(|e| ToolError::ExecutionFailed(format!("embedding failed: {e}")))?;

                                    for (i, emb) in embeddings.iter().enumerate() {
                                        let emb_json = serde_json::to_string(emb)
                                            .map_err(|e| ToolError::ExecutionFailed(format!("json: {e}")))?;
                                        conn.execute(
                                            "INSERT INTO chunks (s3_key, chunk_offset, chunk_text, embedding) VALUES (?1, ?2, ?3, vector32(?4))",
                                            libsql::params![key.clone(), chunks[i].offset as i64, chunks[i].text.clone(), emb_json],
                                        ).await.map_err(|e| ToolError::ExecutionFailed(format!("db insert chunk: {e}")))?;
                                    }
                                }

                                // Set real etag — marks file as fully indexed
                                conn.execute(
                                    "UPDATE files SET etag = ?1 WHERE s3_key = ?2",
                                    libsql::params![etag.clone(), key.clone()],
                                ).await.map_err(|e| ToolError::ExecutionFailed(format!("db update etag: {e}")))?;

                                Ok(())
                            }
                        }))
                        .buffer_unordered(4)
                        .collect()
                        .await;

                        for result in results {
                            result?;
                        }
                    }

                    // 5. Embed query and search
                    let query_embeddings = llm.embed(&[query.to_string()])
                        .await
                        .map_err(|e| ToolError::ExecutionFailed(format!("embedding failed: {e}")))?;
                    if query_embeddings.is_empty() || query_embeddings[0].is_empty() {
                        return Ok("(no embeddings returned)".into());
                    }
                    let query_embedding = &query_embeddings[0];

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

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    fn entry(key: &str, etag: &str) -> S3Entry {
        S3Entry { key: key.into(), etag: etag.into() }
    }

    #[test]
    fn empty_both() {
        let diff = diff_index(&[], &HashMap::new());
        assert!(diff.stale.is_empty());
        assert!(diff.new.is_empty());
    }

    #[test]
    fn all_new() {
        let entries = vec![entry("a.md", "e1"), entry("b.md", "e2")];
        let diff = diff_index(&entries, &HashMap::new());
        assert!(diff.stale.is_empty());
        assert_eq!(diff.new, vec![0, 1]);
    }

    #[test]
    fn all_cached() {
        let entries = vec![entry("a.md", "e1"), entry("b.md", "e2")];
        let cached: HashMap<String, String> = [
            ("a.md".into(), "e1".into()),
            ("b.md".into(), "e2".into()),
        ].into();
        let diff = diff_index(&entries, &cached);
        assert!(diff.stale.is_empty());
        assert!(diff.new.is_empty());
    }

    #[test]
    fn deleted_file() {
        let entries = vec![entry("a.md", "e1")];
        let cached: HashMap<String, String> = [
            ("a.md".into(), "e1".into()),
            ("gone.md".into(), "e9".into()),
        ].into();
        let diff = diff_index(&entries, &cached);
        assert_eq!(diff.stale, vec!["gone.md"]);
        assert!(diff.new.is_empty());
    }

    #[test]
    fn etag_changed() {
        let entries = vec![entry("a.md", "e2")];
        let cached: HashMap<String, String> = [
            ("a.md".into(), "e1".into()),
        ].into();
        let diff = diff_index(&entries, &cached);
        assert_eq!(diff.stale, vec!["a.md"]);
        assert_eq!(diff.new, vec![0]);
    }

    #[test]
    fn permission_revoked() {
        // File exists in cache but not in entries (filtered out by permission)
        let entries = vec![entry("public.md", "e1")];
        let cached: HashMap<String, String> = [
            ("public.md".into(), "e1".into()),
            ("secret.md".into(), "e5".into()),
        ].into();
        let diff = diff_index(&entries, &cached);
        assert_eq!(diff.stale, vec!["secret.md"]);
        assert!(diff.new.is_empty());
    }

    #[test]
    fn all_permissions_lost() {
        // No readable entries but cache has files — everything stale
        let cached: HashMap<String, String> = [
            ("a.md".into(), "e1".into()),
            ("b.md".into(), "e2".into()),
        ].into();
        let diff = diff_index(&[], &cached);
        let mut stale = diff.stale.clone();
        stale.sort();
        assert_eq!(stale, vec!["a.md", "b.md"]);
        assert!(diff.new.is_empty());
    }

    #[test]
    fn all_modified() {
        let entries = vec![entry("a.md", "e2"), entry("b.md", "e4")];
        let cached: HashMap<String, String> = [
            ("a.md".into(), "e1".into()),
            ("b.md".into(), "e3".into()),
        ].into();
        let diff = diff_index(&entries, &cached);
        let mut stale = diff.stale.clone();
        stale.sort();
        assert_eq!(stale, vec!["a.md", "b.md"]);
        assert_eq!(diff.new, vec![0, 1]);
    }

    #[test]
    fn mixed_new_stale_unchanged() {
        let entries = vec![
            entry("unchanged.md", "e1"),
            entry("modified.md", "e3"),
            entry("brand_new.md", "e4"),
        ];
        let cached: HashMap<String, String> = [
            ("unchanged.md".into(), "e1".into()),
            ("modified.md".into(), "e2".into()),
            ("deleted.md".into(), "e9".into()),
        ].into();
        let diff = diff_index(&entries, &cached);
        let mut stale = diff.stale.clone();
        stale.sort();
        assert_eq!(stale, vec!["deleted.md", "modified.md"]);
        assert_eq!(diff.new, vec![1, 2]); // modified + brand_new
    }
}

