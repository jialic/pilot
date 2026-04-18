use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
use std::path::PathBuf;
use std::sync::Arc;

use crate::llm::{LlmClient, ToolDefinition};
use serde_json::json;

use super::{Tool, ToolError};

/// Expand ~ to the user's home directory.
pub fn expand_home(path: &str) -> PathBuf {
    if let Some(rest) = path.strip_prefix("~/") {
        if let Some(home) = home_dir() {
            return home.join(rest);
        }
    }
    PathBuf::from(path)
}

fn home_dir() -> Option<PathBuf> {
    #[cfg(unix)]
    {
        std::env::var_os("HOME").map(PathBuf::from)
    }
    #[cfg(windows)]
    {
        std::env::var_os("USERPROFILE").map(PathBuf::from)
    }
}

/// FileTool provides read, list, and write operations scoped by glob patterns.
/// Paths must be absolute or ~/relative.
pub struct FileTool {
    read_patterns: Vec<glob::Pattern>,
    write_patterns: Vec<glob::Pattern>,
    semantic_index: bool,
    cache_key: String,
    llm: Option<Arc<dyn LlmClient>>,
}

impl FileTool {
    pub fn new(
        read_globs: Vec<String>,
        write_globs: Vec<String>,
        semantic_index: bool,
        llm: Option<Arc<dyn LlmClient>>,
        yaml_path: &str,
    ) -> Result<Self, String> {
        if semantic_index && llm.is_none() {
            return Err("semantic_index requires OpenAI API key for embeddings".into());
        }

        let read_patterns = read_globs
            .iter()
            .map(|g| {
                let expanded = expand_glob(g);
                glob::Pattern::new(&expanded)
                    .map_err(|e| format!("invalid read glob '{g}': {e}"))
            })
            .collect::<Result<Vec<_>, _>>()?;

        let write_patterns = write_globs
            .iter()
            .map(|g| {
                let expanded = expand_glob(g);
                glob::Pattern::new(&expanded)
                    .map_err(|e| format!("invalid write glob '{g}': {e}"))
            })
            .collect::<Result<Vec<_>, _>>()?;

        let mut hasher = DefaultHasher::new();
        yaml_path.hash(&mut hasher);
        let cache_key = format!("{:x}", hasher.finish());

        Ok(Self {
            read_patterns,
            write_patterns,
            semantic_index,
            cache_key,
            llm,
        })
    }

    fn matches_any(patterns: &[glob::Pattern], path: &str) -> bool {
        let expanded = expand_home(path);
        let path_str = expanded.to_string_lossy();
        patterns.iter().any(|p| {
            p.matches(&path_str)
                || p.matches(&format!("{path_str}/"))
                || p.as_str().starts_with(&format!("{path_str}/"))
        })
    }

    fn is_readable(&self, path: &str) -> bool {
        // Readable if in read paths OR write paths (write implies read)
        Self::matches_any(&self.read_patterns, path)
            || Self::matches_any(&self.write_patterns, path)
    }

    fn is_writable(&self, path: &str) -> bool {
        Self::matches_any(&self.write_patterns, path)
    }

    fn validate_path(path: &str) -> Result<PathBuf, String> {
        if !path.starts_with('/') && !path.starts_with("~/") {
            return Err(format!("path must be absolute or ~/relative: {path}"));
        }
        Ok(expand_home(path))
    }
}

/// Expand ~/  in glob patterns so they match against expanded paths.
fn expand_glob(pattern: &str) -> String {
    if let Some(rest) = pattern.strip_prefix("~/") {
        if let Some(home) = std::env::var("HOME").ok() {
            return format!("{home}/{rest}");
        }
    }
    pattern.to_string()
}

impl FileTool {
    fn scope_desc(&self) -> String {
        let read_globs: Vec<String> = self.read_patterns.iter().map(|p| p.as_str().to_string()).collect();
        let write_globs: Vec<String> = self.write_patterns.iter().map(|p| p.as_str().to_string()).collect();
        format!(
            "Allowed read paths: {}. Allowed write paths: {}.",
            if read_globs.is_empty() { "none".to_string() } else { read_globs.join(", ") },
            if write_globs.is_empty() { "none".to_string() } else { write_globs.join(", ") + " (write paths are also readable)" },
        )
    }

    async fn do_search(&self, args: &serde_json::Value) -> Result<String, ToolError> {
        use crate::chunker::{chunk_text, DEFAULT_CHUNK_SIZE, DEFAULT_OVERLAP};
        use futures::stream::{self, StreamExt};

        let query = args["query"].as_str()
            .ok_or_else(|| ToolError::ExecutionFailed("search requires 'query'".into()))?;
        let prefix = args["prefix"].as_str();

        let llm = self.llm.as_ref()
            .ok_or_else(|| ToolError::ExecutionFailed("search requires LLM client for embeddings".into()))?;

        // 1. List readable files by walking globs; collect path + mtime
        let mut entries: Vec<FileEntry> = Vec::new();
        for pat in &self.read_patterns {
            let glob_results = glob::glob(pat.as_str())
                .map_err(|e| ToolError::ExecutionFailed(format!("invalid glob pattern: {e}")))?;
            for entry in glob_results {
                let path = match entry {
                    Ok(p) => p,
                    Err(_) => continue,
                };
                if !path.is_file() {
                    continue;
                }
                let path_str = path.to_string_lossy().to_string();
                if !is_text_file(&path_str) {
                    continue;
                }
                let mtime = std::fs::metadata(&path)
                    .and_then(|m| m.modified())
                    .map(|t| t.duration_since(std::time::UNIX_EPOCH).unwrap_or_default().as_nanos().to_string())
                    .unwrap_or_default();
                entries.push(FileEntry { path: path_str, mtime });
            }
        }

        // 2. Open libsql db
        let home = std::env::var_os("HOME")
            .ok_or_else(|| ToolError::ExecutionFailed("HOME not set".into()))?;
        let cache_dir = std::path::PathBuf::from(home)
            .join(".pilot")
            .join("cache")
            .join("file");
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

        super::migrate::run_file_migrations(&conn, &db_path)
            .await
            .map_err(|e| ToolError::ExecutionFailed(e))?;

        // 3. Diff
        let mut cached: std::collections::HashMap<String, String> = std::collections::HashMap::new();
        let mut rows = conn.query("SELECT path, mtime FROM files", ())
            .await.map_err(|e| ToolError::ExecutionFailed(format!("db files read: {e}")))?;
        while let Some(row) = rows.next().await
            .map_err(|e| ToolError::ExecutionFailed(format!("db files row: {e}")))? {
            let p: String = row.get(0).map_err(|e| ToolError::ExecutionFailed(format!("row: {e}")))?;
            let m: String = row.get(1).map_err(|e| ToolError::ExecutionFailed(format!("row: {e}")))?;
            cached.insert(p, m);
        }

        let diff = diff_file_index(&entries, &cached);

        for path in &diff.stale {
            conn.execute("DELETE FROM files WHERE path = ?1", libsql::params![path.clone()])
                .await.map_err(|e| ToolError::ExecutionFailed(format!("db delete file: {e}")))?;
        }

        if entries.is_empty() {
            return Ok("(no searchable files found)".into());
        }

        tracing::info!(
            total = entries.len(),
            cached = entries.len() - diff.new.len(),
            new = diff.new.len().saturating_sub(diff.stale.len()),
            stale = diff.stale.len(),
            "file search index sync",
        );

        if !diff.new.is_empty() {
            let tasks: Vec<_> = diff.new.iter().map(|&idx| {
                (entries[idx].path.clone(), entries[idx].mtime.clone())
            }).collect();

            let results: Vec<Result<(), ToolError>> = stream::iter(tasks.into_iter().map(|(path, mtime)| {
                let llm = llm.clone();
                let conn = conn.clone();
                async move {
                    let body = std::fs::read_to_string(&path)
                        .map_err(|e| ToolError::ExecutionFailed(format!("read failed {path}: {e}")))?;
                    let chunks = chunk_text(&body, DEFAULT_CHUNK_SIZE, DEFAULT_OVERLAP);

                    // Insert placeholder with empty mtime (satisfies FK, marks as in-progress)
                    conn.execute(
                        "INSERT OR REPLACE INTO files (path, mtime) VALUES (?1, '')",
                        libsql::params![path.clone()],
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
                                "INSERT INTO chunks (path, chunk_offset, chunk_text, embedding) VALUES (?1, ?2, ?3, vector32(?4))",
                                libsql::params![path.clone(), chunks[i].offset as i64, chunks[i].text.clone(), emb_json],
                            ).await.map_err(|e| ToolError::ExecutionFailed(format!("db insert chunk: {e}")))?;
                        }
                    }

                    // Set real mtime — marks file as fully indexed
                    conn.execute(
                        "UPDATE files SET mtime = ?1 WHERE path = ?2",
                        libsql::params![mtime.clone(), path.clone()],
                    ).await.map_err(|e| ToolError::ExecutionFailed(format!("db update mtime: {e}")))?;

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

        // 4. Embed query and search
        let query_embeddings = llm.embed(&[query.to_string()])
            .await
            .map_err(|e| ToolError::ExecutionFailed(format!("embedding failed: {e}")))?;
        if query_embeddings.is_empty() || query_embeddings[0].is_empty() {
            return Ok("(no embeddings returned)".into());
        }
        let query_embedding = &query_embeddings[0];

        let query_json = serde_json::to_string(query_embedding)
            .map_err(|e| ToolError::ExecutionFailed(format!("json: {e}")))?;

        let mut rows = if let Some(pfx) = prefix {
            let expanded = expand_home(pfx).to_string_lossy().to_string();
            conn.query(
                "SELECT path, chunk_text, vector_distance_cos(embedding, vector32(?1)) AS distance FROM chunks WHERE path LIKE ?2 ORDER BY distance ASC LIMIT 10",
                libsql::params![query_json, format!("{expanded}%")],
            ).await
        } else {
            conn.query(
                "SELECT path, chunk_text, vector_distance_cos(embedding, vector32(?1)) AS distance FROM chunks ORDER BY distance ASC LIMIT 10",
                libsql::params![query_json],
            ).await
        }.map_err(|e| ToolError::ExecutionFailed(format!("db query: {e}")))?;

        let mut output: Vec<serde_json::Value> = Vec::new();
        while let Some(row) = rows.next().await
            .map_err(|e| ToolError::ExecutionFailed(format!("db row: {e}")))? {
            let path: String = row.get(0).map_err(|e| ToolError::ExecutionFailed(format!("row get: {e}")))?;
            let chunk_text: String = row.get(1).map_err(|e| ToolError::ExecutionFailed(format!("row get: {e}")))?;
            let distance: f64 = row.get(2).map_err(|e| ToolError::ExecutionFailed(format!("row get: {e}")))?;
            let similarity = 1.0 - distance;
            output.push(serde_json::json!({
                "path": path,
                "cosine_similarity": (similarity * 1000.0).round() / 1000.0,
                "chunk": chunk_text,
            }));
        }

        Ok(serde_json::to_string_pretty(&output)
            .unwrap_or_else(|_| "[]".into()))
    }
}

struct FileEntry {
    path: String,
    mtime: String,
}

struct FileIndexDiff {
    stale: Vec<String>,
    new: Vec<usize>,
}

fn diff_file_index(entries: &[FileEntry], cached: &std::collections::HashMap<String, String>) -> FileIndexDiff {
    let current_paths: std::collections::HashSet<&str> = entries.iter().map(|e| e.path.as_str()).collect();

    let mut stale: Vec<String> = Vec::new();
    for (cached_path, cached_mtime) in cached {
        if !current_paths.contains(cached_path.as_str()) {
            stale.push(cached_path.clone());
        } else if let Some(entry) = entries.iter().find(|e| &e.path == cached_path) {
            if entry.mtime != *cached_mtime {
                stale.push(cached_path.clone());
            }
        }
    }

    let stale_set: std::collections::HashSet<&str> = stale.iter().map(|s| s.as_str()).collect();
    let new: Vec<usize> = entries.iter().enumerate()
        .filter(|(_, e)| !cached.contains_key(&e.path) || stale_set.contains(e.path.as_str()))
        .map(|(i, _)| i)
        .collect();

    FileIndexDiff { stale, new }
}

fn is_text_file(path: &str) -> bool {
    matches!(
        path.rsplit('.').next(),
        Some("md" | "txt" | "yaml" | "yml" | "json" | "toml" | "csv" | "xml" | "html" | "rst" | "org" | "log" | "rs" | "py" | "js" | "ts" | "go" | "java" | "c" | "cpp" | "h" | "hpp")
    )
}

impl Tool for FileTool {
    fn name(&self) -> &str {
        "file"
    }

    fn description(&self) -> &str {
        "Read, list, and write files within allowed paths"
    }

    fn definitions(&self) -> Vec<ToolDefinition> {
        let scope = self.scope_desc();
        let mut defs = vec![
            ToolDefinition::new(json!({
                "type": "function",
                "function": {
                    "name": "file_read",
                    "description": format!("Read file contents. Returns '(file does not exist)' if missing. Path must be absolute or ~/relative.\n\n{scope}"),
                    "parameters": {
                        "type": "object",
                        "properties": {
                            "path": { "type": "string", "description": "Absolute path (/...) or home-relative path (~/...)" }
                        },
                        "required": ["path"]
                    }
                }
            })),
            ToolDefinition::new(json!({
                "type": "function",
                "function": {
                    "name": "file_list",
                    "description": format!("List directory entries, one per line, directories suffixed with /. Path must be absolute or ~/relative.\n\n{scope}"),
                    "parameters": {
                        "type": "object",
                        "properties": {
                            "path": { "type": "string", "description": "Absolute path (/...) or home-relative path (~/...)" }
                        },
                        "required": ["path"]
                    }
                }
            })),
            ToolDefinition::new(json!({
                "type": "function",
                "function": {
                    "name": "file_find",
                    "description": format!("Find files matching a glob pattern. Path must be absolute or ~/relative.\n\n{scope}"),
                    "parameters": {
                        "type": "object",
                        "properties": {
                            "pattern": { "type": "string", "description": "Glob pattern (e.g. '~/projects/**/*.rs', '/tmp/*.log')" }
                        },
                        "required": ["pattern"]
                    }
                }
            })),
            ToolDefinition::new(json!({
                "type": "function",
                "function": {
                    "name": "file_write",
                    "description": format!("Write full file content. Creates file if it doesn't exist. Path must be absolute or ~/relative.\n\n{scope}"),
                    "parameters": {
                        "type": "object",
                        "properties": {
                            "path": { "type": "string", "description": "Absolute path (/...) or home-relative path (~/...)" },
                            "content": { "type": "string", "description": "Full file content" }
                        },
                        "required": ["path", "content"]
                    }
                }
            })),
            ToolDefinition::new(json!({
                "type": "function",
                "function": {
                    "name": "file_append",
                    "description": format!("Append text to end of file. Creates file if it doesn't exist. Path must be absolute or ~/relative.\n\n{scope}"),
                    "parameters": {
                        "type": "object",
                        "properties": {
                            "path": { "type": "string", "description": "Absolute path (/...) or home-relative path (~/...)" },
                            "content": { "type": "string", "description": "Text to append" }
                        },
                        "required": ["path", "content"]
                    }
                }
            })),
            ToolDefinition::new(json!({
                "type": "function",
                "function": {
                    "name": "file_edit",
                    "description": format!("Find and replace text in an existing file. Path must be absolute or ~/relative.\n\n{scope}"),
                    "parameters": {
                        "type": "object",
                        "properties": {
                            "path": { "type": "string", "description": "Absolute path (/...) or home-relative path (~/...)" },
                            "search": { "type": "string", "description": "Exact text to find" },
                            "replace": { "type": "string", "description": "Text to replace it with" }
                        },
                        "required": ["path", "search", "replace"]
                    }
                }
            })),
        ];
        if self.semantic_index {
            defs.push(ToolDefinition::new(json!({
                "type": "function",
                "function": {
                    "name": "file_search",
                    "description": format!("Semantic search across readable files. Returns top matches ranked by relevance. Optional prefix to scope to a subfolder.\n\n{scope}"),
                    "parameters": {
                        "type": "object",
                        "properties": {
                            "query": { "type": "string", "description": "Natural language query" },
                            "prefix": { "type": "string", "description": "Filter results to paths starting with this prefix" }
                        },
                        "required": ["query"]
                    }
                }
            })));
        }
        defs
    }

    fn cli_help(&self) -> Option<String> {
        let scope = self.scope_desc();
        let mut s = format!(r#"File operations. All paths must be absolute or ~/relative.
{scope}

Usage: pilot tool <name> --operation <op> [args]

Operations:

  read     Read file contents. Returns '(file does not exist)' if missing.
    Required: --operation read --path <path>
    Example: pilot tool <name> --operation read --path ~/notes/foo.md

  list     List directory entries, one per line (directories suffixed with /).
    Required: --operation list --path <dir>
    Example: pilot tool <name> --operation list --path ~/notes/

  find     Find files matching a glob pattern.
    Required: --operation find --pattern <glob>
    Example: pilot tool <name> --operation find --pattern "~/notes/**/*.md"

  write    Write file content. Choose exactly ONE mode:
    overwrite (full replace): pilot tool <name> --operation write --path ~/notes/foo.md --overwrite.content "hello"
    append    (add to end):   pilot tool <name> --operation write --path ~/notes/foo.md --append.content "\nmore"
    edit      (find/replace): pilot tool <name> --operation write --path ~/notes/foo.md --edit.search "old" --edit.replace "new""#);
        if self.semantic_index {
            s.push_str(r#"

  search   Semantic search across readable files. Returns top matches ranked by relevance.
    Required: --operation search --query <text>
    Optional: --prefix <path-prefix>   (scope results to paths starting with prefix)
    Example: pilot tool <name> --operation search --query "rate limit retry"
    Example: pilot tool <name> --operation search --query "todo" --prefix ~/notes/projects/"#);
        } else {
            s.push_str("\n\n  (search operation is disabled. Enable via 'semantic_index: true' in the tool's YAML.)");
        }
        Some(s)
    }

    fn cli_definition(&self) -> ToolDefinition {
        let scope = self.scope_desc();
        let ops: &[&str] = if self.semantic_index {
            &["read", "list", "find", "write", "search"]
        } else {
            &["read", "list", "find", "write"]
        };
        let op_desc = ops.join(" | ");
        let ops_help = if self.semantic_index {
            "read, list, find, write (with overwrite/append/edit mode), search (semantic)"
        } else {
            "read, list, find, write (with overwrite/append/edit mode)"
        };
        let mut properties = json!({
            "operation": { "type": "string", "enum": ops, "description": op_desc },
            "path": { "type": "string", "description": "Absolute path (/...) or home-relative path (~/...)" },
            "pattern": { "type": "string", "description": "For find: glob pattern" },
            "overwrite": {
                "type": "object",
                "description": "Write mode: write full file content.",
                "properties": { "content": { "type": "string", "description": "Full file content" } },
                "required": ["content"]
            },
            "append": {
                "type": "object",
                "description": "Write mode: append to end of file.",
                "properties": { "content": { "type": "string", "description": "Text to append" } },
                "required": ["content"]
            },
            "edit": {
                "type": "object",
                "description": "Write mode: find and replace text.",
                "properties": {
                    "search": { "type": "string", "description": "Exact text to find" },
                    "replace": { "type": "string", "description": "Text to replace it with" }
                },
                "required": ["search", "replace"]
            }
        });
        if self.semantic_index {
            properties["query"] = json!({ "type": "string", "description": "For search: natural language query" });
            properties["prefix"] = json!({ "type": "string", "description": "For search: filter results by path prefix" });
        }
        ToolDefinition::new(json!({
            "type": "function",
            "function": {
                "name": "file",
                "description": format!("File operations. All paths must be absolute or ~/relative.\n\n{scope}\n\nOperations: {ops_help}."),
                "parameters": {
                    "type": "object",
                    "properties": properties,
                    "required": ["operation"]
                }
            }
        }))
    }

    fn execute<'a>(
        &'a self,
        name: &'a str,
        arguments: &'a str,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<String, ToolError>> + Send + 'a>>
    {
        Box::pin(async move {
            let args: serde_json::Value = serde_json::from_str(arguments)
                .map_err(|e| ToolError::ExecutionFailed(format!("invalid arguments: {e}")))?;

            // Remap flat LLM tool calls (file_read, file_write, ...) to internal format
            let (args, operation): (serde_json::Value, String) = match name {
                "file_read" => (args.clone(), "read".into()),
                "file_list" => (args.clone(), "list".into()),
                "file_find" => (args.clone(), "find".into()),
                "file_write" => {
                    let mut r = json!({"operation": "write", "path": args["path"]});
                    r["overwrite"] = json!({"content": args["content"]});
                    (r, "write".into())
                }
                "file_append" => {
                    let mut r = json!({"operation": "write", "path": args["path"]});
                    r["append"] = json!({"content": args["content"]});
                    (r, "write".into())
                }
                "file_edit" => {
                    let mut r = json!({"operation": "write", "path": args["path"]});
                    r["edit"] = json!({"search": args["search"], "replace": args["replace"]});
                    (r, "write".into())
                }
                "file_search" => (args.clone(), "search".into()),
                _ => {
                    // CLI path: uses original combined format
                    let op = args["operation"].as_str()
                        .ok_or_else(|| ToolError::ExecutionFailed("missing 'operation' field".into()))?
                        .to_string();
                    (args.clone(), op)
                }
            };
            let operation: &str = &operation;

            if operation == "search" && !self.semantic_index {
                return Err(ToolError::ExecutionFailed(
                    "search not available: semantic_index is not enabled for this tool".into()
                ));
            }

            // find uses pattern, not path
            if operation == "find" {
                let pattern = args["pattern"]
                    .as_str()
                    .ok_or_else(|| ToolError::ExecutionFailed("find requires 'pattern'".into()))?;
                let expanded_pattern = expand_home(pattern).to_string_lossy().to_string();
                let entries = glob::glob(&expanded_pattern)
                    .map_err(|e| ToolError::ExecutionFailed(format!("invalid glob pattern: {e}")))?;
                let mut results: Vec<String> = Vec::new();
                for entry in entries {
                    let path = entry
                        .map_err(|e| ToolError::ExecutionFailed(format!("glob entry: {e}")))?;
                    let path_str = path.to_string_lossy().to_string();
                    if !self.is_readable(&path_str) {
                        continue;
                    }
                    results.push(path_str);
                }
                results.sort();
                return if results.is_empty() {
                    Ok("no files found".to_string())
                } else {
                    Ok(results.join("\n"))
                };
            }

            // search doesn't use path — handle before path extraction
            if operation == "search" {
                return self.do_search(&args).await;
            }

            let path = args["path"]
                .as_str()
                .ok_or_else(|| ToolError::ExecutionFailed("missing 'path' field".into()))?;

            let expanded = Self::validate_path(path)
                .map_err(|e| ToolError::ExecutionFailed(e))?;

            match operation {
                "read" => {
                    if !self.is_readable(path) {
                        return Err(ToolError::ExecutionFailed(
                            format!("read not allowed for path: {path}. Check the tool's 'read' glob patterns.")
                        ));
                    }
                    if !expanded.exists() {
                        return Ok(format!("(file does not exist: {path})"));
                    }
                    let content = std::fs::read_to_string(&expanded)
                        .map_err(|e| ToolError::ExecutionFailed(format!("read failed: {e}")))?;
                    Ok(content)
                }
                "list" => {
                    if !self.is_readable(path) {
                        return Err(ToolError::ExecutionFailed(
                            format!("list not allowed for path: {path}. Check the tool's 'read' glob patterns.")
                        ));
                    }
                    let entries = std::fs::read_dir(&expanded)
                        .map_err(|e| ToolError::ExecutionFailed(format!("list failed: {e}")))?;
                    let mut names: Vec<String> = Vec::new();
                    for entry in entries {
                        let entry = entry
                            .map_err(|e| ToolError::ExecutionFailed(format!("list entry: {e}")))?;
                        let name = entry.file_name().to_string_lossy().to_string();
                        let suffix = if entry.file_type().map(|t| t.is_dir()).unwrap_or(false) {
                            "/"
                        } else {
                            ""
                        };
                        names.push(format!("{name}{suffix}"));
                    }
                    names.sort();
                    Ok(names.join("\n"))
                }
                "write" => {
                    if !self.is_writable(path) {
                        return Err(ToolError::ExecutionFailed(
                            format!("write not allowed for path: {path}. Check the tool's 'write' glob patterns.")
                        ));
                    }

                    // Create parent directories if needed
                    if let Some(parent) = expanded.parent() {
                        std::fs::create_dir_all(parent)
                            .map_err(|e| ToolError::ExecutionFailed(format!("create dir: {e}")))?;
                    }

                    if let Some(obj) = args["overwrite"].as_object() {
                        let content = obj["content"].as_str()
                            .ok_or_else(|| ToolError::ExecutionFailed("overwrite requires 'content'".into()))?;
                        std::fs::write(&expanded, content)
                            .map_err(|e| ToolError::ExecutionFailed(format!("write failed: {e}")))?;
                        Ok(format!("wrote {path}"))
                    } else if let Some(obj) = args["append"].as_object() {
                        let content = obj["content"].as_str()
                            .ok_or_else(|| ToolError::ExecutionFailed("append requires 'content'".into()))?;
                        use std::io::Write;
                        let mut file = std::fs::OpenOptions::new()
                            .create(true)
                            .append(true)
                            .open(&expanded)
                            .map_err(|e| ToolError::ExecutionFailed(format!("open failed: {e}")))?;
                        file.write_all(content.as_bytes())
                            .map_err(|e| ToolError::ExecutionFailed(format!("append failed: {e}")))?;
                        Ok(format!("appended to {path}"))
                    } else if let Some(obj) = args["edit"].as_object() {
                        let search = obj["search"].as_str()
                            .ok_or_else(|| ToolError::ExecutionFailed("edit requires 'search'".into()))?;
                        let replace = obj["replace"].as_str()
                            .ok_or_else(|| ToolError::ExecutionFailed("edit requires 'replace'".into()))?;
                        let content = std::fs::read_to_string(&expanded)
                            .map_err(|e| ToolError::ExecutionFailed(format!("read failed: {e}")))?;
                        if !content.contains(search) {
                            return Ok(format!("search string not found in {path}. Read the file first, then retry with exact text."));
                        }
                        let new_content = content.replacen(search, replace, 1);
                        std::fs::write(&expanded, &new_content)
                            .map_err(|e| ToolError::ExecutionFailed(format!("write failed: {e}")))?;
                        Ok(format!("edited {path}"))
                    } else {
                        Ok("write requires one of: overwrite, append, or edit. Example: {\"operation\": \"write\", \"path\": \"/tmp/f.txt\", \"overwrite\": {\"content\": \"hello\"}}".into())
                    }
                }
                _ => Err(ToolError::ExecutionFailed(
                    format!("unknown operation: {operation}. Valid operations: read, list, find, write. Note: overwrite/append/edit are write modes, not operations — use operation: \"write\" with an overwrite/append/edit object.")
                )),
            }
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn valid_glob_patterns() {
        let tool = FileTool::new(
            vec!["/tmp/test/**".into()],
            vec!["/tmp/output/**".into()],
            false,
            None,
            "",
        );
        assert!(tool.is_ok());
    }

    #[test]
    fn invalid_glob_pattern_errors() {
        let result = FileTool::new(vec!["[invalid".into()], vec![], false, None, "");
        assert!(result.is_err());
    }

    #[test]
    fn read_pattern_matches() {
        let tool = FileTool::new(
            vec!["/tmp/src/**".into()],
            vec![],
            false,
            None,
            "",
        ).unwrap();
        assert!(tool.is_readable("/tmp/src/main.rs"));
        assert!(tool.is_readable("/tmp/src/deep/nested/file.rs"));
        assert!(!tool.is_readable("/tmp/other/file.rs"));
    }

    #[test]
    fn write_pattern_matches() {
        let tool = FileTool::new(
            vec![],
            vec!["/tmp/output/**".into()],
            false,
            None,
            "",
        ).unwrap();
        assert!(tool.is_writable("/tmp/output/result.txt"));
        assert!(!tool.is_writable("/tmp/src/main.rs"));
    }

    #[test]
    fn empty_patterns_reject_all() {
        let tool = FileTool::new(vec![], vec![], false, None, "").unwrap();
        assert!(!tool.is_readable("/any/path"));
        assert!(!tool.is_writable("/any/path"));
    }

    #[test]
    fn validate_path_absolute() {
        assert!(FileTool::validate_path("/tmp/file.txt").is_ok());
    }

    #[test]
    fn validate_path_home() {
        assert!(FileTool::validate_path("~/file.txt").is_ok());
    }

    #[test]
    fn validate_path_relative_rejected() {
        let result = FileTool::validate_path("./relative/path");
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("absolute or ~/relative"));
    }

    #[test]
    fn validate_path_bare_name_rejected() {
        let result = FileTool::validate_path("file.txt");
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn read_allowed_file() {
        let dir = std::env::temp_dir().join("pilot_test_file_read");
        std::fs::create_dir_all(&dir).unwrap();
        let file = dir.join("test.txt");
        std::fs::write(&file, "hello").unwrap();

        let dir_str = dir.to_string_lossy();
        let tool = FileTool::new(
            vec![format!("{dir_str}/**")],
            vec![],
            false,
            None,
            "",
        ).unwrap();

        let path_str = file.to_string_lossy();
        let result = tool.execute(
            "file",
            &format!(r#"{{"operation":"read","path":"{path_str}"}}"#),
        ).await;

        assert!(result.is_ok());
        assert_eq!(result.unwrap(), "hello");

        std::fs::remove_dir_all(&dir).unwrap();
    }

    #[tokio::test]
    async fn read_rejected_path() {
        let tool = FileTool::new(
            vec!["/tmp/allowed/**".into()],
            vec![],
            false,
            None,
            "",
        ).unwrap();

        let result = tool.execute(
            "file",
            r#"{"operation":"read","path":"/etc/passwd"}"#,
        ).await;

        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("not allowed"));
    }

    #[tokio::test]
    async fn write_new_file() {
        let dir = std::env::temp_dir().join("pilot_test_file_write");
        let file = dir.join("output.txt");

        let dir_str = dir.to_string_lossy();
        let tool = FileTool::new(
            vec![],
            vec![format!("{dir_str}/**")],
            false,
            None,
            "",
        ).unwrap();

        let path_str = file.to_string_lossy();
        let result = tool.execute(
            "file",
            &format!(r#"{{"operation":"write","path":"{path_str}","overwrite":{{"content":"written"}}}}"#),
        ).await;

        assert!(result.is_ok());
        assert_eq!(std::fs::read_to_string(&file).unwrap(), "written");

        std::fs::remove_dir_all(&dir).unwrap();
    }

    #[tokio::test]
    async fn write_overwrite_existing() {
        let dir = std::env::temp_dir().join("pilot_test_file_overwrite");
        std::fs::create_dir_all(&dir).unwrap();
        let file = dir.join("existing.txt");
        std::fs::write(&file, "old content").unwrap();

        let dir_str = dir.to_string_lossy();
        let tool = FileTool::new(vec![], vec![format!("{dir_str}/**")], false, None, "").unwrap();

        let path_str = file.to_string_lossy();
        let result = tool.execute(
            "file",
            &format!(r#"{{"operation":"write","path":"{path_str}","overwrite":{{"content":"new content"}}}}"#),
        ).await;

        assert!(result.is_ok());
        assert_eq!(std::fs::read_to_string(&file).unwrap(), "new content");

        std::fs::remove_dir_all(&dir).unwrap();
    }

    #[tokio::test]
    async fn write_append_creates_file() {
        let dir = std::env::temp_dir().join("pilot_test_file_append_new");
        let file = dir.join("new.txt");
        // Don't create the file — append should create it
        let _ = std::fs::remove_dir_all(&dir);

        let dir_str = dir.to_string_lossy();
        let tool = FileTool::new(vec![], vec![format!("{dir_str}/**")], false, None, "").unwrap();

        let path_str = file.to_string_lossy();
        let result = tool.execute(
            "file",
            &format!(r#"{{"operation":"write","path":"{path_str}","append":{{"content":"first line\n"}}}}"#),
        ).await;

        assert!(result.is_ok());
        assert_eq!(std::fs::read_to_string(&file).unwrap(), "first line\n");

        std::fs::remove_dir_all(&dir).unwrap();
    }

    #[tokio::test]
    async fn write_search_replace() {
        let dir = std::env::temp_dir().join("pilot_test_file_edit");
        std::fs::create_dir_all(&dir).unwrap();
        let file = dir.join("config.txt");
        std::fs::write(&file, "hello world").unwrap();

        let dir_str = dir.to_string_lossy();
        let tool = FileTool::new(
            vec![],
            vec![format!("{dir_str}/**")],
            false,
            None,
            "",
        ).unwrap();

        let path_str = file.to_string_lossy();
        let result = tool.execute(
            "file",
            &format!(r#"{{"operation":"write","path":"{path_str}","edit":{{"search":"world","replace":"rust"}}}}"#),
        ).await;

        assert!(result.is_ok());
        assert_eq!(std::fs::read_to_string(&file).unwrap(), "hello rust");

        std::fs::remove_dir_all(&dir).unwrap();
    }

    #[tokio::test]
    async fn write_append() {
        let dir = std::env::temp_dir().join("pilot_test_file_append");
        let file = dir.join("log.txt");
        std::fs::create_dir_all(&dir).unwrap();
        std::fs::write(&file, "line1\n").unwrap();

        let dir_str = dir.to_string_lossy();
        let tool = FileTool::new(vec![], vec![format!("{dir_str}/**")], false, None, "").unwrap();

        let path_str = file.to_string_lossy();
        let result = tool.execute(
            "file",
            &format!(r#"{{"operation":"write","path":"{path_str}","append":{{"content":"line2\n"}}}}"#),
        ).await;

        assert!(result.is_ok());
        assert_eq!(std::fs::read_to_string(&file).unwrap(), "line1\nline2\n");

        std::fs::remove_dir_all(&dir).unwrap();
    }

    #[tokio::test]
    async fn write_search_not_found() {
        let dir = std::env::temp_dir().join("pilot_test_file_notfound");
        std::fs::create_dir_all(&dir).unwrap();
        let file = dir.join("data.txt");
        std::fs::write(&file, "hello").unwrap();

        let dir_str = dir.to_string_lossy();
        let tool = FileTool::new(
            vec![],
            vec![format!("{dir_str}/**")],
            false,
            None,
            "",
        ).unwrap();

        let path_str = file.to_string_lossy();
        let result = tool.execute(
            "file",
            &format!(r#"{{"operation":"write","path":"{path_str}","edit":{{"search":"missing","replace":"x"}}}}"#),
        ).await;

        assert!(result.is_ok());
        assert!(result.unwrap().contains("not found"));

        std::fs::remove_dir_all(&dir).unwrap();
    }

    #[tokio::test]
    async fn write_to_read_only_path_rejected() {
        let tool = FileTool::new(
            vec!["/tmp/**".into()],
            vec![], // no write access
            false,
            None,
            "",
        ).unwrap();

        let result = tool.execute(
            "file",
            r#"{"operation":"write","path":"/tmp/test.txt","search":"","replace":"data"}"#,
        ).await;

        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("not allowed"));
    }

    #[tokio::test]
    async fn list_directory() {
        let dir = std::env::temp_dir().join("pilot_test_file_list");
        std::fs::create_dir_all(dir.join("subdir")).unwrap();
        std::fs::write(dir.join("a.txt"), "").unwrap();
        std::fs::write(dir.join("b.txt"), "").unwrap();

        let dir_str = dir.to_string_lossy();
        let tool = FileTool::new(
            vec![format!("{dir_str}/**")],
            vec![],
            false,
            None,
            "",
        ).unwrap();

        let result = tool.execute(
            "file",
            &format!(r#"{{"operation":"list","path":"{dir_str}"}}"#),
        ).await;

        assert!(result.is_ok());
        let output = result.unwrap();
        assert!(output.contains("a.txt"));
        assert!(output.contains("b.txt"));
        assert!(output.contains("subdir/"));

        std::fs::remove_dir_all(&dir).unwrap();
    }

    #[tokio::test]
    async fn find_files_by_glob() {
        let dir = std::env::temp_dir().join("pilot_test_file_find");
        std::fs::create_dir_all(dir.join("sub")).unwrap();
        std::fs::write(dir.join("foo.rs"), "").unwrap();
        std::fs::write(dir.join("bar.txt"), "").unwrap();
        std::fs::write(dir.join("sub").join("baz.rs"), "").unwrap();

        let dir_str = dir.to_string_lossy();
        let tool = FileTool::new(
            vec![format!("{dir_str}/**")],
            vec![],
            false,
            None,
            "",
        ).unwrap();

        let result = tool.execute(
            "file",
            &format!(r#"{{"operation":"find","pattern":"{dir_str}/**/*.rs"}}"#),
        ).await.unwrap();

        assert!(result.contains("foo.rs"), "should find foo.rs: {result}");
        assert!(result.contains("baz.rs"), "should find baz.rs: {result}");
        assert!(!result.contains("bar.txt"), "should not find bar.txt: {result}");

        std::fs::remove_dir_all(&dir).unwrap();
    }

    #[tokio::test]
    async fn find_no_matches_returns_message() {
        let tool = FileTool::new(vec!["/tmp/**".into()], vec![], false, None, "").unwrap();

        let result = tool.execute(
            "file",
            r#"{"operation":"find","pattern":"/tmp/nonexistent_pilot_test_dir_xyz/**/*.rs"}"#,
        ).await.unwrap();
        assert_eq!(result, "no files found");
    }

    #[tokio::test]
    async fn find_respects_read_permissions() {
        let dir = std::env::temp_dir().join("pilot_test_file_find_perm");
        std::fs::create_dir_all(&dir).unwrap();
        std::fs::write(dir.join("allowed.rs"), "").unwrap();

        let tool = FileTool::new(
            vec!["/nonexistent/**".into()], // no access to temp dir
            vec![],
            false,
            None,
            "",
        ).unwrap();

        let dir_str = dir.to_string_lossy();
        let result = tool.execute(
            "file",
            &format!(r#"{{"operation":"find","pattern":"{dir_str}/**/*.rs"}}"#),
        ).await.unwrap();

        assert_eq!(result, "no files found");

        std::fs::remove_dir_all(&dir).unwrap();
    }

    #[tokio::test]
    async fn relative_path_rejected() {
        let tool = FileTool::new(vec!["/**".into()], vec![], false, None, "").unwrap();

        let result = tool.execute(
            "file",
            r#"{"operation":"read","path":"./relative/file.txt"}"#,
        ).await;
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("absolute"));
    }

    #[test]
    fn write_implies_read() {
        let tool = FileTool::new(
            vec![], // no read patterns
            vec!["/tmp/output/**".into()],
            false,
            None,
            "",
        ).unwrap();
        // Write path is also readable
        assert!(tool.is_readable("/tmp/output/file.txt"));
        assert!(tool.is_writable("/tmp/output/file.txt"));
        // Other paths still not readable
        assert!(!tool.is_readable("/tmp/other/file.txt"));
    }

    #[tokio::test]
    async fn read_from_write_path_allowed() {
        let dir = std::env::temp_dir().join("pilot_test_write_implies_read");
        std::fs::create_dir_all(&dir).unwrap();
        let file = dir.join("data.txt");
        std::fs::write(&file, "readable via write").unwrap();

        let dir_str = dir.to_string_lossy();
        let tool = FileTool::new(
            vec![], // no explicit read
            vec![format!("{dir_str}/**")], // write implies read
            false,
            None,
            "",
        ).unwrap();

        let path_str = file.to_string_lossy();
        let result = tool.execute(
            "file",
            &format!(r#"{{"operation":"read","path":"{path_str}"}}"#),
        ).await;
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), "readable via write");

        std::fs::remove_dir_all(&dir).unwrap();
    }

    #[tokio::test]
    async fn list_rejected_path() {
        let tool = FileTool::new(
            vec!["/tmp/allowed/**".into()],
            vec![],
            false,
            None,
            "",
        ).unwrap();

        let result = tool.execute(
            "file",
            r#"{"operation":"list","path":"/etc"}"#,
        ).await;
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("not allowed"));
    }

    #[tokio::test]
    async fn write_missing_mode() {
        let tool = FileTool::new(vec![], vec!["/tmp/**".into()], false, None, "").unwrap();

        let result = tool.execute(
            "file",
            r#"{"operation":"write","path":"/tmp/test.txt"}"#,
        ).await;
        assert!(result.is_ok());
        assert!(result.unwrap().contains("overwrite, append, or edit"));
    }

    #[tokio::test]
    async fn unknown_operation() {
        let tool = FileTool::new(vec!["/**".into()], vec![], false, None, "").unwrap();

        let result = tool.execute(
            "file",
            r#"{"operation":"delete","path":"/tmp/file.txt"}"#,
        ).await;
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("unknown operation"));
    }

    #[tokio::test]
    async fn read_nonexistent_file() {
        let tool = FileTool::new(vec!["/tmp/**".into()], vec![], false, None, "").unwrap();

        let result = tool.execute(
            "file",
            r#"{"operation":"read","path":"/tmp/nonexistent_pilot_test_file_xyz.txt"}"#,
        ).await;
        assert!(result.is_ok());
        assert!(result.unwrap().contains("does not exist"));
    }
}
