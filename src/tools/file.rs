use std::path::PathBuf;

use crate::llm::ToolDefinition;
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
}

impl FileTool {
    pub fn new(read_globs: Vec<String>, write_globs: Vec<String>) -> Result<Self, String> {
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

        Ok(Self {
            read_patterns,
            write_patterns,
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

impl Tool for FileTool {
    fn name(&self) -> &str {
        "file"
    }

    fn description(&self) -> &str {
        "Read, list, and write files within allowed paths"
    }

    fn definition(&self) -> ToolDefinition {
        let read_globs: Vec<String> = self.read_patterns.iter().map(|p| p.as_str().to_string()).collect();
        let write_globs: Vec<String> = self.write_patterns.iter().map(|p| p.as_str().to_string()).collect();
        let scope_desc = format!(
            "Allowed read paths: {}. Allowed write paths: {}.",
            if read_globs.is_empty() { "none".to_string() } else { read_globs.join(", ") },
            if write_globs.is_empty() { "none".to_string() } else { write_globs.join(", ") + " (write paths are also readable)" },
        );

        ToolDefinition::new(json!({
            "type": "function",
            "function": {
                "name": "file",
                "description": format!("File operations. All paths must be absolute or ~/relative.\n\n{scope_desc}\n\nOperations:\n- read: return file contents. Returns '(file does not exist)' if missing.\n- list: return directory entries, one per line, directories suffixed with /.\n- write: set operation to \"write\" AND provide exactly one of the overwrite/append/edit objects.\n\nExamples:\n  Read:  {{\"operation\": \"write\", \"path\": \"~/f.md\", \"overwrite\": {{\"content\": \"hello\"}}}}\n  Edit:  {{\"operation\": \"write\", \"path\": \"~/f.md\", \"edit\": {{\"search\": \"old\", \"replace\": \"new\"}}}}\n  WRONG: {{\"operation\": \"overwrite\", ...}} — overwrite is NOT an operation."),
                "parameters": {
                    "type": "object",
                    "properties": {
                        "operation": {
                            "type": "string",
                            "enum": ["read", "list", "find", "write"],
                            "description": "read | list | find | write"
                        },
                        "path": {
                            "type": "string",
                            "description": "Absolute path (/...) or home-relative path (~/...)"
                        },
                        "pattern": {
                            "type": "string",
                            "description": "For find: glob pattern to search for files (e.g. '~/projects/**/*.rs', '/tmp/*.log')"
                        },
                        "overwrite": {
                            "type": "object",
                            "description": "Write mode: write full file content. Creates file if it doesn't exist.",
                            "properties": {
                                "content": { "type": "string", "description": "Full file content" }
                            },
                            "required": ["content"]
                        },
                        "append": {
                            "type": "object",
                            "description": "Write mode: append to end of file. Creates file if it doesn't exist.",
                            "properties": {
                                "content": { "type": "string", "description": "Text to append" }
                            },
                            "required": ["content"]
                        },
                        "edit": {
                            "type": "object",
                            "description": "Write mode: find and replace text in an existing file.",
                            "properties": {
                                "search": { "type": "string", "description": "Exact text to find" },
                                "replace": { "type": "string", "description": "Text to replace it with" }
                            },
                            "required": ["search", "replace"]
                        }
                    },
                    "required": ["operation", "path"]
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
        );
        assert!(tool.is_ok());
    }

    #[test]
    fn invalid_glob_pattern_errors() {
        let result = FileTool::new(vec!["[invalid".into()], vec![]);
        assert!(result.is_err());
    }

    #[test]
    fn read_pattern_matches() {
        let tool = FileTool::new(
            vec!["/tmp/src/**".into()],
            vec![],
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
        ).unwrap();
        assert!(tool.is_writable("/tmp/output/result.txt"));
        assert!(!tool.is_writable("/tmp/src/main.rs"));
    }

    #[test]
    fn empty_patterns_reject_all() {
        let tool = FileTool::new(vec![], vec![]).unwrap();
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
        ).unwrap();

        let path_str = file.to_string_lossy();
        let result = tool.execute(
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
        ).unwrap();

        let result = tool.execute(
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
        ).unwrap();

        let path_str = file.to_string_lossy();
        let result = tool.execute(
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
        let tool = FileTool::new(vec![], vec![format!("{dir_str}/**")]).unwrap();

        let path_str = file.to_string_lossy();
        let result = tool.execute(
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
        let tool = FileTool::new(vec![], vec![format!("{dir_str}/**")]).unwrap();

        let path_str = file.to_string_lossy();
        let result = tool.execute(
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
        ).unwrap();

        let path_str = file.to_string_lossy();
        let result = tool.execute(
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
        let tool = FileTool::new(vec![], vec![format!("{dir_str}/**")]).unwrap();

        let path_str = file.to_string_lossy();
        let result = tool.execute(
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
        ).unwrap();

        let path_str = file.to_string_lossy();
        let result = tool.execute(
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
        ).unwrap();

        let result = tool.execute(
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
        ).unwrap();

        let result = tool.execute(
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
        ).unwrap();

        let result = tool.execute(
            &format!(r#"{{"operation":"find","pattern":"{dir_str}/**/*.rs"}}"#),
        ).await.unwrap();

        assert!(result.contains("foo.rs"), "should find foo.rs: {result}");
        assert!(result.contains("baz.rs"), "should find baz.rs: {result}");
        assert!(!result.contains("bar.txt"), "should not find bar.txt: {result}");

        std::fs::remove_dir_all(&dir).unwrap();
    }

    #[tokio::test]
    async fn find_no_matches_returns_message() {
        let tool = FileTool::new(vec!["/tmp/**".into()], vec![]).unwrap();

        let result = tool.execute(
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
        ).unwrap();

        let dir_str = dir.to_string_lossy();
        let result = tool.execute(
            &format!(r#"{{"operation":"find","pattern":"{dir_str}/**/*.rs"}}"#),
        ).await.unwrap();

        assert_eq!(result, "no files found");

        std::fs::remove_dir_all(&dir).unwrap();
    }

    #[tokio::test]
    async fn relative_path_rejected() {
        let tool = FileTool::new(vec!["/**".into()], vec![]).unwrap();

        let result = tool.execute(
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
        ).unwrap();

        let path_str = file.to_string_lossy();
        let result = tool.execute(
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
        ).unwrap();

        let result = tool.execute(
            r#"{"operation":"list","path":"/etc"}"#,
        ).await;
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("not allowed"));
    }

    #[tokio::test]
    async fn write_missing_mode() {
        let tool = FileTool::new(vec![], vec!["/tmp/**".into()]).unwrap();

        let result = tool.execute(
            r#"{"operation":"write","path":"/tmp/test.txt"}"#,
        ).await;
        assert!(result.is_ok());
        assert!(result.unwrap().contains("overwrite, append, or edit"));
    }

    #[tokio::test]
    async fn unknown_operation() {
        let tool = FileTool::new(vec!["/**".into()], vec![]).unwrap();

        let result = tool.execute(
            r#"{"operation":"delete","path":"/tmp/file.txt"}"#,
        ).await;
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("unknown operation"));
    }

    #[tokio::test]
    async fn read_nonexistent_file() {
        let tool = FileTool::new(vec!["/tmp/**".into()], vec![]).unwrap();

        let result = tool.execute(
            r#"{"operation":"read","path":"/tmp/nonexistent_pilot_test_file_xyz.txt"}"#,
        ).await;
        assert!(result.is_ok());
        assert!(result.unwrap().contains("does not exist"));
    }
}
