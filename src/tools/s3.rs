use serde_json::json;

use crate::llm::ToolDefinition;

use super::{Tool, ToolError};

/// S3-compatible object storage tool with read/write path scoping.
pub struct S3Tool {
    bucket: Box<s3::Bucket>,
    read_patterns: Vec<glob::Pattern>,
    write_patterns: Vec<glob::Pattern>,
    unrestricted: bool,
}

impl S3Tool {
    pub fn new(
        endpoint: &str,
        access_key: &str,
        secret_key: &str,
        bucket_name: &str,
        read_globs: &[String],
        write_globs: &[String],
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
                    "S3 object storage. Bucket: {}.\n\n{scope_desc}\n\nOperations:\n- list: list objects. Optional 'prefix' to filter.\n- read: get object content by key.\n- write: use exactly one of 'overwrite', 'append', or 'edit'. overwrite: write full content (creates if missing). append: add to end (creates if missing). edit: search/replace text in existing object.\n- delete: remove an object.",
                    self.bucket.name,
                ),
                "parameters": {
                    "type": "object",
                    "properties": {
                        "operation": {
                            "type": "string",
                            "enum": ["list", "read", "write", "delete"],
                            "description": "list | read | write | delete"
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
                other => {
                    Ok(format!("unknown operation: {other}. Supported: list, read, write, delete"))
                }
            }
        })
    }
}
