use std::path::PathBuf;

use crate::args::parse_args_to_json;
use crate::config::Config;
use crate::workflow::ExposedToolDef;

/// Discover tool YAML file from .pilot/tools/ directories.
fn find_tool_yaml(name: &str) -> Result<PathBuf, String> {
    // Search current directory and parents for .pilot/tools/<name>.yaml
    let mut dir = std::env::current_dir()
        .map_err(|e| format!("cannot get cwd: {e}"))?;

    loop {
        let path = dir.join(".pilot").join("tools").join(format!("{name}.yaml"));
        if path.exists() {
            return Ok(path);
        }
        let yml_path = dir.join(".pilot").join("tools").join(format!("{name}.yml"));
        if yml_path.exists() {
            return Ok(yml_path);
        }
        if !dir.pop() {
            break;
        }
    }

    Err(format!("tool '{name}' not found. Create .pilot/tools/{name}.yaml"))
}

/// Load and parse an exposed tool definition.
fn load_tool_def(path: &PathBuf) -> Result<ExposedToolDef, String> {
    let content = std::fs::read_to_string(path)
        .map_err(|e| format!("cannot read {}: {e}", path.display()))?;
    serde_yaml::from_str(&content)
        .map_err(|e| format!("cannot parse {}: {e}", path.display()))
}

/// Construct a Tool instance from an ExposedToolDef.
fn construct_tool(
    tool_def: &ExposedToolDef,
    cfg: &Config,
) -> Result<Box<dyn crate::tools::Tool>, String> {
    // Build LLM client for embedding support
    let llm: Option<std::sync::Arc<dyn crate::llm::LlmClient>> = if !cfg.openai_api_key.is_empty() {
        let llm_config = cfg.to_llm_config();
        Some(std::sync::Arc::new(crate::llm::LLM::new(llm_config)))
    } else {
        None
    };

    match tool_def {
        ExposedToolDef::S3 { config } => {
            Ok(Box::new(crate::tools::s3::S3Tool::new(
                &cfg.s3_endpoint,
                &cfg.s3_access_key,
                &cfg.s3_secret_key,
                &config.bucket,
                &config.read,
                &config.write,
                llm,
            )
            .map_err(|e| format!("S3 tool error: {e}"))?))
        }
    }
}

/// Print help for a tool by reading its JSON schema definition.
fn print_tool_help(tool: &dyn crate::tools::Tool) {
    let def = tool.definition();
    let schema = &def.0;

    // Description (all lines before the Operations section)
    if let Some(desc) = schema["function"]["description"].as_str() {
        for line in desc.lines() {
            if line.starts_with("Operations:") { break; }
            if !line.is_empty() {
                eprintln!("{}", line);
            }
        }
    }

    let params = &schema["function"]["parameters"]["properties"];
    let required: Vec<&str> = schema["function"]["parameters"]["required"]
        .as_array()
        .map(|arr| arr.iter().filter_map(|v| v.as_str()).collect())
        .unwrap_or_default();

    if let Some(obj) = params.as_object() {
        eprintln!("\nParameters:");
        for (name, prop) in obj {
            let desc = prop["description"].as_str().unwrap_or("");
            let req = if required.contains(&name.as_str()) { " (required)" } else { "" };

            // Show enum values if present
            let enum_str = prop["enum"]
                .as_array()
                .map(|arr| {
                    let vals: Vec<&str> = arr.iter().filter_map(|v| v.as_str()).collect();
                    format!(" [{}]", vals.join(", "))
                })
                .unwrap_or_default();

            // Show nested object properties
            if prop["type"].as_str() == Some("object") {
                eprintln!("  --{name}{req}  {desc}");
                if let Some(sub_props) = prop["properties"].as_object() {
                    for (sub_name, sub_prop) in sub_props {
                        let sub_desc = sub_prop["description"].as_str().unwrap_or("");
                        eprintln!("    .{sub_name}  {sub_desc}");
                    }
                }
            } else {
                eprintln!("  --{name}{enum_str}{req}  {desc}");
            }
        }
    }
}

/// Run a tool from .pilot/tools/<name>.yaml with the given args.
pub async fn run_tool(
    name: &str,
    args: &[String],
    cfg: &Config,
) -> Result<String, String> {
    let path = find_tool_yaml(name)?;
    let tool_def = load_tool_def(&path)?;
    let tool = construct_tool(&tool_def, cfg)?;

    // No args — show help
    if args.is_empty() {
        print_tool_help(tool.as_ref());
        return Ok(String::new());
    }

    let json_args = parse_args_to_json(args)?;

    // Validate parsed keys against tool schema
    validate_args(&json_args, tool.as_ref())?;

    tool.execute(&json_args)
        .await
        .map_err(|e| format!("{e}"))
}

/// Validate that parsed arg keys exist in the tool's JSON schema.
fn validate_args(json_args: &str, tool: &dyn crate::tools::Tool) -> Result<(), String> {
    let parsed: serde_json::Value = serde_json::from_str(json_args)
        .map_err(|e| format!("invalid JSON: {e}"))?;
    let schema = tool.definition();
    let properties = &schema.0["function"]["parameters"]["properties"];

    if let (Some(parsed_obj), Some(schema_obj)) = (parsed.as_object(), properties.as_object()) {
        let valid_keys: Vec<&str> = schema_obj.keys().map(|k| k.as_str()).collect();
        for key in parsed_obj.keys() {
            if !schema_obj.contains_key(key) {
                return Err(format!(
                    "unknown argument: --{key}. Valid: {}",
                    valid_keys.join(", ")
                ));
            }
        }
    }

    Ok(())
}

