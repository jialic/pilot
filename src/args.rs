/// Parse --key value CLI arguments into a JSON string.
/// Supports dot notation for nesting: --a.b.c value → {"a": {"b": {"c": "value"}}}
/// Values are auto-typed: "true"/"false" → bool, integers → number, else string.
pub fn parse_args_to_json(args: &[String]) -> Result<String, String> {
    let mut root = serde_json::Map::new();

    let mut i = 0;
    while i < args.len() {
        let arg = &args[i];
        let key = arg.strip_prefix("--")
            .ok_or_else(|| format!("expected --key, got: {arg}"))?;

        if i + 1 >= args.len() {
            return Err(format!("--{key} requires a value"));
        }
        let value = &args[i + 1];
        i += 2;

        let parts: Vec<&str> = key.split('.').collect();
        set_nested(&mut root, &parts, value);
    }

    serde_json::to_string(&serde_json::Value::Object(root))
        .map_err(|e| format!("JSON serialization error: {e}"))
}

/// Set a value in a nested JSON map via dot-separated key parts.
fn set_nested(map: &mut serde_json::Map<String, serde_json::Value>, parts: &[&str], value: &str) {
    if parts.len() == 1 {
        let json_value = if value == "true" {
            serde_json::Value::Bool(true)
        } else if value == "false" {
            serde_json::Value::Bool(false)
        } else if let Ok(n) = value.parse::<i64>() {
            serde_json::Value::Number(n.into())
        } else {
            serde_json::Value::String(value.to_string())
        };
        map.insert(parts[0].to_string(), json_value);
    } else {
        let child = map
            .entry(parts[0].to_string())
            .or_insert_with(|| serde_json::Value::Object(serde_json::Map::new()));
        if let serde_json::Value::Object(child_map) = child {
            set_nested(child_map, &parts[1..], value);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn simple_flat_args() {
        let args = vec![
            "--operation".into(), "list".into(),
            "--prefix".into(), "todos/".into(),
        ];
        let json = parse_args_to_json(&args).unwrap();
        let v: serde_json::Value = serde_json::from_str(&json).unwrap();
        assert_eq!(v["operation"], "list");
        assert_eq!(v["prefix"], "todos/");
    }

    #[test]
    fn nested_dot_notation() {
        let args = vec![
            "--operation".into(), "write".into(),
            "--overwrite.content".into(), "hello world".into(),
        ];
        let json = parse_args_to_json(&args).unwrap();
        let v: serde_json::Value = serde_json::from_str(&json).unwrap();
        assert_eq!(v["operation"], "write");
        assert_eq!(v["overwrite"]["content"], "hello world");
    }

    #[test]
    fn deep_nesting() {
        let args = vec!["--a.b.c.d".into(), "deep".into()];
        let json = parse_args_to_json(&args).unwrap();
        let v: serde_json::Value = serde_json::from_str(&json).unwrap();
        assert_eq!(v["a"]["b"]["c"]["d"], "deep");
    }

    #[test]
    fn auto_type_bool() {
        let args = vec!["--flag".into(), "true".into(), "--off".into(), "false".into()];
        let json = parse_args_to_json(&args).unwrap();
        let v: serde_json::Value = serde_json::from_str(&json).unwrap();
        assert_eq!(v["flag"], true);
        assert_eq!(v["off"], false);
    }

    #[test]
    fn auto_type_number() {
        let args = vec!["--count".into(), "42".into(), "--neg".into(), "-5".into()];
        let json = parse_args_to_json(&args).unwrap();
        let v: serde_json::Value = serde_json::from_str(&json).unwrap();
        assert_eq!(v["count"], 42);
        assert_eq!(v["neg"], -5);
    }

    #[test]
    fn string_not_coerced() {
        let args = vec!["--name".into(), "test".into(), "--path".into(), "/tmp/foo".into()];
        let json = parse_args_to_json(&args).unwrap();
        let v: serde_json::Value = serde_json::from_str(&json).unwrap();
        assert_eq!(v["name"], "test");
        assert_eq!(v["path"], "/tmp/foo");
    }

    #[test]
    fn missing_value_errors() {
        let args = vec!["--key".into()];
        assert!(parse_args_to_json(&args).is_err());
    }

    #[test]
    fn missing_prefix_errors() {
        let args = vec!["noflag".into(), "value".into()];
        assert!(parse_args_to_json(&args).is_err());
    }

    #[test]
    fn empty_args() {
        let args: Vec<String> = vec![];
        let json = parse_args_to_json(&args).unwrap();
        assert_eq!(json, "{}");
    }

    #[test]
    fn multiple_nested_siblings() {
        let args = vec![
            "--edit.search".into(), "old".into(),
            "--edit.replace".into(), "new".into(),
        ];
        let json = parse_args_to_json(&args).unwrap();
        let v: serde_json::Value = serde_json::from_str(&json).unwrap();
        assert_eq!(v["edit"]["search"], "old");
        assert_eq!(v["edit"]["replace"], "new");
    }
}
