use crate::llm::ToolDefinition;
use serde_json::json;

use super::{Tool, ToolError};

/// InputTool provides keyboard and mouse control via enigo.
pub struct InputTool;

impl Tool for InputTool {
    fn name(&self) -> &str {
        "input"
    }

    fn description(&self) -> &str {
        "Control keyboard and mouse input"
    }

    fn definitions(&self) -> Vec<ToolDefinition> {
        vec![ToolDefinition::new(json!({
            "type": "function",
            "function": {
                "name": "input",
                "description": "Control keyboard and mouse to interact with the screen.\n\nOperations:\n- click: Click a UI element by its letter ID (from OCR text elements in the screenshot, e.g. 'aab', 'abd'). Preferred over raw coordinates.\n- type: Type a string of text at the current cursor position.\n- key: Press a single special key.\n- hotkey: Press a key combination with modifiers (cmd, ctrl, alt, shift).\n- scroll: Scroll vertically.\n- move: Move mouse cursor to a position.\n\nExamples:\n  Click a button: {\"operation\": \"click\", \"element\": \"abd\"}\n  Type text: {\"operation\": \"type\", \"text\": \"hello world\"}\n  Press Enter: {\"operation\": \"key\", \"key\": \"enter\"}\n  Open Spotlight: {\"operation\": \"hotkey\", \"modifiers\": [\"cmd\"], \"key\": \"space\"}\n  Copy: {\"operation\": \"hotkey\", \"modifiers\": [\"cmd\"], \"key\": \"c\"}\n  Scroll down: {\"operation\": \"scroll\", \"amount\": 3}",
                "parameters": {
                    "type": "object",
                    "properties": {
                        "operation": {
                            "type": "string",
                            "enum": ["click", "type", "key", "hotkey", "scroll", "move"],
                            "description": "The input operation to perform"
                        },
                        "element": {
                            "type": "string",
                            "description": "For click: the 3-letter element ID from the screenshot's OCR text elements list (e.g. 'aab', 'abd'). Always prefer this over x,y coordinates."
                        },
                        "x": {
                            "type": "integer",
                            "description": "For click/move: x coordinate in image space. Only use when no element ID is available."
                        },
                        "y": {
                            "type": "integer",
                            "description": "For click/move: y coordinate in image space. Only use when no element ID is available."
                        },
                        "text": {
                            "type": "string",
                            "description": "For type: the text string to type at the current cursor position"
                        },
                        "key": {
                            "type": "string",
                            "enum": ["enter", "tab", "escape", "backspace", "delete", "space", "up", "down", "left", "right", "home", "end", "pageup", "pagedown", "f1", "f2", "f3", "f4", "f5", "f6", "f7", "f8", "f9", "f10", "f11", "f12"],
                            "description": "For key: the special key to press"
                        },
                        "modifiers": {
                            "type": "array",
                            "items": { "type": "string", "enum": ["cmd", "ctrl", "alt", "shift"] },
                            "description": "For hotkey: modifier keys to hold while pressing the key"
                        },
                        "amount": {
                            "type": "integer",
                            "description": "For scroll: number of scroll steps. Positive = down, negative = up."
                        },
                        "reason": {
                            "type": "string",
                            "description": "Why you are performing this action. Explain your reasoning."
                        }
                    },
                    "required": ["operation", "reason"]
                }
            }
        }))]
    }

    fn execute<'a>(
        &'a self,
        _name: &'a str,
        arguments: &'a str,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<String, ToolError>> + Send + 'a>>
    {
        Box::pin(async move {
            let args: serde_json::Value = serde_json::from_str(arguments)
                .map_err(|e| ToolError::ExecutionFailed(format!("invalid arguments: {e}")))?;

            let operation = args["operation"]
                .as_str()
                .ok_or_else(|| ToolError::ExecutionFailed("missing 'operation'".into()))?;

            let reason = args["reason"].as_str().unwrap_or("");

            // Get backing scale factor for coordinate translation
            let scale = get_backing_scale();

            match operation {
                "move" => {
                    let (x, y) = resolve_coordinates(&args, scale)?;
                    execute_move(x, y)?;
                    Ok(format!("[reason: {reason}] moved to ({x}, {y})"))
                }
                "click" => {
                    let (x, y) = resolve_coordinates(&args, scale)?;
                    execute_move(x, y)?;
                    std::thread::sleep(std::time::Duration::from_millis(50));
                    execute_click()?;
                    Ok(format!("[reason: {reason}] clicked at ({x}, {y})"))
                }
                "type" => {
                    let text = args["text"]
                        .as_str()
                        .ok_or_else(|| ToolError::ExecutionFailed("type requires 'text'".into()))?;
                    execute_type(text)?;
                    Ok(format!("[reason: {reason}] typed: {text}"))
                }
                "key" => {
                    let key_name = args["key"]
                        .as_str()
                        .ok_or_else(|| ToolError::ExecutionFailed("key requires 'key' field with key name".into()))?;
                    execute_key(key_name)?;
                    Ok(format!("[reason: {reason}] pressed: {key_name}"))
                }
                "hotkey" => {
                    let key_name = args["key"]
                        .as_str()
                        .ok_or_else(|| ToolError::ExecutionFailed("hotkey requires 'key'".into()))?;
                    let modifiers = args["modifiers"]
                        .as_array()
                        .ok_or_else(|| ToolError::ExecutionFailed("hotkey requires 'modifiers' array".into()))?;
                    let mod_names: Vec<&str> = modifiers.iter()
                        .filter_map(|m| m.as_str())
                        .collect();
                    execute_hotkey(&mod_names, key_name)?;
                    Ok(format!("[reason: {reason}] hotkey: {:?}+{}", mod_names, key_name))
                }
                "scroll" => {
                    let amount = args["amount"]
                        .as_i64()
                        .ok_or_else(|| ToolError::ExecutionFailed("scroll requires 'amount'".into()))? as i32;
                    execute_scroll(amount)?;
                    Ok(format!("[reason: {reason}] scrolled: {amount}"))
                }
                _ => Err(ToolError::ExecutionFailed(format!("unknown operation: {operation}"))),
            }
        })
    }
}

/// Resolve coordinates from either element ID or raw x,y.
/// Coordinates are scaled from image space to screen logical space.
fn resolve_coordinates(
    args: &serde_json::Value,
    scale: f64,
) -> Result<(i32, i32), ToolError> {
    if let Some(element_id) = args["element"].as_str() {
        // TODO: element lookup needs a new mechanism now that SharedContext is removed
        Err(ToolError::ExecutionFailed(format!(
            "element lookup not yet supported (element: {element_id}). Use x,y coordinates instead."
        )))
    } else {
        let x = args["x"].as_i64()
            .ok_or_else(|| ToolError::ExecutionFailed("click/move requires 'element' or 'x','y'".into()))? as i32;
        let y = args["y"].as_i64()
            .ok_or_else(|| ToolError::ExecutionFailed("click/move requires 'y'".into()))? as i32;
        let screen_x = (x as f64 / scale) as i32;
        let screen_y = (y as f64 / scale) as i32;
        Ok((screen_x, screen_y))
    }
}

/// Get the backing scale factor (retina = 2.0).
fn get_backing_scale() -> f64 {
    // On macOS, try to detect. Default to 2.0 for retina.
    2.0
}

fn execute_move(x: i32, y: i32) -> Result<(), ToolError> {
    use enigo::{Enigo, Mouse, Settings, Coordinate};
    let mut enigo = Enigo::new(&Settings::default())
        .map_err(|e| ToolError::ExecutionFailed(format!("enigo init: {e}")))?;
    enigo.move_mouse(x, y, Coordinate::Abs)
        .map_err(|e| ToolError::ExecutionFailed(format!("move failed: {e}")))?;
    Ok(())
}

fn execute_click() -> Result<(), ToolError> {
    use enigo::{Enigo, Mouse, Settings, Button, Direction};
    let mut enigo = Enigo::new(&Settings::default())
        .map_err(|e| ToolError::ExecutionFailed(format!("enigo init: {e}")))?;
    enigo.button(Button::Left, Direction::Click)
        .map_err(|e| ToolError::ExecutionFailed(format!("click failed: {e}")))?;
    Ok(())
}

fn execute_type(text: &str) -> Result<(), ToolError> {
    use enigo::{Enigo, Keyboard, Settings};
    let mut enigo = Enigo::new(&Settings::default())
        .map_err(|e| ToolError::ExecutionFailed(format!("enigo init: {e}")))?;
    enigo.text(text)
        .map_err(|e| ToolError::ExecutionFailed(format!("type failed: {e}")))?;
    Ok(())
}

fn execute_key(key_name: &str) -> Result<(), ToolError> {
    use enigo::{Enigo, Keyboard, Settings, Key, Direction};
    let mut enigo = Enigo::new(&Settings::default())
        .map_err(|e| ToolError::ExecutionFailed(format!("enigo init: {e}")))?;
    let key = match key_name.to_lowercase().as_str() {
        "enter" | "return" => Key::Return,
        "tab" => Key::Tab,
        "escape" | "esc" => Key::Escape,
        "backspace" => Key::Backspace,
        "delete" => Key::Delete,
        "space" => Key::Space,
        "up" => Key::UpArrow,
        "down" => Key::DownArrow,
        "left" => Key::LeftArrow,
        "right" => Key::RightArrow,
        "home" => Key::Home,
        "end" => Key::End,
        "pageup" => Key::PageUp,
        "pagedown" => Key::PageDown,
        "f1" => Key::F1,
        "f2" => Key::F2,
        "f3" => Key::F3,
        "f4" => Key::F4,
        "f5" => Key::F5,
        "f6" => Key::F6,
        "f7" => Key::F7,
        "f8" => Key::F8,
        "f9" => Key::F9,
        "f10" => Key::F10,
        "f11" => Key::F11,
        "f12" => Key::F12,
        _ => return Err(ToolError::ExecutionFailed(format!("unknown key: {key_name}"))),
    };
    enigo.key(key, Direction::Click)
        .map_err(|e| ToolError::ExecutionFailed(format!("key failed: {e}")))?;
    Ok(())
}

fn execute_hotkey(modifiers: &[&str], key_name: &str) -> Result<(), ToolError> {
    use enigo::{Enigo, Keyboard, Settings, Key, Direction};
    let mut enigo = Enigo::new(&Settings::default())
        .map_err(|e| ToolError::ExecutionFailed(format!("enigo init: {e}")))?;

    // Parse modifier keys
    let mod_keys: Vec<Key> = modifiers.iter()
        .map(|m| match m.to_lowercase().as_str() {
            "cmd" | "command" | "meta" => Ok(Key::Meta),
            "ctrl" | "control" => Ok(Key::Control),
            "alt" | "option" => Ok(Key::Alt),
            "shift" => Ok(Key::Shift),
            _ => Err(ToolError::ExecutionFailed(format!("unknown modifier: {m}"))),
        })
        .collect::<Result<Vec<_>, _>>()?;

    // Parse the main key — try special keys first, then single character
    let main_key = match key_name.to_lowercase().as_str() {
        "space" => Key::Space,
        "enter" | "return" => Key::Return,
        "tab" => Key::Tab,
        "escape" | "esc" => Key::Escape,
        "backspace" => Key::Backspace,
        "delete" => Key::Delete,
        "up" => Key::UpArrow,
        "down" => Key::DownArrow,
        "left" => Key::LeftArrow,
        "right" => Key::RightArrow,
        s if s.len() == 1 => Key::Unicode(s.chars().next().unwrap()),
        _ => return Err(ToolError::ExecutionFailed(format!("unknown key for hotkey: {key_name}"))),
    };

    // Press modifiers, press key, release key, release modifiers
    for mk in &mod_keys {
        enigo.key(*mk, Direction::Press)
            .map_err(|e| ToolError::ExecutionFailed(format!("modifier press: {e}")))?;
    }
    enigo.key(main_key, Direction::Click)
        .map_err(|e| ToolError::ExecutionFailed(format!("key press: {e}")))?;
    for mk in mod_keys.iter().rev() {
        enigo.key(*mk, Direction::Release)
            .map_err(|e| ToolError::ExecutionFailed(format!("modifier release: {e}")))?;
    }

    Ok(())
}

fn execute_scroll(amount: i32) -> Result<(), ToolError> {
    use enigo::{Enigo, Mouse, Settings, Axis};
    let mut enigo = Enigo::new(&Settings::default())
        .map_err(|e| ToolError::ExecutionFailed(format!("enigo init: {e}")))?;
    enigo.scroll(amount, Axis::Vertical)
        .map_err(|e| ToolError::ExecutionFailed(format!("scroll failed: {e}")))?;
    Ok(())
}
