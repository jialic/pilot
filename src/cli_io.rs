//! Adapters wiring the `pilot` CLI to the runtime traits.
//!
//! The runner takes `UserIO` and `RunnerEvents` trait objects so the same
//! core can drive interactive CLI, tests, or embedded hosts. This module
//! supplies the CLI-host implementations: `StdinIO` for interactive
//! prompts, `TracingEvents` for runner lifecycle logging.

use std::io::{self, BufRead, Write};

use colored::Colorize;

use crate::events::RunnerEvents;
use crate::user_io::UserIO;

/// Reads user input from stdin (multi-line until blank).
pub struct StdinIO;

impl UserIO for StdinIO {
    fn ask(
        &self,
        question: String,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<String, String>> + Send + '_>>
    {
        Box::pin(async move {
            if !question.is_empty() {
                println!("{}", question);
            }
            print!("{} ", ">".yellow());
            io::stdout().flush().map_err(|e| e.to_string())?;

            let stdin = io::stdin();
            let mut lines = Vec::new();
            for line in stdin.lock().lines() {
                let line = line.map_err(|e| e.to_string())?;
                if line.is_empty() && !lines.is_empty() {
                    break;
                }
                if line.is_empty() && lines.is_empty() {
                    // Single enter on empty input = submit empty
                    break;
                }
                lines.push(line);
            }

            if !lines.is_empty() {
                println!("{}", "✓".green());
            }

            Ok(lines.join("\n"))
        })
    }
}

/// Forwards runner lifecycle events to the `tracing` subscriber.
pub struct TracingEvents;

impl RunnerEvents for TracingEvents {
    fn workflow_done(&self) {
        tracing::info!("✓ Workflow complete.");
    }

    fn workflow_error(&self, error: &str) {
        tracing::error!("✗ {}", error);
    }
}
