use std::io::{self, BufRead, Write};
use std::path::PathBuf;
use std::sync::Arc;

use clap::{Parser, Subcommand};
use colored::Colorize;

use pilot::config::{load_config, mask_token, save_config, set_config_key};
use pilot::events::RunnerEvents;
use pilot::llm::LlmClient;
use pilot::user_io::UserIO;
use pilot::runner::Runner;
use pilot::workflow::load_workflow;

#[derive(Parser)]
#[command(
    name = "pilot",
    version,
    about = "AI scripting tool — run workflows defined in YAML",
    disable_help_subcommand = true,
)]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Execute a workflow (interactive picker if no name given)
    Run {
        /// Workflow name or path to YAML file
        workflow: Option<String>,
        /// Errors only
        #[arg(short, long)]
        quiet: bool,
        /// Show lifecycle events
        #[arg(short, long)]
        verbose: bool,
        /// Show debug details (LLM requests, internals)
        #[arg(long)]
        debug: bool,
        /// Extra arguments passed to the workflow (e.g., --question "what is 2+2")
        #[arg(trailing_var_arg = true, allow_hyphen_values = true)]
        extra: Vec<String>,
    },

    /// Explain what a workflow does and how to run it
    Explain {
        /// Workflow name or path to YAML file
        workflow: Option<String>,
    },

    /// Edit a workflow YAML via chat with an LLM
    Update {
        /// Workflow name or path to YAML file
        workflow: Option<String>,
        /// Model to use for editing (default: anthropic/claude-sonnet-4-6)
        #[arg(short, long)]
        model: Option<String>,
    },

    /// List available workflows discovered from .pilot/ directories
    Ls,

    /// Test API connection with a simple request
    Test,

    /// Reference docs — actions, tools, models
    Help {
        #[command(subcommand)]
        topic: Option<HelpTopic>,
    },

    /// Call an exposed tool from .pilot/tools/
    Tool {
        /// Tool name (matches .pilot/tools/<name>.yaml)
        tool_name: String,
        /// Tool arguments as --key value pairs (supports dot notation for nesting)
        #[arg(trailing_var_arg = true, allow_hyphen_values = true)]
        args: Vec<String>,
    },

    /// Manage configuration
    Config {
        #[command(subcommand)]
        action: ConfigAction,
    },
}

#[derive(Subcommand)]
enum HelpTopic {
    /// Show detailed reference for all action types
    Actions,
    /// Show detailed reference for all LLM tools
    Tools,
    /// List supported models
    Models,
}

#[derive(Subcommand)]
enum ConfigAction {
    /// Set a config value
    Set {
        /// Config key (openai_api_key, anthropic_api_key, gemini_api_key)
        key: String,
        /// Value to set
        value: String,
    },

    /// Show current configuration
    Show,
}

// ---------------------------------------------------------------------------
// StdinIO — user interaction via stdin/stdout
// ---------------------------------------------------------------------------

struct StdinIO;

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

// ---------------------------------------------------------------------------
// ColoredEvents — formatted terminal output
// ---------------------------------------------------------------------------

struct TracingEvents;

impl RunnerEvents for TracingEvents {
    fn workflow_done(&self) {
        tracing::info!("✓ Workflow complete.");
    }

    fn workflow_error(&self, error: &str) {
        tracing::error!("✗ {}", error);
    }
}

// ---------------------------------------------------------------------------
// Workflow resolution — shared by run and update
// ---------------------------------------------------------------------------

/// Resolve a workflow name/path to a file path.
/// If arg is Some: try as file path, then as name via discovery.
/// If arg is None: show interactive fuzzy picker.
fn resolve_workflow(arg: Option<String>) -> PathBuf {
    use dialoguer::FuzzySelect;
    use pilot::discovery::{discover_workflows, tilde_path};

    match arg {
        Some(arg) => {
            let path = PathBuf::from(&arg);
            if path.exists() {
                return path;
            }

            let dirs = discover_workflows();
            for dir in &dirs {
                if dir.workflows.contains(&arg) {
                    return dir.abs_path.join(format!("{}.yaml", arg));
                }
            }
            for dir in &dirs {
                let yml_path = dir.abs_path.join(format!("{}.yml", arg));
                if yml_path.exists() {
                    return yml_path;
                }
            }

            eprintln!("{} workflow '{}' not found", "error:".red(), arg);
            std::process::exit(1);
        }
        None => {
            let dirs = discover_workflows();
            if dirs.is_empty() {
                eprintln!("{}", "No .pilot/ directories found.".dimmed());
                std::process::exit(1);
            }

            let mut items: Vec<(String, PathBuf)> = Vec::new();
            let max_name_len = dirs
                .iter()
                .flat_map(|d| d.workflows.iter())
                .map(|n| n.len())
                .max()
                .unwrap_or(0);

            for dir in &dirs {
                let source = if dir.is_cwd {
                    ".pilot/".to_string()
                } else {
                    tilde_path(&dir.abs_path).unwrap_or_else(|| dir.abs_path.display().to_string())
                };
                for name in &dir.workflows {
                    let display = format!(
                        "{:<width$}  {}",
                        name,
                        source.dimmed(),
                        width = max_name_len
                    );
                    let path = dir.abs_path.join(format!("{}.yaml", name));
                    items.push((display, path));
                }
            }

            if items.is_empty() {
                eprintln!("{}", "No workflows found.".dimmed());
                std::process::exit(1);
            }

            let display_items: Vec<&str> = items.iter().map(|(d, _)| d.as_str()).collect();
            let selection = FuzzySelect::new()
                .with_prompt("Select workflow")
                .items(&display_items)
                .interact_opt()
                .unwrap_or(None);

            match selection {
                Some(idx) => items[idx].1.clone(),
                None => std::process::exit(0),
            }
        }
    }
}

// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// Main
// ---------------------------------------------------------------------------

#[tokio::main]
async fn main() {
    let cli = Cli::parse();

    match cli.command {
        Commands::Ls => {
            use pilot::discovery::{discover_workflows, tilde_path};

            let dirs = discover_workflows();
            if dirs.is_empty() {
                println!("{}", "No .pilot/ directories found.".dimmed());
                return;
            }

            for dir in &dirs {
                // Build display path
                let display = if dir.is_cwd {
                    match tilde_path(&dir.abs_path) {
                        Some(tp) => format!(".pilot/ ({})", tp),
                        None => ".pilot/".to_string(),
                    }
                } else {
                    let abs = dir.abs_path.display().to_string();
                    match tilde_path(&dir.abs_path) {
                        Some(tp) => format!("{} ({})", abs, tp),
                        None => abs,
                    }
                };

                println!("\n{}", display.bold());
                for name in &dir.workflows {
                    println!("  {}", name);
                }
            }
            println!();
        }

        Commands::Run { workflow, quiet, verbose, debug, extra } => {
            // Init tracing — default is warn, flags override, RUST_LOG overrides all
            let filter = if debug {
                "debug"
            } else if verbose {
                "info"
            } else if quiet {
                "error"
            } else {
                "warn"
            };
            tracing_subscriber::fmt()
                .with_env_filter(
                    tracing_subscriber::EnvFilter::try_from_default_env()
                        .unwrap_or_else(|_| filter.into()),
                )
                .with_target(false)
                .without_time()
                .init();
            // Parse --key value pairs — duplicate keys accumulate values
            let mut workflow_args: std::collections::HashMap<String, Vec<String>> =
                std::collections::HashMap::new();
            let mut i = 0;
            while i < extra.len() {
                let a = &extra[i];
                if let Some(key) = a.strip_prefix("--") {
                    if i + 1 < extra.len() {
                        workflow_args
                            .entry(key.to_string())
                            .or_default()
                            .push(extra[i + 1].clone());
                        i += 2;
                    } else {
                        eprintln!("{} --{} requires a value", "error:".red(), key);
                        std::process::exit(1);
                    }
                } else {
                    eprintln!("{} unexpected argument: {}", "error:".red(), a);
                    std::process::exit(1);
                }
            }

            let workflow_path = resolve_workflow(workflow);

            let cfg = match load_config() {
                Ok(cfg) => cfg,
                Err(e) => {
                    eprintln!("{} {e}", "error:".red());
                    eprintln!("Run 'pilot config set' to configure credentials.");
                    std::process::exit(1);
                }
            };

            tracing::info!("Loading: {}", workflow_path.display());

            let wf = match load_workflow(&workflow_path) {
                Ok(wf) => wf,
                Err(e) => {
                    eprintln!("{} {e}", "error:".red());
                    std::process::exit(1);
                }
            };

            let llm_config = cfg.to_llm_config();
            let io: Arc<dyn UserIO> = Arc::new(StdinIO);
            let events: Arc<dyn RunnerEvents> = Arc::new(TracingEvents);
            let runner = Runner::new(llm_config, &cfg, wf, io, events, workflow_args);

            match runner.run().await {
                Ok(output_bytes) => {
                    let json = pilot::sql::cli::arrow_bytes_to_json(&output_bytes)
                        .unwrap_or_else(|_| String::from_utf8_lossy(&output_bytes).to_string());
                    println!("{}", json);
                }
                Err(e) => {
                    eprintln!("\n{} {e}", "✗ Workflow failed:".red().bold());
                    std::process::exit(1);
                }
            }
        }

        Commands::Explain { workflow } => {
            let workflow_path = resolve_workflow(workflow);

            let cfg = match load_config() {
                Ok(cfg) => cfg,
                Err(e) => {
                    eprintln!("{} {e}", "error:".red());
                    eprintln!("Run 'pilot config set' to configure credentials.");
                    std::process::exit(1);
                }
            };

            let yaml_content = match std::fs::read_to_string(&workflow_path) {
                Ok(s) => s,
                Err(e) => {
                    eprintln!("{} {e}", "error:".red());
                    std::process::exit(1);
                }
            };

            let workflow_name = workflow_path
                .file_stem()
                .map(|s| s.to_string_lossy().to_string())
                .unwrap_or_else(|| "workflow".to_string());
            let system_prompt = pilot::explain::build_explain_prompt(&yaml_content, &workflow_name);

            let llm = pilot::llm::LLM::new(pilot::llm::LLMConfig {
                openai_api_key: cfg.openai_api_key.clone(),
                anthropic_api_key: cfg.anthropic_api_key.clone(),
                gemini_api_key: cfg.gemini_api_key.clone(),
                xai_api_key: cfg.xai_api_key.clone(),
                model: "anthropic/claude-sonnet-4-6".to_string(),
            });

            let messages = vec![
                pilot::llm::ChatMessage::System { content: system_prompt, name: None },
                pilot::llm::ChatMessage::User { content: "Explain this workflow.".to_string(), name: None },
            ];

            match llm.chat(messages, None, None, None).await {
                Ok(resp) => {
                    if let Some(content) = &resp.content {
                        println!("{}", content);
                    }
                }
                Err(e) => {
                    eprintln!("{} {e}", "error:".red());
                    std::process::exit(1);
                }
            }
        }

        Commands::Update { workflow, model } => {
            let workflow_path = resolve_workflow(workflow);

            let cfg = match load_config() {
                Ok(cfg) => cfg,
                Err(e) => {
                    eprintln!("{} {e}", "error:".red());
                    eprintln!("Run 'pilot config set' to configure credentials.");
                    std::process::exit(1);
                }
            };

            let model_name = model.unwrap_or_else(|| "anthropic/claude-sonnet-4-6".to_string());
            println!(
                "{} {} {}",
                "Editing:".dimmed(),
                workflow_path.display(),
                format!("({})", model_name).dimmed()
            );

            let mut llm_config = cfg.to_llm_config();
            llm_config.model = model_name;
            let llm = Arc::new(pilot::llm::LLM::new(llm_config));
            let io: Arc<dyn UserIO> = Arc::new(StdinIO);

            match pilot::update::run_update(llm, workflow_path, io).await {
                Ok(()) => {}
                Err(e) => {
                    eprintln!("\n{} {e}", "✗ Update failed:".red().bold());
                    std::process::exit(1);
                }
            }
        }

        Commands::Test => {
            let cfg = match load_config() {
                Ok(cfg) => cfg,
                Err(e) => {
                    eprintln!("{} {e}", "error:".red());
                    eprintln!("Run 'pilot config set' to configure credentials.");
                    std::process::exit(1);
                }
            };

            let test_models = vec![
                "anthropic/claude-haiku-4-5",
                "openai/gpt-5-mini",
                "google-ai-studio/gemini-2.5-flash",
                "xai/grok-4-1-fast-non-reasoning",
            ];

            let mut all_ok = true;
            for model in &test_models {
                print!("{model}... ");
                io::stdout().flush().ok();

                let llm = pilot::llm::LLM::new(pilot::llm::LLMConfig {
                    openai_api_key: cfg.openai_api_key.clone(),
                    anthropic_api_key: cfg.anthropic_api_key.clone(),
                    gemini_api_key: cfg.gemini_api_key.clone(),
                    xai_api_key: cfg.xai_api_key.clone(),
                    model: model.to_string(),
                });

                let messages = vec![
                    pilot::llm::ChatMessage::User { content: "Say hello in one word.".to_string(), name: None },
                ];

                match llm.chat(messages, None, None, None).await {
                    Ok(resp) => {
                        println!("{} ({})", "OK".green(), resp.content.unwrap_or_default().trim());
                    }
                    Err(e) => {
                        println!("{} {e}", "FAIL".red());
                        all_ok = false;
                    }
                }
            }

            if !all_ok {
                std::process::exit(1);
            }
        }


        Commands::Help { topic } => {
            match topic {
                Some(HelpTopic::Actions) => pilot::help::print_actions(),
                Some(HelpTopic::Tools) => pilot::help::print_tools(),
                Some(HelpTopic::Models) => {
                    for model in pilot::models::VALID_MODELS {
                        println!("{model}");
                    }
                }
                None => println!("{}", pilot::help::HELP_TEXT),
            }
        }

        Commands::Tool { tool_name, args } => {
            tracing_subscriber::fmt()
                .with_env_filter(
                    tracing_subscriber::EnvFilter::try_from_default_env()
                        .unwrap_or_else(|_| "warn".into()),
                )
                .with_target(false)
                .without_time()
                .init();

            let cfg = match load_config() {
                Ok(cfg) => cfg,
                Err(e) => {
                    eprintln!("{} {e}", "error:".red());
                    std::process::exit(1);
                }
            };

            match pilot::tool_cli::run_tool(&tool_name, &args, &cfg).await {
                Ok(result) => println!("{result}"),
                Err(e) => {
                    eprintln!("{} {e}", "error:".red());
                    std::process::exit(1);
                }
            }
        }

        Commands::Config { action } => match action {
            ConfigAction::Set { key, value } => {
                let mut cfg = load_config().unwrap_or_default();

                if let Err(e) = set_config_key(&mut cfg, &key, &value) {
                    eprintln!("{} {e}", "error:".red());
                    std::process::exit(1);
                }

                if let Err(e) = save_config(&cfg) {
                    eprintln!("{} {e}", "error:".red());
                    std::process::exit(1);
                }

                println!("{} {key}", "Set".green());
            }

            ConfigAction::Show => {
                let cfg = match load_config() {
                    Ok(cfg) => cfg,
                    Err(e) => {
                        eprintln!("{} {e}", "error:".red());
                        eprintln!("Run 'pilot config set <key> <value>' to configure.");
                        std::process::exit(1);
                    }
                };

                println!("openai_api_key:    {}", mask_token(&cfg.openai_api_key));
                println!("anthropic_api_key: {}", mask_token(&cfg.anthropic_api_key));
                println!("gemini_api_key:    {}", mask_token(&cfg.gemini_api_key));
                println!("xai_api_key:       {}", mask_token(&cfg.xai_api_key));
                println!("s3_endpoint:       {}", if cfg.s3_endpoint.is_empty() { "(not set)" } else { &cfg.s3_endpoint });
                println!("s3_access_key:     {}", mask_token(&cfg.s3_access_key));
                println!("s3_secret_key:     {}", mask_token(&cfg.s3_secret_key));
            }
        },
    }
}
