use std::io::{self, Write};

use clap::{Parser, Subcommand};
use colored::Colorize;

use pilot::config::{load_config, mask_token, save_config, set_config_key};
use pilot::llm::LlmClient;

// Workflow-era CLI glue (StdinIO, TracingEvents, resolve_workflow,
// HELP_TEXT_LEGACY_WORKFLOW) lives in src/legacy.rs. The workflow
// subsystem itself (runner, workflow, dag_v2, etc.) remains compiled
// inside the `pilot` crate for reference but isn't reachable from the
// CLI in main.rs.
mod legacy;

#[derive(Parser)]
#[command(
    name = "pilot",
    version,
    about = "Permission-scoped tool broker for AI assistants",
    disable_help_subcommand = true,
)]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
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
// Main
// ---------------------------------------------------------------------------

#[tokio::main]
async fn main() {
    let cli = Cli::parse();

    // ----------------------------------------------------------------------
    // Workflow-era dispatch (run / ls / explain / update) was removed from
    // the CLI surface. The helper types (Runner, TracingEvents,
    // resolve_workflow, load_workflow, discovery, explain, update) stay
    // compiled in `pilot` so the behavior can be revived without rebuilding
    // the subsystem. See HELP_TEXT_LEGACY_WORKFLOW in src/help.rs for the
    // prior user-facing docs.
    // ----------------------------------------------------------------------
    match cli.command {
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
