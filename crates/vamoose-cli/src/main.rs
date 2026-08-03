//! `vamoose` — unified CLI binary.
//!
//! Composes the worker, coordinator, TUI, and data-plane crates into a single
//! command surface. This binary owns process-level argument parsing, command
//! dispatch, configuration composition, logging startup/shutdown, and final
//! exit status. Protocols, schemas, claim logic, copy execution, coordinator
//! runtime behavior, and TUI behavior remain owned by their underlying crates.

use clap::Parser;

mod cli;
mod cmd;
mod config;
mod dispatch;
mod logging;

#[tokio::main(flavor = "multi_thread")]
async fn main() -> anyhow::Result<()> {
    let cli = cli::Cli::parse();
    let filter = cli::build_filter(cli.log.as_deref());

    // Load config first so logging can use the source format's explicit
    // policy and normalized storage settings. Missing or invalid input
    // falls back to a mode-appropriate minimal subscriber so the
    // operator sees the load error through the normal error path (F39:
    // the TUI's fallback must stay off stderr too — the alternate
    // screen is corrupted by any fmt line).
    let log_mode = cli::log_mode_for(&cli.command);
    let log_handle = match config::Config::load(cli.config.clone()) {
        Ok(cfg) => match cfg.logging_policy() {
            config::LoggingPolicy::MinimalFallback => logging::init_fallback(filter, &log_mode),
            config::LoggingPolicy::Standard(logging_cfg) => Some(logging::init(
                filter,
                &logging_cfg,
                cfg.storage(),
                &log_mode,
            )?),
        },
        Err(_) => logging::init_fallback(filter, &log_mode),
    };

    let result = dispatch::run(cli.command, cli.config).await;
    let shutdown_deadline = dispatch::logging_shutdown_deadline(&result);

    if let Some(handle) = log_handle {
        handle.shutdown(shutdown_deadline).await;
    }

    let exit_code = result?.exit_code();
    if exit_code != 0 {
        std::process::exit(exit_code);
    }
    Ok(())
}
