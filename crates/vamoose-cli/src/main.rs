//! `vamoose` — unified CLI binary.
//!
//! Composes the existing crates (`migration-core`, `migration-mover`,
//! `migration-worker`, `migration-aggr`) into a single binary with
//! subcommand dispatch. The protocol, schema, claim logic, and
//! orchestrator are all owned by the underlying crates; this binary
//! is only wiring.

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

    // Load config first so logging::init can read [logging] / [s3] /
    // [global].bucket. If the config is missing or invalid, fall back
    // to a mode-appropriate minimal subscriber so the operator sees
    // the load error through the normal error path (F39: the TUI's
    // fallback must stay off stderr too — the alternate screen is
    // corrupted by any fmt line).
    let log_mode = cli::log_mode_for(&cli.command);
    let log_handle = match config::Config::load(cli.config.clone()) {
        Ok(cfg) => {
            let logging_cfg = cfg.logging.clone().unwrap_or_default();
            Some(logging::init(
                filter,
                &logging_cfg,
                &cfg.s3,
                &cfg.global.bucket,
                &log_mode,
            )?)
        }
        Err(_) => logging::init_fallback(filter, &log_mode),
    };

    let result = dispatch::run(cli.command, cli.config).await;
    let shutdown_deadline = dispatch::logging_shutdown_deadline(&result);

    if let Some(handle) = log_handle {
        handle.shutdown(shutdown_deadline).await;
    }

    let exit_code = result?.exit_code();
    if exit_code != 0 {
        std::process::exit(exit_code.into());
    }
    Ok(())
}
