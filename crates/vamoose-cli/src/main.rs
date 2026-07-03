//! `vamoose` — unified CLI binary.
//!
//! Composes the existing crates (`migration-core`, `migration-mover`,
//! `migration-worker`, `migration-aggr`) into a single binary with
//! subcommand dispatch. The protocol, schema, claim logic, and
//! orchestrator are all owned by the underlying crates; this binary
//! is only wiring.

use clap::{Parser, Subcommand};
use std::path::PathBuf;
use std::time::Duration;

mod cmd;
mod config;
mod logging;

#[derive(Parser)]
#[command(
    name = "vamoose",
    version,
    about = "Distributed NFS migration with S3 coordination"
)]
struct Cli {
    /// Path to config file (default: vamoose.toml in CWD).
    #[arg(short, long, env = "VAMOOSE_CONFIG", global = true)]
    config: Option<PathBuf>,

    /// Logging filter (default: info).
    #[arg(long, env = "RUST_LOG", global = true)]
    log: Option<String>,

    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    /// Run a migration worker.
    Worker(cmd::worker::Args),
    /// Walk a source filesystem and emit parquet shards.
    Walker(cmd::walker::Args),
    /// Rewrite legacy walker output to the canonical schema.
    Rewrite(cmd::rewrite::Args),
    /// Aggregate worker progress to a single status snapshot.
    Aggr(cmd::aggr::Args),
    /// Show current migration status (one-shot or watch).
    Status(cmd::status::Args),
    /// Verify environment health (S3, NFS, perms).
    Doctor(cmd::doctor::Args),
    /// Initialize an empty bucket layout.
    Init(cmd::init::Args),
    /// End-to-end pipeline: walker + rewrite + workers.
    Run(cmd::run::Args),
    /// Control-plane HTTP daemon (REST + SSE).
    Coord(cmd::coord::Args),
    /// Operator dashboard (terminal UI). Connects to a running coord.
    Tui(cmd::tui::Args),
}

/// Pick the logging mode by subcommand (F39, COORD_PLAN §3.7): the
/// TUI owns the terminal, so it gets the quiet mode — no stderr
/// layer, no S3 uploader, optional `--log-file`. Everything else
/// keeps the standard stderr + file + uploader stack.
fn log_mode_for(command: &Command) -> logging::LogMode {
    match command {
        Command::Tui(a) => logging::LogMode::TuiQuiet {
            log_file: a.log_file.clone(),
        },
        _ => logging::LogMode::Standard,
    }
}

fn build_filter(arg: Option<&str>) -> tracing_subscriber::EnvFilter {
    match arg {
        Some(f) => tracing_subscriber::EnvFilter::try_new(f)
            .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        None => tracing_subscriber::EnvFilter::try_from_default_env()
            .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
    }
}

#[tokio::main(flavor = "multi_thread")]
async fn main() -> anyhow::Result<()> {
    let cli = Cli::parse();
    let filter = build_filter(cli.log.as_deref());

    // Load config first so logging::init can read [logging] / [s3] /
    // [global].bucket. If the config is missing or invalid, fall back
    // to a mode-appropriate minimal subscriber so the operator sees
    // the load error through the normal error path (F39: the TUI's
    // fallback must stay off stderr too — the alternate screen is
    // corrupted by any fmt line).
    let log_mode = log_mode_for(&cli.command);
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

    let result = match cli.command {
        Command::Worker(a) => cmd::worker::run(a, cli.config).await,
        Command::Walker(a) => cmd::walker::run(a, cli.config).await,
        Command::Rewrite(a) => cmd::rewrite::run(a, cli.config).await,
        Command::Aggr(a) => cmd::aggr::run(a, cli.config).await,
        Command::Status(a) => cmd::status::run(a, cli.config).await,
        Command::Doctor(a) => cmd::doctor::run(a, cli.config).await,
        Command::Init(a) => cmd::init::run(a, cli.config).await,
        Command::Run(a) => cmd::run::run(a, cli.config).await,
        Command::Coord(a) => cmd::coord::run(a, cli.config).await,
        Command::Tui(a) => cmd::tui::run(a, cli.config).await,
    };

    if let Some(handle) = log_handle {
        handle.shutdown(Duration::from_secs(10)).await;
    }
    result
}

#[cfg(test)]
mod tests {
    use super::*;
    use clap::CommandFactory;

    /// F31: clap's own consistency checks (conflicting flags, bad
    /// defaults, duplicate names, malformed subcommand wiring) run at
    /// test time instead of panicking at first `vamoose --help` in
    /// the field.
    #[test]
    fn clap_wiring_is_valid() {
        Cli::command().debug_assert();
    }

    /// F39: the `tui` subcommand gets the quiet log mode (no stderr
    /// layer, no uploader, `--log-file` honored); everything else
    /// keeps Standard.
    #[test]
    fn tui_subcommand_selects_quiet_log_mode() {
        let cli = Cli::parse_from([
            "vamoose",
            "tui",
            "--coord-url",
            "http://127.0.0.1:8443",
            "--log-file",
            "/tmp/tui-trace.log",
        ]);
        match log_mode_for(&cli.command) {
            logging::LogMode::TuiQuiet { log_file } => {
                assert_eq!(
                    log_file.as_deref(),
                    Some(std::path::Path::new("/tmp/tui-trace.log"))
                );
            }
            other => panic!("tui must be TuiQuiet, got {other:?}"),
        }
        // Without the flag: still quiet, just fileless.
        let cli = Cli::parse_from(["vamoose", "tui", "--coord-url", "http://127.0.0.1:8443"]);
        match log_mode_for(&cli.command) {
            logging::LogMode::TuiQuiet { log_file } => assert!(log_file.is_none()),
            other => panic!("tui must be TuiQuiet, got {other:?}"),
        }
        // A non-TUI subcommand keeps today's behavior.
        let cli = Cli::parse_from(["vamoose", "doctor"]);
        assert!(matches!(
            log_mode_for(&cli.command),
            logging::LogMode::Standard
        ));
    }

    /// An unparsable `--log` filter falls back to `info` rather than
    /// erroring out before logging exists.
    #[test]
    fn build_filter_falls_back_to_info_on_garbage() {
        let f = build_filter(Some("not[a(filter"));
        assert_eq!(f.to_string(), "info");
        let f = build_filter(Some("shutdown=debug"));
        assert_eq!(f.to_string(), "shutdown=debug");
    }
}
