//! Command-line syntax and per-command process policies.

use clap::{Parser, Subcommand};
use std::path::PathBuf;

use crate::{cmd, logging};

#[derive(Parser)]
#[command(
    name = "vamoose",
    version,
    about = "Distributed NFS migration with S3 coordination"
)]
pub(crate) struct Cli {
    /// Path to config file. Default search order: /etc/vamoose/workers/$VAMOOSE_INSTANCE.toml (systemd template instances), ./vamoose.toml, then /etc/vamoose/vamoose.toml.
    #[arg(short, long, env = "VAMOOSE_CONFIG", global = true)]
    pub(crate) config: Option<PathBuf>,

    /// Logging filter (default: info).
    #[arg(long, env = "RUST_LOG", global = true)]
    pub(crate) log: Option<String>,

    #[command(subcommand)]
    pub(crate) command: Command,
}

#[derive(Subcommand)]
pub(crate) enum Command {
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
pub(crate) fn log_mode_for(command: &Command) -> logging::LogMode {
    match command {
        Command::Tui(a) => logging::LogMode::TuiQuiet {
            log_file: a.log_file.clone(),
        },
        _ => logging::LogMode::Standard,
    }
}

pub(crate) fn build_filter(arg: Option<&str>) -> tracing_subscriber::EnvFilter {
    match arg {
        Some(f) => tracing_subscriber::EnvFilter::try_new(f)
            .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        None => tracing_subscriber::EnvFilter::try_from_default_env()
            .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
    }
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

    /// BETA_POLISH_BATCH Item 2: `vamoose worker --help` documents
    /// the exit codes — in particular the dedicated fenced code 3 —
    /// so operators wiring supervisors don't have to read source.
    #[test]
    fn worker_help_documents_exit_codes() {
        let mut cmd = Cli::command();
        let worker = cmd
            .find_subcommand_mut("worker")
            .expect("worker subcommand exists");
        let help = worker.render_long_help().to_string();
        assert!(
            help.contains("Exit codes"),
            "worker help must have an exit-codes section, got:\n{help}",
        );
        for needle in ["0", "1", "2", "3", "fenced"] {
            assert!(
                help.contains(needle),
                "worker help exit-codes section must mention {needle:?}, got:\n{help}",
            );
        }
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
