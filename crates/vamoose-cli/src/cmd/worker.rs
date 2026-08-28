//! `vamoose worker` — loads the composition configuration, applies
//! CLI overrides, and invokes the existing worker orchestrator.
//!
//! # Exit codes
//!
//! See [`EXIT_CODES_HELP`] (rendered in `vamoose worker --help`):
//! 0 clean completion or a SIGTERM/SIGINT stop, 1 run error, 2 wedged
//! shutdown (hard-exit watchdog), 3 run ended because the worker
//! fenced. The 0-vs-3
//! mapping is `migration_worker::orchestrator::exit_code_for_outcome`;
//! [`run`] returns the semantic outcome and the process boundary
//! applies its code after log shutdown.

use crate::config::Config;
use anyhow::Context;
use clap::Args as ClapArgs;
use migration_worker::orchestrator::{exit_code_for_outcome, RunOutcome};
use std::path::PathBuf;

/// Exit-code contract, shown in `vamoose worker --help`. Codes 0/1/2
/// predate the fenced code and must not be renumbered.
const EXIT_CODES_HELP: &str = "Exit codes:
  0  migration ran to clean completion, a coord-requested drain/cancel, or a
     SIGTERM/SIGINT stop (the shard in hand is released for a peer)
  1  run error (config, S3, or orchestrator failure)
  2  shutdown wedged past its deadline; the hard-exit watchdog fired
  3  run ended because the worker fenced (claim lost / clock jump / 412 storm)
  4  run ended because the store was unreachable for a full lease window; the
     shard was surrendered and the unit restarts the worker (only 3 is held)";

#[derive(ClapArgs)]
#[command(after_help = EXIT_CODES_HELP, after_long_help = EXIT_CODES_HELP)]
pub struct Args {
    /// Override worker host_id (default: `<hostname>-<pid>`).
    #[arg(long)]
    pub id: Option<String>,
    /// Route regular-file copies through the bucketed async libnfs
    /// pool (Phase 2 of the multi-pass mover). Off by default during
    /// the rollout. Non-regular rows (symlinks / hardlinks / dirs /
    /// empty / skip) still use the sync path either way.
    #[arg(long)]
    pub use_bucketed_pool: bool,
}

/// Returns the semantic outcome for a completed run; `Err` keeps
/// meaning an ordinary command failure (exit 1 at the process boundary).
pub async fn run(args: Args, config_path: Option<PathBuf>) -> anyhow::Result<RunOutcome> {
    let (config, path) = Config::load_with_path(config_path)?;
    let (mut worker_cfg, host_id_from_cfg) = config
        .into_worker_config()
        .with_context(|| format!("loading config at {}", path.display()))?;

    worker_cfg.mover.use_bucketed_pool =
        effective_bucketed_pool(worker_cfg.mover.use_bucketed_pool, args.use_bucketed_pool);

    let host_id = selected_host_id(args.id, host_id_from_cfg).unwrap_or_else(|| {
        let host = hostname::get()
            .ok()
            .and_then(|s| s.into_string().ok())
            .unwrap_or_else(|| "unknown".to_string());
        format!("{}-{}", host, std::process::id())
    });

    tracing::info!(host_id = %host_id, "vamoose worker starting");
    let outcome = migration_worker::orchestrator::run(worker_cfg, host_id).await?;
    let exit_code = exit_code_for_outcome(outcome);
    if exit_code != 0 {
        tracing::warn!(
            ?outcome,
            code = exit_code,
            "worker run ended without completing; exiting non-zero"
        );
    }
    Ok(outcome)
}

fn effective_bucketed_pool(configured: bool, cli_flag: bool) -> bool {
    configured || cli_flag
}

fn selected_host_id(cli: Option<String>, configured: Option<String>) -> Option<String> {
    cli.or(configured)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn bucketed_pool_flag_only_enables_the_setting() {
        assert!(!effective_bucketed_pool(false, false));
        assert!(effective_bucketed_pool(false, true));
        assert!(effective_bucketed_pool(true, false));
        assert!(effective_bucketed_pool(true, true));
    }

    #[test]
    fn cli_host_id_takes_precedence_over_configuration() {
        assert_eq!(
            selected_host_id(Some("cli-host".into()), Some("config-host".into())).as_deref(),
            Some("cli-host")
        );
        assert_eq!(
            selected_host_id(None, Some("config-host".into())).as_deref(),
            Some("config-host")
        );
    }
}
