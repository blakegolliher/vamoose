//! The embedded scan and rewrite stages, shared by `prepare` (pass 0)
//! and `sync` (every resync pass). A pass dir has the same layout as
//! the root work dir, so both call these with their own [`WorkDir`].

use crate::util::{read_json_opt, utc_now, write_json_atomic};
use crate::workdir::WorkDir;
use anyhow::{Context, Result};
use migration_core::prepare_tools as tools;
use serde::{Deserialize, Serialize};
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};

/// Minimum seconds between scan-progress log lines.
const SCAN_LOG_SECS: u64 = 10;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ScanCheckpoint {
    pub complete: bool,
    /// Directory holding the part files (what the rewrite reads).
    pub scan_dir: PathBuf,
    pub walker_version: String,
    pub scan_url: String,
    pub finished_utc: String,
}

/// Everything that shapes one scan, resolved by the caller.
#[derive(Debug, Clone)]
pub struct ScanParams {
    pub scan_url: String,
    pub workers: usize,
    pub shard_size_mb: u64,
    pub exclude: Vec<String>,
    /// Adopt an existing nfs-walker output instead of scanning.
    pub scan_dir_override: Option<PathBuf>,
}

/// Version label of the compiled-in scanner, recorded in the scan
/// checkpoint and stamped into each canonical shard's KV metadata.
pub fn embedded_walker_version() -> String {
    use clap::CommandFactory;
    let v = nfs_walker::CliArgs::command()
        .get_version()
        .unwrap_or("unknown")
        .to_string();
    format!("nfs-walker {v} (embedded)")
}

/// Reuse a completed scan checkpoint, adopt an operator-supplied scan
/// directory, or run the embedded walker into a fresh attempt dir.
pub async fn ensure_scan(wd: &WorkDir, params: &ScanParams) -> Result<ScanCheckpoint> {
    let checkpoint_path = wd.scan_json();
    if let Some(cp) = read_json_opt::<ScanCheckpoint>(&checkpoint_path)? {
        if cp.complete && tools::resolve_scan_dir(&cp.scan_dir).is_ok() {
            println!("  scan checkpoint valid; not rescanning");
            return Ok(cp);
        }
    }

    let (scan_dir, walker_version) = match &params.scan_dir_override {
        Some(dir) => {
            // Absolute, so the checkpoint survives a resume from
            // another working directory.
            let resolved = std::fs::canonicalize(tools::resolve_scan_dir(dir)?)?;
            println!("  using existing scan {}", resolved.display());
            (
                resolved,
                "unknown (scan supplied with --scan-dir)".to_string(),
            )
        }
        None => {
            let attempt_dir = wd.next_attempt_dir()?;
            std::fs::create_dir_all(&attempt_dir)?;
            let invocation = tools::WalkerInvocation {
                scan_url: params.scan_url.clone(),
                output: attempt_dir.join("walk.parquet"),
                workers: params.workers,
                exclude: params.exclude.clone(),
                shard_size_mb: params.shard_size_mb,
                log: attempt_dir.join("walker-progress.jsonl"),
            };
            println!(
                "  scanning {} -> {} ({} workers, {})",
                params.scan_url,
                invocation.output.display(),
                invocation.workers,
                embedded_walker_version(),
            );
            let stats = run_embedded_walker(&invocation).await?;
            println!(
                "  scanned {} dirs, {} files, {} bytes in {:.0?} ({} errors)",
                stats.dirs, stats.files, stats.bytes, stats.duration, stats.errors,
            );
            (
                tools::resolve_scan_dir(&invocation.output)?,
                embedded_walker_version(),
            )
        }
    };

    let cp = ScanCheckpoint {
        complete: true,
        scan_dir,
        walker_version,
        scan_url: params.scan_url.clone(),
        finished_utc: utc_now(),
    };
    write_json_atomic(&checkpoint_path, &cp)?;
    Ok(cp)
}

/// Delete the raw walker scan output under this work dir's `scan/`
/// (`--purge-intermediates`). Safe once the canonical shards and
/// manifest are committed: the scan is a pure intermediate that
/// doubles the index footprint. Only `<wd>/scan/` is removed, so a
/// `--scan-dir` override outside the work dir is never touched.
/// Best-effort — a failed purge is a warning, never an error.
pub fn purge_scan_output(wd: &WorkDir) {
    let root = wd.scan_root();
    match std::fs::remove_dir_all(&root) {
        Ok(()) => println!("  purged scan output {}", root.display()),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
        Err(e) => tracing::warn!(
            error = %e,
            path = %root.display(),
            "scan purge failed (non-fatal)",
        ),
    }
}

/// Rewrite a completed scan into canonical shards under
/// `<wd>/canonical/` with `<wd>/rewrite.json` as the resumable
/// report. In-process (`mig_walker_rewrite` library), off-runtime.
pub async fn ensure_canonical(wd: &WorkDir, scan: &ScanCheckpoint) -> Result<()> {
    let rewrite = mig_walker_rewrite::Cli {
        input: scan.scan_dir.clone(),
        output: wd.canonical_dir(),
        // Not the configured source root: the scan was anchored there
        // already (see prepare_tools::REWRITE_SOURCE_ROOT).
        source_root: tools::REWRITE_SOURCE_ROOT.to_string(),
        walker_version: scan.walker_version.clone(),
        resume: true,
        report: Some(wd.rewrite_json()),
        verbose: false,
    };
    std::fs::create_dir_all(&rewrite.output)?;
    // Parquet decode/encode is CPU work — keep it off the runtime.
    tokio::task::spawn_blocking(move || mig_walker_rewrite::run_rewrite(&rewrite))
        .await
        .context("rewrite task panicked")?
        .context("canonical rewrite failed (finished shards are checkpointed; re-run to resume)")
}

/// Parse a [`tools::WalkerInvocation`] into the embedded walker's own
/// CLI struct. Going through the real clap surface (instead of
/// constructing `WalkConfig` by hand) keeps the argument semantics
/// identical to the standalone `nfs-walker` binary — and makes flag
/// drift a unit-test failure instead of a runtime surprise.
pub(crate) fn walker_cli(invocation: &tools::WalkerInvocation) -> Result<nfs_walker::CliArgs> {
    use clap::Parser;
    let mut argv: Vec<std::ffi::OsString> = vec!["nfs-walker".into()];
    argv.extend(invocation.args());
    // Progress goes to tracing + the JSONL log, not a terminal bar.
    argv.push("--quiet".into());
    nfs_walker::CliArgs::try_parse_from(argv)
        .map_err(|e| anyhow::anyhow!("embedded nfs-walker rejected the scan arguments: {e}"))
}

/// Run the compiled-in scanner on the blocking pool, relaying its
/// progress into tracing every [`SCAN_LOG_SECS`].
async fn run_embedded_walker(
    invocation: &tools::WalkerInvocation,
) -> Result<nfs_walker::WalkStats> {
    let cli = walker_cli(invocation)?;
    let config =
        nfs_walker::WalkConfig::from_args(cli).context("invalid embedded scan configuration")?;
    let stats = tokio::task::spawn_blocking(move || {
        let walker = nfs_walker::SimpleWalker::new(config);
        let last_logged = AtomicU64::new(0);
        walker.run_with_progress(move |p| {
            let elapsed = p.elapsed.as_secs();
            let prev = last_logged.load(Ordering::Relaxed);
            if elapsed >= prev + SCAN_LOG_SECS
                && last_logged
                    .compare_exchange(prev, elapsed, Ordering::Relaxed, Ordering::Relaxed)
                    .is_ok()
            {
                let secs = p.elapsed.as_secs_f64().max(0.001);
                tracing::info!(
                    dirs = p.dirs,
                    files = p.files,
                    bytes = p.bytes,
                    errors = p.errors,
                    entries_s = format!("{:.0}", (p.dirs + p.files) as f64 / secs),
                    "scanning",
                );
            }
        })
    })
    .await
    .context("walker task panicked")?
    .context("scan failed")?;
    if !stats.completed {
        anyhow::bail!("scan was interrupted before completion; re-run to rescan");
    }
    Ok(stats)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The successor to vamoose's runtime `check_walker_flags` probe:
    /// every flag `WalkerInvocation::args()` emits must parse into the
    /// embedded walker's own CLI. Flag drift between the pinned
    /// nfs-walker rev and prepare/sync fails here, at test time.
    #[test]
    fn walker_invocation_parses_into_the_embedded_cli() {
        let invocation = tools::WalkerInvocation {
            scan_url: "nfs://h/export/data".into(),
            output: "/w/scan/attempt-0001/walk.parquet".into(),
            workers: 8,
            exclude: vec![".snapshot".into(), "tmp".into()],
            shard_size_mb: 256,
            log: "/w/scan/attempt-0001/walker-progress.jsonl".into(),
        };
        let cli = walker_cli(&invocation).expect("embedded CLI accepts prepare's arguments");
        assert_eq!(cli.nfs_url.as_deref(), Some("nfs://h/export/data"));
        assert_eq!(cli.workers, 8);
        assert_eq!(
            cli.output,
            std::path::PathBuf::from("/w/scan/attempt-0001/walk.parquet")
        );
        assert_eq!(cli.exclude_patterns, vec![".snapshot", "tmp"]);
        assert!(cli.quiet, "terminal progress bar suppressed");
    }

    #[test]
    fn embedded_walker_version_names_the_scanner() {
        let v = embedded_walker_version();
        assert!(v.starts_with("nfs-walker "), "{v}");
        assert!(v.ends_with("(embedded)"), "{v}");
    }
}
