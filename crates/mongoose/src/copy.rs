//! `mongoose copy` — process the prepared local shards with the
//! vamoose mover.
//!
//! Shards are processed **sequentially**; within each shard the
//! existing `migration_worker::shard_processor::ShardProcessor`
//! dispatches rows concurrently (hardlink groups sequential, size-class
//! inflight limits, deepest-first dir attrs). Mongoose supplies the
//! single-host equivalents of the worker's distributed collaborators:
//! a fence that never trips, a disabled coord event emitter, no run
//! control, and local JSONL sinks instead of S3 flushes.
//!
//! SIGINT/SIGTERM is honored at the next batch boundary — no row is
//! ever interrupted mid-copy. The interrupted shard is not marked
//! completed, so a re-run reprocesses it from the top (copies are
//! idempotent: `.partial` + rename, or truncate-and-heal on the
//! direct-commit path).

use crate::cli::CopyTuning;
use crate::manifest::{self, LocalManifest};
use crate::progress::{write_shard_jsonl, CopyProgress};
use crate::util::raise_fd_limit;
use crate::workdir::WorkDir;
use anyhow::{Context, Result};
use migration_core::fence::Fence;
use migration_mover::batch::{BatchBudget, InflightLimiter, InflightProfile};
use migration_mover::{DowngradeSink, FailureSink};
use migration_worker::caps;
use migration_worker::coord_driver::EventEmitter;
use migration_worker::heartbeat::LivePending;
use migration_worker::mover_factory::{self, MoverParams};
use migration_worker::shard_processor::ShardProcessor;
use migration_worker::throughput::ThroughputCounter;
use std::path::Path;
use std::sync::Arc;
use tokio_util::sync::CancellationToken;

/// How often the ticker logs live counters while a shard runs.
const TICK_SECS: u64 = 15;

#[derive(Debug, Default, Clone)]
pub struct CopySummary {
    pub shards_total: u64,
    pub shards_done: u64,
    pub files_ok: u64,
    pub files_failed: u64,
    pub files_torn: u64,
    pub bytes_moved: u64,
    /// Stopped by SIGINT/SIGTERM at a batch boundary; re-run
    /// `mongoose copy` to resume from the interrupted shard.
    pub interrupted: bool,
}

/// Project the local manifest + CLI tuning onto the shared mover
/// factory parameters. Pure; unit-tested without libnfs.
pub fn mover_params(
    m: &LocalManifest,
    tuning: &CopyTuning,
    require_chown: bool,
    host_id: String,
) -> MoverParams {
    let inflight = InflightProfile {
        small: tuning.inflight_small,
        medium: tuning.inflight_medium,
        large: tuning.inflight_large,
        ..InflightProfile::default()
    };
    MoverParams {
        source_url: m.source.url.clone(),
        dest_url: m.dest.url.clone(),
        source_root: m.source.root.clone(),
        dest_root: m.dest.root.clone(),
        options: m.options.clone(),
        nfs_connections: tuning.nfs_connections.max(1) as usize,
        use_bucketed_pool: tuning.bucketed_async,
        use_raw_fh: tuning.use_raw_fh,
        direct_commit: tuning.direct_commit,
        rpc_timeout_ms: tuning.rpc_timeout_ms,
        require_chown,
        // Walker `size` is advisory (SCHEMA_CONTRACT.md "Size
        // semantics"); source truth wins, same as the worker default.
        require_unchanged_size: false,
        inflight,
        host_id,
    }
}

pub async fn run(work_dir: &Path, tuning: &CopyTuning) -> Result<CopySummary> {
    run_manifest(work_dir, tuning, "manifest.json").await
}

/// [`run`] against a named manifest inside the work dir. `mongoose
/// sync` points this at a pass dir's delta manifest; everything else
/// (progress, sinks, resume) behaves identically.
pub async fn run_manifest(
    work_dir: &Path,
    tuning: &CopyTuning,
    manifest_name: &str,
) -> Result<CopySummary> {
    let wd = WorkDir::new(work_dir);
    let m = manifest::load_file(&wd.root().join(manifest_name))?.ok_or_else(|| {
        anyhow::anyhow!(
            "no {manifest_name} under {}; run `mongoose prepare` (or `mongoose run`) first",
            wd.root().display()
        )
    })?;

    // The manifest was overlap-checked at prepare time; check again in
    // case it was hand-edited.
    migration_core::overlap::check(&m.source, &m.dest)?;

    // SAFETY: geteuid has no preconditions.
    if unsafe { libc::geteuid() } != 0 {
        tracing::warn!("mongoose copy is not running as root; libnfs needs reserved ports");
    }
    raise_fd_limit();
    let cap_chown = caps::has_cap_chown();
    if m.options.preserve_owner && !cap_chown {
        tracing::warn!(
            "preserve_owner=true but CAP_CHOWN not held; running in degraded mode \
             (chown EPERM will be recorded as downgrades, not failures)",
        );
    }

    println!(
        "mongoose copy\n  run     {}\n  source  {}{}\n  dest    {}{}\n  shards  {} ({} rows, {} bytes)\n",
        m.run_id,
        m.source.url,
        m.source.root,
        m.dest.url,
        m.dest.root,
        m.shards.len(),
        m.total_rows,
        m.total_bytes,
    );

    // Stop token: SIGINT/SIGTERM finishes the batch in flight and
    // leaves the shard at the boundary. A second signal is logged and
    // ignored (SIGKILL is the escape hatch).
    let stop = CancellationToken::new();
    spawn_signal_listener(stop.clone());

    let host_id = hostname();
    let fence = Fence::new();
    let downgrades = DowngradeSink::new();
    let failures = FailureSink::new();
    let params = mover_params(&m, tuning, cap_chown, host_id);
    let built = mover_factory::build(&params, downgrades.clone(), fence.clone()).await?;
    let throughput = ThroughputCounter::new();
    let inflight = InflightLimiter::new(&params.inflight);
    let live = Arc::new(LivePending::default());

    let mut progress = CopyProgress::load_or_fresh(&wd, &m.run_id, m.shards.len() as u64)?;
    let resumed = progress.completed_shards.len();
    if resumed > 0 {
        println!(
            "  resuming: {resumed} of {} shards already completed\n",
            m.shards.len()
        );
    }
    progress.write(&wd)?;

    // Live visibility while a big shard runs: log counters + MB/s on
    // an interval. The per-shard summary remains the durable record.
    let ticker = spawn_ticker(Arc::clone(&live), throughput.clone());

    let mut summary = CopySummary {
        shards_total: m.shards.len() as u64,
        shards_done: resumed as u64,
        files_ok: progress.files_ok,
        files_failed: progress.files_failed,
        files_torn: progress.files_torn,
        bytes_moved: progress.bytes_moved,
        interrupted: false,
    };

    for shard in &m.shards {
        if progress.is_completed(&shard.path) {
            continue;
        }
        if stop.is_cancelled() {
            summary.interrupted = true;
            break;
        }
        let parquet = wd.shard_path(&shard.path);
        let size = std::fs::metadata(&parquet)
            .map(|md| md.len())
            .with_context(|| format!("shard {} is missing", parquet.display()))?;
        if size != shard.bytes {
            anyhow::bail!(
                "shard {} is {size} bytes; manifest says {} — the index changed since \
                 prepare, refusing to copy from it",
                parquet.display(),
                shard.bytes
            );
        }

        println!("shard {} ({} rows)", shard.file_name(), shard.rows);
        downgrades.set_current_shard(shard.file_name());
        failures.set_current_shard(shard.file_name());
        live.reset();

        let mut processor = ShardProcessor {
            mover: Arc::clone(&built.mover),
            live: Arc::clone(&live),
            fence: fence.clone(),
            budget: BatchBudget::default(),
            inflight: inflight.clone(),
            failures: failures.clone(),
            throughput: throughput.clone(),
            dir_restamp: Vec::new(),
            fsid_fallback_warned: false,
            emitter: EventEmitter::disabled(),
            run_control: None,
            stop: stop.clone(),
        };
        let outcome = processor
            .process(&parquet)
            .await
            .with_context(|| format!("processing shard {}", parquet.display()))?;

        // Drain this shard's failure/downgrade records to local JSONL
        // before the shard is marked complete, so an interruption
        // between the two never loses records.
        if let Some(p) =
            write_shard_jsonl(&wd.failures_dir(), shard.stem(), &failures.drain_jsonl())?
        {
            println!("  failures  -> {}", p.display());
        }
        if let Some(p) = write_shard_jsonl(
            &wd.downgrades_dir(),
            shard.stem(),
            &downgrades.drain_jsonl(),
        )? {
            println!("  downgrades -> {}", p.display());
        }

        summary.files_ok += outcome.files_ok;
        summary.files_failed += outcome.files_failed;
        summary.files_torn += outcome.files_torn;
        summary.bytes_moved += outcome.bytes_moved;

        if outcome.interrupted {
            // Rows already committed are durable; the shard is NOT
            // completed and a re-run reprocesses it from row 0. Its
            // partial counters are deliberately NOT persisted into
            // progress.json — the re-run's full-shard counters would
            // double-count them.
            println!(
                "  interrupted at a batch boundary ({}/{} rows done); \
                 re-run `mongoose copy` to resume",
                outcome.files_ok + outcome.files_failed,
                outcome.rows_total
            );
            summary.interrupted = true;
            progress.write(&wd)?;
            break;
        }

        progress.files_ok += outcome.files_ok;
        progress.files_failed += outcome.files_failed;
        progress.files_torn += outcome.files_torn;
        progress.bytes_moved += outcome.bytes_moved;
        progress.throughput_mb_s_1m = throughput.sample_mb_s(60);
        progress.completed_shards.push(shard.path.clone());
        summary.shards_done += 1;
        progress.write(&wd)?;
        println!(
            "  done: {} ok, {} failed, {} bytes",
            outcome.files_ok, outcome.files_failed, outcome.bytes_moved
        );
    }

    ticker.abort();

    if !summary.interrupted && summary.shards_done == summary.shards_total {
        // The walker emits no row for the migration root, so every
        // file commit under it has bumped its mtime; stamp it back
        // from source. Best-effort — the bytes are already durable.
        if let Err(e) = migration_mover::restore_root_mtime(
            Arc::clone(&built.pool),
            m.source.root.as_bytes(),
            m.dest.root.as_bytes(),
        )
        .await
        {
            tracing::warn!(error = ?e, "root-dir mtime restore failed (non-fatal)");
        }
        progress.done = true;
        progress.write(&wd)?;
    }

    println!(
        "\n{}: {}/{} shards, {} files ok ({} torn), {} failed, {} bytes moved",
        if summary.interrupted {
            "interrupted"
        } else if summary.shards_done == summary.shards_total {
            "complete"
        } else {
            "stopped"
        },
        summary.shards_done,
        summary.shards_total,
        summary.files_ok,
        summary.files_torn,
        summary.files_failed,
        summary.bytes_moved,
    );
    if summary.files_failed > 0 {
        println!(
            "per-file failures recorded under {}",
            wd.failures_dir().display()
        );
    }
    Ok(summary)
}

fn hostname() -> String {
    std::fs::read_to_string("/proc/sys/kernel/hostname")
        .map(|s| s.trim().to_string())
        .ok()
        .filter(|s| !s.is_empty())
        .unwrap_or_else(|| "mongoose".to_string())
}

fn spawn_signal_listener(stop: CancellationToken) {
    tokio::spawn(async move {
        let mut term =
            match tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate()) {
                Ok(s) => s,
                Err(e) => {
                    tracing::error!(error = ?e, "cannot install SIGTERM handler");
                    return;
                }
            };
        tokio::select! {
            _ = tokio::signal::ctrl_c() => {}
            _ = term.recv() => {}
        }
        tracing::info!("stop requested; finishing the batch in flight, then leaving the shard");
        stop.cancel();
        // Further signals: log and keep going; the batch always
        // finishes (SIGKILL is the escape hatch).
        loop {
            tokio::select! {
                _ = tokio::signal::ctrl_c() => {}
                _ = term.recv() => {}
            }
            tracing::warn!(
                "already stopping; the batch in flight will finish (use SIGKILL to abandon it)"
            );
        }
    });
}

fn spawn_ticker(
    live: Arc<LivePending>,
    throughput: ThroughputCounter,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        let mut tick = tokio::time::interval(std::time::Duration::from_secs(TICK_SECS));
        tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        tick.tick().await; // immediate first tick carries no info
        loop {
            tick.tick().await;
            use std::sync::atomic::Ordering::Relaxed;
            tracing::info!(
                shard_rows_done = live.rows_done.load(Relaxed),
                inflight = live.inflight(),
                files_ok = live.files_ok.load(Relaxed),
                files_failed = live.files_failed.load(Relaxed),
                mb_s_1m = format!("{:.1}", throughput.sample_mb_s(60)),
                "copying",
            );
        }
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::manifest::{LocalShard, MANIFEST_FORMAT_VERSION};
    use migration_core::records::{Endpoint, EndpointKind, MigrationOptions};

    fn tuning() -> CopyTuning {
        CopyTuning {
            nfs_connections: 24,
            inflight_small: 128,
            inflight_medium: 8,
            inflight_large: 2,
            use_raw_fh: true,
            direct_commit: false,
            bucketed_async: false,
            rpc_timeout_ms: 42_000,
        }
    }

    fn local_manifest() -> LocalManifest {
        LocalManifest {
            format_version: MANIFEST_FORMAT_VERSION,
            run_id: "run-t".into(),
            created_utc: "2026-08-31T00:00:00Z".into(),
            source: Endpoint {
                kind: EndpointKind::Nfs,
                url: "nfs://s/e".into(),
                root: "/data".into(),
            },
            dest: Endpoint {
                kind: EndpointKind::Nfs,
                url: "nfs://d/e".into(),
                root: "/copy".into(),
            },
            options: MigrationOptions::default(),
            shards: vec![LocalShard {
                path: "canonical/part-0000.parquet".into(),
                rows: 1,
                bytes: 1,
                sha256: "x".into(),
            }],
            total_rows: 1,
            total_bytes: 1,
        }
    }

    #[test]
    fn mover_params_project_manifest_and_tuning() {
        let p = mover_params(&local_manifest(), &tuning(), true, "h".into());
        assert_eq!(p.source_url, "nfs://s/e");
        assert_eq!(p.dest_url, "nfs://d/e");
        assert_eq!(p.source_root, "/data");
        assert_eq!(p.dest_root, "/copy");
        assert_eq!(p.nfs_connections, 24);
        assert!(p.use_raw_fh);
        assert!(!p.direct_commit);
        assert!(!p.use_bucketed_pool);
        assert_eq!(p.rpc_timeout_ms, 42_000);
        assert!(p.require_chown);
        assert!(!p.require_unchanged_size, "walker size stays advisory");
        assert_eq!(p.inflight.small, 128);
        assert_eq!(p.inflight.medium, 8);
        assert_eq!(p.inflight.large, 2);
        // Stripe knobs keep the engine defaults.
        assert_eq!(
            p.inflight.large_stripe_size,
            InflightProfile::default().large_stripe_size
        );

        // Projection through the shared factory keeps the same values.
        let cfg = mover_factory::mover_config(&p);
        assert_eq!(cfg.source_root, "/data");
        assert!(cfg.use_raw_fh);
        assert_eq!(cfg.inflight.small, 128);
    }

    #[tokio::test]
    async fn copy_without_a_manifest_names_prepare() {
        let dir = tempfile::tempdir().unwrap();
        let err = run(dir.path(), &tuning()).await.unwrap_err();
        assert!(format!("{err:#}").contains("mongoose prepare"), "{err:#}");
    }

    #[tokio::test]
    async fn copy_rejects_an_overlapping_manifest() {
        let dir = tempfile::tempdir().unwrap();
        let wd = WorkDir::new(dir.path());
        let mut m = local_manifest();
        m.source.url = "nfs://h/export".into();
        m.dest.url = "nfs://h/export".into();
        m.source.root = "/".into();
        m.dest.root = "/dst".into();
        crate::util::write_json_atomic(&wd.manifest_json(), &m).unwrap();
        let err = run(dir.path(), &tuning()).await.unwrap_err();
        assert!(format!("{err:#}").contains("overlap"), "{err:#}");
    }
}
