//! Worker orchestrator — top-level loop for the M1 worker.
//!
//! Sequence:
//!
//! 1. Load manifest, verify format version.
//! 2. Build the mover (no-op data path in M1) and a shared
//!    `ProgressState`.
//! 3. Spawn the heartbeat task.
//! 4. Reconcile any self-owned claims left behind by a previous run
//!    (logged, not resumed in M1 — see DESIGN.md "Future work:
//!    resume-after-restart").
//! 5. Loop:
//!    a. Scan `shards/` for a claimable shard (free or stale-leased).
//!    b. Try to acquire via `If-None-Match: *` or reclaim via
//!       `If-Match: <stale-etag>`.
//!    c. Download the parquet index shard to local scratch.
//!    d. Run the shard processor; update HeldClaim + ProgressState.
//!    e. On clean completion: mark the claim `Completed`.
//!    f. On fence trip: leave the claim where it is and exit.
//! 6. Exit cleanly when every shard is Completed/Failed or the worker
//!    is fenced.

use crate::backpressure::Backpressure;
use crate::caps;
use crate::config::Config;
use crate::coord_driver::{self, CoordDriverHandle, DriverInputs};
use crate::heartbeat::{HeartbeatTask, HeldClaim, ProgressState};
use crate::run_control::RunControlReader;
use crate::shard_processor::{ProcessOutcome, ShardProcessor};
use crate::throughput::ThroughputCounter;

use migration_core::claim::{
    self, AcquireOutcome, ClaimStore, CompleteOutcome, FailOutcome, ListEntry, ReclaimOutcome,
};
use migration_core::fence::Fence;
use migration_core::layout;
use migration_core::overlap;
use migration_core::records::{
    ClaimRecord, ClaimState, Manifest, MigrationOptions, ProgressRecord, ServerSideCopy,
    ShardEntry, RUN_FORMAT_VERSION,
};
use migration_core::s3::S3Client;
use migration_core::time::UtcTime;
use migration_mover::batch::{BatchBudget, InflightLimiter, InflightProfile};
use migration_mover::{
    AsyncBucketedFileMover, BucketedAsyncPool, DowngradeSink, FailureSink, FileMover,
    LibnfsContextPool, Mover, MoverConfig, MultiPool,
};

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{Mutex, RwLock};
use tokio_util::sync::CancellationToken;

pub async fn run(cfg: Config, host_id: String) -> anyhow::Result<()> {
    // Worker-process start time. Captured before any awaits so the
    // value reflects the actual process boot, not the first config
    // I/O — coord-side register dedup pairs this with (host, pid)
    // to identify the worker instance across restarts.
    let process_start = chrono::Utc::now();
    let pid = std::process::id();

    // ---- 1. S3 client + manifest -----------------------------------
    let s3 = S3Client::from_config(
        &cfg.run.endpoint,
        &cfg.run.region,
        &cfg.run.bucket,
        cfg.run.profile.as_deref(),
        cfg.run.verify_tls,
    )
    .await?;
    let s3 = Arc::new(s3);

    // Bucket-versioning guard. The v2 claim protocol depends on
    // `DELETE If-Match` actually removing the object — under bucket
    // versioning a DELETE creates a delete marker instead, and the
    // following `PUT If-None-Match: *` can race the marker into
    // surprising 412s. Refuse to start on Enabled / Suspended;
    // warn-only on probe failure (operator may have restricted the
    // GetBucketVersioning IAM action — that's a separate decision).
    match s3.get_bucket_versioning().await {
        Ok(migration_core::s3::BucketVersioning::NotEnabled) => {
            tracing::info!(
                bucket = %s3.bucket(),
                "bucket versioning is off (required for v2 claim protocol)",
            );
        }
        Ok(state) => {
            anyhow::bail!(
                "bucket {} has versioning state {:?}; the v2 claim protocol \
                 requires versioning OFF. Disable versioning on the bucket \
                 (and clear any existing non-current versions / delete markers) \
                 before running. See docs/CLAIM_PROTOCOL.md.",
                s3.bucket(),
                state,
            );
        }
        Err(e) => {
            tracing::warn!(
                bucket = %s3.bucket(),
                error = ?e,
                "could not verify bucket versioning state — proceeding; \
                 operator must confirm versioning is OFF (see docs/CLAIM_PROTOCOL.md)",
            );
        }
    }

    let manifest = load_manifest(&s3).await?;
    if manifest.format_version != RUN_FORMAT_VERSION {
        anyhow::bail!(
            "manifest format_version {} does not match worker {}",
            manifest.format_version,
            RUN_FORMAT_VERSION,
        );
    }
    tracing::info!(
        run_id = %manifest.run_id,
        shards = manifest.shards.len(),
        total_rows = manifest.total_rows,
        "manifest loaded",
    );

    // Source/dest overlap guard — refuses to start before mounting
    // libnfs or claiming any shard. See BUGFIX_PLAN.md "Fix 3" and
    // `migration_core::overlap`.
    overlap::check(&manifest.source, &manifest.dest)?;

    // Local scratch.
    tokio::fs::create_dir_all(&cfg.shard.local_scratch).await?;

    // ---- 2. Capability check ---------------------------------------
    // Failing fast at startup beats burning a shard on per-file EPERM.
    let opts = effective_options(&manifest.options, &cfg);
    let cap_chown = caps::has_cap_chown();
    let require_chown = cfg.copy.require_chown_capability;
    if opts.preserve_owner && require_chown && !cap_chown {
        anyhow::bail!(
            "preserve_owner=true requires CAP_CHOWN; \
             grant the capability or set [copy].require_chown_capability=false to downgrade",
        );
    }
    if opts.preserve_owner && !cap_chown {
        tracing::warn!(
            "preserve_owner=true but CAP_CHOWN not held; running in degraded mode \
             (chown EPERM will be recorded as warnings, not failures)",
        );
    }

    // ---- 3. Mount libnfs pool + build mover -----------------------
    // M3: pre-mount cfg.mover.nfs_connections context pairs so
    // concurrent shard dispatch has distinct contexts to draw from.
    let pool_size = cfg.mover.nfs_connections.max(1) as usize;
    let pool: Arc<dyn LibnfsContextPool> =
        MultiPool::build(&manifest.source.url, &manifest.dest.url, pool_size)?;
    // Keep a clone for the end-of-run root-mtime restore (slice 3 of
    // MTIME_PARITY_FIX). `pool` itself is moved into Mover::new below.
    let pool_for_root_mtime = Arc::clone(&pool);
    tracing::info!(
        pool_size,
        src = %manifest.source.url,
        dst = %manifest.dest.url,
        "libnfs pool mounted",
    );

    let mut mover_cfg = MoverConfig::from_options(
        manifest.source.url.clone(),
        manifest.dest.url.clone(),
        manifest.source.root.clone(),
        manifest.dest.root.clone(),
        false, // same_server_v42 detection lands in M4
        &opts,
    );
    mover_cfg.require_chown = require_chown && cap_chown;
    mover_cfg.require_unchanged_size = cfg.copy.require_unchanged_size;
    // Apply the [batch].inflight_* profile so the mover and the
    // shard processor share the same view of size-class concurrency.
    mover_cfg.inflight = InflightProfile {
        small: cfg.batch.inflight_small,
        medium: cfg.batch.inflight_medium,
        large: cfg.batch.inflight_large,
        large_stripe_size: parse_size(&cfg.batch.large_stripe_size).unwrap_or(4 * 1024 * 1024),
        large_stripe_depth: cfg.batch.large_stripe_depth,
    };
    let downgrades = DowngradeSink::new();
    let failures = FailureSink::new();

    // ---- 4. Shared progress + heartbeat ----------------------------
    let progress = Arc::new(RwLock::new(ProgressState::new()));
    let current = Arc::new(Mutex::new(None::<HeldClaim>));
    let fence = Fence::new();

    // Build the file mover behind the FileMover trait so we can swap
    // between the sync path and the bucketed-async path at startup.
    // The async path additionally mounts a BucketedAsyncPool (six
    // contexts: src+dst × small/medium/large) and wraps a sync Mover
    // for the non-regular-file fallback rows (symlinks / hardlinks /
    // dirs / empty / skip) per Phase 2 Decision #3.
    let mover: Arc<dyn FileMover> = if cfg.mover.use_bucketed_pool {
        let async_pool =
            Arc::new(BucketedAsyncPool::new(&manifest.source.url, &manifest.dest.url).await?);
        tracing::info!(
            src = %manifest.source.url,
            dst = %manifest.dest.url,
            "bucketed async libnfs pool mounted (6 contexts)",
        );
        let sync_mover = Mover::new(
            mover_cfg.clone(),
            pool,
            host_id.clone(),
            downgrades.clone(),
            fence.clone(),
        );
        Arc::new(AsyncBucketedFileMover::new(
            async_pool,
            sync_mover,
            Arc::new(mover_cfg),
            fence.clone(),
            host_id.clone(),
            downgrades.clone(),
        ))
    } else {
        Arc::new(Mover::new(
            mover_cfg,
            pool,
            host_id.clone(),
            downgrades.clone(),
            fence.clone(),
        ))
    };
    let throughput = ThroughputCounter::new();
    let inflight = InflightLimiter::new(&InflightProfile {
        small: cfg.batch.inflight_small,
        medium: cfg.batch.inflight_medium,
        large: cfg.batch.inflight_large,
        large_stripe_size: 0,
        large_stripe_depth: 0,
    });

    // Bounded channel for the heartbeat task to notify the
    // coord_driver of a self-fence trip. Created here so both
    // HeartbeatTask (sender) and coord_driver (receiver) can be
    // wired with matching halves. Only allocated when [coord] is
    // configured — legacy mode passes None to both sides.
    let (coord_fence_tx, coord_fence_rx): (
        Option<tokio::sync::mpsc::Sender<String>>,
        Option<tokio::sync::mpsc::Receiver<String>>,
    ) = if cfg.coord.is_some() {
        let (tx, rx) = tokio::sync::mpsc::channel::<String>(1);
        (Some(tx), Some(rx))
    } else {
        (None, None)
    };

    // Bounded channel for the shard processor to push per-file
    // event drafts to the coord_driver. Capacity intentionally
    // generous (4096) — a bursty shard can produce ~1000 outcomes/s
    // and the drainer flushes at events_flush_sec (default 1s). On
    // overflow the processor's try_send drops the draft and the
    // coord sees lower numbers; preferable to blocking the copy
    // loop on a momentary backlog.
    let (coord_events_tx, coord_events_rx): (
        Option<tokio::sync::mpsc::Sender<crate::coord_driver::WorkerEventDraft>>,
        Option<tokio::sync::mpsc::Receiver<crate::coord_driver::WorkerEventDraft>>,
    ) = if cfg.coord.is_some() {
        let (tx, rx) = tokio::sync::mpsc::channel(4096);
        (Some(tx), Some(rx))
    } else {
        (None, None)
    };
    let event_emitter = match coord_events_tx {
        Some(tx) => crate::coord_driver::EventEmitter::from_sender(tx),
        None => crate::coord_driver::EventEmitter::disabled(),
    };

    let hb = HeartbeatTask {
        store: s3.clone() as Arc<dyn ClaimStore>,
        fence: fence.clone(),
        host_id: host_id.clone(),
        interval: Duration::from_secs(cfg.worker.heartbeat_sec),
        lease_timeout: Duration::from_secs(cfg.worker.lease_timeout_sec),
        current: current.clone(),
        progress: progress.clone(),
        throughput: throughput.clone(),
        throughput_window_secs: 60,
        coord_fence: coord_fence_tx,
    };
    let hb_handle = tokio::spawn(async move { hb.run().await });

    // ---- 4b. Coord wiring (Phase 3, optional) ----------------------
    // When [coord] is configured, spawn a background driver that
    // registers + heartbeats over HTTP. The driver writes the coord-
    // supplied control mode into a `RunControl`; the claim loop
    // below reads it to honor operator-issued pause/cancel commands
    // without touching the claim-protocol primitives.
    //
    // When [coord] is absent, the worker runs in legacy S3-only
    // mode — `run_control_reader` stays None and the claim loop is
    // bit-for-bit unchanged.
    let coord_cancel = CancellationToken::new();
    let coord_handle: Option<CoordDriverHandle> = match cfg.coord.as_ref() {
        Some(c) => {
            let driver_inputs = DriverInputs {
                progress: progress.clone(),
                throughput: throughput.clone(),
                fence: fence.clone(),
                fence_rx: coord_fence_rx,
                events_rx: coord_events_rx,
            };
            match coord_driver::spawn(
                c,
                host_id.clone(),
                pid,
                process_start,
                env!("CARGO_PKG_VERSION").to_string(),
                driver_inputs,
                coord_cancel.clone(),
            ) {
                Ok(h) => {
                    tracing::info!(coord_url = %c.url, job_id = %c.job_id,
                        "coord driver spawned");
                    Some(h)
                }
                Err(e) => {
                    // Fast-fail at startup on config bugs (bad URL,
                    // missing secret env). The worker should not
                    // keep running half-configured.
                    anyhow::bail!("coord driver spawn failed: {e}");
                }
            }
        }
        None => None,
    };
    let mut run_control_reader: Option<RunControlReader> =
        coord_handle.as_ref().map(|h| h.run_control.subscribe());

    // ---- 5. Reclaim self-owned claims from a previous run ----------
    // On a fast worker restart (host_id reuse), any claims we held
    // are still Active on S3 with our host_id and the previous
    // run's etag. Reclaiming them ourselves is race-free (we're
    // the same host_id) and skips the wait for the cross-check
    // staleness window or the lease.
    let self_reclaim_queue: std::collections::VecDeque<(String, String, u64)> =
        reclaim_self_owned_claims(&*s3, &host_id)
            .await
            .unwrap_or_else(|e| {
                tracing::warn!(error = ?e, "self-claim reclaim scan failed (continuing)");
                Vec::new()
            })
            .into_iter()
            .collect();
    let mut self_reclaim_queue = self_reclaim_queue;

    // ---- 6. Main shard loop ----------------------------------------
    let lease = Duration::from_secs(cfg.worker.lease_timeout_sec);
    let budget = parse_batch_budget(&cfg).unwrap_or_default();
    let mut backpressure = Backpressure::new(
        cfg.backpressure.failure_pct_threshold,
        cfg.backpressure.throughput_floor_mb_s,
    );
    // Cross-pass claim-body cache. See `ClaimBodyCache` doc.
    let mut claim_body_cache: ClaimBodyCache = HashMap::new();

    loop {
        if !fence.is_valid() {
            tracing::warn!(reason = ?fence.reason(), "worker fenced; exiting main loop");
            break;
        }

        // Coord-driven run control. Honored AFTER the fence check
        // so safety invariants always win — a tripped fence exits
        // immediately regardless of operator pause/cancel intent.
        //
        // Drain / Cancel break the loop with no new claim — there's
        // nothing in flight here at the top of the loop, so the
        // exit path is identical to a clean "all shards terminal"
        // finish below. The R-rules are unaffected: no claim is
        // touched, no batch is interrupted mid-commit.
        //
        // Pause blocks the loop on a `watch::Receiver::changed()` —
        // O(1) wake on resume, no busy poll. After wake, `continue`
        // restarts the loop from the top so the fence check and the
        // run-control check both re-evaluate (fence may have tripped
        // during the wait; the new mode may be Cancel).
        if let Some(rc) = run_control_reader.as_mut() {
            if rc.is_terminating() {
                tracing::info!(mode = ?rc.mode(),
                    "coord requested termination; exiting main loop");
                break;
            }
            if rc.is_paused() {
                tracing::info!("coord requested pause; waiting for resume");
                let after = rc.wait_while_paused().await;
                tracing::info!(?after, "coord pause released");
                continue;
            }
        }

        // Backpressure gate per DESIGN.md "Backpressure": if the last
        // shard ended in poor shape, don't pile onto a struggling
        // dest. Sleep one heartbeat interval and re-check.
        if let Some(reason) = backpressure.degraded() {
            {
                let mut p = progress.write().await;
                p.status = format!("degraded:{}", reason.as_str());
            }
            tracing::warn!(
                reason = reason.as_str(),
                last_failure_pct = backpressure.last_failure_pct(),
                last_throughput_mb_s = backpressure.last_throughput_mb_s(),
                "worker degraded; sleeping before next claim",
            );
            tokio::time::sleep(Duration::from_secs(cfg.worker.heartbeat_sec)).await;
            // Don't continue around — keep evaluating, but don't spin
            // claims if still degraded after sleep.
            continue;
        }

        // Self-restart queue drains first — these are shards we
        // reclaimed at startup from a previous run. They're already
        // Active under our (new) etag; skip scan_shards for them
        // and go straight to processing.
        let from_self_reclaim = self_reclaim_queue.pop_front();
        let scan_or_self: ScanResult = if let Some((shard, etag, epoch)) = from_self_reclaim {
            tracing::info!(
                shard = %shard,
                epoch,
                "processing self-reclaimed orphan from previous run",
            );
            ScanResult {
                next_target: Some(ClaimTarget::AlreadyReclaimed {
                    shard: shard.clone(),
                    etag,
                    epoch,
                }),
                all_terminal: false,
                last_target_filename: shard,
            }
        } else {
            scan_shards(&*s3, &manifest, lease, &mut claim_body_cache).await?
        };
        let scan = scan_or_self;
        if scan.all_terminal {
            tracing::info!("all shards terminal; worker exiting");
            // Slice 3 of MTIME_PARITY_FIX: walker doesn't emit a row
            // for the migration root, so the per-shard DirAttrs path
            // never touches `dest.root` — yet every file commit inside
            // it bumps its mtime. Source-stat the root once at
            // shutdown and apply the captured (atime, mtime) to the
            // dest. Idempotent across workers because the value comes
            // from source. Failures are warnings, not fatal — the
            // bytes are already durable.
            let src_root_bytes = manifest.source.root.as_bytes().to_vec();
            let dst_root_bytes = manifest.dest.root.as_bytes().to_vec();
            if let Err(e) = migration_mover::restore_root_mtime(
                Arc::clone(&pool_for_root_mtime),
                &src_root_bytes,
                &dst_root_bytes,
            )
            .await
            {
                tracing::warn!(
                    error = ?e,
                    src_root = %manifest.source.root,
                    dst_root = %manifest.dest.root,
                    "end-of-run root-dir mtime restore failed (non-fatal)",
                );
            } else {
                tracing::info!(
                    src_root = %manifest.source.root,
                    dst_root = %manifest.dest.root,
                    "end-of-run root-dir mtime restored",
                );
            }
            break;
        }
        let Some(target) = scan.next_target else {
            // Everything's Active and live with someone else. Idle a
            // bit and re-scan; this is a soft backoff so we don't burn
            // S3 LIST quota.
            tracing::debug!("no claimable shards this pass; idling");
            tokio::time::sleep(Duration::from_secs(cfg.worker.heartbeat_sec)).await;
            continue;
        };

        let (etag, record) = match target {
            ClaimTarget::Free { shard } => {
                match claim::try_acquire(&*s3, &shard, &host_id).await? {
                    AcquireOutcome::Acquired { etag, record } => (etag, record),
                    AcquireOutcome::Contended { existing, .. } => {
                        // Thundering-herd dampener: when many workers race
                        // the same Free/Stale shard, all losers hit this
                        // path simultaneously, and a bare `continue` would
                        // immediately re-issue scan_shards (LIST + per-Active
                        // GETs). Backoff with jitter spreads them out so the
                        // S3 LIST/GET storm doesn't pile up.
                        backoff_after_lost_race(cfg.worker.heartbeat_sec).await;
                        tracing::debug!(
                            existing_host = %existing.host,
                            "shard contended; backing off before next scan",
                        );
                        continue;
                    }
                }
            }
            ClaimTarget::Stale {
                shard,
                stale_etag,
                prior_epoch,
            } => {
                let new_epoch = prior_epoch + 1;
                match claim::reclaim(&*s3, &shard, &stale_etag, &host_id, new_epoch).await? {
                    ReclaimOutcome::Won { etag, record } => (etag, record),
                    ReclaimOutcome::LostRace => {
                        backoff_after_lost_race(cfg.worker.heartbeat_sec).await;
                        tracing::debug!(
                            shard = %shard,
                            "reclaim lost race; backing off before next scan",
                        );
                        continue;
                    }
                }
            }
            ClaimTarget::AlreadyReclaimed { shard, etag, epoch } => {
                // Synthesize a stand-in record for the downstream
                // code that expects `(etag, ClaimRecord)`. The
                // authoritative on-disk record was written by
                // `claim::reclaim` inside `reclaim_self_owned_claims`
                // with its own `claimed_utc`. This local record is
                // consumed only by `current_shard_filename` (which
                // ignores it; reads only the fallback shard name)
                // and the HeldClaim cell's `epoch` field below.
                // `claimed_utc` from this struct is NEVER used —
                // the on-disk value is what lease/cross-check
                // arithmetic reads.
                tracing::debug!(
                    %shard,
                    epoch,
                    "claiming self-reclaimed orphan (skipping acquire/reclaim)",
                );
                let record = ClaimRecord {
                    host: host_id.clone(),
                    claimed_utc: UtcTime::now(),
                    epoch,
                    state: ClaimState::Active,
                };
                (etag, record)
            }
        };

        let shard_filename = current_shard_filename(&record, &scan.last_target_filename);
        tracing::info!(shard = %shard_filename, etag = %etag, "shard claimed");

        // Update shared state so the heartbeat reflects the new shard.
        {
            let mut g = current.lock().await;
            *g = Some(HeldClaim {
                shard: shard_filename.clone(),
                etag: etag.clone(),
                epoch: record.epoch,
            });
            let mut p = progress.write().await;
            p.current_shard = Some(shard_filename.clone());
            p.shard_rows_total = 0;
            p.shard_rows_done = 0;
            p.shard_bytes_done = 0;
            p.status = "active".into();
        }

        // Download the parquet shard to scratch.
        let scratch = cfg.shard.local_scratch.join(&shard_filename);
        let download_etag = s3
            .download_to(&layout::index_key(&shard_filename), &scratch)
            .await?;
        verify_shard_etag(&manifest, &shard_filename, &download_etag)?;

        // Stamp the current shard onto both sinks so records carry
        // the right shard name (the mover doesn't otherwise know).
        downgrades.set_current_shard(shard_filename.clone());
        failures.set_current_shard(shard_filename.clone());

        // M3: spawn a fresh processor with the worker's shared
        // limiter/sinks/throughput so all shards report into one
        // throughput counter and one failure log per host.
        let mut processor = ShardProcessor {
            mover: Arc::clone(&mover),
            fence: fence.clone(),
            budget,
            inflight: inflight.clone(),
            failures: failures.clone(),
            throughput: throughput.clone(),
            fsid_fallback_warned: false,
            emitter: event_emitter.clone(),
        };
        let outcome = match processor.process(&scratch).await {
            Ok(o) => o,
            Err(e) => {
                // Shard-fatal: the parquet won't decode, or a row
                // schema is malformed. A peer reclaiming after lease
                // expiry would just hit the same error → infinite
                // fleet-wide reclaim loop. Mark the claim `Failed`
                // (terminal, scanners skip it) and continue to the
                // next shard. Operator inspects the error log + the
                // Failed claim record and re-uploads / re-indexes.
                tracing::error!(
                    error = ?e,
                    shard = %shard_filename,
                    "shard-fatal error; marking claim Failed and moving on",
                );
                // Best-effort scratch cleanup before we give up on
                // this shard.
                if let Err(rm) = tokio::fs::remove_file(&scratch).await {
                    tracing::warn!(
                        error = ?rm,
                        scratch = %scratch.display(),
                        "scratch cleanup failed (post-shard-fatal)",
                    );
                }
                // Same R4 reasoning as the complete() path: clear the
                // held-claim cell BEFORE calling fail() so the
                // heartbeat doesn't fence us on the transient absent
                // window between DELETE and PUT.
                let (fail_etag, fail_epoch) = {
                    let mut g = current.lock().await;
                    let (e, ep) = match g.as_ref() {
                        Some(c) if c.shard == shard_filename => (c.etag.clone(), c.epoch),
                        _ => (etag.clone(), record.epoch),
                    };
                    *g = None;
                    (e, ep)
                };
                match claim::fail(&*s3, &shard_filename, &fail_etag, &host_id, fail_epoch).await {
                    Ok(FailOutcome::Failed { .. }) => {
                        tracing::warn!(
                            shard = %shard_filename,
                            "claim marked Failed (terminal); requires operator follow-up",
                        );
                    }
                    Ok(FailOutcome::Lost) => {
                        // Another worker took over while we were
                        // processing — they'll hit the same error and
                        // mark Failed themselves. Drop the claim
                        // cleanly here.
                        tracing::warn!(
                            shard = %shard_filename,
                            "claim lost while marking Failed; new owner will retry-then-fail",
                        );
                    }
                    Err(write_err) => {
                        tracing::warn!(
                            error = ?write_err,
                            shard = %shard_filename,
                            "claim Failed write errored; shard will remain Active until lease expiry",
                        );
                    }
                }
                continue;
            }
        };

        // Push final per-shard counters into the shared progress.
        {
            let mut p = progress.write().await;
            p.shard_rows_total = outcome.rows_total;
            p.shard_rows_done = outcome.files_ok + outcome.files_failed;
            p.shard_bytes_done = p.shard_bytes_done.saturating_add(outcome.bytes_moved);
            p.files_ok = p.files_ok.saturating_add(outcome.files_ok);
            p.files_failed = p.files_failed.saturating_add(outcome.files_failed);
            p.files_fenced = p.files_fenced.saturating_add(outcome.files_fenced);
        }

        // Flush downgrade + failure JSONL produced this shard. PUTs
        // are unconditional (one object per host, one PUT per
        // non-empty shard); the aggregator stitches the JSONL back
        // together. Shard names on each record were stamped at the
        // start of this shard.
        let drained = downgrades.drain_jsonl();
        downgrades.set_current_shard("");
        if !drained.is_empty() {
            let key = layout::downgrades_key(&host_id);
            if let Err(e) = s3.put(&key, drained).await {
                tracing::warn!(
                    error = ?e,
                    shard = %shard_filename,
                    "downgrade flush failed (records lost; copy itself succeeded)",
                );
            }
        }
        let drained_failures = failures.drain_jsonl();
        failures.set_current_shard("");
        if !drained_failures.is_empty() {
            let key = layout::failures_key(&host_id);
            if let Err(e) = s3.put(&key, drained_failures).await {
                tracing::warn!(
                    error = ?e,
                    shard = %shard_filename,
                    "failure flush failed (failure records lost)",
                );
            }
        }

        // Update the backpressure gate with this shard's stats and
        // the latest throughput sample. The throughput sample here
        // races slightly with the heartbeat, but both pull from the
        // same atomic counter so values are at most one sample apart.
        let throughput_now = throughput.sample_mb_s(60);
        backpressure.update(outcome.files_ok, outcome.files_failed, throughput_now);

        // Tear down the local scratch copy as soon as we're done.
        if let Err(e) = tokio::fs::remove_file(&scratch).await {
            tracing::warn!(error = ?e, scratch = %scratch.display(), "scratch cleanup failed");
        }

        if outcome.fenced {
            tracing::warn!(shard = %shard_filename, "shard processing fenced");
            // Don't mark complete — leave the claim where it is so the
            // next worker can reclaim after lease timeout.
            break;
        }

        // R4 fix: snapshot the final etag/epoch AND clear the held-claim
        // cell in a single lock-held block, BEFORE calling complete().
        //
        // complete() is two HTTP ops (DELETE If-Match + PUT If-None-Match).
        // While those are in flight the claim object on S3 is transiently
        // gone, then re-appears with a new etag. If `current` still held
        // the old etag during that window, the heartbeat task's HEAD
        // would see 404-then-new-etag, both of which trip
        // `RefreshOutcome::Lost` and fence the worker — a spurious
        // self-fence on a perfectly clean shard completion. Clearing
        // `current` first means heartbeat sees `None`, skips HEAD, and
        // just writes progress.
        let (final_etag, final_epoch) = {
            let mut g = current.lock().await;
            let (e, ep) = match g.as_ref() {
                Some(c) if c.shard == shard_filename => (c.etag.clone(), c.epoch),
                _ => (etag.clone(), record.epoch),
            };
            *g = None;
            (e, ep)
        };

        match claim::complete(&*s3, &shard_filename, &final_etag, &host_id, final_epoch).await {
            Ok(CompleteOutcome::Completed { .. }) => { /* terminal-state written */ }
            Ok(CompleteOutcome::Lost) => {
                // Claim was reclaimed at some point during this shard
                // — treat as "we don't hold it anymore", same as a
                // mid-shard fence trip from the operator's POV. The
                // shard processor's row-level fence checks should have
                // already prevented further writes; here we just
                // surface the fact and stop trying to finalize.
                tracing::warn!(
                    shard = %shard_filename,
                    "claim lost during shard; not marking complete (another worker now owns it)",
                );
            }
            Err(e) => {
                tracing::warn!(error = ?e, shard = %shard_filename, "claim complete write failed");
            }
        }
    }

    // ---- 7. Shutdown -----------------------------------------------
    eprintln!("[shutdown] section 7 entered");
    {
        eprintln!("[shutdown] acquiring progress write lock");
        let mut p = progress.write().await;
        eprintln!("[shutdown] got progress write lock");
        p.status = "exiting".into();
    }
    eprintln!("[shutdown] released progress write lock");
    // Cancel the coord driver so it stops heartbeating and the
    // task joins. Bounded await — if the HTTP layer is wedged the
    // worker should still get to clean exit; the driver leaks at
    // process termination, which is benign (no shared resources).
    if let Some(handle) = coord_handle {
        coord_cancel.cancel();
        eprintln!("[shutdown] awaiting coord_driver with 5s timeout");
        match tokio::time::timeout(std::time::Duration::from_secs(5), handle.task).await {
            Ok(Ok(Ok(()))) => tracing::info!("coord_driver: clean exit"),
            Ok(Ok(Err(e))) => tracing::warn!(error = %e, "coord_driver returned error"),
            Ok(Err(e)) => tracing::warn!(join_error = %e, "coord_driver join failed"),
            Err(_) => tracing::warn!("coord_driver did not exit within 5s; dropping handle"),
        }
        eprintln!("[shutdown] coord_driver done");
    }
    // Drop the held claim so the heartbeat stops refreshing.
    {
        eprintln!("[shutdown] acquiring current lock");
        let mut g = current.lock().await;
        eprintln!("[shutdown] got current lock");
        *g = None;
    }
    eprintln!("[shutdown] released current lock");
    // Best-effort: trip fence to wake the heartbeat loop out of its tick.
    fence.trip("worker shutting down");
    eprintln!("[shutdown] fence tripped");
    // Bound the heartbeat-join. If the heartbeat task is wedged (e.g.
    // a stale S3 connection-pool entry blocking write_progress),
    // hb_handle.await would hang the worker process forever. Abort on
    // timeout; the runtime drop reaps the task on its own schedule.
    let mut hb_handle = hb_handle;
    eprintln!("[shutdown] awaiting hb_handle with 5s timeout");
    if tokio::time::timeout(std::time::Duration::from_secs(5), &mut hb_handle)
        .await
        .is_err()
    {
        eprintln!("[shutdown] hb_handle timeout - aborting");
        tracing::warn!("heartbeat task did not exit within 5s of shutdown; aborting it",);
        hb_handle.abort();
    }
    eprintln!("[shutdown] hb_handle done");

    // Hard-exit deadline. libnfs's nfs_destroy_context (called from
    // NfsContext::Drop) can block indefinitely on RPC traffic after
    // a long SIGSTOP/SIGCONT cycle wedges the underlying TCP socket.
    // All durable state has been committed to S3 by this point — the
    // claim has been released, the heartbeat has stopped, the shard
    // processor has terminated — so it's safe to bypass Drop chains
    // if they don't complete promptly. Use a std::thread (not a tokio
    // task) because the tokio runtime itself is what we're trying to
    // get past; tokio task scheduling can't help during runtime
    // shutdown when a Drop is blocking the executor.
    eprintln!("[shutdown] spawning hard-exit watchdog");
    std::thread::spawn(|| {
        std::thread::sleep(std::time::Duration::from_secs(5));
        // Direct kernel syscalls — bypass Rust stdio (which can be
        // buffered or wedged during shutdown) and std::process::exit
        // (which runs atexit handlers that may touch the same C
        // library state that's hanging us). _exit(2) terminates the
        // process at kernel level immediately.
        let msg: &[u8] = b"watchdog: forcing process exit (shutdown took >5s)\n";
        unsafe {
            libc::write(2, msg.as_ptr() as *const libc::c_void, msg.len());
            libc::_exit(0);
        }
    });
    eprintln!("[shutdown] watchdog spawned, returning Ok(())");

    Ok(())
}

// =============================================================================
// Manifest + shard discovery helpers
// =============================================================================

async fn load_manifest(s3: &S3Client) -> anyhow::Result<Manifest> {
    let (body, _etag) = s3
        .get(layout::MANIFEST_KEY)
        .await?
        .ok_or_else(|| anyhow::anyhow!("manifest.json not found in bucket {}", s3.bucket()))?;
    let m: Manifest = serde_json::from_slice(&body)?;
    Ok(m)
}

/// Effective copy options = manifest defaults overlaid with worker
/// config. Worker config wins because operators sometimes need to
/// disable e.g. xattr preservation per-host without re-uploading the
/// manifest.
fn effective_options(manifest_opts: &MigrationOptions, cfg: &Config) -> MigrationOptions {
    MigrationOptions {
        preserve_owner: cfg.copy.preserve_owner && manifest_opts.preserve_owner,
        preserve_mode: cfg.copy.preserve_mode && manifest_opts.preserve_mode,
        preserve_times: cfg.copy.preserve_times && manifest_opts.preserve_times,
        preserve_xattr: cfg.copy.preserve_xattr && manifest_opts.preserve_xattr,
        server_side_copy: parse_ssc(&cfg.copy.server_side_copy)
            .unwrap_or(manifest_opts.server_side_copy),
    }
}

fn parse_ssc(s: &str) -> Option<ServerSideCopy> {
    match s {
        "auto" => Some(ServerSideCopy::Auto),
        "force" => Some(ServerSideCopy::Force),
        "off" => Some(ServerSideCopy::Off),
        _ => None,
    }
}

fn verify_shard_etag(
    manifest: &Manifest,
    shard_filename: &str,
    actual_etag: &str,
) -> anyhow::Result<()> {
    let expected = manifest
        .shards
        .iter()
        .find(|s| key_basename(&s.key) == shard_filename)
        .ok_or_else(|| anyhow::anyhow!("shard {shard_filename} not in manifest"))?;
    if expected.etag.is_empty() || actual_etag.is_empty() {
        // Best-effort: some test fixtures don't fill etags.
        return Ok(());
    }
    if expected.etag != actual_etag {
        return Err(migration_core::Error::ManifestChanged {
            expected: expected.etag.clone(),
            actual: actual_etag.to_string(),
        }
        .into());
    }
    Ok(())
}

fn key_basename(k: &str) -> &str {
    k.rsplit('/').next().unwrap_or(k)
}

#[derive(Debug)]
enum ClaimTarget {
    Free {
        shard: String,
    },
    Stale {
        shard: String,
        stale_etag: String,
        prior_epoch: u64,
    },
    /// Reclaimed at startup from a previous run with the same host_id
    /// (self-restart shortcut). The shard is already ours; the
    /// orchestrator skips try_acquire/reclaim and goes straight to
    /// processing. See `reclaim_self_owned_claims`.
    AlreadyReclaimed {
        shard: String,
        etag: String,
        epoch: u64,
    },
}

#[derive(Debug, Default)]
struct ScanResult {
    next_target: Option<ClaimTarget>,
    /// True iff every shard in the manifest is in a terminal state
    /// (Completed or Failed). Worker exits when this flips.
    all_terminal: bool,
    last_target_filename: String,
}

/// Cross-pass claim-body cache. Keyed by claim key (e.g.
/// `shards/part-0042.parquet.claim`); value is `(LIST etag,
/// parsed body)`. A scan only re-GETs a claim object when the LIST
/// response carries a new etag for it (which can only happen when
/// another worker has reclaimed / completed / failed it). In steady
/// state — all claims still owned and unchanged — this drops
/// per-Active-claim GETs to zero; scan cost becomes pure LIST plus
/// progress GETs (which the intra-pass cache also dedupes).
pub(crate) type ClaimBodyCache = HashMap<String, (String, ClaimRecord)>;

/// Intra-pass progress cache. Built fresh inside each scan pass.
/// Lets M Active claims owned by N hosts cost N progress GETs
/// instead of M.
#[derive(Debug, Clone)]
enum ProgressFetch {
    /// `progress/host-<id>.json` was present and we have its body.
    Hit(Vec<u8>),
    /// 404 — the owner never wrote progress (or it was deleted).
    /// `check_progress_liveness` treats this as eligible-for-reclaim.
    Miss,
    /// S3 erroring out on us. Defer to the lease path; don't
    /// fast-reclaim on the basis of a failed read.
    Error,
}

async fn scan_shards(
    store: &dyn ClaimStore,
    manifest: &Manifest,
    lease: Duration,
    claim_body_cache: &mut ClaimBodyCache,
) -> anyhow::Result<ScanResult> {
    let entries = store.list(layout::SHARDS_PREFIX).await?;
    let by_key: HashMap<String, &ListEntry> = entries.iter().map(|e| (e.key.clone(), e)).collect();

    // GC: drop cache entries for keys no longer present in LIST.
    // Without this, completed shards' bodies would linger
    // forever in the cache. The worker is long-lived; the manifest
    // shard set is bounded, but operator re-runs could rename
    // shards, and we don't want stale bodies surviving across
    // those.
    claim_body_cache.retain(|k, _| by_key.contains_key(k));

    // Intra-pass progress cache. Built fresh each scan because
    // heartbeat_utc must be re-read every iteration to be useful.
    let mut progress_cache: HashMap<String, ProgressFetch> = HashMap::new();

    let now = chrono::Utc::now();
    let mut all_terminal = true;
    let mut next: Option<ClaimTarget> = None;
    let mut next_name = String::new();

    for shard in &manifest.shards {
        let shard_filename = key_basename(&shard.key).to_string();
        let claim_key = layout::claim_key(&shard_filename);

        let entry = by_key.get(claim_key.as_str()).copied();

        match entry {
            None => {
                all_terminal = false;
                if next.is_none() {
                    next = Some(ClaimTarget::Free {
                        shard: shard_filename.clone(),
                    });
                    next_name = shard_filename;
                }
            }
            Some(e) => {
                // Cache lookup: skip the GET when LIST etag matches
                // what we already have parsed.
                let record = match claim_body_cache.get(&claim_key) {
                    Some((cached_etag, cached_record)) if cached_etag == &e.etag => {
                        cached_record.clone()
                    }
                    _ => {
                        let Some((body, _)) = store.get(&claim_key).await? else {
                            all_terminal = false;
                            if next.is_none() {
                                next = Some(ClaimTarget::Free {
                                    shard: shard_filename.clone(),
                                });
                                next_name = shard_filename;
                            }
                            claim_body_cache.remove(&claim_key);
                            continue;
                        };
                        let Ok(record) = serde_json::from_slice::<ClaimRecord>(&body) else {
                            // Unparseable body: log-and-skip would let
                            // the worker declare `all_terminal=true` and
                            // exit prematurely with an unprocessed
                            // shard sitting on S3. Force the worker to
                            // keep running so an operator notices the
                            // ERROR line and can intervene (delete the
                            // corrupt claim object → next scan sees the
                            // shard as Free).
                            tracing::error!(
                                claim = %claim_key,
                                "unparseable claim record; operator intervention required \
                                 (worker will not exit while this persists)",
                            );
                            all_terminal = false;
                            claim_body_cache.remove(&claim_key);
                            continue;
                        };
                        claim_body_cache
                            .insert(claim_key.clone(), (e.etag.clone(), record.clone()));
                        record
                    }
                };
                match record.state {
                    ClaimState::Completed | ClaimState::Failed => { /* terminal */ }
                    ClaimState::Active => {
                        all_terminal = false;
                        let age = now.signed_duration_since(record.claimed_utc.0);
                        let stale_by_lease = age.to_std().map(|d| d > lease).unwrap_or(false);
                        // Cross-check the per-host progress file. Lets
                        // a peer reclaim within ~2× the owner's
                        // heartbeat_sec when the owner has stopped
                        // heartbeating, instead of waiting the full
                        // lease window. See
                        // docs/work-items/PROGRESS_LIVENESS_CROSS_CHECK.md.
                        //
                        // Only fetch when we'd act on the result — and
                        // dedupe by host within a single pass so M
                        // active shards owned by N hosts cost at most
                        // N progress GETs.
                        let stale_by_progress = if next.is_none() && !stale_by_lease {
                            let fetch = match progress_cache.get(&record.host).cloned() {
                                Some(cached) => cached,
                                None => {
                                    let fetched = match store
                                        .get(&layout::progress_key(&record.host))
                                        .await
                                    {
                                        Ok(Some((body, _))) => ProgressFetch::Hit(body),
                                        Ok(None) => ProgressFetch::Miss,
                                        Err(err) => {
                                            tracing::warn!(
                                                error = ?err,
                                                host = %record.host,
                                                "progress GET failed; deferring to lease check",
                                            );
                                            ProgressFetch::Error
                                        }
                                    };
                                    progress_cache.insert(record.host.clone(), fetched.clone());
                                    fetched
                                }
                            };
                            match fetch {
                                ProgressFetch::Hit(body) => {
                                    check_progress_liveness(Some(&body), &e.etag, now)
                                }
                                ProgressFetch::Miss => check_progress_liveness(None, &e.etag, now),
                                ProgressFetch::Error => false,
                            }
                        } else {
                            false
                        };
                        if (stale_by_lease || stale_by_progress) && next.is_none() {
                            next = Some(ClaimTarget::Stale {
                                shard: shard_filename.clone(),
                                stale_etag: e.etag.clone(),
                                prior_epoch: record.epoch,
                            });
                            next_name = shard_filename;
                        }
                    }
                }
            }
        }
    }

    Ok(ScanResult {
        next_target: next,
        all_terminal,
        last_target_filename: next_name,
    })
}

/// Peer-side liveness cross-check for an `Active` claim.
///
/// Returns `true` iff the per-host progress file confirms the owning
/// worker has stopped heartbeating against this specific claim — i.e.
/// the shard is fast-reclaim-eligible without waiting for the lease
/// window. The caller ORs this with `stale_by_lease`, so a `false`
/// here just means "defer to the lease check".
///
/// Pure & synchronous — the caller fetches the progress body and
/// passes it in. The S3 GET error path is its responsibility (the
/// reference implementation in `scan_shards` swallows GET errors and
/// defers to lease, which is the conservative choice).
///
/// Edges, per `PROGRESS_LIVENESS_CROSS_CHECK.md` §6:
///
/// | Progress file state                       | Returns |
/// |-------------------------------------------|---------|
/// | Absent (`body=None`)                      | `true`  (owner never started or crashed pre-tick) |
/// | Parse failure                             | `false` (don't act on garbage; defer to lease) |
/// | `heartbeat_sec == 0` (pre-cross-check)    | `false` (no calibrated freshness window) |
/// | `held_etag` is `None`                     | `true`  (writer announced "not holding this") |
/// | `held_etag` differs from current claim    | `true`  (claim has been replaced; old progress is stale → same-host_id restart safety) |
/// | etag matches; heartbeat age ≤ 2 × hb_sec  | `false` (alive) |
/// | etag matches; heartbeat age > 2 × hb_sec  | `true`  (dead) |
fn check_progress_liveness(
    progress_body: Option<&[u8]>,
    claim_etag: &str,
    now: chrono::DateTime<chrono::Utc>,
) -> bool {
    let Some(body) = progress_body else {
        // No progress object at all — owner never started, or it
        // crashed before its first tick landed. Eligible.
        return true;
    };
    let Ok(p) = serde_json::from_slice::<ProgressRecord>(body) else {
        // Garbage body — defer to lease.
        return false;
    };
    if p.heartbeat_sec == 0 {
        // Pre-cross-check progress object: no calibrated threshold.
        return false;
    }
    match p.held_etag.as_deref() {
        Some(e) if e == claim_etag => {}
        // None or mismatched etag — the progress object isn't bound
        // to this claim's ownership window (worker self-fenced or
        // same-host_id restart minted a new claim). Eligible.
        _ => return true,
    }
    let age = now.signed_duration_since(p.heartbeat_utc.0);
    let threshold_secs = (p.heartbeat_sec.saturating_mul(2)) as i64;
    age.num_seconds() > threshold_secs
}

/// Reclaim any Active claims still tagged with our `host_id` from a
/// previous run. Returns `(shard_filename, new_etag, new_epoch)` for
/// each successfully reclaimed orphan, in LIST order. The orchestrator
/// queues these and processes them before falling through to
/// `scan_shards`.
///
/// Race-free because the host_id is unique to us — no other worker can
/// observe an `Active` claim with our host_id and assume the original
/// us is still alive. (If another operator misconfigures two workers
/// with the same host_id, the loser of any race here gets `LostRace`
/// and the orphan is just skipped — same outcome as the cross-check
/// path would produce.)
async fn reclaim_self_owned_claims(
    store: &dyn ClaimStore,
    host_id: &str,
) -> anyhow::Result<Vec<(String, String, u64)>> {
    let entries = store.list(layout::SHARDS_PREFIX).await?;
    let mut reclaimed = Vec::new();
    for e in entries {
        let Some(shard) = layout::shard_from_claim_key(&e.key) else {
            continue;
        };
        let Some((body, _)) = store.get(&e.key).await? else {
            continue;
        };
        let Ok(record) = serde_json::from_slice::<ClaimRecord>(&body) else {
            continue;
        };
        if record.host != host_id || !matches!(record.state, ClaimState::Active) {
            continue;
        }
        let new_epoch = record.epoch + 1;
        match claim::reclaim(store, &shard, &e.etag, host_id, new_epoch).await {
            Ok(ReclaimOutcome::Won { etag: new_etag, .. }) => {
                tracing::info!(
                    shard = %shard,
                    prior_epoch = record.epoch,
                    new_epoch,
                    "self-restart: reclaimed orphan from previous run",
                );
                reclaimed.push((shard.to_string(), new_etag, new_epoch));
            }
            Ok(ReclaimOutcome::LostRace) => {
                tracing::warn!(
                    shard = %shard,
                    "self-restart reclaim LostRace; another worker took the orphan first",
                );
            }
            Err(err) => {
                tracing::warn!(
                    error = ?err,
                    shard = %shard,
                    "self-restart reclaim errored; orphan will be picked up via cross-check on a later iteration",
                );
            }
        }
    }
    Ok(reclaimed)
}

// `record.host` already carries our identity; this helper exists so
// callers don't need to pass the shard name twice when the acquire
// outcome and the target are still in scope together.
fn current_shard_filename(_record: &ClaimRecord, fallback: &str) -> String {
    fallback.to_string()
}

/// Parse a TOML size string like `"8 GiB"` or `"4 MiB"` into bytes.
/// Accepts decimal multipliers (KB, MB, GB, TB) and binary
/// multipliers (KiB, MiB, GiB, TiB). Returns `None` on parse error
/// so callers can fall back to a sensible default.
fn parse_size(s: &str) -> Option<u64> {
    let s = s.trim();
    let (num, unit) = match s.find(|c: char| c.is_alphabetic()) {
        Some(i) => (s[..i].trim(), s[i..].trim()),
        None => (s, ""),
    };
    let n: u64 = num.parse().ok()?;
    let mult: u64 = match unit {
        "" | "B" => 1,
        "KB" => 1_000,
        "MB" => 1_000_000,
        "GB" => 1_000_000_000,
        "TB" => 1_000_000_000_000,
        "KiB" => 1 << 10,
        "MiB" => 1 << 20,
        "GiB" => 1 << 30,
        "TiB" => 1 << 40,
        _ => return None,
    };
    n.checked_mul(mult)
}

fn parse_batch_budget(cfg: &Config) -> Option<BatchBudget> {
    let bytes = parse_size(&cfg.batch.bytes_budget)?;
    Some(BatchBudget {
        bytes,
        files: cfg.batch.files_budget,
    })
}

/// Sleep `heartbeat_sec / 4 + jitter` before the next scan iteration
/// after a `Contended` or `LostRace`. The jitter is uniform in
/// `[0, base)` so the total backoff is in `[base, 2 × base)` —
/// enough spread to break a thundering herd of M workers all
/// racing the same Free/Stale shard.
///
/// Uses `getrandom` (already a worker dep). On the practically-
/// impossible case that `getrandom` fails we fall back to a
/// pid-derived jitter so the worker still makes progress; safety
/// of the backoff doesn't depend on cryptographic randomness, just
/// on workers landing at different wall-clock instants.
async fn backoff_after_lost_race(heartbeat_sec: u64) {
    let base_ms = (heartbeat_sec * 1000) / 4;
    let base_ms = base_ms.max(100); // floor for absurd configs
    let mut buf = [0u8; 8];
    let jitter_ms = if getrandom::getrandom(&mut buf).is_ok() {
        u64::from_le_bytes(buf) % base_ms
    } else {
        (std::process::id() as u64) % base_ms
    };
    let total_ms = base_ms + jitter_ms;
    tokio::time::sleep(Duration::from_millis(total_ms)).await;
}

// Touch the imported types so cargo doesn't warn about unused names
// when the code paths above evolve.
#[allow(dead_code)]
fn _types_anchor(_o: ProcessOutcome, _u: UtcTime, _s: ShardEntry) {}

#[cfg(test)]
mod tests {
    //! Tests for `check_progress_liveness` — one per row of
    //! `docs/work-items/PROGRESS_LIVENESS_CROSS_CHECK.md` §6's safety
    //! table. The predicate is pure & sync, so each test constructs a
    //! `ProgressRecord`, serializes it to JSON, and asserts the
    //! returned boolean.
    use super::check_progress_liveness;
    use chrono::{Duration as ChronoDuration, Utc};
    use migration_core::records::ProgressRecord;
    use migration_core::time::UtcTime;

    const CLAIM_ETAG: &str = "etag-claim-abc";
    const HB_SEC: u64 = 30;

    fn record_at(heartbeat: chrono::DateTime<Utc>, held_etag: Option<&str>) -> ProgressRecord {
        ProgressRecord {
            host: "host-A".into(),
            started_utc: UtcTime::now(),
            heartbeat_utc: UtcTime(heartbeat),
            current_shard: Some("part-0001.parquet".into()),
            shard_rows_total: 0,
            shard_rows_done: 0,
            shard_bytes_done: 0,
            files_ok: 0,
            files_failed: 0,
            files_fenced: 0,
            throughput_mb_s_1m: 0.0,
            status: "active".into(),
            held_etag: held_etag.map(str::to_string),
            heartbeat_sec: HB_SEC,
        }
    }

    fn body_of(r: &ProgressRecord) -> Vec<u8> {
        serde_json::to_vec(r).unwrap()
    }

    /// Absent progress object → eligible. Owner crashed before its
    /// first tick landed, or never started; peer reclaims fast.
    #[test]
    fn absent_body_is_eligible() {
        assert!(check_progress_liveness(None, CLAIM_ETAG, Utc::now()));
    }

    /// Parse failure → conservative; defer to lease.
    #[test]
    fn unparseable_body_defers_to_lease() {
        let garbage = b"not-json-at-all";
        assert!(!check_progress_liveness(
            Some(garbage),
            CLAIM_ETAG,
            Utc::now()
        ));
    }

    /// Old-schema progress (no cross-check fields → heartbeat_sec=0
    /// via serde default) → no calibrated threshold; defer to lease.
    #[test]
    fn old_schema_progress_defers_to_lease() {
        // Hand-build a pre-cross-check progress body (no held_etag /
        // heartbeat_sec fields present). With recent heartbeat_utc to
        // rule out the freshness path firing.
        let json = format!(
            r#"{{
              "host": "host-A",
              "started_utc": "2025-01-01T00:00:00Z",
              "heartbeat_utc": "{}",
              "current_shard": null,
              "shard_rows_total": 0,
              "shard_rows_done": 0,
              "shard_bytes_done": 0,
              "files_ok": 0,
              "files_failed": 0,
              "throughput_mb_s_1m": 0.0,
              "status": "active"
            }}"#,
            Utc::now().to_rfc3339(),
        );
        assert!(!check_progress_liveness(
            Some(json.as_bytes()),
            CLAIM_ETAG,
            Utc::now()
        ));
    }

    /// `held_etag = None` means the writer announced "I am not
    /// holding any claim right now" (worker self-fenced, exiting, or
    /// between shards) → peer is eligible to reclaim.
    #[test]
    fn held_etag_none_is_eligible() {
        let r = record_at(Utc::now(), None);
        assert!(check_progress_liveness(
            Some(&body_of(&r)),
            CLAIM_ETAG,
            Utc::now()
        ));
    }

    /// Same-host_id restart: the live claim has a new etag, but the
    /// pre-restart progress object still carries the old one. The
    /// mismatch makes the shard fast-reclaim-eligible — and the
    /// post-restart progress write will carry the new etag, so the
    /// new ownership window won't ever match the dead claim.
    #[test]
    fn held_etag_mismatch_is_eligible() {
        let r = record_at(Utc::now(), Some("etag-old-from-prior-acquire"));
        assert!(check_progress_liveness(
            Some(&body_of(&r)),
            CLAIM_ETAG,
            Utc::now()
        ));
    }

    /// Owner alive, etag matches, heartbeat is fresh (< 2× hb_sec).
    /// Not stale — defer to lease.
    #[test]
    fn fresh_heartbeat_with_matching_etag_is_alive() {
        let now = Utc::now();
        let r = record_at(now - ChronoDuration::seconds(5), Some(CLAIM_ETAG));
        assert!(!check_progress_liveness(
            Some(&body_of(&r)),
            CLAIM_ETAG,
            now
        ));
    }

    /// Owner stopped heartbeating: etag matches, but heartbeat_utc is
    /// older than 2 × heartbeat_sec. Eligible for fast reclaim.
    #[test]
    fn stale_heartbeat_with_matching_etag_is_eligible() {
        let now = Utc::now();
        let age = ChronoDuration::seconds((HB_SEC * 2 + 1) as i64);
        let r = record_at(now - age, Some(CLAIM_ETAG));
        assert!(check_progress_liveness(Some(&body_of(&r)), CLAIM_ETAG, now));
    }

    /// Boundary case: heartbeat age exactly equal to the threshold is
    /// NOT stale (the predicate uses strict `>`). Documents the
    /// inclusive/exclusive semantics so a future refactor doesn't
    /// silently flip it.
    #[test]
    fn heartbeat_at_threshold_is_alive() {
        let now = Utc::now();
        let age = ChronoDuration::seconds((HB_SEC * 2) as i64);
        let r = record_at(now - age, Some(CLAIM_ETAG));
        assert!(!check_progress_liveness(
            Some(&body_of(&r)),
            CLAIM_ETAG,
            now
        ));
    }
}
