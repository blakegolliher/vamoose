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
//!    b. Acquire via `If-None-Match: *`, or v2-reclaim if stale.
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
    self, AcquireOutcome, ClaimStore, CompleteOutcome, DeleteOutcome, FailOutcome, ListEntry,
    ReclaimOutcome,
};
use migration_core::errors::Error as CoreError;
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

use std::collections::{HashMap, HashSet};
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
    // F13: per-run skip set. Shards this worker released after a
    // worker-local `process()` error — this process never re-claims
    // them (a healthy peer takes them instead), but they still count
    // toward `all_terminal` via their true claim state. See
    // `handle_process_error` and `classify_shard_error`.
    let mut skip_shards: HashSet<String> = HashSet::new();

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
            scan_shards(
                &*s3,
                &manifest,
                lease,
                cfg.worker.heartbeat_sec,
                &mut claim_body_cache,
                &skip_shards,
            )
            .await?
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
                // Best-effort scratch cleanup before we give up on
                // this shard.
                if let Err(rm) = tokio::fs::remove_file(&scratch).await {
                    tracing::warn!(
                        error = ?rm,
                        scratch = %scratch.display(),
                        "scratch cleanup failed (post-shard-error)",
                    );
                }
                // F13: classify before deciding the claim's fate.
                // Shard-fatal (corrupt parquet, undecodable rows) →
                // terminal `Failed`, as before. Worker-local (stale
                // binary, scratch I/O, S3 hiccup) → release the claim
                // for a healthy peer, skip the shard locally, and
                // back off before re-scanning. See
                // `handle_process_error` / `classify_shard_error`.
                handle_process_error(
                    ProcessErrorContext {
                        store: &*s3,
                        host_id: &host_id,
                        shard_filename: &shard_filename,
                        current: &current,
                        fallback_etag: &etag,
                        fallback_epoch: record.epoch,
                        heartbeat_sec: cfg.worker.heartbeat_sec,
                    },
                    &e,
                    &mut skip_shards,
                )
                .await;
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

        // Flush downgrade + failure JSONL produced this shard. Shard
        // names on each record were stamped at the start of this
        // shard. Flush errors are surfaced loudly but do not abort
        // the run — the copies themselves already happened.
        for e in flush_sinks(
            &*s3,
            &host_id,
            &shard_filename,
            record.epoch,
            &downgrades,
            &failures,
        )
        .await
        {
            match e {
                FlushError::Collision { ref key } => {
                    tracing::error!(
                        key = %key,
                        shard = %shard_filename,
                        "sink flush collided with an existing object; refusing to \
                         overwrite (records for this flush are lost)",
                    );
                }
                FlushError::Store {
                    ref key,
                    ref source,
                } => {
                    tracing::warn!(
                        error = ?source,
                        key = %key,
                        shard = %shard_filename,
                        "sink flush failed (records lost; copies themselves succeeded)",
                    );
                }
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

/// Error surfaced by [`flush_sinks`] for one sink's PUT.
#[derive(Debug)]
pub(crate) enum FlushError {
    /// The flush key already exists on S3. Never expected; refusing
    /// to overwrite is what keeps the reconciliation trail intact.
    Collision { key: String },
    /// Transport / store error from the PUT itself.
    Store {
        key: String,
        source: migration_core::errors::Error,
    },
}

/// Drain the downgrade + failure sinks for one completed shard and
/// PUT the JSONL to S3. Empty drains write nothing. Both sinks are
/// flushed independently; every error is returned (an empty vec is
/// full success).
///
/// Each flush gets its own key — `layout::failures_flush_key` /
/// `layout::downgrades_flush_key`, unique per (host, shard, claim
/// epoch) — so a flush never overwrites an earlier shard's records
/// (F04). Because each key has exactly one legitimate writer, the
/// write is a conditional create (`PUT If-None-Match: *`): an
/// unexpected 412 means something already wrote our key and is
/// surfaced as a loud [`FlushError::Collision`], never treated as
/// success.
pub(crate) async fn flush_sinks(
    store: &dyn ClaimStore,
    host_id: &str,
    shard_filename: &str,
    epoch: u64,
    downgrades: &DowngradeSink,
    failures: &FailureSink,
) -> Vec<FlushError> {
    let mut errors = Vec::new();

    let drained = downgrades.drain_jsonl();
    downgrades.set_current_shard("");
    if !drained.is_empty() {
        let key = layout::downgrades_flush_key(host_id, shard_filename, epoch);
        if let Err(e) = flush_one(store, key, drained).await {
            errors.push(e);
        }
    }

    let drained_failures = failures.drain_jsonl();
    failures.set_current_shard("");
    if !drained_failures.is_empty() {
        let key = layout::failures_flush_key(host_id, shard_filename, epoch);
        if let Err(e) = flush_one(store, key, drained_failures).await {
            errors.push(e);
        }
    }

    errors
}

/// One sink PUT: conditional create, with the 412 case mapped to the
/// distinguishable [`FlushError::Collision`].
async fn flush_one(store: &dyn ClaimStore, key: String, body: Vec<u8>) -> Result<(), FlushError> {
    match store.put_if_absent(&key, body).await {
        Ok(_) => Ok(()),
        Err(migration_core::errors::Error::PreconditionFailed) => {
            Err(FlushError::Collision { key })
        }
        Err(e) => Err(FlushError::Store { key, source: e }),
    }
}

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

/// Public for in-process integration tests — see `scan_shards`.
#[derive(Debug)]
pub enum ClaimTarget {
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

/// Public for in-process integration tests — see `scan_shards`.
#[derive(Debug, Default)]
pub struct ScanResult {
    pub next_target: Option<ClaimTarget>,
    /// True iff every shard in the manifest is in a terminal state
    /// (Completed or Failed). Worker exits when this flips.
    pub all_terminal: bool,
    pub last_target_filename: String,
}

/// Cross-pass claim-body cache. Keyed by claim key (e.g.
/// `shards/part-0042.parquet.claim`); value is `(LIST etag,
/// parsed body)`. A scan only re-GETs a claim object when the LIST
/// response carries a new etag for it (which can only happen when
/// another worker has reclaimed / completed / failed it). In steady
/// state — all claims still owned and unchanged — this drops
/// per-Active-claim GETs to zero; scan cost becomes pure LIST plus
/// progress GETs (which the intra-pass cache also dedupes).
pub type ClaimBodyCache = HashMap<String, (String, ClaimRecord)>;

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

/// Public for in-process integration tests (e.g.
/// `tests/two_live_workers.rs`), which drive a minimal worker loop
/// against a mock `ClaimStore` without the hardware-bound `run()`
/// path. `heartbeat_sec` is this scanner's configured heartbeat
/// interval — used as the fresh-claim grace calibration when the
/// owner has no progress object to read a writer-side value from.
///
/// `skip_shards` (F13) is this worker's per-run release-and-skip set:
/// shards it released after a worker-local `process()` error. A
/// skipped shard is never offered as `next_target` (this worker must
/// not thrash re-claiming it), but its true claim state still counts
/// toward `all_terminal` — so the worker exits normally once healthy
/// peers drive every shard terminal.
pub async fn scan_shards(
    store: &dyn ClaimStore,
    manifest: &Manifest,
    lease: Duration,
    heartbeat_sec: u64,
    claim_body_cache: &mut ClaimBodyCache,
    skip_shards: &HashSet<String>,
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
        // F13: a skipped shard is never a claim target for this
        // worker, but its claim state still feeds `all_terminal`.
        let skipped = skip_shards.contains(&shard_filename);

        let entry = by_key.get(claim_key.as_str()).copied();

        match entry {
            None => {
                all_terminal = false;
                if next.is_none() && !skipped {
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
                            if next.is_none() && !skipped {
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
                        let stale_by_progress = if next.is_none() && !skipped && !stale_by_lease {
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
                                ProgressFetch::Hit(body) => check_progress_liveness(
                                    Some(&body),
                                    &e.etag,
                                    record.claimed_utc.0,
                                    heartbeat_sec,
                                    now,
                                ),
                                ProgressFetch::Miss => check_progress_liveness(
                                    None,
                                    &e.etag,
                                    record.claimed_utc.0,
                                    heartbeat_sec,
                                    now,
                                ),
                                ProgressFetch::Error => false,
                            }
                        } else {
                            false
                        };
                        if (stale_by_lease || stale_by_progress) && next.is_none() && !skipped {
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
/// Edges, per `PROGRESS_LIVENESS_CROSS_CHECK.md` §6, plus the
/// fresh-claim grace window from `CLAIM_FRESH_GRACE.md` (F01): the
/// progress object is only rewritten on heartbeat ticks, so between
/// an acquire and the owner's next tick it still describes the
/// *previous* ownership window (`held_etag = None` at startup, or the
/// previous shard's etag). The absent / `None` / mismatched branches
/// therefore additionally require the claim itself to be older than
/// `2 × heartbeat_sec` (`claim age = now - claimed_utc`, strict `>`,
/// future timestamps → not eligible) before returning eligible —
/// otherwise every fleet startup is a near-deterministic theft
/// cascade. The matching-etag branch needs no grace: a matching
/// `held_etag` proves the progress record was written inside this
/// ownership window.
///
/// | Progress file state                       | Returns |
/// |-------------------------------------------|---------|
/// | Absent (`body=None`); claim age ≤ 2 × scanner hb_sec | `false` (owner may not have ticked yet — grace) |
/// | Absent (`body=None`); claim age > 2 × scanner hb_sec | `true`  (owner never started or crashed pre-tick) |
/// | Parse failure                             | `false` (don't act on garbage; defer to lease) |
/// | `heartbeat_sec == 0` (pre-cross-check)    | `false` (no calibrated freshness window) |
/// | `held_etag` `None`/mismatch; claim age ≤ 2 × writer hb_sec | `false` (progress hasn't caught up to the acquire — grace) |
/// | `held_etag` is `None`; claim age > grace  | `true`  (writer announced "not holding this") |
/// | `held_etag` differs; claim age > grace    | `true`  (claim has been replaced; old progress is stale → same-host_id restart safety) |
/// | etag matches; heartbeat age ≤ 2 × hb_sec  | `false` (alive) |
/// | etag matches; heartbeat age > 2 × hb_sec  | `true`  (dead) |
///
/// The writer-side `p.heartbeat_sec` calibrates the grace when a
/// progress object exists (same from-writer preference as the
/// heartbeat-staleness threshold); with no progress object to read,
/// the scanner's own configured `heartbeat_sec` is the fallback.
fn check_progress_liveness(
    progress_body: Option<&[u8]>,
    claim_etag: &str,
    claimed_utc: chrono::DateTime<chrono::Utc>,
    scanner_heartbeat_sec: u64,
    now: chrono::DateTime<chrono::Utc>,
) -> bool {
    // Fresh-claim grace: true iff the claim is strictly older than
    // 2 × hb_sec. Conservative on every uncertain edge, matching the
    // heartbeat-staleness math below: a future `claimed_utc` (clock
    // skew) yields a negative age → not elapsed → not eligible; an
    // uncalibrated hb_sec of 0 → not elapsed → defer to lease.
    let grace_elapsed = |hb_sec: u64| -> bool {
        if hb_sec == 0 {
            return false;
        }
        let claim_age = now.signed_duration_since(claimed_utc);
        claim_age.num_seconds() > (hb_sec.saturating_mul(2)) as i64
    };

    let Some(body) = progress_body else {
        // No progress object at all — owner never started, crashed
        // before its first tick landed, or simply hasn't ticked yet.
        // Only eligible once the claim has outlived the grace window;
        // there is no writer-side heartbeat_sec to read, so calibrate
        // on the scanner's own configured interval.
        return grace_elapsed(scanner_heartbeat_sec);
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
        // to this claim's ownership window. Either the owner acquired
        // it after its last tick (progress is one tick behind S3
        // reality — NOT eligible until the grace window elapses), or
        // the claim is a genuine orphan (worker self-fenced, or a
        // same-host_id restart minted a new claim) — eligible once
        // the claim has outlived the grace window.
        _ => return grace_elapsed(p.heartbeat_sec),
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
        match claim::reclaim(store, shard, &e.etag, host_id, new_epoch).await {
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

// =============================================================================
// F13: shard-error classification (worker-local vs shard-fatal)
// =============================================================================

/// Classification of an error returned by `ShardProcessor::process`.
///
/// The `fail()` contract (`docs/CLAIM_PROTOCOL.md`) reserves the
/// terminal `Failed` state for errors "that would re-occur for any
/// worker reclaiming the shard." Everything else is a property of
/// *this* worker and must not poison the shard fleet-wide. See
/// `docs/work-items/WORKER_ERROR_CLASSIFICATION.md` (ledger F13).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ShardErrorClass {
    /// A property of the shard bytes: any worker reclaiming the shard
    /// hits the same error. The claim is marked terminal `Failed` so
    /// the fleet stops cycling it and an operator intervenes.
    Fatal,
    /// A property of THIS worker (binary version, local scratch,
    /// network path to S3, claim-plane state). A healthy peer can
    /// process the shard; the worker releases the claim and skips the
    /// shard locally instead of terminal-failing it.
    WorkerLocal,
}

/// Pure classifier over the migration-core error taxonomy. Public for
/// in-process integration tests (F13 acceptance test 1).
///
/// Row-level per-file failures never reach this function — they are
/// recorded in the failures sink by `record_outcome` and `process()`
/// still returns `Ok`. This classifier only sees whole-shard errors.
pub fn classify_shard_error(e: &CoreError) -> ShardErrorClass {
    match e {
        // Shard-fatal: the shard's bytes/schema are bad. Reclaim by a
        // peer would deterministically re-fail.
        CoreError::ShardCorrupt { .. }
        | CoreError::CorruptRow { .. }
        | CoreError::MissingColumn(_)
        | CoreError::Parquet(_)
        | CoreError::Arrow(_) => ShardErrorClass::Fatal,

        // `Other` carries untyped errors — today that's the shard
        // reader's "unexpected arrow type" (a shard-schema property).
        // It is also the conservative default for anything a future
        // refactor forgets to type: an unknown misclassified as Fatal
        // degrades to the old loud, operator-visible failure mode
        // rather than an unbounded fleet-wide claim/release loop.
        CoreError::Other(_) => ShardErrorClass::Fatal,

        // Worker-local: nothing about the shard itself is wrong.
        //  - SchemaVersionMismatch: this binary is stale for the
        //    shard's format_version; a current peer reads it fine.
        //  - Io: local scratch (EIO/ENOMEM reading the downloaded
        //    parquet) — host-specific.
        //  - S3: transport to the bucket — host/network-specific.
        //    (Retry policy for these on scan/acquire is F42, not
        //    built here.)
        //  - Json: control-plane bodies (claim/progress/manifest),
        //    never shard bytes.
        //  - PreconditionFailed / ClaimInvalidated: claim-plane
        //    signals about our ownership, not about the shard.
        //  - ManifestChanged / SourceDestOverlap: run-level guards;
        //    the worker stops, the shard is untouched.
        CoreError::SchemaVersionMismatch { .. }
        | CoreError::Io(_)
        | CoreError::S3(_)
        | CoreError::Json(_)
        | CoreError::PreconditionFailed
        | CoreError::ClaimInvalidated(_)
        | CoreError::ManifestChanged { .. }
        | CoreError::SourceDestOverlap { .. } => ShardErrorClass::WorkerLocal,
    }
}

/// Inputs `handle_process_error` borrows from the orchestrator loop.
/// Grouped in a struct so the handler stays callable from in-process
/// integration tests (F13 acceptance tests 2, 3, 5) without a live
/// `run()`.
pub struct ProcessErrorContext<'a> {
    pub store: &'a dyn ClaimStore,
    pub host_id: &'a str,
    pub shard_filename: &'a str,
    /// The shared held-claim cell. Cleared BEFORE any claim write
    /// (same R4 reasoning as `complete()`) so the heartbeat doesn't
    /// fence the worker on the transient absent/replaced window.
    pub current: &'a Mutex<Option<HeldClaim>>,
    /// Ownership proof used if the cell no longer carries this shard.
    pub fallback_etag: &'a str,
    pub fallback_epoch: u64,
    /// Calibrates the release-path backoff (same base as the
    /// contention backoff: `heartbeat_sec / 4` + jitter).
    pub heartbeat_sec: u64,
}

/// Handle a `processor.process()` error for the shard we currently
/// hold. Public for in-process integration tests; the orchestrator
/// loop calls this from its process-error arm and then `continue`s.
///
/// - [`ShardErrorClass::Fatal`] → unchanged pre-F13 behavior: write a
///   terminal `Failed` claim via `claim::fail` so scanners skip the
///   shard and an operator intervenes.
/// - [`ShardErrorClass::WorkerLocal`] → release the claim with the
///   worker's own held etag via the existing delete-if-match atom
///   (spec-clean: the owner deletes its own claim; the shard returns
///   to Free and a healthy peer picks it up), record the shard in the
///   per-run skip set so THIS worker never re-claims it, and sleep
///   the contention backoff so a fleet-wide transient (e.g. an S3
///   blip) doesn't become a claim/release storm. The error is NOT
///   retried in place — release-and-skip keeps the failure domain
///   small (see the work item's "Out of scope").
///
/// Returns the classification so callers/tests can assert on it.
pub async fn handle_process_error(
    ctx: ProcessErrorContext<'_>,
    error: &anyhow::Error,
    skip_shards: &mut HashSet<String>,
) -> ShardErrorClass {
    let class = error
        .downcast_ref::<CoreError>()
        .map(classify_shard_error)
        // Errors that aren't typed migration-core errors are
        // unmatchable — same conservative default as
        // `CoreError::Other` (see `classify_shard_error`).
        .unwrap_or(ShardErrorClass::Fatal);

    // Same R4 reasoning as the complete() path: snapshot the held
    // etag/epoch AND clear the held-claim cell in a single lock-held
    // block, BEFORE the claim write below. Both arms transiently
    // remove (release) or replace (fail) the claim object on S3; a
    // heartbeat HEAD during that window would otherwise spuriously
    // fence the worker.
    let (held_etag, held_epoch) = {
        let mut g = ctx.current.lock().await;
        let (e, ep) = match g.as_ref() {
            Some(c) if c.shard == ctx.shard_filename => (c.etag.clone(), c.epoch),
            _ => (ctx.fallback_etag.to_string(), ctx.fallback_epoch),
        };
        *g = None;
        (e, ep)
    };

    match class {
        ShardErrorClass::WorkerLocal => {
            tracing::error!(
                error = ?error,
                shard = %ctx.shard_filename,
                classification = "worker-local",
                "worker-local shard error; releasing claim for a healthy peer \
                 and skipping this shard locally (NOT marking Failed)",
            );
            // Release via the existing delete-if-match atom. We own
            // `held_etag`, so this is spec-clean deletion by the
            // owner — no new terminal state, no PUT If-Match.
            let claim_key = layout::claim_key(ctx.shard_filename);
            match ctx.store.delete_if_match(&claim_key, &held_etag).await {
                Ok(DeleteOutcome::Deleted) => {
                    tracing::warn!(
                        shard = %ctx.shard_filename,
                        "claim released; shard is Free for healthy peers",
                    );
                }
                Ok(DeleteOutcome::EtagMismatch | DeleteOutcome::NotFound) => {
                    // Someone already reclaimed or replaced the claim
                    // — nothing of ours left to release.
                    tracing::warn!(
                        shard = %ctx.shard_filename,
                        "claim already replaced while releasing; nothing to do",
                    );
                }
                Err(release_err) => {
                    // Leave the claim Active; a peer reclaims it via
                    // the lease / progress-cross-check path (bounded
                    // recovery). Never fall back to fail() here.
                    tracing::warn!(
                        error = ?release_err,
                        shard = %ctx.shard_filename,
                        "claim release errored; shard stays Active until a \
                         peer reclaims via lease/cross-check",
                    );
                }
            }
            // Per-run skip set: this worker never re-claims the shard,
            // so a persistent local fault (stale binary, sick scratch
            // disk) can't thrash claim/release cycles on it.
            skip_shards.insert(ctx.shard_filename.to_string());
            // Reuse the contention backoff before the caller re-scans
            // so a fleet-wide transient spreads out across workers.
            backoff_after_lost_race(ctx.heartbeat_sec).await;
        }
        ShardErrorClass::Fatal => {
            // Unchanged pre-F13 behavior: the parquet won't decode, or
            // a row schema is malformed. A peer reclaiming after lease
            // expiry would just hit the same error → infinite
            // fleet-wide reclaim loop. Mark the claim `Failed`
            // (terminal, scanners skip it); the operator inspects the
            // error log + Failed record and re-uploads / re-indexes.
            tracing::error!(
                error = ?error,
                shard = %ctx.shard_filename,
                classification = "shard-fatal",
                "shard-fatal error; marking claim Failed and moving on",
            );
            match claim::fail(
                ctx.store,
                ctx.shard_filename,
                &held_etag,
                ctx.host_id,
                held_epoch,
            )
            .await
            {
                Ok(FailOutcome::Failed { .. }) => {
                    tracing::warn!(
                        shard = %ctx.shard_filename,
                        "claim marked Failed (terminal); requires operator follow-up",
                    );
                }
                Ok(FailOutcome::Lost) => {
                    // Another worker took over while we were
                    // processing — they'll hit the same error and
                    // mark Failed themselves. Drop the claim cleanly.
                    tracing::warn!(
                        shard = %ctx.shard_filename,
                        "claim lost while marking Failed; new owner will retry-then-fail",
                    );
                }
                Err(write_err) => {
                    tracing::warn!(
                        error = ?write_err,
                        shard = %ctx.shard_filename,
                        "claim Failed write errored; shard will remain Active until lease expiry",
                    );
                }
            }
        }
    }
    class
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
    use chrono::{DateTime, Duration as ChronoDuration, Utc};
    use migration_core::records::ProgressRecord;
    use migration_core::time::UtcTime;

    const CLAIM_ETAG: &str = "etag-claim-abc";
    const HB_SEC: u64 = 30;

    /// A claim old enough that the fresh-claim grace window
    /// (`2 × heartbeat_sec`) has strictly elapsed.
    fn aged_claim(now: DateTime<Utc>) -> DateTime<Utc> {
        now - ChronoDuration::seconds((HB_SEC * 2 + 1) as i64)
    }

    /// A claim acquired seconds ago — inside the grace window.
    fn fresh_claim(now: DateTime<Utc>) -> DateTime<Utc> {
        now - ChronoDuration::seconds(5)
    }

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

    /// Absent progress object on an aged claim → eligible. Owner
    /// crashed before its first tick landed, or never started; peer
    /// reclaims fast once the fresh-claim grace window has elapsed.
    #[test]
    fn absent_body_is_eligible() {
        let now = Utc::now();
        assert!(check_progress_liveness(
            None,
            CLAIM_ETAG,
            aged_claim(now),
            HB_SEC,
            now
        ));
    }

    /// Parse failure → conservative; defer to lease (even on an aged
    /// claim — don't act on garbage).
    #[test]
    fn unparseable_body_defers_to_lease() {
        let garbage = b"not-json-at-all";
        let now = Utc::now();
        assert!(!check_progress_liveness(
            Some(garbage),
            CLAIM_ETAG,
            aged_claim(now),
            HB_SEC,
            now
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
        let now = Utc::now();
        assert!(!check_progress_liveness(
            Some(json.as_bytes()),
            CLAIM_ETAG,
            aged_claim(now),
            HB_SEC,
            now
        ));
    }

    /// `held_etag = None` means the writer announced "I am not
    /// holding any claim right now" (worker self-fenced, exiting, or
    /// between shards) → peer is eligible to reclaim once the
    /// fresh-claim grace window has elapsed.
    #[test]
    fn held_etag_none_is_eligible() {
        let now = Utc::now();
        let r = record_at(now, None);
        assert!(check_progress_liveness(
            Some(&body_of(&r)),
            CLAIM_ETAG,
            aged_claim(now),
            HB_SEC,
            now
        ));
    }

    /// Same-host_id restart: the live claim has a new etag, but the
    /// pre-restart progress object still carries the old one. The
    /// mismatch makes the shard fast-reclaim-eligible (once the grace
    /// window has elapsed) — and the post-restart progress write will
    /// carry the new etag, so the new ownership window won't ever
    /// match the dead claim.
    #[test]
    fn held_etag_mismatch_is_eligible() {
        let now = Utc::now();
        let r = record_at(now, Some("etag-old-from-prior-acquire"));
        assert!(check_progress_liveness(
            Some(&body_of(&r)),
            CLAIM_ETAG,
            aged_claim(now),
            HB_SEC,
            now
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
            fresh_claim(now),
            HB_SEC,
            now
        ));
    }

    /// Owner stopped heartbeating: etag matches, but heartbeat_utc is
    /// older than 2 × heartbeat_sec. Eligible for fast reclaim. The
    /// fresh-claim grace does NOT gate this branch — a matching
    /// held_etag proves the progress record was written inside this
    /// ownership window, so heartbeat staleness is already calibrated
    /// against the writer.
    #[test]
    fn stale_heartbeat_with_matching_etag_is_eligible() {
        let now = Utc::now();
        let age = ChronoDuration::seconds((HB_SEC * 2 + 1) as i64);
        let r = record_at(now - age, Some(CLAIM_ETAG));
        assert!(check_progress_liveness(
            Some(&body_of(&r)),
            CLAIM_ETAG,
            aged_claim(now),
            HB_SEC,
            now
        ));
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
            aged_claim(now),
            HB_SEC,
            now
        ));
    }

    // -------------------------------------------------------------------------
    // Fresh-claim grace window — docs/work-items/CLAIM_FRESH_GRACE.md
    // acceptance tests 1–7. The etag-mismatch and progress-absent
    // branches must additionally require
    // `now - claimed_utc > 2 × heartbeat_sec` before returning
    // eligible; the matching-etag branches are unchanged.
    // -------------------------------------------------------------------------

    /// Acceptance test 1 (red before fix): a claim acquired 5s ago
    /// whose owner's progress object still carries the previous
    /// ownership window's etag (fresh heartbeat) must NOT be
    /// reclaim-eligible — the owner simply hasn't ticked yet.
    #[test]
    fn fresh_claim_mismatched_progress_not_eligible() {
        let now = Utc::now();
        let r = record_at(now, Some("different-etag"));
        assert!(!check_progress_liveness(
            Some(&body_of(&r)),
            CLAIM_ETAG,
            fresh_claim(now),
            HB_SEC,
            now
        ));
    }

    /// Acceptance test 2 (red before fix): a claim acquired 5s ago
    /// with no progress object for the host must NOT be eligible —
    /// at fleet startup every worker is in this state until its
    /// first heartbeat tick lands.
    #[test]
    fn fresh_claim_absent_progress_not_eligible() {
        let now = Utc::now();
        assert!(!check_progress_liveness(
            None,
            CLAIM_ETAG,
            fresh_claim(now),
            HB_SEC,
            now
        ));
    }

    /// Acceptance test 3 (red before fix): claim age 5s, progress
    /// present with `held_etag = None` (owner between shards / just
    /// started). NOT eligible inside the grace window.
    #[test]
    fn fresh_claim_none_held_etag_not_eligible() {
        let now = Utc::now();
        let r = record_at(now, None);
        assert!(!check_progress_liveness(
            Some(&body_of(&r)),
            CLAIM_ETAG,
            fresh_claim(now),
            HB_SEC,
            now
        ));
    }

    /// Acceptance test 4: once the claim is older than the grace
    /// window, a mismatched held_etag makes it eligible again —
    /// orphan reclaim must keep working.
    #[test]
    fn aged_claim_mismatched_progress_eligible() {
        let now = Utc::now();
        let r = record_at(now, Some("different-etag"));
        assert!(check_progress_liveness(
            Some(&body_of(&r)),
            CLAIM_ETAG,
            aged_claim(now),
            HB_SEC,
            now
        ));
    }

    /// Acceptance test 5: aged claim, no progress object (owner
    /// crashed before its first tick). Eligible — dead-worker
    /// reclaim is the feature's reason to exist.
    #[test]
    fn aged_claim_absent_progress_eligible() {
        let now = Utc::now();
        assert!(check_progress_liveness(
            None,
            CLAIM_ETAG,
            aged_claim(now),
            HB_SEC,
            now
        ));
    }

    /// Acceptance test 6: claim age exactly `2 × heartbeat_sec` is
    /// NOT eligible — strictly-greater comparison, matching the
    /// heartbeat-staleness convention in the existing rows.
    #[test]
    fn boundary_exactly_grace_not_eligible() {
        let now = Utc::now();
        let claimed = now - ChronoDuration::seconds((HB_SEC * 2) as i64);
        // Mismatched-etag branch at the boundary.
        let r = record_at(now, Some("different-etag"));
        assert!(!check_progress_liveness(
            Some(&body_of(&r)),
            CLAIM_ETAG,
            claimed,
            HB_SEC,
            now
        ));
        // Absent-progress branch at the boundary.
        assert!(!check_progress_liveness(
            None, CLAIM_ETAG, claimed, HB_SEC, now
        ));
    }

    /// Acceptance test 7: matching held_etag with a fresh heartbeat
    /// is never eligible, no matter how old the claim is — the
    /// happy-path regression guard.
    #[test]
    fn matching_etag_never_eligible_regardless_of_age() {
        let now = Utc::now();
        let r = record_at(now, Some(CLAIM_ETAG));
        // Fresh claim.
        assert!(!check_progress_liveness(
            Some(&body_of(&r)),
            CLAIM_ETAG,
            fresh_claim(now),
            HB_SEC,
            now
        ));
        // Very old claim (hours past any lease/grace window).
        let ancient = now - ChronoDuration::hours(6);
        assert!(!check_progress_liveness(
            Some(&body_of(&r)),
            CLAIM_ETAG,
            ancient,
            HB_SEC,
            now
        ));
    }

    /// Conservative time handling: a `claimed_utc` in the future
    /// (clock skew between the owner and this scanner) must read as
    /// "grace not elapsed" → NOT eligible, same convention as the
    /// existing heartbeat-staleness math.
    #[test]
    fn future_claimed_utc_not_eligible() {
        let now = Utc::now();
        let future = now + ChronoDuration::seconds(30);
        let r = record_at(now, Some("different-etag"));
        assert!(!check_progress_liveness(
            Some(&body_of(&r)),
            CLAIM_ETAG,
            future,
            HB_SEC,
            now
        ));
        assert!(!check_progress_liveness(
            None, CLAIM_ETAG, future, HB_SEC, now
        ));
    }
}

#[cfg(test)]
mod flush_sinks_tests {
    //! F04 acceptance tests 2–5 (`docs/work-items/
    //! WORKER_FAILURE_SINK_APPEND.md`): per-shard sink flushes must
    //! never overwrite a previous flush's records.

    use super::{flush_sinks, FlushError};
    use async_trait::async_trait;
    use migration_core::claim::{ClaimStore, DeleteOutcome, ListEntry};
    use migration_core::errors::{Error, Result};
    use migration_core::records::{FailurePhase, FailureRecord};
    use migration_mover::{DowngradeSink, FailureSink};
    use std::collections::BTreeMap;
    use std::sync::Mutex;

    /// Which PUT primitive wrote a key — the mock records the
    /// precondition so tests can assert the flush is conditional.
    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    enum PutKind {
        Conditional,
        Unconditional,
    }

    /// In-memory ClaimStore for the flush path. `put_if_absent`
    /// mimics S3 `PUT If-None-Match: *` (412 → PreconditionFailed if
    /// the key exists); `put_unconditional` overwrites, like plain
    /// PUT. Every write records which primitive was used.
    #[derive(Default)]
    struct FlushStore {
        objects: Mutex<BTreeMap<String, Vec<u8>>>,
        puts: Mutex<Vec<(String, PutKind)>>,
    }

    impl FlushStore {
        fn new() -> Self {
            Self::default()
        }

        fn body(&self, key: &str) -> Option<Vec<u8>> {
            self.objects.lock().unwrap().get(key).cloned()
        }

        fn puts(&self) -> Vec<(String, PutKind)> {
            self.puts.lock().unwrap().clone()
        }
    }

    #[async_trait]
    impl ClaimStore for FlushStore {
        async fn put_if_absent(&self, key: &str, body: Vec<u8>) -> Result<String> {
            self.puts
                .lock()
                .unwrap()
                .push((key.to_string(), PutKind::Conditional));
            let mut g = self.objects.lock().unwrap();
            if g.contains_key(key) {
                return Err(Error::PreconditionFailed);
            }
            g.insert(key.to_string(), body);
            Ok(format!("etag-{}", g.len()))
        }

        async fn put_unconditional(&self, key: &str, body: Vec<u8>) -> Result<String> {
            self.puts
                .lock()
                .unwrap()
                .push((key.to_string(), PutKind::Unconditional));
            let mut g = self.objects.lock().unwrap();
            g.insert(key.to_string(), body);
            Ok(format!("etag-{}", g.len()))
        }

        async fn head_object(&self, key: &str) -> Result<Option<(String, Vec<u8>)>> {
            let g = self.objects.lock().unwrap();
            Ok(g.get(key).map(|b| ("etag".to_string(), b.clone())))
        }

        async fn delete_if_match(&self, _key: &str, _etag: &str) -> Result<DeleteOutcome> {
            unimplemented!("flush path never deletes")
        }

        async fn get(&self, key: &str) -> Result<Option<(Vec<u8>, String)>> {
            let g = self.objects.lock().unwrap();
            Ok(g.get(key).map(|b| (b.clone(), "etag".to_string())))
        }

        async fn list(&self, prefix: &str) -> Result<Vec<ListEntry>> {
            let g = self.objects.lock().unwrap();
            Ok(g.iter()
                .filter(|(k, _)| k.starts_with(prefix))
                .map(|(k, b)| ListEntry {
                    key: k.clone(),
                    etag: "etag".to_string(),
                    size: b.len() as u64,
                })
                .collect())
        }
    }

    const HOST: &str = "A";

    fn parse_jsonl(body: &[u8]) -> Vec<FailureRecord> {
        body.split(|&b| b == b'\n')
            .filter(|l| !l.is_empty())
            .map(|l| serde_json::from_slice(l).unwrap())
            .collect()
    }

    /// F04 acceptance test 2 (red before fix): two consecutive shard
    /// flushes on the same host must BOTH survive. Under the old flat
    /// per-host key, the second flush overwrote the first — silently
    /// losing the first shard's failure records.
    #[tokio::test]
    async fn flush_two_shards_preserves_both() {
        let store = FlushStore::new();
        let downgrades = DowngradeSink::new();
        let failures = FailureSink::new();

        // Shard 1 records one failure, then flushes.
        failures.set_current_shard("part-0001.parquet");
        failures.record(7, b"/one", FailurePhase::Open, "EACCES");
        let errs = flush_sinks(&store, HOST, "part-0001.parquet", 1, &downgrades, &failures).await;
        assert!(errs.is_empty(), "first flush errored: {errs:?}");

        // Shard 2 records a different failure, then flushes.
        failures.set_current_shard("part-0002.parquet");
        failures.record(9, b"/two", FailurePhase::Write, "ENOSPC");
        let errs = flush_sinks(&store, HOST, "part-0002.parquet", 1, &downgrades, &failures).await;
        assert!(errs.is_empty(), "second flush errored: {errs:?}");

        // Both flushes must exist under the failures prefix.
        let listed = store.list("failures/").await.unwrap();
        assert_eq!(
            listed.len(),
            2,
            "expected one object per shard flush, got keys: {:?}",
            listed.iter().map(|e| &e.key).collect::<Vec<_>>(),
        );

        // And their contents must round-trip: every record written is
        // still readable, none overwritten.
        let mut all: Vec<FailureRecord> = Vec::new();
        for entry in &listed {
            all.extend(parse_jsonl(&store.body(&entry.key).unwrap()));
        }
        all.sort_by_key(|r| r.row_id);
        assert_eq!(all.len(), 2, "a flush was lost: {all:?}");
        assert_eq!(all[0].row_id, 7);
        assert_eq!(all[0].shard, "part-0001.parquet");
        assert_eq!(all[0].error, "EACCES");
        assert_eq!(all[1].row_id, 9);
        assert_eq!(all[1].shard, "part-0002.parquet");
        assert_eq!(all[1].error, "ENOSPC");
    }

    /// F04 acceptance test 3: the flush must use the conditional
    /// create (`PUT If-None-Match: *`), refuse to clobber an existing
    /// key, and surface a distinguishable Collision outcome instead
    /// of silently overwriting.
    #[tokio::test]
    async fn flush_uses_put_if_absent() {
        let store = FlushStore::new();
        let downgrades = DowngradeSink::new();
        let failures = FailureSink::new();

        failures.set_current_shard("part-0001.parquet");
        failures.record(1, b"/a", FailurePhase::Open, "EIO");
        let errs = flush_sinks(&store, HOST, "part-0001.parquet", 1, &downgrades, &failures).await;
        assert!(errs.is_empty(), "flush errored: {errs:?}");

        // The mock recorded the precondition of every PUT: all must
        // be conditional creates, never unconditional overwrites.
        let puts = store.puts();
        assert!(!puts.is_empty());
        for (key, kind) in &puts {
            assert_eq!(
                *kind,
                PutKind::Conditional,
                "flush of {key} used an unconditional PUT",
            );
        }
        let written_key = puts[0].0.clone();
        let original_body = store.body(&written_key).unwrap();

        // Re-flush the same (host, shard, epoch) with new records —
        // the key collides. The flush must refuse to overwrite and
        // surface a distinguishable outcome.
        failures.set_current_shard("part-0001.parquet");
        failures.record(2, b"/b", FailurePhase::Write, "ENOSPC");
        let errs = flush_sinks(&store, HOST, "part-0001.parquet", 1, &downgrades, &failures).await;
        assert_eq!(errs.len(), 1, "collision must surface an error: {errs:?}");
        match &errs[0] {
            FlushError::Collision { key } => assert_eq!(key, &written_key),
            other => panic!("expected Collision, got {other:?}"),
        }

        // The existing object was not clobbered.
        assert_eq!(store.body(&written_key).unwrap(), original_body);
    }

    /// F04 acceptance test 4: empty drains write nothing — the
    /// current "skip empty" behavior is preserved.
    #[tokio::test]
    async fn empty_drain_writes_nothing() {
        let store = FlushStore::new();
        let downgrades = DowngradeSink::new();
        let failures = FailureSink::new();

        let errs = flush_sinks(&store, HOST, "part-0001.parquet", 1, &downgrades, &failures).await;
        assert!(errs.is_empty(), "empty flush errored: {errs:?}");
        assert!(store.puts().is_empty(), "empty drain must not PUT");
        assert!(store.list("").await.unwrap().is_empty());
    }

    /// F04 acceptance test 5: consumers find records by listing the
    /// per-host prefix (`failures/host-<id>` / `downgrades/host-<id>`)
    /// — the new per-flush keys must still live under it.
    #[tokio::test]
    async fn flush_keys_listable_under_host_prefix() {
        use migration_core::records::DowngradeKind;

        let store = FlushStore::new();
        let downgrades = DowngradeSink::new();
        let failures = FailureSink::new();

        downgrades.set_current_shard("part-0042.parquet");
        downgrades.record(1, b"/d", DowngradeKind::NullMtime);
        failures.set_current_shard("part-0042.parquet");
        failures.record(2, b"/f", FailurePhase::Open, "EACCES");

        let errs = flush_sinks(&store, HOST, "part-0042.parquet", 3, &downgrades, &failures).await;
        assert!(errs.is_empty(), "flush errored: {errs:?}");

        let f = store.list(&format!("failures/host-{HOST}")).await.unwrap();
        assert_eq!(f.len(), 1, "failures not under per-host prefix");
        let d = store
            .list(&format!("downgrades/host-{HOST}"))
            .await
            .unwrap();
        assert_eq!(d.len(), 1, "downgrades not under per-host prefix");

        // Top-level prefixes (used by init/doctor and operators)
        // still cover everything too.
        assert_eq!(store.list("failures/").await.unwrap().len(), 1);
        assert_eq!(store.list("downgrades/").await.unwrap().len(), 1);
    }
}
