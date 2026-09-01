//! Worker orchestrator — top-level migration lifecycle.
//!
//! Sequence:
//!
//! 1. Load manifest, verify format version.
//! 2. Reject source/destination overlap, build the configured libnfs mover,
//!    and initialize shared progress/fence state.
//! 3. Spawn the S3 heartbeat and optional coordinator driver.
//! 4. Reconcile self-owned claims left behind by a previous run.
//! 5. Loop:
//!    a. Scan `shards/` for a claimable shard (free or stale-leased).
//!    b. Acquire via `If-None-Match: *`, or v2-reclaim if stale.
//!    c. Download the parquet index shard to local scratch.
//!    d. Run the shard processor; update HeldClaim + ProgressState.
//!    e. On clean completion: mark the claim `Completed`.
//!    f. On fence trip: leave the claim where it is and exit.
//! 6. Exit cleanly when every shard is Completed/Failed or the worker
//!    is fenced.
//!
//! # Process stop (SIGTERM / SIGINT)
//!
//! `systemctl stop` sends SIGTERM. [`run`] installs a listener that
//! flips a stop token; the claim loop stops claiming, the shard
//! processor leaves the shard in hand at its next batch boundary, the
//! claim is released (DELETE If-Match by the owner) so a peer can
//! take the shard immediately instead of waiting out the lease, and
//! the normal section-7 shutdown runs. The outcome is
//! [`RunOutcome::Interrupted`], exit code 0 — a deliberate stop is
//! not a failure. A second signal is logged and otherwise ignored:
//! the batch in hand always finishes (SIGKILL is the escape hatch).
//!
//! # Shutdown diagnostics
//!
//! The step-by-step shutdown trace (section 7) is emitted at `debug`
//! level under the `shutdown` tracing target — enable it with
//! `RUST_LOG=shutdown=debug` when diagnosing a wedged or slow
//! shutdown. The raw `libc::write` lines in the hard-exit watchdog
//! thread (and in `main.rs` after [`run`] returns) are deliberate,
//! not candidates for tracing: they run when the tokio runtime and
//! the tracing stack may already be gone or wedged.

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
use migration_core::fence::{Fence, FenceCause};
use migration_core::layout;
use migration_core::overlap;
use migration_core::records::{
    ClaimRecord, ClaimState, Manifest, MigrationOptions, ProgressRecord, ServerSideCopy,
    ShardEntry, RUN_FORMAT_VERSION,
};
use migration_core::s3::S3Client;
use migration_core::time::UtcTime;
use migration_mover::batch::{BatchBudget, InflightLimiter, InflightProfile};
use migration_mover::{DowngradeSink, FailureSink};

use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{Mutex, RwLock};
use tokio_util::sync::CancellationToken;

/// Interval between arming the watchdog on [`run`]'s successful return
/// path and forcefully terminating a process that has not exited.
pub const HARD_EXIT_WATCHDOG_INTERVAL: Duration = Duration::from_secs(5);

/// Returns how the run ended ([`RunOutcome::Clean`] /
/// [`RunOutcome::Interrupted`] / [`RunOutcome::Fenced`]); callers map
/// it to the process exit code via [`exit_code_for_outcome`] so a
/// fenced-but-clean shutdown is distinguishable from a completed
/// migration at the supervisor level.
///
/// Installs the SIGTERM / SIGINT listener (see the module docs) and
/// delegates to [`run_with_stop`].
pub async fn run(cfg: Config, host_id: String) -> anyhow::Result<RunOutcome> {
    let stop = CancellationToken::new();
    spawn_signal_listener(stop.clone());
    run_with_stop(cfg, host_id, stop).await
}

/// [`run`] with an injected stop token. Cancelling `stop` has the
/// same effect as SIGTERM: no new claims, the shard in hand is left
/// at its next batch boundary and released, orderly shutdown, exit 0.
pub async fn run_with_stop(
    cfg: Config,
    host_id: String,
    stop: CancellationToken,
) -> anyhow::Result<RunOutcome> {
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
    .await?
    .with_prefix(&cfg.run.prefix);
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

    // F42: transient-store retry budget for the scan / manifest /
    // acquire call sites below — R6's shape, see
    // `transient_retry_budget`.
    let retry_budget =
        transient_retry_budget(cfg.worker.lease_timeout_sec, cfg.worker.heartbeat_sec);

    // A missing manifest is not an error: workers are enabled at
    // install time and idle until `vamoose prepare` publishes one.
    // Transient store failures still consume the F42 budget. A stop
    // request ends the wait at once: the worker holds nothing here,
    // and an idle worker that ignores SIGTERM is killed by systemd
    // after its stop timeout (found on the rig: 5 min, then SIGKILL).
    let manifest = loop {
        if stop.is_cancelled() {
            tracing::info!("stop requested while waiting for manifest.json; exiting");
            return Ok(RunOutcome::Interrupted);
        }
        let mut st = &*s3;
        let found = retry_transient(
            "manifest GET",
            retry_budget,
            cfg.worker.heartbeat_sec,
            &mut st,
            |s3c| Box::pin(load_manifest(s3c)),
        )
        .await?;
        match found {
            Some(m) => break m,
            None => {
                tracing::info!(
                    bucket = %s3.bucket(),
                    retry_sec = MANIFEST_WAIT_POLL_SEC,
                    "no manifest.json in bucket yet; waiting for `vamoose prepare`",
                );
                sleep_unless_stopped(&stop, Duration::from_secs(MANIFEST_WAIT_POLL_SEC)).await;
            }
        }
    };
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
    // Pool sizing, MoverConfig projection, and the sync-vs-bucketed
    // wiring live in `mover_factory` (shared with mongoose).
    let downgrades = DowngradeSink::new();
    let failures = FailureSink::new();

    // ---- 4. Shared progress + heartbeat ----------------------------
    let progress = Arc::new(RwLock::new(ProgressState::new()));
    let live = Arc::new(crate::heartbeat::LivePending::default());
    let current = Arc::new(Mutex::new(None::<HeldClaim>));
    let fence = Fence::new();

    let mover_params = crate::mover_factory::MoverParams {
        source_url: manifest.source.url.clone(),
        dest_url: manifest.dest.url.clone(),
        source_root: manifest.source.root.clone(),
        dest_root: manifest.dest.root.clone(),
        options: opts.clone(),
        nfs_connections: cfg.mover.nfs_connections.max(1) as usize,
        use_bucketed_pool: cfg.mover.use_bucketed_pool,
        use_raw_fh: cfg.mover.use_raw_fh,
        direct_commit: cfg.mover.direct_commit,
        // F12: [mover] rpc_timeout_ms flows config → MoverConfig →
        // MountOpts (both pools read it from here / from cfg.mover).
        rpc_timeout_ms: cfg.mover.rpc_timeout_ms,
        require_chown: require_chown && cap_chown,
        require_unchanged_size: cfg.copy.require_unchanged_size,
        // Apply the [batch].inflight_* profile so the mover and the
        // shard processor share the same view of size-class concurrency.
        inflight: InflightProfile {
            small: cfg.batch.inflight_small,
            medium: cfg.batch.inflight_medium,
            large: cfg.batch.inflight_large,
            large_stripe_size: parse_size(&cfg.batch.large_stripe_size).unwrap_or(4 * 1024 * 1024),
            large_stripe_depth: cfg.batch.large_stripe_depth,
        },
        host_id: host_id.clone(),
    };
    let built =
        crate::mover_factory::build(&mover_params, downgrades.clone(), fence.clone()).await?;
    let mover = built.mover;
    // Kept for the end-of-run root-mtime restore (slice 3 of
    // MTIME_PARITY_FIX).
    let pool_for_root_mtime = built.pool;
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
        live: Arc::clone(&live),
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
        clock: Arc::new(crate::heartbeat::SystemDriftClock),
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
    let coord_cfg = cfg.coord.as_ref().map(|c| {
        // An omitted job_id follows the manifest: `vamoose coord`
        // seeds its job from the same manifest under the run id.
        let mut resolved = c.clone();
        if resolved.job_id.is_none() {
            resolved.job_id = Some(manifest.run_id.clone());
        }
        resolved
    });
    let coord_handle: Option<CoordDriverHandle> = match coord_cfg.as_ref() {
        Some(c) => {
            let driver_inputs = DriverInputs {
                live: Arc::clone(&live),
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
                    tracing::info!(coord_url = %c.url,
                        job_id = c.job_id.as_deref().unwrap_or("?"),
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
        // Process stop (SIGTERM / SIGINT). Same shape as Drain: no
        // new claim, straight to the orderly shutdown. Checked after
        // the fence for the same reason run control is.
        if stop.is_cancelled() {
            tracing::info!("stop requested; not claiming another shard");
            break;
        }

        if let Some(rc) = run_control_reader.as_mut() {
            if rc.is_terminating() {
                tracing::info!(mode = ?rc.mode(),
                    "coord requested termination; exiting main loop");
                break;
            }
            if rc.is_paused() {
                tracing::info!("coord requested pause; waiting for resume");
                // A stop ends the wait; the next pass breaks above.
                tokio::select! {
                    after = rc.wait_while_paused() => {
                        tracing::info!(?after, "coord pause released");
                    }
                    _ = stop.cancelled() => {
                        tracing::info!("stop requested while paused");
                    }
                }
                continue;
            }
        }

        // Backpressure gate per DESIGN.md "Batching and backpressure": if the last
        // shard ended in poor shape, don't pile onto a struggling
        // dest. Sleep one heartbeat interval and re-check — until the
        // cooldown elapses, at which point exactly ONE probe shard is
        // allowed through so the inputs can update at all (F14:
        // without a probe, `update()` never runs again and degraded
        // is a one-way trap).
        //
        // A leftover `probing` state at the top of the loop means the
        // previous probe pass consumed the token but its shard never
        // fed `update()` (nothing claimable, lost claim race, or a
        // shard-fatal decode error). That pass told us nothing about
        // destination health — return the token so the probe retries
        // instead of wedging in `probing` forever.
        if backpressure.is_probing() {
            backpressure.reset_probe();
        }
        if let Some(reason) = backpressure.degraded() {
            if backpressure.try_claim_probe() {
                {
                    let mut p = progress.write().await;
                    p.status = format!("degraded:{}:probing", reason.as_str());
                }
                tracing::info!(
                    reason = reason.as_str(),
                    "backpressure cooldown elapsed; probing with a single shard",
                );
                // Fall through: this pass claims and processes exactly
                // one shard; its `update()` decides recovery (healthy)
                // vs re-degrade with a doubled cooldown (unhealthy).
            } else {
                {
                    let mut p = progress.write().await;
                    p.status = format!(
                        "degraded:{}:{}",
                        reason.as_str(),
                        backpressure.probe_phase().unwrap_or("probe-pending"),
                    );
                }
                tracing::warn!(
                    reason = reason.as_str(),
                    last_failure_pct = backpressure.last_failure_pct(),
                    last_throughput_mb_s = backpressure.last_throughput_mb_s(),
                    "worker degraded; sleeping before next probe window",
                );
                sleep_unless_stopped(&stop, Duration::from_secs(cfg.worker.heartbeat_sec)).await;
                // Don't continue around — keep evaluating, but don't
                // spin claims if still degraded after sleep.
                continue;
            }
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
            // F42: a transient LIST/GET blip inside a scan pass must
            // not take the worker down — retry under the budget. All
            // borrows go through the state tuple (see retry_transient).
            let heartbeat_sec = cfg.worker.heartbeat_sec;
            let mut st = (&*s3, &manifest, &mut claim_body_cache, &skip_shards);
            retry_transient("shard scan", retry_budget, heartbeat_sec, &mut st, |st| {
                Box::pin(scan_shards(st.0, st.1, lease, heartbeat_sec, st.2, st.3))
            })
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
            sleep_unless_stopped(&stop, Duration::from_secs(cfg.worker.heartbeat_sec)).await;
            continue;
        };

        let (etag, record) = match target {
            ClaimTarget::Free { shard } => {
                // F42: transient store errors on the acquire back off
                // and retry (bounded by the budget) rather than exit.
                // The shard is NOT skip-set — nothing is wrong with
                // it. The atom itself is untouched; only the call
                // site is wrapped.
                let mut st = (&*s3, shard.as_str(), host_id.as_str());
                match retry_transient(
                    "claim acquire",
                    retry_budget,
                    cfg.worker.heartbeat_sec,
                    &mut st,
                    |st| Box::pin(claim::try_acquire(st.0, st.1, st.2)),
                )
                .await?
                {
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
                // F42: same treatment as the acquire site.
                let mut st = (&*s3, shard.as_str(), stale_etag.as_str(), host_id.as_str());
                match retry_transient(
                    "claim reclaim",
                    retry_budget,
                    cfg.worker.heartbeat_sec,
                    &mut st,
                    |st| Box::pin(claim::reclaim(st.0, st.1, st.2, st.3, new_epoch)),
                )
                .await?
                {
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
        live.reset();

        // Download the parquet shard to scratch and verify its etag
        // against the manifest (F40). F42: neither failure may take
        // the worker down with the claim still held — route through
        // handle_process_error, which classifies per F13: a typed
        // Error::S3/Io transport failure is WorkerLocal (release +
        // skip + backoff; a healthy peer takes the shard), the F40
        // empty-etag errors are untyped and default to Fatal, and a
        // ManifestChanged mismatch is WorkerLocal (release-and-skip —
        // correct for a manifest swap mid-run).
        let scratch = cfg.shard.local_scratch.join(&shard_filename);
        if let Err(e) = fetch_shard_index(
            s3.download_to(&layout::index_key(&shard_filename), &scratch),
            &manifest,
            &shard_filename,
        )
        .await
        {
            // Best-effort cleanup of a partial download. A missing
            // file (GET failed before creating it) is not noteworthy.
            if let Err(rm) = tokio::fs::remove_file(&scratch).await {
                tracing::debug!(
                    error = ?rm,
                    scratch = %scratch.display(),
                    "scratch cleanup failed (post-download-error)",
                );
            }
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

        // Stamp the current shard onto both sinks so records carry
        // the right shard name (the mover doesn't otherwise know).
        downgrades.set_current_shard(shard_filename.clone());
        failures.set_current_shard(shard_filename.clone());

        // M3: spawn a fresh processor with the worker's shared
        // limiter/sinks/throughput so all shards report into one
        // throughput counter and one failure log per host.
        let mut processor = ShardProcessor {
            dir_restamp: Vec::new(),
            live: Arc::clone(&live),
            mover: Arc::clone(&mover),
            fence: fence.clone(),
            budget,
            inflight: inflight.clone(),
            failures: failures.clone(),
            throughput: throughput.clone(),
            fsid_fallback_warned: false,
            emitter: event_emitter.clone(),
            run_control: coord_handle.as_ref().map(|h| h.run_control.subscribe()),
            stop: stop.clone(),
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
        // Reset the live pending set FIRST: a heartbeat tick landing
        // between reset and merge briefly under-counts; the reverse
        // order would double-count.
        live.reset();
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

        if outcome.interrupted {
            // Process stop landed mid-shard. Everything the batches
            // committed is durable; hand the shard back NOW (owner
            // DELETE If-Match, same atom as the worker-local error
            // path) so a peer claims it as Free instead of waiting
            // for the lease / cross-check window. Same R4 ordering as
            // complete(): clear the held-claim cell first so the
            // heartbeat cannot mistake our own delete for a reclaim.
            let held_etag = {
                let mut g = current.lock().await;
                let e = match g.as_ref() {
                    Some(c) if c.shard == shard_filename => c.etag.clone(),
                    _ => etag.clone(),
                };
                *g = None;
                e
            };
            tracing::info!(
                shard = %shard_filename,
                rows_done = outcome.files_ok + outcome.files_failed,
                rows_total = outcome.rows_total,
                "stop requested; releasing the shard in hand for a peer",
            );
            release_claim_by_owner(&*s3, &shard_filename, &held_etag).await;
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
    // Capture the run outcome for the hard-exit watchdog NOW, before
    // the unconditional `fence.trip()` below erases the distinction
    // between "fenced mid-run" and "fence tripped as part of a normal
    // shutdown".
    let run_outcome = run_outcome_for(fence.is_valid(), fence.cause(), stop.is_cancelled());
    tracing::debug!(target: "shutdown", ?run_outcome, "section 7 entered");
    {
        tracing::debug!(target: "shutdown", "acquiring progress write lock");
        let mut p = progress.write().await;
        tracing::debug!(target: "shutdown", "got progress write lock");
        p.status = "exiting".into();
    }
    tracing::debug!(target: "shutdown", "released progress write lock");
    // Cancel the coord driver so it stops heartbeating and the
    // task joins. Bounded await — if the HTTP layer is wedged the
    // worker should still get to clean exit; the driver leaks at
    // process termination, which is benign (no shared resources).
    if let Some(handle) = coord_handle {
        coord_cancel.cancel();
        tracing::debug!(target: "shutdown", "awaiting coord_driver with 5s timeout");
        match tokio::time::timeout(std::time::Duration::from_secs(5), handle.task).await {
            Ok(Ok(Ok(()))) => tracing::info!("coord_driver: clean exit"),
            Ok(Ok(Err(e))) => tracing::warn!(error = %e, "coord_driver returned error"),
            Ok(Err(e)) => tracing::warn!(join_error = %e, "coord_driver join failed"),
            Err(_) => tracing::warn!("coord_driver did not exit within 5s; dropping handle"),
        }
        tracing::debug!(target: "shutdown", "coord_driver done");
    }
    // Drop the held claim so the heartbeat stops refreshing.
    {
        tracing::debug!(target: "shutdown", "acquiring current lock");
        let mut g = current.lock().await;
        tracing::debug!(target: "shutdown", "got current lock");
        *g = None;
    }
    tracing::debug!(target: "shutdown", "released current lock");
    // Best-effort: close the fence to wake the heartbeat loop out of
    // its tick (it writes the final "exiting" progress record on the
    // way out). `close_for_shutdown` logs at info — this is not a
    // self-fence, and a real one (already tripped) keeps its reason.
    fence.close_for_shutdown();
    tracing::debug!(target: "shutdown", "fence closed");
    // Bound the heartbeat-join. If the heartbeat task is wedged (e.g.
    // a stale S3 connection-pool entry blocking write_progress),
    // hb_handle.await would hang the worker process forever. Abort on
    // timeout; the runtime drop reaps the task on its own schedule.
    let mut hb_handle = hb_handle;
    tracing::debug!(target: "shutdown", "awaiting hb_handle with 5s timeout");
    if tokio::time::timeout(std::time::Duration::from_secs(5), &mut hb_handle)
        .await
        .is_err()
    {
        tracing::debug!(target: "shutdown", "hb_handle timeout - aborting");
        tracing::warn!("heartbeat task did not exit within 5s of shutdown; aborting it",);
        hb_handle.abort();
    }
    tracing::debug!(target: "shutdown", "hb_handle done");

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
    tracing::debug!(target: "shutdown", "spawning hard-exit watchdog");
    // F17: the exit code and the stderr line are computed HERE, while
    // allocation is still safe, and moved into the thread — the thread
    // itself must stay allocation-free past the sleep.
    let watchdog_code = watchdog_exit_code(run_outcome);
    let watchdog_msg = format!(
        "watchdog: forcing process exit {watchdog_code} \
         (shutdown took >{}s; run outcome: {run_outcome:?})\n",
        HARD_EXIT_WATCHDOG_INTERVAL.as_secs(),
    );
    std::thread::spawn(move || {
        std::thread::sleep(HARD_EXIT_WATCHDOG_INTERVAL);
        // Direct kernel syscalls — bypass Rust stdio (which can be
        // buffered or wedged during shutdown) and std::process::exit
        // (which runs atexit handlers that may touch the same C
        // library state that's hanging us). _exit(2) terminates the
        // process at kernel level immediately.
        unsafe {
            libc::write(
                2,
                watchdog_msg.as_ptr() as *const libc::c_void,
                watchdog_msg.len(),
            );
            libc::_exit(watchdog_code);
        }
    });
    tracing::debug!(target: "shutdown", "watchdog spawned, returning run outcome");

    Ok(run_outcome)
}

/// How the main loop ended, as known at section 7 of [`run`] (the
/// watchdog-arm point). Error returns (`?`) bypass section 7 entirely
/// — the process exit paths map them to code 1 — so the only
/// non-error outcomes are "clean" (all shards terminal, or
/// coord-requested drain/cancel), "interrupted" (SIGTERM / SIGINT)
/// and "fenced". Was the internal `WatchdogRunOutcome`; promoted to
/// the function's return value so callers (`main.rs`, `vamoose
/// worker`) can map "fenced" to its dedicated process exit code via
/// [`exit_code_for_outcome`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RunOutcome {
    /// Main loop exited without a fence trip.
    Clean,
    /// A process stop (SIGTERM / SIGINT) ended the run: no fence
    /// trip, the shard in hand (if any) was released for a peer.
    Interrupted,
    /// Worker self-fenced mid-run because its claim was taken (or its
    /// clock cannot be trusted). Needs an operator.
    Fenced,
    /// Worker self-fenced mid-run because the store was unreachable
    /// for a full lease window ([`FenceCause::StoreUnreachable`]).
    /// The shard was surrendered defensively; once the store is back
    /// a fresh worker simply rejoins, so this is restartable.
    StoreUnreachable,
}

/// Section-7 outcome from the facts known there. A fence trip wins
/// over a stop request: a worker that was fenced while stopping still
/// needs the operator to notice — unless the fence was the defensive
/// store-unreachable kind, which is a restart, not an alert.
fn run_outcome_for(
    fence_valid: bool,
    fence_cause: Option<FenceCause>,
    stop_requested: bool,
) -> RunOutcome {
    if !fence_valid {
        match fence_cause {
            Some(FenceCause::StoreUnreachable) => RunOutcome::StoreUnreachable,
            _ => RunOutcome::Fenced,
        }
    } else if stop_requested {
        RunOutcome::Interrupted
    } else {
        RunOutcome::Clean
    }
}

/// Install the SIGTERM / SIGINT listener behind `stop`. The first
/// signal cancels the token; later ones are logged only — the batch
/// in hand always finishes (SIGKILL is the escape hatch, and systemd
/// sends it at `TimeoutStopSec`). A listener that cannot be
/// installed is logged and the worker runs without one, exactly as
/// before.
fn spawn_signal_listener(stop: CancellationToken) {
    use tokio::signal::unix::{signal, SignalKind};
    let (mut term, mut int) = match (
        signal(SignalKind::terminate()),
        signal(SignalKind::interrupt()),
    ) {
        (Ok(t), Ok(i)) => (t, i),
        (Err(e), _) | (_, Err(e)) => {
            tracing::warn!(error = ?e, "signal listener not installed; a stop signal will kill the worker");
            return;
        }
    };
    tokio::spawn(async move {
        loop {
            let which = tokio::select! {
                r = term.recv() => match r { Some(()) => "SIGTERM", None => break },
                r = int.recv() => match r { Some(()) => "SIGINT", None => break },
            };
            if stop.is_cancelled() {
                tracing::warn!(
                    signal = which,
                    "stop already in progress; the batch in hand still finishes (SIGKILL to abandon it)",
                );
                continue;
            }
            tracing::info!(
                signal = which,
                "stop requested; finishing the batch in hand, releasing the claim, then exiting",
            );
            stop.cancel();
        }
    });
}

/// `tokio::time::sleep` that returns early when `stop` is cancelled,
/// so idle and backpressure waits do not delay a requested stop by a
/// heartbeat interval.
async fn sleep_unless_stopped(stop: &CancellationToken, dur: Duration) {
    tokio::select! {
        _ = tokio::time::sleep(dur) => {}
        _ = stop.cancelled() => {}
    }
}

/// Hand a held claim back as Free via the owner's DELETE If-Match —
/// the same atom `claim::complete` starts with, so this never
/// creates a new terminal state. Outcomes are logged, never fatal:
/// on an error the claim stays Active and a peer recovers it through
/// the lease / progress cross-check path.
async fn release_claim_by_owner(store: &dyn ClaimStore, shard: &str, held_etag: &str) {
    let claim_key = layout::claim_key(shard);
    match store.delete_if_match(&claim_key, held_etag).await {
        Ok(DeleteOutcome::Deleted) => {
            tracing::info!(shard = %shard, "claim released; shard is Free for peers");
        }
        Ok(DeleteOutcome::EtagMismatch | DeleteOutcome::NotFound) => {
            // Someone already reclaimed or replaced the claim —
            // nothing of ours left to release.
            tracing::warn!(
                shard = %shard,
                "claim already replaced while releasing; nothing to do",
            );
        }
        Err(release_err) => {
            tracing::warn!(
                error = ?release_err,
                shard = %shard,
                "claim release errored; shard stays Active until a \
                 peer reclaims via lease/cross-check",
            );
        }
    }
}

/// Process exit code for a [`run`] that returned `Ok(outcome)` — the
/// normal (non-wedged, non-error) exit path.
///
/// A fenced run shuts down cleanly but must NOT exit 0: supervisors
/// need to tell "fenced — alert/restart" apart from "migration
/// complete". Codes already spoken for and left untouched: 0 = clean
/// completion, 1 = run error (the `Err` path in `main.rs` /
/// anyhow-from-`vamoose`), 2 = shutdown wedged (`watchdog_exit_code`,
/// also clap usage errors). Fenced therefore gets the dedicated
/// code 3. An interrupted run (SIGTERM / SIGINT) exits 0: the stop
/// was asked for, the shard was handed back, nothing needs an alert
/// — and the unit's `SuccessExitStatus=0` / `Restart=on-failure`
/// pairing relies on it.
///
/// A store-unreachable fence exits 4: the worker gave its shard up
/// because it could not reach S3 for a full lease window, not because
/// anyone took it. The unit restarts on 4 (only 3 is in
/// `RestartPreventExitStatus`), so a network outage costs one lease
/// window plus `RestartSec` per worker instead of the rest of the
/// migration — the 600M retest lost two of three workers for 18 h to
/// exactly that.
pub fn exit_code_for_outcome(outcome: RunOutcome) -> i32 {
    match outcome {
        RunOutcome::Clean | RunOutcome::Interrupted => 0,
        RunOutcome::Fenced => 3,
        RunOutcome::StoreUnreachable => 4,
    }
}

/// Exit code the hard-exit watchdog passes to `libc::_exit` when it
/// fires (F17).
///
/// The watchdog only fires when shutdown wedges past its 5s deadline,
/// which is a failure regardless of the run's own outcome — so this
/// never returns 0 (the pre-F17 bug: `_exit(0)` made supervisors see
/// success for wedged fenced runs). Decision: every wedge exits 2,
/// keeping 0 (clean) and 1 (run error) unambiguous on the normal
/// `main.rs` exit path and making "exit 2" a single supervisor signal
/// for "shutdown wedged"; the run outcome is carried in the watchdog's
/// stderr line, not the code.
pub(crate) fn watchdog_exit_code(outcome: RunOutcome) -> i32 {
    match outcome {
        RunOutcome::Clean
        | RunOutcome::Interrupted
        | RunOutcome::Fenced
        | RunOutcome::StoreUnreachable => 2,
    }
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

/// Poll cadence while the bucket has no `manifest.json` yet.
const MANIFEST_WAIT_POLL_SEC: u64 = 15;

/// `Ok(None)` means the bucket is reachable but has no manifest yet;
/// the caller waits rather than failing.
async fn load_manifest(s3: &S3Client) -> anyhow::Result<Option<Manifest>> {
    let Some((body, _etag)) = s3.get(layout::MANIFEST_KEY).await? else {
        return Ok(None);
    };
    let m: Manifest = serde_json::from_slice(&body)?;
    Ok(Some(m))
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
    // F40: an empty etag on either side means the integrity check
    // CANNOT run — that is an error, never a silent pass. These are
    // deliberately plain (untyped) anyhow errors, matching the
    // missing-shard style above: `classify_shard_error`'s conservative
    // untyped default routes them to Fatal, which is correct — an
    // absent/empty etag is a property of the manifest (or the store),
    // not of this worker, and would re-occur for any worker that
    // reclaimed the shard.
    if expected.etag.is_empty() {
        anyhow::bail!(
            "shard {shard_filename}: manifest etag is missing (empty); cannot verify \
             shard integrity — re-index or repair manifest.json",
        );
    }
    if actual_etag.is_empty() {
        anyhow::bail!(
            "shard {shard_filename}: download returned an empty etag; cannot verify \
             shard integrity against the manifest",
        );
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
                        // Progress-liveness cross-check — AUTHORITATIVE
                        // for Active claims. `claimed_utc` is written
                        // once at acquire and never advances (refresh
                        // is HEAD-and-compare), so `stale_by_lease` is
                        // true for EVERY shard held longer than the
                        // lease window; on its own it is not evidence
                        // of a dead owner. Acting on it alone stole a
                        // live, heartbeating owner's shard on the
                        // 2026-08-22 600M rig run (dual-writer window
                        // closed only by the owner's self-fence).
                        //
                        // The per-host progress file is the real
                        // heartbeat: a fresh, matching-`held_etag`
                        // record VETOES reclaim regardless of claim
                        // age; a stale/absent/mismatched one makes the
                        // shard reclaimable within ~2× the owner's
                        // heartbeat_sec (see
                        // docs/work-items/PROGRESS_LIVENESS_CROSS_CHECK.md).
                        // `stale_by_lease` survives only as the
                        // conservative fallback when the progress
                        // object cannot be read at all.
                        //
                        // Only fetch when we'd act on the result — and
                        // dedupe by host within a single pass so M
                        // active shards owned by N hosts cost at most
                        // N progress GETs.
                        let stale = if next.is_none() && !skipped {
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
                                // Progress unreadable: fall back to the
                                // lease window as the only (coarse)
                                // dead-owner signal.
                                ProgressFetch::Error => stale_by_lease,
                            }
                        } else {
                            false
                        };
                        if stale && next.is_none() && !skipped {
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
// F42: transient-store retry (scan / manifest / acquire paths)
// =============================================================================

/// F42: consecutive-failure budget for transient store errors on the
/// scan / manifest / acquire paths. Same shape as the heartbeat's R6
/// budget (`heartbeat.rs`): one lease window's worth of
/// heartbeat-spaced attempts, floored at 1 so absurd configs stay
/// sane. Past this many CONSECUTIVE failures the worker gives up and
/// surfaces the error — a worker must not spin forever on a dead
/// bucket, and by then a peer has been unable to see our heartbeats
/// for a full lease window anyway.
pub fn transient_retry_budget(lease_timeout_sec: u64, heartbeat_sec: u64) -> u64 {
    (lease_timeout_sec / heartbeat_sec.max(1)).max(1)
}

/// One attempt's future, as returned by a [`retry_transient`] closure.
/// Boxed so the closure can lend its `&mut S` state to the future
/// (needed for `scan_shards`' `&mut ClaimBodyCache`).
pub type AttemptFuture<'a, T, E> =
    std::pin::Pin<Box<dyn std::future::Future<Output = Result<T, E>> + Send + 'a>>;

/// F42: drive `attempt` until it succeeds or `budget` consecutive
/// failures accumulate, sleeping the jittered contention backoff
/// (`backoff_after_lost_race`: `heartbeat_sec/4` base + jitter,
/// paused-time friendly) between attempts. Every error is treated as
/// transient here — the wrapped ops (LIST / GET / conditional PUT via
/// the claim atoms) either succeed, return a typed outcome (e.g.
/// `Contended`), or fail on the store/transport path; the budget
/// bounds the worst case. Success returns immediately, so the budget
/// only ever counts CONSECUTIVE failures (mirror of the R6 counter
/// reset on a successful HEAD).
///
/// This wraps CALL SITES only — never the claim atoms themselves.
///
/// Usage note: every reference the attempt future needs must be
/// threaded through `state` (a tuple works) — the closure itself may
/// only capture `Copy` values. A closure that captures references
/// from its environment cannot satisfy the higher-ranked bound (the
/// returned future would have to outlive an arbitrary `'a`), and
/// rustc surfaces that as "borrowed for `'static`" at the call site.
pub async fn retry_transient<S, T, E, F>(
    op: &str,
    budget: u64,
    heartbeat_sec: u64,
    state: &mut S,
    mut attempt: F,
) -> Result<T, E>
where
    S: ?Sized,
    E: std::fmt::Debug,
    F: for<'a> FnMut(&'a mut S) -> AttemptFuture<'a, T, E>,
{
    let budget = budget.max(1);
    let mut consec: u64 = 0;
    loop {
        match attempt(state).await {
            Ok(v) => return Ok(v),
            Err(e) => {
                consec += 1;
                if consec >= budget {
                    tracing::error!(
                        op,
                        consecutive_failures = consec,
                        budget,
                        error = ?e,
                        "transient-error budget exhausted; surfacing the error",
                    );
                    return Err(e);
                }
                tracing::warn!(
                    op,
                    consecutive_failures = consec,
                    budget,
                    error = ?e,
                    "transient store error; backing off before retry",
                );
                backoff_after_lost_race(heartbeat_sec).await;
            }
        }
    }
}

/// F42 download seam: await the shard-index download and run the F40
/// etag verification on its result. The downloader is passed as a
/// future (production: `S3Client::download_to`, an `S3Client`-only
/// method unreachable via `FakeStore`; tests: a canned result) so the
/// composed error path is testable without S3 — the S3-backed wiring
/// in `run()` stays untested, like the F29 classifiers.
///
/// Download errors are NOT retried in place: the caller routes them
/// to [`handle_process_error`], which classifies per F13 — a typed
/// `Error::S3` transport failure is WorkerLocal (release + skip +
/// backoff; a healthy peer takes the shard), while the F40 empty-etag
/// errors are untyped and default to Fatal.
pub async fn fetch_shard_index<F>(
    download: F,
    manifest: &Manifest,
    shard_filename: &str,
) -> anyhow::Result<()>
where
    F: std::future::Future<Output = Result<String, CoreError>>,
{
    let actual_etag = download.await?;
    verify_shard_etag(manifest, shard_filename, &actual_etag)
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
        //
        // F12 carve-out: an RPC-timeout-shaped message is transient
        // by definition — it says nothing about the shard bytes and
        // must never poison the shard with a terminal `Failed`. See
        // `error_is_timeout_shaped`.
        CoreError::Other(inner) => {
            if error_is_timeout_shaped(&format!("{inner:#}")) {
                ShardErrorClass::WorkerLocal
            } else {
                ShardErrorClass::Fatal
            }
        }

        // Worker-local: nothing about the shard itself is wrong.
        //  - SchemaVersionMismatch: this binary is stale for the
        //    shard's format_version; a current peer reads it fine.
        //  - Io: local scratch (EIO/ENOMEM reading the downloaded
        //    parquet) — host-specific.
        //  - S3: transport to the bucket — host/network-specific.
        //    (F42: scan/manifest/acquire sites retry these via
        //    `retry_transient`; download failures land here through
        //    `fetch_shard_index` → `handle_process_error`.)
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

/// F12: true iff an error message carries libnfs's RPC-timeout
/// signature. The pinned libnfs (`libnfs-6.0.2-148-gdc7e6f8`)
/// surfaces timed-out RPCs two ways:
///
/// - nfs-level callbacks fire with `-EINTR` and the detail string
///   `"Command timed out"` (`lib/nfs_v3.c:check_nfs3_error`);
/// - the rpc-level error string is lowercase `"command timed out"`
///   (`lib/socket.c:rpc_timeout_scan`, status `RPC_STATUS_TIMEOUT`).
///
/// Matched case-insensitively, plus the status tag itself in case a
/// future wrapper surfaces it verbatim. Deliberately narrow: only
/// the strings the linked library actually produces, so ordinary
/// decode errors keep the conservative `Other → Fatal` default.
fn error_is_timeout_shaped(msg: &str) -> bool {
    let lower = msg.to_ascii_lowercase();
    lower.contains("command timed out") || msg.contains("RPC_STATUS_TIMEOUT")
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
            // owner — no new terminal state, no PUT If-Match. On an
            // error the claim stays Active for lease / cross-check
            // recovery; never fall back to fail() here.
            release_claim_by_owner(ctx.store, ctx.shard_filename, &held_etag).await;
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
            latency: None,
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
mod verify_shard_etag_tests {
    //! F40 acceptance tests (`docs/work-items/WORKER_RESILIENCE.md`
    //! item 1): an empty etag on either side of the shard integrity
    //! check must be an error, never a silent pass. Tests 1–2 were
    //! written first and observed red (the old code returned `Ok(())`
    //! when either etag was empty, "best-effort" style).

    use super::verify_shard_etag;
    use migration_core::errors::Error as CoreError;
    use migration_core::records::{
        Endpoint, EndpointKind, Manifest, MigrationOptions, ShardEntry, RUN_FORMAT_VERSION,
    };
    use migration_core::time::UtcTime;

    const SHARD: &str = "part-0001.parquet";

    fn manifest_with_etag(etag: &str) -> Manifest {
        Manifest {
            format_version: RUN_FORMAT_VERSION,
            run_id: "f40-verify-etag".into(),
            created_utc: UtcTime::now(),
            shards: vec![ShardEntry {
                key: format!("index/{SHARD}"),
                rows: 10,
                bytes: 4096,
                etag: etag.to_string(),
            }],
            total_rows: 10,
            source: Endpoint {
                kind: EndpointKind::Nfs,
                url: "nfs://src/export".into(),
                root: "/".into(),
            },
            dest: Endpoint {
                kind: EndpointKind::Nfs,
                url: "nfs://dst/export".into(),
                root: "/".into(),
            },
            options: MigrationOptions::default(),
        }
    }

    /// F40 acceptance test 1 (red before fix): a manifest entry with
    /// `etag: ""` and a non-empty download etag is an error. The
    /// message names the shard and says the MANIFEST etag is missing,
    /// so an operator can tell it apart from an etag mismatch.
    #[test]
    fn empty_manifest_etag_is_an_error() {
        let manifest = manifest_with_etag("");
        let err = verify_shard_etag(&manifest, SHARD, "etag-download-1")
            .expect_err("empty manifest etag must not bypass verification");
        let msg = format!("{err}");
        assert!(msg.contains(SHARD), "error must name the shard: {msg}");
        assert!(
            msg.contains("manifest etag") && (msg.contains("missing") || msg.contains("empty")),
            "error must say the manifest etag is missing/empty: {msg}",
        );
        // Classification pin: this is a plain (untyped) anyhow error,
        // so `classify_shard_error`'s conservative default routes it
        // to Fatal — correct, because an empty manifest etag recurs
        // for any worker.
        assert!(
            err.downcast_ref::<CoreError>().is_none(),
            "empty-etag error must NOT reuse a typed CoreError \
             (it must flow to Fatal via the untyped default)",
        );
    }

    /// F40 acceptance test 2 (red before fix): symmetric — the store
    /// returning an empty etag for the downloaded object is an error,
    /// with a message distinguishable from test 1's.
    #[test]
    fn empty_download_etag_is_an_error() {
        let manifest = manifest_with_etag("etag-manifest-1");
        let err = verify_shard_etag(&manifest, SHARD, "")
            .expect_err("empty download etag must not bypass verification");
        let msg = format!("{err}");
        assert!(msg.contains(SHARD), "error must name the shard: {msg}");
        assert!(
            msg.contains("download") && (msg.contains("missing") || msg.contains("empty")),
            "error must say the DOWNLOAD etag is missing/empty: {msg}",
        );
        assert!(
            !msg.contains("manifest etag is missing"),
            "download-side message must be distinguishable from the manifest-side one: {msg}",
        );
        assert!(err.downcast_ref::<CoreError>().is_none());
    }

    /// F40 acceptance test 3a: matching non-empty etags still pass.
    #[test]
    fn matching_etags_pass() {
        let manifest = manifest_with_etag("etag-abc");
        verify_shard_etag(&manifest, SHARD, "etag-abc").expect("matching etags must verify");
    }

    /// F40 acceptance test 3b: a mismatch stays the typed
    /// `Error::ManifestChanged` (which F13 classifies WorkerLocal —
    /// release-and-skip, correct for a manifest swap mid-run).
    #[test]
    fn mismatched_etags_fail_with_manifest_changed() {
        let manifest = manifest_with_etag("etag-abc");
        let err = verify_shard_etag(&manifest, SHARD, "etag-xyz")
            .expect_err("mismatched etags must fail");
        match err.downcast_ref::<CoreError>() {
            Some(CoreError::ManifestChanged { expected, actual }) => {
                assert_eq!(expected, "etag-abc");
                assert_eq!(actual, "etag-xyz");
            }
            other => panic!("expected ManifestChanged, got {other:?}"),
        }
    }

    /// Existing behavior pin: a shard that isn't in the manifest at
    /// all is an error naming the shard.
    #[test]
    fn unknown_shard_is_an_error() {
        let manifest = manifest_with_etag("etag-abc");
        let err = verify_shard_etag(&manifest, "part-9999.parquet", "etag-abc")
            .expect_err("unknown shard must fail");
        assert!(format!("{err}").contains("part-9999.parquet"));
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

    // ------------------------------------------------------------------
    // F17: shutdown-watchdog exit code
    // ------------------------------------------------------------------

    /// The watchdog only ever fires when shutdown wedges (>5s past the
    /// point where all durable state is committed). A wedged shutdown
    /// is a failure from the supervisor's perspective regardless of how
    /// the run itself went, so the watchdog exit code is 2 for every
    /// outcome — never 0. The normal (non-wedged) exit path in
    /// `main.rs` keeps 0 (clean) / 1 (run error); "clean → 0" is not
    /// the watchdog's business because the watchdog firing at all means
    /// the shutdown was not clean.
    #[test]
    fn watchdog_exit_code_never_reports_success() {
        use super::{watchdog_exit_code, RunOutcome, HARD_EXIT_WATCHDOG_INTERVAL};

        assert_eq!(
            HARD_EXIT_WATCHDOG_INTERVAL,
            std::time::Duration::from_secs(5)
        );

        // Wedged after a clean run → 2 (was the F17 bug: _exit(0)).
        assert_eq!(watchdog_exit_code(RunOutcome::Clean), 2);
        // A stop that then wedges is still a wedge.
        assert_eq!(watchdog_exit_code(RunOutcome::Interrupted), 2);
        // Wedged after a fenced run → 2. Decision (doc allowed 2 or 1):
        // 2 for every wedge, so "exit 2" is a single unambiguous
        // supervisor signal for "shutdown wedged; watchdog fired"; the
        // run outcome is carried in the watchdog's stderr line instead.
        assert_eq!(watchdog_exit_code(RunOutcome::Fenced), 2);
        assert_eq!(watchdog_exit_code(RunOutcome::StoreUnreachable), 2);
    }

    // ------------------------------------------------------------------
    // Fenced-run exit code (BETA_POLISH_BATCH Item 2)
    // ------------------------------------------------------------------

    /// A run that ended because the worker fenced must exit with the
    /// dedicated code 3 so supervisors can tell "fenced —
    /// alert/restart" apart from "migration complete" (0) and from
    /// plain run errors (1).
    #[test]
    fn fenced_outcome_maps_to_exit_3() {
        use super::{exit_code_for_outcome, RunOutcome};
        assert_eq!(
            exit_code_for_outcome(RunOutcome::Fenced),
            3,
            "fenced clean shutdown must exit with the dedicated code 3",
        );
    }

    /// Clean completion stays 0, and no existing code is renumbered:
    /// 1 (run error) and 2 (wedged shutdown / watchdog) remain out of
    /// bounds for the normal-outcome mapping.
    #[test]
    fn clean_outcome_maps_to_exit_0_and_existing_codes_are_untouched() {
        use super::{exit_code_for_outcome, watchdog_exit_code, RunOutcome};
        assert_eq!(exit_code_for_outcome(RunOutcome::Clean), 0);
        for outcome in [
            RunOutcome::Clean,
            RunOutcome::Interrupted,
            RunOutcome::Fenced,
            RunOutcome::StoreUnreachable,
        ] {
            let code = exit_code_for_outcome(outcome);
            assert_ne!(code, 1, "1 stays reserved for run errors: {outcome:?}");
            assert_ne!(
                code, 2,
                "2 stays reserved for wedged shutdowns: {outcome:?}"
            );
        }
        // The watchdog mapping is untouched by the fenced-exit work.
        assert_eq!(watchdog_exit_code(RunOutcome::Clean), 2);
        assert_eq!(watchdog_exit_code(RunOutcome::Fenced), 2);
    }

    // ------------------------------------------------------------------
    // Process stop (SIGTERM / SIGINT)
    // ------------------------------------------------------------------

    /// A deliberate stop is not a failure: exit 0, so
    /// `Restart=on-failure` does not resurrect a worker the operator
    /// just stopped and `SuccessExitStatus=0` reads as success.
    #[test]
    fn interrupted_outcome_maps_to_exit_0() {
        use super::{exit_code_for_outcome, RunOutcome};
        assert_eq!(exit_code_for_outcome(RunOutcome::Interrupted), 0);
    }

    /// Section-7 outcome: the fence wins over a stop request (a worker
    /// fenced while stopping still exits 3), a stop without a fence is
    /// `Interrupted`, and neither is `Clean`.
    #[test]
    fn run_outcome_prefers_fence_over_stop() {
        use super::{run_outcome_for, FenceCause, RunOutcome};
        assert_eq!(run_outcome_for(true, None, false), RunOutcome::Clean);
        assert_eq!(run_outcome_for(true, None, true), RunOutcome::Interrupted);
        let lost = Some(FenceCause::OwnershipLost);
        assert_eq!(run_outcome_for(false, lost, false), RunOutcome::Fenced);
        assert_eq!(run_outcome_for(false, lost, true), RunOutcome::Fenced);
        // A tripped fence with no recorded cause is still a fence.
        assert_eq!(run_outcome_for(false, None, false), RunOutcome::Fenced);
    }

    /// A fence tripped because the store was unreachable is its own
    /// outcome with its own exit code (4): restartable, unlike a
    /// genuinely lost claim (3), and still never 0.
    #[test]
    fn store_unreachable_fence_is_restartable_not_an_alert() {
        use super::{exit_code_for_outcome, run_outcome_for, FenceCause, RunOutcome};
        let gone = Some(FenceCause::StoreUnreachable);
        assert_eq!(
            run_outcome_for(false, gone, false),
            RunOutcome::StoreUnreachable
        );
        assert_eq!(
            run_outcome_for(false, gone, true),
            RunOutcome::StoreUnreachable
        );
        assert_eq!(
            run_outcome_for(false, Some(FenceCause::ClockJump), false),
            RunOutcome::Fenced
        );
        assert_eq!(exit_code_for_outcome(RunOutcome::StoreUnreachable), 4);
        assert_ne!(exit_code_for_outcome(RunOutcome::StoreUnreachable), 3);
    }

    /// The idle / backpressure sleeps return as soon as a stop is
    /// requested instead of running out the heartbeat interval.
    #[tokio::test(start_paused = true)]
    async fn sleep_unless_stopped_wakes_on_stop() {
        use super::sleep_unless_stopped;
        use tokio_util::sync::CancellationToken;

        let stop = CancellationToken::new();
        let waiter = {
            let stop = stop.clone();
            tokio::spawn(async move {
                sleep_unless_stopped(&stop, std::time::Duration::from_secs(3600)).await;
            })
        };
        tokio::task::yield_now().await;
        stop.cancel();
        tokio::time::timeout(std::time::Duration::from_secs(1), waiter)
            .await
            .expect("sleep must end on stop, not after the full interval")
            .unwrap();

        // Without a stop, the full interval elapses (paused time).
        let stop = CancellationToken::new();
        let before = tokio::time::Instant::now();
        sleep_unless_stopped(&stop, std::time::Duration::from_secs(30)).await;
        assert_eq!(
            tokio::time::Instant::now().duration_since(before),
            std::time::Duration::from_secs(30)
        );
    }

    /// An owner release deletes the claim so the shard reads as Free;
    /// a claim that was already replaced is left alone.
    #[tokio::test]
    async fn release_claim_by_owner_frees_only_the_held_etag() {
        use super::release_claim_by_owner;
        use migration_core::claim::test_util::FakeStore;
        use migration_core::claim::{self, AcquireOutcome, ClaimStore};
        use migration_core::layout;

        let store = FakeStore::new();
        let AcquireOutcome::Acquired { etag, .. } =
            claim::try_acquire(&store, "shard-0001.parquet", "host-a")
                .await
                .unwrap()
        else {
            panic!("fresh shard must be claimable");
        };
        // A stale etag does not touch the live claim.
        release_claim_by_owner(&store, "shard-0001.parquet", "\"not-ours\"").await;
        assert!(store
            .get(&layout::claim_key("shard-0001.parquet"))
            .await
            .unwrap()
            .is_some());
        // The owner's etag frees it.
        release_claim_by_owner(&store, "shard-0001.parquet", &etag).await;
        assert!(store
            .get(&layout::claim_key("shard-0001.parquet"))
            .await
            .unwrap()
            .is_none());
    }
}
