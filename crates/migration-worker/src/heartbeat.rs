//! Background heartbeat task (claim protocol v2).
//!
//! Heartbeat task verifies continued ownership via HEAD-and-compare on
//! the claim object; the per-host progress object is the actual
//! liveness signal that aggregator + reclaimers observe. The claim's
//! `claimed_utc` field is only updated on reclaim, not on every
//! heartbeat — leases are interpreted by reclaimers as
//! `claimed_utc + lease window`, not as "last heartbeat".
//!
//! On every tick:
//!   1. Write `progress/host-<id>.json` unconditionally — this is the
//!      per-host liveness signal.
//!   2. Snapshot the currently-held claim (if any).
//!   3. If we hold a claim, `claim::refresh` (HEAD-and-compare)
//!      detects whether someone replaced the claim under us. On
//!      `Lost`, trip the fence and exit.
//!
//! See docs/work-items/CLAIM_PROTOCOL_V2_DELETE_THEN_CREATE.md §3.2.

use crate::throughput::ThroughputCounter;
use migration_core::claim::{self, ClaimStore, RefreshOutcome};
use migration_core::fence::{Fence, FenceCause};
use migration_core::layout;
use migration_core::records::ProgressRecord;
use migration_core::time::UtcTime;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{Mutex, RwLock};

pub struct HeartbeatTask {
    pub store: Arc<dyn ClaimStore>,
    pub fence: Fence,
    pub host_id: String,
    pub interval: Duration,
    /// Coord notification channel. When Some, every R6/R7/R8 fence
    /// trip in this task fires `try_send(reason)` on the sender
    /// BEFORE calling `fence.trip(reason)`. Best-effort — a full or
    /// closed channel is silently ignored. The fence trip itself is
    /// the source of truth; the channel exists only so the
    /// `coord_driver` can POST `/workers/{id}/fence` with the same
    /// reason. None when the worker runs in legacy S3-only mode.
    pub coord_fence: Option<tokio::sync::mpsc::Sender<String>>,
    /// Lease window. Used by two fence-trip guards in addition to the
    /// HEAD-and-compare path:
    ///   - R6 retry budget: if HEAD has been failing for at least one
    ///     lease window's worth of consecutive ticks, trip preemptively
    ///     rather than wait for an eventual etag-divergence detection.
    ///   - R7 clock-jump: if wall-clock vs monotonic-clock drift exceeds
    ///     `lease_timeout / 2`, our claimed_utc reasoning is unreliable.
    pub lease_timeout: Duration,
    /// Updated by the worker as it claims/releases shards.
    pub current: Arc<Mutex<Option<HeldClaim>>>,
    /// Updated by the shard processor as batches complete; read-only
    /// here. Initialized by the orchestrator at startup.
    pub progress: Arc<RwLock<ProgressState>>,
    /// Pending per-row deltas for the in-flight shard (see
    /// [`LivePending`]); added on top of the `ProgressState` snapshot
    /// so published counters move at heartbeat granularity.
    pub live: Arc<LivePending>,
    /// Cumulative byte counter; sampled here on every tick to compute
    /// the rolling 60s MB/s the heartbeat publishes.
    pub throughput: ThroughputCounter,
    /// Window in seconds for the throughput rolling average. Default
    /// 60 to match the published `throughput_mb_s_1m` field name.
    pub throughput_window_secs: u64,
    /// Clock pair for the R7 wall-vs-monotonic drift guard. Production
    /// uses [`SystemDriftClock`]; tests inject a jumpable clock.
    pub clock: Arc<dyn DriftClock>,
}

/// Clock pair used by the R7 clock-jump guard. Injected so tests can
/// step the wall clock without touching the host; both readings are
/// taken from the same source object so baselines and per-tick
/// samples stay in one clock domain.
pub trait DriftClock: Send + Sync {
    /// Monotonic reading — never jumps; freezes across VM suspend.
    fn mono(&self) -> std::time::Instant;
    /// Wall-clock reading — jumps on NTP steps / manual adjustment.
    fn wall(&self) -> chrono::DateTime<chrono::Utc>;
}

/// Production clock: `Instant::now()` + `chrono::Utc::now()`.
#[derive(Debug, Default)]
pub struct SystemDriftClock;

impl DriftClock for SystemDriftClock {
    fn mono(&self) -> std::time::Instant {
        std::time::Instant::now()
    }
    fn wall(&self) -> chrono::DateTime<chrono::Utc> {
        chrono::Utc::now()
    }
}

#[derive(Debug, Clone)]
pub struct HeldClaim {
    pub shard: String,
    pub etag: String,
    pub epoch: u64,
}

/// Snapshot of worker state, written into `progress/host-<id>.json` on
/// each heartbeat tick. The processor and orchestrator update fields
/// here; the heartbeat task only reads.
/// Per-row counters the shard processor bumps as each row commits,
/// read by the heartbeat tick so `progress/host-*.json` moves every
/// 30s instead of only at shard completion (a 7.68M-row shard is
/// hours of zeros otherwise — the "run looks hung" trap from the
/// 2026-08-22 rig run). These are the *pending* deltas for the shard
/// in flight; the orchestrator's existing shard-end merge into
/// `ProgressState` remains the durable accumulation, and it resets
/// the pending set immediately before merging (a tick landing in the
/// gap briefly under-counts, never double-counts).
#[derive(Debug, Default)]
pub struct LivePending {
    /// Rows admitted to the mover (permit acquired, copy underway).
    /// `rows_started - rows_done` = live in-flight ops, sampled by
    /// the coord heartbeat's `inflight_ops` gauge.
    pub rows_started: std::sync::atomic::AtomicU64,
    pub rows_done: std::sync::atomic::AtomicU64,
    pub bytes_moved: std::sync::atomic::AtomicU64,
    pub files_ok: std::sync::atomic::AtomicU64,
    pub files_failed: std::sync::atomic::AtomicU64,
    pub files_fenced: std::sync::atomic::AtomicU64,
}

impl LivePending {
    /// Live in-flight ops (started but not yet recorded).
    pub fn inflight(&self) -> u64 {
        use std::sync::atomic::Ordering::Relaxed;
        self.rows_started
            .load(Relaxed)
            .saturating_sub(self.rows_done.load(Relaxed))
    }

    pub fn reset(&self) {
        use std::sync::atomic::Ordering::Relaxed;
        self.rows_started.store(0, Relaxed);
        self.rows_done.store(0, Relaxed);
        self.bytes_moved.store(0, Relaxed);
        self.files_ok.store(0, Relaxed);
        self.files_failed.store(0, Relaxed);
        self.files_fenced.store(0, Relaxed);
    }
}

#[derive(Debug, Clone)]
pub struct ProgressState {
    pub started_utc: UtcTime,
    pub current_shard: Option<String>,
    pub shard_rows_total: u64,
    pub shard_rows_done: u64,
    pub shard_bytes_done: u64,
    pub files_ok: u64,
    pub files_failed: u64,
    /// R8 hits — rows whose commit was short-circuited by a fence trip.
    pub files_fenced: u64,
    pub status: String,
}

impl ProgressState {
    pub fn new() -> Self {
        Self {
            started_utc: UtcTime::now(),
            current_shard: None,
            shard_rows_total: 0,
            shard_rows_done: 0,
            shard_bytes_done: 0,
            files_ok: 0,
            files_failed: 0,
            files_fenced: 0,
            status: "starting".to_string(),
        }
    }
}

impl Default for ProgressState {
    fn default() -> Self {
        Self::new()
    }
}

impl HeartbeatTask {
    /// Best-effort: forward the fence-trip reason to the coord_driver
    /// over `coord_fence` if it is configured. Always called BEFORE
    /// `self.fence.trip(reason)` at each R6/R7/R8 site, never instead
    /// of it. A full or closed channel is silently ignored — the
    /// fence trip is the source of truth and never depends on this
    /// notification reaching the coord.
    fn notify_coord_fence(&self, reason: &str) {
        if let Some(tx) = &self.coord_fence {
            let _ = tx.try_send(reason.to_string());
        }
    }

    pub async fn run(self) {
        let mut ticker = tokio::time::interval(self.interval);
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

        // R6 retry budget. After this many consecutive HEAD failures
        // (while holding a claim), trip the fence — at that point a
        // peer has almost certainly reclaimed and we just haven't
        // been able to observe it, so writing more files is unsafe.
        // Minimum 1 to keep behavior sane on absurd configs.
        let retry_budget = (self.lease_timeout.as_secs() / self.interval.as_secs().max(1)).max(1);
        let mut consec_failures: u64 = 0;

        // R7 clock-jump baseline. Wall-vs-monotonic divergence beyond
        // lease/2 means our reasoning about claimed_utc-based lease
        // expiry is unreliable; self-fence rather than trust it.
        let start_mono = self.clock.mono();
        let start_wall = self.clock.wall();
        let max_drift_secs = (self.lease_timeout.as_secs() / 2).max(1) as i64;

        let cancel = self.fence.cancel_token();
        // Latency window: this task owns it (see
        // `migration_core::latency`) — one snapshot per tick, diffed
        // against the previous, published for the coord heartbeat
        // and written into the progress record.
        let mut latency_prev = migration_core::latency::global().snapshot();
        let mut latency_prev_at = std::time::Instant::now();
        loop {
            tokio::select! {
                _ = ticker.tick() => {}
                _ = cancel.cancelled() => {
                    // Fence tripped — write a final progress record so
                    // the aggregator sees a clean exit, then break.
                    self.write_progress_bounded("exiting", None).await;
                    break;
                }
            }

            tracing::info!(
                interval_ms = self.interval.as_millis() as u64,
                "heartbeat: tick"
            );

            {
                let now_mono = std::time::Instant::now();
                let snap = migration_core::latency::global().snapshot();
                let summary = snap.since(&latency_prev).summarize(
                    now_mono.saturating_duration_since(latency_prev_at),
                    migration_core::latency::pairs(),
                );
                latency_prev = snap;
                latency_prev_at = now_mono;
                if !summary.ops.is_empty() {
                    tracing::info!(
                        src_busy_pct = format_args!("{:.0}", summary.src_busy_pct),
                        dst_busy_pct = format_args!("{:.0}", summary.dst_busy_pct),
                        s3_wait_pct = format_args!("{:.0}", summary.s3_wait_pct),
                        detail = %summary.one_line(),
                        "heartbeat: latency window"
                    );
                }
                migration_core::latency::publish(summary);
            }

            // R7: check wall-vs-monotonic drift on every tick. Both
            // clocks advance together in normal operation; if they
            // diverge it's either a manual clock adjustment, a VM
            // suspend that froze CLOCK_MONOTONIC, or an NTP step. Any
            // of those invalidates our lease reasoning.
            let mono_secs = self
                .clock
                .mono()
                .saturating_duration_since(start_mono)
                .as_secs() as i64;
            let wall_secs = self
                .clock
                .wall()
                .signed_duration_since(start_wall)
                .num_seconds();
            let drift_secs = (wall_secs - mono_secs).abs();
            if drift_secs > max_drift_secs {
                let reason = format!(
                    "clock jump detected: wall-mono drift {drift_secs}s > lease/2 ({max_drift_secs}s); self-fencing"
                );
                self.notify_coord_fence(&reason);
                self.fence.trip_with_cause(FenceCause::ClockJump, reason);
                self.write_progress_bounded("fenced", None).await;
                break;
            }

            // Step 1: snapshot what we currently hold. We do this
            // BEFORE writing progress so the progress object can
            // carry our owning etag for the peer-side liveness
            // cross-check. A reclaimer reads `held_etag` from the
            // progress file and compares it against the live claim
            // etag to decide whether to fast-reclaim.
            let held = {
                let g = self.current.lock().await;
                g.clone()
            };

            tracing::info!(
                held = held.is_some(),
                shard = ?held.as_ref().map(|h| h.shard.clone()),
                epoch = ?held.as_ref().map(|h| h.epoch),
                "heartbeat: held-state snapshot"
            );

            // Step 2: write progress unconditionally. This is the
            // per-host liveness signal aggregators + reclaimers observe;
            // it must land before any HEAD round-trip can stall the loop.
            // Bounded by one tick: the SDK has its own timeouts, but
            // this loop is the worker's liveness — it must never
            // depend on a client implementation detail to keep
            // ticking. (2026-08-28: this PUT hung for 19 h on a
            // black-holed connection; the fence never tripped and the
            // worker copied duplicates of every shard it claimed.)
            match tokio::time::timeout(self.interval, self.write_progress("active", held.as_ref()))
                .await
            {
                Ok(Ok(())) => {}
                Ok(Err(e)) => tracing::warn!(error = ?e, "progress write failed (transient)"),
                Err(_) => tracing::warn!(
                    timeout_secs = self.interval.as_secs(),
                    "progress write timed out (transient)"
                ),
            }

            // Step 3: HEAD-and-compare to detect ownership loss. The
            // v2 refresh does not write — epoch is not bumped here; it
            // only advances on reclaim/complete, which the orchestrator
            // drives.
            if let Some(held) = held {
                // Same bound as the progress write: a HEAD that never
                // returns is a failed HEAD for retry-budget purposes,
                // not a paused loop.
                let refreshed = match tokio::time::timeout(
                    self.interval,
                    claim::refresh(&*self.store, &held.shard, &held.etag),
                )
                .await
                {
                    Ok(r) => r,
                    Err(_) => Err(migration_core::errors::Error::Other(anyhow::anyhow!(
                        "claim HEAD timed out after {}s",
                        self.interval.as_secs()
                    ))),
                };
                match refreshed {
                    Ok(RefreshOutcome::StillHeld { etag }) => {
                        consec_failures = 0;
                        // Invariant: `claim::refresh` returns `StillHeld`
                        // only on an exact etag match, so `etag` always
                        // equals the etag we asked about. There is nothing
                        // to "adopt" here — silently adopting a foreign
                        // etag would be ownership forgery. If refresh's
                        // contract is ever widened, this must become a
                        // hard failure (fence + exit), never an adoption.
                        debug_assert_eq!(
                            etag, held.etag,
                            "claim::refresh StillHeld must echo the held etag"
                        );
                        tracing::info!(etag = %etag, "refresh: still held");
                    }
                    Ok(RefreshOutcome::Lost) => {
                        // R4 residual race (F16): the orchestrator clears
                        // the held-claim cell BEFORE running complete()'s
                        // delete-then-create, but this tick may have
                        // snapshotted the cell just before that clear.
                        // The HEAD then lands during/after complete and
                        // legitimately observes 404 or the terminal
                        // record's new etag — a spurious Lost on a
                        // perfectly clean completion. Re-check the cell
                        // under the same lock the orchestrator's clear
                        // takes (the cell lock is a leaf; no new
                        // lock-ordering edges): if our snapshot no longer
                        // matches the live cell, the Lost signal is
                        // stale — suppress the trip.
                        let snapshot_still_current = {
                            let g = self.current.lock().await;
                            g.as_ref()
                                .is_some_and(|c| c.shard == held.shard && c.etag == held.etag)
                        };
                        if !snapshot_still_current {
                            tracing::info!(
                                shard = %held.shard,
                                snapshot_etag = %held.etag,
                                "refresh observed Lost but the held-claim cell no longer \
                                 matches this tick's snapshot (clean completion/release in \
                                 flight); suppressing fence trip",
                            );
                            consec_failures = 0;
                            continue;
                        }
                        tracing::info!(
                            shard = %held.shard,
                            expected_etag = %held.etag,
                            "refresh: claim lost (claim object replaced under us)"
                        );
                        let reason = format!(
                            "claim refresh: HEAD shows different etag, claim was reclaimed (shard {})",
                            held.shard
                        );
                        self.notify_coord_fence(&reason);
                        self.fence.trip(reason);
                        self.write_progress_bounded("fenced", None).await;
                        break;
                    }
                    Err(e) => {
                        // R6 retry budget. A single transient failure is
                        // fine — networks blip — but if HEAD has been
                        // unable to confirm ownership for a full lease
                        // window's worth of ticks, a peer has almost
                        // certainly reclaimed without us seeing it. Trip
                        // the fence here rather than wait for the
                        // eventual etag-divergence detection, which may
                        // never arrive if S3 stays unreachable for us.
                        consec_failures += 1;
                        tracing::warn!(
                            error = ?e,
                            consec_failures,
                            retry_budget,
                            "heartbeat refresh failed (transient)",
                        );
                        if consec_failures >= retry_budget {
                            let reason = format!(
                                "heartbeat HEAD failing for {consec_failures} consecutive ticks (>= lease window); self-fencing"
                            );
                            self.notify_coord_fence(&reason);
                            self.fence
                                .trip_with_cause(FenceCause::StoreUnreachable, reason);
                            self.write_progress_bounded("fenced", None).await;
                            break;
                        }
                    }
                }
            } else {
                // No claim held — there is nothing to refresh, and a
                // long idle period must not look like exhausted retry
                // budget. Reset the counter so the next acquire starts
                // with a full budget.
                consec_failures = 0;
            }
        }
    }

    /// Bounded variant for shutdown paths. The progress object is
    /// observability-only — never load-bearing for correctness — so
    /// dropping it on timeout is safe. Without this bound, a stale
    /// AWS SDK connection-pool entry after a long SIGSTOP/SIGCONT
    /// cycle can hang the heartbeat task here, which in turn hangs
    /// the orchestrator's hb_handle.await.
    async fn write_progress_bounded(&self, status: &str, held: Option<&HeldClaim>) {
        let r = tokio::time::timeout(
            std::time::Duration::from_secs(3),
            self.write_progress(status, held),
        )
        .await;
        match r {
            Ok(Ok(())) => {}
            Ok(Err(e)) => tracing::warn!(
                error = ?e, status = %status,
                "shutdown progress write failed",
            ),
            Err(_) => tracing::warn!(
                status = %status,
                "shutdown progress write timed out (likely stale S3 connection); proceeding with shutdown",
            ),
        }
    }

    async fn write_progress(
        &self,
        status: &str,
        held: Option<&HeldClaim>,
    ) -> migration_core::Result<()> {
        let throughput_mb_s = self.throughput.sample_mb_s(self.throughput_window_secs);
        let snap = self.progress.read().await.clone();
        let live = &self.live;
        use std::sync::atomic::Ordering::Relaxed;
        let record = ProgressRecord {
            host: self.host_id.clone(),
            started_utc: snap.started_utc,
            heartbeat_utc: UtcTime::now(),
            current_shard: snap.current_shard,
            shard_rows_total: snap.shard_rows_total,
            shard_rows_done: snap
                .shard_rows_done
                .saturating_add(live.rows_done.load(Relaxed)),
            shard_bytes_done: snap
                .shard_bytes_done
                .saturating_add(live.bytes_moved.load(Relaxed)),
            files_ok: snap.files_ok.saturating_add(live.files_ok.load(Relaxed)),
            files_failed: snap
                .files_failed
                .saturating_add(live.files_failed.load(Relaxed)),
            files_fenced: snap
                .files_fenced
                .saturating_add(live.files_fenced.load(Relaxed)),
            throughput_mb_s_1m: throughput_mb_s,
            status: status.to_string(),
            latency: migration_core::latency::latest(),
            // Cross-check fields (PROGRESS_LIVENESS_CROSS_CHECK.md):
            // peers compare `held_etag` to the live claim etag and
            // use `heartbeat_sec` to set the freshness threshold
            // (2× the writer's configured interval).
            held_etag: held.map(|h| h.etag.clone()),
            heartbeat_sec: self.interval.as_secs(),
        };
        let body = serde_json::to_vec(&record)?;
        let key = layout::progress_key(&self.host_id);
        self.store.put_unconditional(&key, body).await?;
        Ok(())
    }
}

// =============================================================================
// Tests — F28. The heartbeat is the S3-side trigger of the fence;
// these tests drive the real `run()` loop against the canonical
// FakeStore (wrapped for failure injection and race interleaving)
// under `start_paused` mock time. See
// docs/work-items/PROTOCOL_TEST_PACK.md.
// =============================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use async_trait::async_trait;
    use migration_core::claim::test_util::FakeStore;
    use migration_core::claim::{
        AcquireOutcome, CompleteOutcome, DeleteOutcome, ListEntry, ReclaimOutcome,
    };
    use migration_core::errors::{Error, Result as CoreResult};
    use std::sync::atomic::{AtomicI64, AtomicU32, Ordering};

    const SHARD: &str = "part-0042.parquet";
    const HOST: &str = "host-A";

    // -------------------------------------------------------------------------
    // Test rig
    // -------------------------------------------------------------------------

    /// What the rig does when the heartbeat's progress PUT lands —
    /// i.e. exactly between the tick's held-claim snapshot and its
    /// refresh HEAD. Models the orchestrator (or a peer) mutating the
    /// claim inside that window. One-shot: disarms after firing.
    #[derive(Clone, Copy)]
    enum RaceAction {
        /// The orchestrator finishes the shard cleanly: clear the
        /// held-claim cell first (single lock-held block, the R4
        /// ordering), then run `complete()`'s delete-then-create.
        /// The tick's HEAD then sees the terminal record's new etag.
        CompleteAndClearCell,
        /// A peer reclaims the shard out from under us; the cell
        /// still holds our snapshot etag — a genuine ownership loss.
        ReclaimKeepCell,
    }

    /// `ClaimStore` wrapper around the canonical [`FakeStore`], adding
    /// what the heartbeat tests need: riggable consecutive HEAD
    /// failures, call counters for synchronization under mock time,
    /// and a one-shot [`RaceAction`] fired from inside the progress
    /// PUT.
    struct TestStore {
        inner: FakeStore,
        /// Consecutive `head_object` failures left to inject.
        head_failures: AtomicU32,
        /// Total `head_object` calls (failed and successful).
        head_calls: AtomicU32,
        /// Total progress-object PUTs.
        progress_puts: AtomicU32,
        armed: std::sync::Mutex<Option<RaceAction>>,
        /// Held-claim cell shared with the `HeartbeatTask`, so
        /// `RaceAction` can replay the orchestrator's cell handling.
        cell: std::sync::Mutex<Option<Arc<Mutex<Option<HeldClaim>>>>>,
    }

    impl TestStore {
        fn new() -> Self {
            Self {
                inner: FakeStore::new(),
                head_failures: AtomicU32::new(0),
                head_calls: AtomicU32::new(0),
                progress_puts: AtomicU32::new(0),
                armed: std::sync::Mutex::new(None),
                cell: std::sync::Mutex::new(None),
            }
        }

        fn arm(&self, action: RaceAction) {
            *self.armed.lock().unwrap() = Some(action);
        }

        async fn fire(&self, action: RaceAction) {
            let cell = self
                .cell
                .lock()
                .unwrap()
                .clone()
                .expect("race action requires the cell to be wired");
            let key = layout::claim_key(SHARD);
            let (etag, _body) = self
                .inner
                .head_object(&key)
                .await
                .unwrap()
                .expect("claim object present");
            match action {
                RaceAction::CompleteAndClearCell => {
                    // Same order as the orchestrator's R4 hunk: clear
                    // the cell, THEN run complete()'s two S3 ops.
                    {
                        let mut g = cell.lock().await;
                        *g = None;
                    }
                    match claim::complete(&self.inner, SHARD, &etag, HOST, 1)
                        .await
                        .unwrap()
                    {
                        CompleteOutcome::Completed { .. } => {}
                        other => panic!("rig complete() should win, got {other:?}"),
                    }
                }
                RaceAction::ReclaimKeepCell => {
                    let out = claim::reclaim(&self.inner, SHARD, &etag, "host-B", 2)
                        .await
                        .unwrap();
                    assert!(
                        matches!(out, ReclaimOutcome::Won { .. }),
                        "rig reclaim should win, got {out:?}"
                    );
                }
            }
        }
    }

    #[async_trait]
    impl ClaimStore for TestStore {
        async fn put_if_absent(&self, key: &str, body: Vec<u8>) -> CoreResult<String> {
            self.inner.put_if_absent(key, body).await
        }

        async fn put_unconditional(&self, key: &str, body: Vec<u8>) -> CoreResult<String> {
            // The heartbeat tick's order is: snapshot held claim →
            // progress PUT (this call) → refresh HEAD. Firing the
            // armed action here lands it exactly inside the
            // snapshot-to-HEAD window the R4/F16 race needs.
            let action = self.armed.lock().unwrap().take();
            if let Some(action) = action {
                self.fire(action).await;
            }
            let r = self.inner.put_unconditional(key, body).await;
            if key == layout::progress_key(HOST) {
                self.progress_puts.fetch_add(1, Ordering::SeqCst);
            }
            r
        }

        async fn head_object(&self, key: &str) -> CoreResult<Option<(String, Vec<u8>)>> {
            self.head_calls.fetch_add(1, Ordering::SeqCst);
            let inject = self
                .head_failures
                .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |n| n.checked_sub(1))
                .is_ok();
            if inject {
                return Err(Error::Other(anyhow::anyhow!(
                    "rigged transient HEAD failure"
                )));
            }
            self.inner.head_object(key).await
        }

        async fn delete_if_match(&self, key: &str, etag: &str) -> CoreResult<DeleteOutcome> {
            self.inner.delete_if_match(key, etag).await
        }

        async fn get(&self, key: &str) -> CoreResult<Option<(Vec<u8>, String)>> {
            self.inner.get(key).await
        }

        async fn list(&self, prefix: &str) -> CoreResult<Vec<ListEntry>> {
            self.inner.list(prefix).await
        }
    }

    /// Jumpable [`DriftClock`]. Monotonic reading is frozen at the
    /// construction-time instant (paused-tokio tests never advance it
    /// meaningfully anyway); the wall reading is a fixed base plus a
    /// settable offset — `jump_wall` models an NTP step.
    struct TestClock {
        base_mono: std::time::Instant,
        base_wall: chrono::DateTime<chrono::Utc>,
        wall_offset_secs: AtomicI64,
    }

    impl TestClock {
        fn new() -> Self {
            Self {
                base_mono: std::time::Instant::now(),
                base_wall: chrono::Utc::now(),
                wall_offset_secs: AtomicI64::new(0),
            }
        }

        fn jump_wall(&self, secs: i64) {
            self.wall_offset_secs.store(secs, Ordering::SeqCst);
        }
    }

    impl DriftClock for TestClock {
        fn mono(&self) -> std::time::Instant {
            self.base_mono
        }
        fn wall(&self) -> chrono::DateTime<chrono::Utc> {
            self.base_wall + chrono::Duration::seconds(self.wall_offset_secs.load(Ordering::SeqCst))
        }
    }

    struct Harness {
        fence: Fence,
        current: Arc<Mutex<Option<HeldClaim>>>,
        clock: Arc<TestClock>,
        coord_rx: tokio::sync::mpsc::Receiver<String>,
        handle: tokio::task::JoinHandle<()>,
    }

    fn spawn_heartbeat(
        store: Arc<TestStore>,
        interval_secs: u64,
        lease_secs: u64,
        held: Option<HeldClaim>,
    ) -> Harness {
        let fence = Fence::new();
        let current = Arc::new(Mutex::new(held));
        let clock = Arc::new(TestClock::new());
        let (coord_tx, coord_rx) = tokio::sync::mpsc::channel(8);
        *store.cell.lock().unwrap() = Some(current.clone());
        let task = HeartbeatTask {
            live: std::sync::Arc::new(crate::heartbeat::LivePending::default()),
            store: store.clone() as Arc<dyn ClaimStore>,
            fence: fence.clone(),
            host_id: HOST.to_string(),
            interval: Duration::from_secs(interval_secs),
            coord_fence: Some(coord_tx),
            lease_timeout: Duration::from_secs(lease_secs),
            current: current.clone(),
            progress: Arc::new(RwLock::new(ProgressState::new())),
            throughput: ThroughputCounter::new(),
            throughput_window_secs: 60,
            clock: clock.clone(),
        };
        // Under the current-thread `#[tokio::test]` runtime the task
        // does not run until the test first awaits, so post-spawn
        // setup in the tests below cannot race the first tick.
        let handle = tokio::spawn(task.run());
        Harness {
            fence,
            current,
            clock,
            coord_rx,
            handle,
        }
    }

    async fn acquire_claim(store: &TestStore) -> String {
        match claim::try_acquire(store, SHARD, HOST).await.unwrap() {
            AcquireOutcome::Acquired { etag, .. } => etag,
            other => panic!("expected Acquired, got {other:?}"),
        }
    }

    fn held(etag: &str) -> HeldClaim {
        HeldClaim {
            shard: SHARD.to_string(),
            etag: etag.to_string(),
            epoch: 1,
        }
    }

    async fn last_progress(store: &TestStore) -> Option<ProgressRecord> {
        let key = layout::progress_key(HOST);
        let (body, _etag) = store.inner.get(&key).await.unwrap()?;
        Some(serde_json::from_slice(&body).unwrap())
    }

    /// Wait (in mock time) until the store has seen `n` calls of the
    /// given counter. Panics after 10 mock minutes.
    async fn wait_for(counter: &AtomicU32, n: u32) {
        tokio::time::timeout(Duration::from_secs(600), async {
            while counter.load(Ordering::SeqCst) < n {
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
        })
        .await
        .unwrap_or_else(|_| panic!("timed out waiting for counter to reach {n}"));
    }

    // -------------------------------------------------------------------------
    // 1. Etag mismatch → fence trip, coord notified, loop exits.
    // -------------------------------------------------------------------------

    #[tokio::test(start_paused = true)]
    async fn etag_mismatch_trips_fence() {
        let store = Arc::new(TestStore::new());
        let etag = acquire_claim(&store).await;
        // A peer reclaims before our first tick: the claim object is
        // replaced and carries a new etag; our cell still holds E0.
        let out = claim::reclaim(&store.inner, SHARD, &etag, "host-B", 2)
            .await
            .unwrap();
        assert!(matches!(out, ReclaimOutcome::Won { .. }));

        let mut h = spawn_heartbeat(store.clone(), 10, 60, Some(held(&etag)));

        tokio::time::timeout(Duration::from_secs(300), h.fence.cancel_token().cancelled())
            .await
            .expect("etag mismatch must trip the fence");
        assert!(!h.fence.is_valid());
        let reason = h.fence.reason().expect("trip stores a reason");
        assert!(
            reason.contains("different etag"),
            "unexpected reason: {reason}"
        );
        // The coord notification carries the same reason and is sent
        // for every R6/R7/R8 trip.
        let coord_reason = h.coord_rx.recv().await.expect("coord notified");
        assert_eq!(coord_reason, reason);

        // Task exits its loop after writing a final `fenced` record;
        // no further progress PUTs happen.
        h.handle.await.unwrap();
        assert_eq!(last_progress(&store).await.unwrap().status, "fenced");
        let puts_at_exit = store.progress_puts.load(Ordering::SeqCst);
        tokio::time::sleep(Duration::from_secs(120)).await;
        assert_eq!(
            store.progress_puts.load(Ordering::SeqCst),
            puts_at_exit,
            "no ticks after loop exit"
        );
    }

    // -------------------------------------------------------------------------
    // 2 + 3. R6 retry budget.
    // -------------------------------------------------------------------------

    #[tokio::test(start_paused = true)]
    async fn refresh_transient_errors_within_budget_no_trip() {
        // budget = lease / interval = 40 / 10 = 4 consecutive ticks.
        let store = Arc::new(TestStore::new());
        let etag = acquire_claim(&store).await;
        store.head_failures.store(3, Ordering::SeqCst);
        let h = spawn_heartbeat(store.clone(), 10, 40, Some(held(&etag)));

        // 3 failures then 1 success — under budget, no trip.
        wait_for(&store.head_calls, 4).await;
        assert!(h.fence.is_valid(), "sub-budget failures must not trip");

        // The success reset the budget: a second sub-budget burst
        // (which WOULD exceed the budget if the counter accumulated)
        // must not trip either.
        store.head_failures.store(3, Ordering::SeqCst);
        wait_for(&store.head_calls, 8).await;
        assert!(
            h.fence.is_valid(),
            "budget must reset after a successful HEAD"
        );

        h.fence.trip("test shutdown");
        h.handle.await.unwrap();
    }

    #[tokio::test(start_paused = true)]
    async fn refresh_budget_exhausted_trips() {
        // budget = 40 / 10 = 4; HEAD never succeeds.
        let store = Arc::new(TestStore::new());
        let etag = acquire_claim(&store).await;
        store.head_failures.store(u32::MAX, Ordering::SeqCst);
        let h = spawn_heartbeat(store.clone(), 10, 40, Some(held(&etag)));

        tokio::time::timeout(Duration::from_secs(600), h.fence.cancel_token().cancelled())
            .await
            .expect("exhausted retry budget must trip the fence");
        let reason = h.fence.reason().unwrap();
        assert!(
            reason.contains("consecutive ticks"),
            "unexpected reason: {reason}"
        );
        h.handle.await.unwrap();
        // Trips on the budget-th consecutive failure, not later.
        assert_eq!(store.head_calls.load(Ordering::SeqCst), 4);
        assert_eq!(last_progress(&store).await.unwrap().status, "fenced");
    }

    // -------------------------------------------------------------------------
    // 4. R7 clock jump.
    // -------------------------------------------------------------------------

    #[tokio::test(start_paused = true)]
    async fn clock_jump_trips() {
        // lease 60 → max drift = 30s. No claim needed: the R7 guard
        // runs on every tick regardless of held state.
        let store = Arc::new(TestStore::new());
        let h = spawn_heartbeat(store.clone(), 10, 60, None);

        // Let a tick land with the clocks in agreement.
        wait_for(&store.progress_puts, 1).await;
        assert!(h.fence.is_valid(), "agreeing clocks must not trip");

        // Wall clock steps forward 5 minutes; monotonic stays put.
        h.clock.jump_wall(300);
        tokio::time::timeout(Duration::from_secs(120), h.fence.cancel_token().cancelled())
            .await
            .expect("wall-mono drift > lease/2 must trip the fence");
        let reason = h.fence.reason().unwrap();
        assert!(
            reason.contains("clock jump detected"),
            "unexpected reason: {reason}"
        );
        h.handle.await.unwrap();
        assert_eq!(last_progress(&store).await.unwrap().status, "fenced");
    }

    // -------------------------------------------------------------------------
    // 5. The R4 regression pair (F16). The tick snapshots the held
    // claim, PUTs progress, then HEADs with the (possibly stale)
    // snapshot. If the orchestrator completed the shard inside that
    // window, the HEAD legitimately observes Lost — but the worker
    // must NOT fence on a clean completion. A genuine loss (cell
    // still matching the snapshot) must still fence.
    // -------------------------------------------------------------------------

    #[tokio::test(start_paused = true)]
    async fn clean_completion_race_does_not_fence() {
        let store = Arc::new(TestStore::new());
        let etag = acquire_claim(&store).await;
        // The tick's progress PUT triggers the orchestrator's clean
        // completion: cell cleared, then complete()'s
        // delete-then-create replaces the claim with a terminal
        // record under a new etag.
        store.arm(RaceAction::CompleteAndClearCell);
        let h = spawn_heartbeat(store.clone(), 10, 60, Some(held(&etag)));

        // The tick's HEAD observes the new etag → RefreshOutcome::Lost
        // — but the cell no longer matches the snapshot, so the trip
        // must be suppressed and the loop must keep running.
        let tripped =
            tokio::time::timeout(Duration::from_secs(120), h.fence.cancel_token().cancelled())
                .await;
        assert!(
            tripped.is_err(),
            "clean completion must not fence the worker (fence reason: {:?})",
            h.fence.reason()
        );
        assert!(h.fence.is_valid());
        // The loop kept ticking normally after the suppressed Lost.
        assert!(store.progress_puts.load(Ordering::SeqCst) >= 2);

        h.fence.trip("test shutdown");
        h.handle.await.unwrap();
    }

    #[tokio::test(start_paused = true)]
    async fn real_loss_still_fences() {
        let store = Arc::new(TestStore::new());
        let etag = acquire_claim(&store).await;
        // Same interleaving, but the claim mutation is a peer reclaim
        // and the cell still holds our snapshot etag: genuine loss.
        store.arm(RaceAction::ReclaimKeepCell);
        let h = spawn_heartbeat(store.clone(), 10, 60, Some(held(&etag)));

        tokio::time::timeout(Duration::from_secs(120), h.fence.cancel_token().cancelled())
            .await
            .expect("genuine ownership loss must still trip the fence");
        let reason = h.fence.reason().unwrap();
        assert!(
            reason.contains("different etag"),
            "unexpected reason: {reason}"
        );
        h.handle.await.unwrap();
        assert_eq!(last_progress(&store).await.unwrap().status, "fenced");
    }

    // -------------------------------------------------------------------------
    // 6. Shutdown pin.
    // -------------------------------------------------------------------------

    #[tokio::test(start_paused = true)]
    async fn shutdown_stops_ticks_and_writes_final_progress() {
        let store = Arc::new(TestStore::new());
        let etag = acquire_claim(&store).await;
        let h = spawn_heartbeat(store.clone(), 10, 60, Some(held(&etag)));

        wait_for(&store.progress_puts, 2).await;
        assert!(h.fence.is_valid());

        // Orchestrator shutdown trips the fence; the cancellation arm
        // writes a final `exiting` record and the task returns.
        h.fence.trip("worker shutting down");
        h.handle.await.unwrap();
        assert_eq!(last_progress(&store).await.unwrap().status, "exiting");

        // No ticks (and hence no progress PUTs) after the task exits.
        let puts_at_exit = store.progress_puts.load(Ordering::SeqCst);
        tokio::time::sleep(Duration::from_secs(120)).await;
        assert_eq!(store.progress_puts.load(Ordering::SeqCst), puts_at_exit);
    }

    // -------------------------------------------------------------------------
    // 7. F34 arm pin: a StillHeld tick never rewrites the held-claim
    // cell. `claim::refresh` returns StillHeld only on an exact etag
    // match, so there is nothing to "adopt"; the cell (and the
    // published `held_etag` cross-check field) must stay bit-stable
    // across ticks.
    // -------------------------------------------------------------------------

    #[tokio::test(start_paused = true)]
    async fn still_held_tick_leaves_cell_untouched() {
        let store = Arc::new(TestStore::new());
        let etag = acquire_claim(&store).await;
        let h = spawn_heartbeat(store.clone(), 10, 60, Some(held(&etag)));

        wait_for(&store.head_calls, 3).await;
        assert!(h.fence.is_valid());
        {
            let g = h.current.lock().await;
            let c = g.as_ref().expect("cell still populated");
            assert_eq!(c.etag, etag, "StillHeld ticks must not rewrite the etag");
            assert_eq!(c.shard, SHARD);
        }
        // The progress record's cross-check field carries the same etag.
        assert_eq!(
            last_progress(&store).await.unwrap().held_etag,
            Some(etag.clone())
        );

        h.fence.trip("test shutdown");
        h.handle.await.unwrap();
    }
}
