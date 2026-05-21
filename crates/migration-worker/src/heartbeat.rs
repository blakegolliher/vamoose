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
use migration_core::fence::Fence;
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
    /// Cumulative byte counter; sampled here on every tick to compute
    /// the rolling 60s MB/s the heartbeat publishes.
    pub throughput: ThroughputCounter,
    /// Window in seconds for the throughput rolling average. Default
    /// 60 to match the published `throughput_mb_s_1m` field name.
    pub throughput_window_secs: u64,
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
        let start_mono = std::time::Instant::now();
        let start_wall = chrono::Utc::now();
        let max_drift_secs = (self.lease_timeout.as_secs() / 2).max(1) as i64;

        let cancel = self.fence.cancel_token();
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

            // R7: check wall-vs-monotonic drift on every tick. Both
            // clocks advance together in normal operation; if they
            // diverge it's either a manual clock adjustment, a VM
            // suspend that froze CLOCK_MONOTONIC, or an NTP step. Any
            // of those invalidates our lease reasoning.
            let mono_secs = start_mono.elapsed().as_secs() as i64;
            let wall_secs = chrono::Utc::now()
                .signed_duration_since(start_wall)
                .num_seconds();
            let drift_secs = (wall_secs - mono_secs).abs();
            if drift_secs > max_drift_secs {
                self.fence.trip(format!(
                    "clock jump detected: wall-mono drift {drift_secs}s > lease/2 ({max_drift_secs}s); self-fencing"
                ));
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
            if let Err(e) = self.write_progress("active", held.as_ref()).await {
                tracing::warn!(error = ?e, "progress write failed (transient)");
            }

            // Step 3: HEAD-and-compare to detect ownership loss. The
            // v2 refresh does not write — epoch is not bumped here; it
            // only advances on reclaim/complete, which the orchestrator
            // drives.
            if let Some(held) = held {
                match claim::refresh(&*self.store, &held.shard, &held.etag).await {
                    Ok(RefreshOutcome::StillHeld { etag }) => {
                        consec_failures = 0;
                        if etag == held.etag {
                            tracing::info!(etag = %etag, "refresh: still held");
                        } else {
                            // Etag drift without a Lost — adopt the
                            // observed etag so the next tick's compare
                            // is against current state.
                            tracing::info!(
                                old_etag = %held.etag,
                                new_etag = %etag,
                                "refresh: still held but etag changed; adopting observed etag",
                            );
                            let mut g = self.current.lock().await;
                            if let Some(c) = g.as_mut() {
                                if c.shard == held.shard {
                                    c.etag = etag;
                                }
                            }
                        }
                    }
                    Ok(RefreshOutcome::Lost) => {
                        tracing::info!(
                            shard = %held.shard,
                            expected_etag = %held.etag,
                            "refresh: claim lost (claim object replaced under us)"
                        );
                        self.fence.trip(format!(
                            "claim refresh: HEAD shows different etag, claim was reclaimed (shard {})",
                            held.shard
                        ));
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
                            self.fence.trip(format!(
                                "heartbeat HEAD failing for {consec_failures} consecutive ticks (>= lease window); self-fencing"
                            ));
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
        let record = ProgressRecord {
            host: self.host_id.clone(),
            started_utc: snap.started_utc,
            heartbeat_utc: UtcTime::now(),
            current_shard: snap.current_shard,
            shard_rows_total: snap.shard_rows_total,
            shard_rows_done: snap.shard_rows_done,
            shard_bytes_done: snap.shard_bytes_done,
            files_ok: snap.files_ok,
            files_failed: snap.files_failed,
            files_fenced: snap.files_fenced,
            throughput_mb_s_1m: throughput_mb_s,
            status: status.to_string(),
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
