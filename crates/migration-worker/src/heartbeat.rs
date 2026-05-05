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

use crate::fence::Fence;
use crate::throughput::ThroughputCounter;
use migration_core::claim::{self, ClaimStore, RefreshOutcome};
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

        let cancel = self.fence.cancel_token();
        loop {
            tokio::select! {
                _ = ticker.tick() => {}
                _ = cancel.cancelled() => {
                    // Fence tripped — write a final progress record so
                    // the aggregator sees a clean exit, then break.
                    self.write_progress_bounded("exiting").await;
                    break;
                }
            }

            tracing::info!(
                interval_ms = self.interval.as_millis() as u64,
                "heartbeat: tick"
            );

            // Step 1: write progress unconditionally. This is the
            // per-host liveness signal aggregators + reclaimers observe;
            // it must land before any HEAD round-trip can stall the loop.
            if let Err(e) = self.write_progress("active").await {
                tracing::warn!(error = ?e, "progress write failed (transient)");
            }

            // Step 2: snapshot what we currently hold — the worker may
            // have changed it underneath us.
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

            // Step 3: HEAD-and-compare to detect ownership loss. The
            // v2 refresh does not write — epoch is not bumped here; it
            // only advances on reclaim/complete, which the orchestrator
            // drives.
            if let Some(held) = held {
                match claim::refresh(&*self.store, &held.shard, &held.etag).await {
                    Ok(RefreshOutcome::StillHeld { etag }) => {
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
                        self.write_progress_bounded("fenced").await;
                        break;
                    }
                    Err(e) => {
                        // Transient — log and continue. If the failure
                        // persists past the lease window, a peer will
                        // reclaim and the next tick's HEAD will see the
                        // new etag.
                        tracing::warn!(error = ?e, "heartbeat refresh failed (transient)");
                    }
                }
            }
        }
    }

    /// Bounded variant for shutdown paths. The progress object is
    /// observability-only — never load-bearing for correctness — so
    /// dropping it on timeout is safe. Without this bound, a stale
    /// AWS SDK connection-pool entry after a long SIGSTOP/SIGCONT
    /// cycle can hang the heartbeat task here, which in turn hangs
    /// the orchestrator's hb_handle.await.
    async fn write_progress_bounded(&self, status: &str) {
        let r = tokio::time::timeout(
            std::time::Duration::from_secs(3),
            self.write_progress(status),
        ).await;
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

    async fn write_progress(&self, status: &str) -> migration_core::Result<()> {
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
            throughput_mb_s_1m: throughput_mb_s,
            status: status.to_string(),
        };
        let body = serde_json::to_vec(&record)?;
        let key = layout::progress_key(&self.host_id);
        self.store.put_unconditional(&key, body).await?;
        Ok(())
    }
}
