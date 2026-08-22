//! Two LIVE workers, in-process, no hardware — acceptance test 8 of
//! `docs/work-items/CLAIM_FRESH_GRACE.md` (ledger F01 + F30).
//!
//! Every M5-class harness serializes its workers; nothing anywhere
//! runs two live orchestrator loops concurrently — which is exactly
//! the configuration the fresh-claim theft cascade (F01) breaks. This
//! harness closes that gap: a minimal worker loop (scan → acquire /
//! reclaim → stub-process → complete) plus the real `HeartbeatTask`,
//! driven against the canonical in-memory mock `ClaimStore`
//! re-exported from `migration_core::claim::test_util` (no forked
//! mock copies).
//!
//! - `two_workers_start_simultaneously_no_theft` — red before the
//!   grace-window fix: peer scans see a fresh claim whose owner
//!   hasn't heartbeated yet (progress absent or `held_etag` stale)
//!   and steal it immediately.
//! - `dead_worker_still_reclaimed_after_grace` — guards against
//!   over-correcting into "never reclaim": a worker that dies right
//!   after acquiring must still be reclaimed once the grace window
//!   elapses, long before the lease window.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::Duration;

use migration_core::claim::test_util::{FakeStore, OpKind, OpRecord};
use migration_core::claim::{
    self, AcquireOutcome, ClaimStore, CompleteOutcome, DeleteOutcome, ReclaimOutcome,
};
use migration_core::fence::Fence;
use migration_core::layout;
use migration_core::records::{
    ClaimRecord, ClaimState, Endpoint, EndpointKind, Manifest, MigrationOptions, ShardEntry,
    RUN_FORMAT_VERSION,
};
use migration_core::time::UtcTime;
use migration_worker::heartbeat::{HeartbeatTask, HeldClaim, ProgressState};
use migration_worker::orchestrator::{scan_shards, ClaimBodyCache, ClaimTarget};
use migration_worker::throughput::ThroughputCounter;
use tokio::sync::{Mutex, RwLock};

fn shard_name(i: usize) -> String {
    format!("part-{i:04}.parquet")
}

fn manifest_with_shards(n: usize) -> Manifest {
    Manifest {
        format_version: RUN_FORMAT_VERSION,
        run_id: "two-live-workers".into(),
        created_utc: UtcTime::now(),
        shards: (1..=n)
            .map(|i| ShardEntry {
                key: format!("index/{}", shard_name(i)),
                rows: 10,
                bytes: 4096,
                // F40: fixture etags must be non-empty — production
                // code no longer skips verification on empty etags,
                // and no fixture may rely on that bypass.
                etag: format!("etag-fixture-{i:04}"),
            })
            .collect(),
        total_rows: (n as u64) * 10,
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

/// Minimal in-process worker: the real `scan_shards` + claim atoms +
/// `HeartbeatTask`, with the shard processor replaced by a stub that
/// sleeps `process_time` in fence-polled slices (the real processor
/// polls `fence.is_valid()` between rows).
///
/// Returns the fence reason observed *during* the run — `None` means
/// a clean "all shards terminal" exit; `Some(reason)` means the
/// worker was fenced mid-run (e.g. its claim was stolen). The
/// deliberate shutdown trip after the loop is not reported.
fn spawn_worker(
    store: Arc<FakeStore>,
    manifest: Arc<Manifest>,
    host_id: &str,
    heartbeat_sec: u64,
    lease: Duration,
    process_time: Duration,
) -> tokio::task::JoinHandle<Option<String>> {
    let host_id = host_id.to_string();
    tokio::spawn(async move {
        let fence = Fence::new();
        let current = Arc::new(Mutex::new(None::<HeldClaim>));
        let progress = Arc::new(RwLock::new(ProgressState::new()));
        let hb = HeartbeatTask {
            live: std::sync::Arc::new(migration_worker::heartbeat::LivePending::default()),
            store: store.clone() as Arc<dyn ClaimStore>,
            fence: fence.clone(),
            host_id: host_id.clone(),
            interval: Duration::from_secs(heartbeat_sec),
            lease_timeout: lease,
            current: current.clone(),
            progress: progress.clone(),
            throughput: ThroughputCounter::new(),
            throughput_window_secs: 60,
            coord_fence: None,
            clock: Arc::new(migration_worker::heartbeat::SystemDriftClock),
        };
        let hb_handle = tokio::spawn(hb.run());

        let mut cache = ClaimBodyCache::default();
        let mid_run_fence: Option<String> = loop {
            if !fence.is_valid() {
                break fence.reason();
            }
            // Empty skip set: the mini worker exercises no F13
            // release-and-skip path.
            let scan = scan_shards(
                &*store,
                &manifest,
                lease,
                heartbeat_sec,
                &mut cache,
                &HashSet::new(),
            )
            .await
            .expect("scan_shards");
            if scan.all_terminal {
                break None;
            }
            let Some(target) = scan.next_target else {
                // Everything Active and live with the peer; idle a
                // beat and re-scan, like the real orchestrator.
                tokio::time::sleep(Duration::from_secs(heartbeat_sec)).await;
                continue;
            };
            let (shard, etag, epoch) = match target {
                ClaimTarget::Free { shard } => {
                    match claim::try_acquire(&*store, &shard, &host_id)
                        .await
                        .expect("try_acquire")
                    {
                        AcquireOutcome::Acquired { etag, record } => (shard, etag, record.epoch),
                        AcquireOutcome::Contended { .. } => {
                            tokio::time::sleep(Duration::from_millis(250)).await;
                            continue;
                        }
                    }
                }
                ClaimTarget::Stale {
                    shard,
                    stale_etag,
                    prior_epoch,
                } => {
                    match claim::reclaim(&*store, &shard, &stale_etag, &host_id, prior_epoch + 1)
                        .await
                        .expect("reclaim")
                    {
                        ReclaimOutcome::Won { etag, record } => (shard, etag, record.epoch),
                        ReclaimOutcome::LostRace => {
                            tokio::time::sleep(Duration::from_millis(250)).await;
                            continue;
                        }
                    }
                }
                ClaimTarget::AlreadyReclaimed { .. } => {
                    unreachable!("mini worker performs no startup self-reclaim")
                }
            };

            // Publish the held claim so the heartbeat HEADs it and
            // the progress object carries our owning etag.
            {
                let mut g = current.lock().await;
                *g = Some(HeldClaim {
                    shard: shard.clone(),
                    etag: etag.clone(),
                    epoch,
                });
                let mut p = progress.write().await;
                p.current_shard = Some(shard.clone());
                p.status = "active".into();
            }

            // Stub shard processor: simulated work in fence-polled
            // slices.
            let step = Duration::from_millis(500);
            let mut remaining = process_time;
            let mut fenced = false;
            while remaining > Duration::ZERO {
                if !fence.is_valid() {
                    fenced = true;
                    break;
                }
                let slice = step.min(remaining);
                tokio::time::sleep(slice).await;
                remaining = remaining.saturating_sub(slice);
            }
            if fenced || !fence.is_valid() {
                break fence.reason();
            }

            // R4: clear the held-claim cell BEFORE complete(), so the
            // heartbeat doesn't fence us on the transient
            // DELETE-then-PUT absent window.
            let (final_etag, final_epoch) = {
                let mut g = current.lock().await;
                let v = match g.as_ref() {
                    Some(c) if c.shard == shard => (c.etag.clone(), c.epoch),
                    _ => (etag.clone(), epoch),
                };
                *g = None;
                v
            };
            match claim::complete(&*store, &shard, &final_etag, &host_id, final_epoch)
                .await
                .expect("complete")
            {
                CompleteOutcome::Completed { .. } => {}
                CompleteOutcome::Lost => {
                    // Stolen mid-shard; the new owner finishes it.
                }
            }
        };

        fence.trip("worker shutting down");
        let _ = hb_handle.await;
        mid_run_fence
    })
}

/// A reclaim reconstructed from the mock store's op log: a successful
/// `DELETE If-Match` that removed an `Active` claim body, where the
/// next successful `PUT If-None-Match` on the same key wrote another
/// `Active` record (a `complete`/`fail` writes a terminal record
/// instead, so those pairs are excluded).
struct ReclaimEvent {
    key: String,
    /// The claim record the reclaim displaced.
    deleted_claim: ClaimRecord,
    /// Wall-clock instant of the DELETE.
    at: chrono::DateTime<chrono::Utc>,
}

fn reclaims_in(log: &[OpRecord]) -> Vec<ReclaimEvent> {
    let mut out = Vec::new();
    for (i, op) in log.iter().enumerate() {
        let OpKind::DeleteIfMatch {
            key,
            outcome: DeleteOutcome::Deleted,
            deleted_body: Some(body),
            ..
        } = &op.kind
        else {
            continue;
        };
        if !key.ends_with(".claim") {
            continue;
        }
        let Ok(deleted) = serde_json::from_slice::<ClaimRecord>(body) else {
            continue;
        };
        if deleted.state != ClaimState::Active {
            continue;
        }
        let next_put_body = log[i + 1..].iter().find_map(|o| match &o.kind {
            OpKind::PutIfAbsent {
                key: k,
                body,
                ok: true,
            } if k == key => Some(body.clone()),
            _ => None,
        });
        let Some(put_body) = next_put_body else {
            continue;
        };
        let Ok(new_record) = serde_json::from_slice::<ClaimRecord>(&put_body) else {
            continue;
        };
        if new_record.state == ClaimState::Active {
            out.push(ReclaimEvent {
                key: key.clone(),
                deleted_claim: deleted,
                at: op.at,
            });
        }
    }
    out
}

/// Count of successful `Completed`-state claim PUTs per claim key.
fn completed_put_counts(log: &[OpRecord]) -> HashMap<String, usize> {
    let mut out: HashMap<String, usize> = HashMap::new();
    for op in log {
        let OpKind::PutIfAbsent {
            key,
            body,
            ok: true,
        } = &op.kind
        else {
            continue;
        };
        if !key.ends_with(".claim") {
            continue;
        }
        let Ok(record) = serde_json::from_slice::<ClaimRecord>(body) else {
            continue;
        };
        if record.state == ClaimState::Completed {
            *out.entry(key.clone()).or_default() += 1;
        }
    }
    out
}

/// Acceptance test 8a (red before fix): two live orchestrator loops
/// started concurrently over 4 shards. No worker may steal a live,
/// seconds-old claim just because the owner's progress object hasn't
/// caught up to the acquire yet.
#[tokio::test(start_paused = true)]
async fn two_workers_start_simultaneously_no_theft() {
    const HB_SEC: u64 = 30;
    let lease = Duration::from_secs(180);
    let process_time = Duration::from_secs(3);

    let store = Arc::new(FakeStore::new());
    let manifest = Arc::new(manifest_with_shards(4));

    let w_a = spawn_worker(
        store.clone(),
        manifest.clone(),
        "host-A",
        HB_SEC,
        lease,
        process_time,
    );
    let w_b = spawn_worker(
        store.clone(),
        manifest.clone(),
        "host-B",
        HB_SEC,
        lease,
        process_time,
    );

    // Generous simulated-time budget; paused time auto-advances so
    // this costs nothing in real time unless the run deadlocks.
    let both = tokio::time::timeout(Duration::from_secs(3600), async {
        (w_a.await.unwrap(), w_b.await.unwrap())
    })
    .await
    .expect("workers did not drive the run to completion");
    let (fence_a, fence_b) = both;

    // Zero fence trips: both workers must exit via "all shards
    // terminal", not by having a live claim stolen out from under
    // them.
    assert_eq!(fence_a, None, "worker A was fenced mid-run: {fence_a:?}");
    assert_eq!(fence_b, None, "worker B was fenced mid-run: {fence_b:?}");

    // Every shard Completed exactly once, and terminal on the store.
    let log = store.op_log();
    let completed = completed_put_counts(&log);
    for i in 1..=4 {
        let key = layout::claim_key(&shard_name(i));
        assert_eq!(
            completed.get(&key),
            Some(&1),
            "shard {i} must be Completed exactly once (got {:?})",
            completed.get(&key),
        );
        let (_, body) = store
            .head_object(&key)
            .await
            .unwrap()
            .unwrap_or_else(|| panic!("claim object for shard {i} missing at end of run"));
        let record: ClaimRecord = serde_json::from_slice(&body).unwrap();
        assert_eq!(record.state, ClaimState::Completed);
    }

    // Zero reclaims of claims younger than the grace window
    // (2 × heartbeat_sec), reconstructed from the store's op log.
    let grace = chrono::Duration::seconds((HB_SEC * 2) as i64);
    for ev in reclaims_in(&log) {
        let age = ev.at.signed_duration_since(ev.deleted_claim.claimed_utc.0);
        assert!(
            age > grace,
            "reclaim of a claim younger than the grace window: key={} owner={} age={}s (grace {}s)",
            ev.key,
            ev.deleted_claim.host,
            age.num_seconds(),
            grace.num_seconds(),
        );
    }
}

/// Acceptance test 8b: a worker that claims a shard and then goes
/// silent (heartbeat + processor dropped — crashed before its first
/// tick, so no progress object exists) must still be reclaimed by a
/// live peer once the grace window elapses, and long before the
/// lease window. Guards against over-correcting the F01 fix into
/// "never reclaim".
///
/// Runs on real time: the grace math reads the wall clock, so this
/// uses a 1s heartbeat and genuinely waits the ~2s grace window.
#[tokio::test(flavor = "multi_thread")]
async fn dead_worker_still_reclaimed_after_grace() {
    const HB_SEC: u64 = 1;
    // Lease far beyond the test budget: the reclaim below can only
    // come from the progress cross-check path, never the lease path.
    let lease = Duration::from_secs(300);

    let store = Arc::new(FakeStore::new());
    let manifest = Arc::new(manifest_with_shards(1));
    let shard = shard_name(1);
    let claim_key = layout::claim_key(&shard);

    // Worker A claims the shard, then dies before its first
    // heartbeat tick: no heartbeat task, no processor, no progress
    // object ever written.
    let AcquireOutcome::Acquired { .. } = claim::try_acquire(&*store, &shard, "host-A")
        .await
        .expect("acquire")
    else {
        panic!("host-A must win the first acquire");
    };
    let started = std::time::Instant::now();

    // Worker B is a full live worker.
    let w_b = spawn_worker(
        store.clone(),
        manifest.clone(),
        "host-B",
        HB_SEC,
        lease,
        Duration::from_millis(200),
    );
    let fence_b = tokio::time::timeout(Duration::from_secs(30), w_b)
        .await
        .expect("worker B did not finish the run within 30s (never-reclaim regression?)")
        .unwrap();
    assert_eq!(fence_b, None, "worker B was fenced mid-run: {fence_b:?}");

    // Recovery came from the cross-check path, not the 300s lease.
    let elapsed = started.elapsed();
    assert!(
        elapsed < Duration::from_secs(20),
        "reclaim took {elapsed:?}; cross-check fast path did not fire",
    );

    // Final state: Completed by host-B at epoch 2 (one reclaim).
    let (_, body) = store
        .head_object(&claim_key)
        .await
        .unwrap()
        .expect("claim object missing at end of run");
    let record: ClaimRecord = serde_json::from_slice(&body).unwrap();
    assert_eq!(record.state, ClaimState::Completed);
    assert_eq!(record.host, "host-B");
    assert_eq!(record.epoch, 2);

    // Exactly one reclaim, and only after the grace window elapsed.
    let reclaims = reclaims_in(&store.op_log());
    assert_eq!(reclaims.len(), 1, "expected exactly one reclaim");
    let ev = &reclaims[0];
    assert_eq!(ev.deleted_claim.host, "host-A");
    let age = ev.at.signed_duration_since(ev.deleted_claim.claimed_utc.0);
    let grace = chrono::Duration::seconds((HB_SEC * 2) as i64);
    assert!(
        age > grace,
        "host-A's claim was reclaimed {}s after acquire — inside the {}s grace window",
        age.num_seconds(),
        grace.num_seconds(),
    );
}
