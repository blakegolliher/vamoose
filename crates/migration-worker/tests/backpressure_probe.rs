//! Backpressure recovery probe — acceptance test 5 of
//! `docs/work-items/WORKER_BACKPRESSURE_RECOVERY.md` (ledger F14).
//!
//! Orchestrator-level: a degraded worker must stay fully gated during
//! the cooldown, then let exactly ONE probe claim through; no further
//! claim may pass the gate until that probe's shard completes and its
//! stats are fed back through `Backpressure::update()`. A failed probe
//! re-degrades with a doubled cooldown; a successful probe reopens
//! normal claiming.
//!
//! Harness style follows `two_live_workers.rs`: a minimal worker loop
//! built from the real `scan_shards` + claim atoms over the canonical
//! in-memory `FakeStore`, with the shard processor stubbed by a paused
//! -time sleep. The backpressure gate here is the same shape as the
//! orchestrator's (probe-token check first, sleep-a-heartbeat when
//! gated). Claim attempts are counted from the store's op log.

use std::collections::{HashSet, VecDeque};
use std::sync::Arc;
use std::time::Duration;

use migration_core::claim::test_util::{FakeStore, OpKind};
use migration_core::claim::{self, AcquireOutcome, CompleteOutcome};
use migration_core::records::{
    ClaimRecord, ClaimState, Endpoint, EndpointKind, Manifest, MigrationOptions, ShardEntry,
    RUN_FORMAT_VERSION,
};
use migration_core::time::UtcTime;
use migration_worker::backpressure::Backpressure;
use migration_worker::orchestrator::{scan_shards, ClaimBodyCache, ClaimTarget};

fn shard_name(i: usize) -> String {
    format!("part-{i:04}.parquet")
}

fn manifest_with_shards(n: usize) -> Manifest {
    Manifest {
        format_version: RUN_FORMAT_VERSION,
        run_id: "backpressure-probe".into(),
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

/// Count of successful `Active`-state claim PUTs — i.e. claim
/// attempts that passed the gate and landed.
fn active_claim_puts(store: &FakeStore) -> usize {
    store
        .op_log()
        .iter()
        .filter(|op| {
            let OpKind::PutIfAbsent {
                key,
                body,
                ok: true,
            } = &op.kind
            else {
                return false;
            };
            if !key.ends_with(".claim") {
                return false;
            }
            matches!(
                serde_json::from_slice::<ClaimRecord>(body),
                Ok(r) if r.state == ClaimState::Active
            )
        })
        .count()
}

/// Minimal single worker with the orchestrator's backpressure gate:
/// probe-token check first; when gated, sleep one heartbeat and
/// re-check. Each completed shard consumes one scripted
/// `(files_ok, files_failed, throughput_mb_s)` outcome fed into
/// `Backpressure::update()` — exactly like the orchestrator does after
/// a shard finishes.
fn spawn_gated_worker(
    store: Arc<FakeStore>,
    manifest: Arc<Manifest>,
    mut backpressure: Backpressure,
    mut shard_outcomes: VecDeque<(u64, u64, f64)>,
    heartbeat_sec: u64,
    lease: Duration,
    process_time: Duration,
) -> tokio::task::JoinHandle<()> {
    let host_id = "host-probe".to_string();
    tokio::spawn(async move {
        let mut cache = ClaimBodyCache::default();
        let skip_shards = HashSet::new();
        loop {
            // A leftover `probing` state at the top of the loop means
            // the previous probe pass never fed `update()` (nothing
            // claimable / lost race); return the token — same as the
            // orchestrator.
            if backpressure.is_probing() {
                backpressure.reset_probe();
            }
            if backpressure.degraded().is_some() {
                if backpressure.try_claim_probe() {
                    // Fall through: this pass claims exactly one shard.
                } else {
                    tokio::time::sleep(Duration::from_secs(heartbeat_sec)).await;
                    continue;
                }
            }

            let scan = scan_shards(
                &*store,
                &manifest,
                lease,
                heartbeat_sec,
                &mut cache,
                &skip_shards,
            )
            .await
            .expect("scan_shards");
            if scan.all_terminal {
                break;
            }
            let Some(target) = scan.next_target else {
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
                other => unreachable!("single fresh worker; unexpected target {other:?}"),
            };

            // Stub shard processor.
            tokio::time::sleep(process_time).await;

            // Feed the scripted shard outcome into the gate, then
            // finalize the claim (same order as the orchestrator).
            let (ok, failed, mb_s) = shard_outcomes.pop_front().unwrap_or((100, 0, 1000.0));
            backpressure.update(ok, failed, mb_s);

            match claim::complete(&*store, &shard, &etag, &host_id, epoch)
                .await
                .expect("complete")
            {
                CompleteOutcome::Completed { .. } => {}
                CompleteOutcome::Lost => unreachable!("no peer to steal the claim"),
            }
        }
    })
}

/// Acceptance test 5: while degraded-with-probe-available, at most one
/// claim attempt passes the gate until its shard completes and
/// `update()` runs; a failed probe widens the window (×2); a healthy
/// probe reopens normal claiming.
#[tokio::test(start_paused = true)]
async fn orchestrator_respects_probe_single_flight() {
    const HB_SEC: u64 = 30;
    let lease = Duration::from_secs(3600);
    let process_time = Duration::from_secs(60);

    let store = Arc::new(FakeStore::new());
    let manifest = Arc::new(manifest_with_shards(3));

    // Trip degradation before the worker starts: one bad shard
    // outcome, exactly what the orchestrator would have fed.
    let mut bp = Backpressure::new(5.0, 100);
    bp.update(90, 10, 1000.0); // 10% failures → degraded at t=0
    assert!(bp.degraded().is_some());

    // Shard outcome script: probe 1 fails (still >5% failures),
    // probe 2 is healthy, remaining shard healthy.
    let outcomes = VecDeque::from(vec![
        (90u64, 10u64, 1000.0f64), // probe 1 → re-degrade, cooldown ×2
        (100, 0, 1000.0),          // probe 2 → recovery
        (100, 0, 1000.0),          // normal claiming resumed
    ]);

    let worker = spawn_gated_worker(
        store.clone(),
        manifest,
        bp,
        outcomes,
        HB_SEC,
        lease,
        process_time,
    );

    // t=0..240s: inside the 5-min cooldown — nothing may pass the
    // gate, not even one claim.
    tokio::time::sleep(Duration::from_secs(240)).await;
    assert_eq!(
        active_claim_puts(&store),
        0,
        "no claim may pass the gate during the cooldown"
    );

    // t=330s: cooldown (300s) elapsed → exactly ONE probe claim is
    // out; its shard is still processing (until t=360), so nothing
    // else may pass.
    tokio::time::sleep(Duration::from_secs(90)).await;
    assert_eq!(
        active_claim_puts(&store),
        1,
        "exactly one probe claim passes the gate after the cooldown"
    );

    // Probe 1 completes at t=360 with unhealthy stats → re-degraded,
    // cooldown doubled to 10 min (eligible at t=960). At t=900 —
    // well past another default 5-min window — still only the one
    // claim.
    tokio::time::sleep(Duration::from_secs(570)).await; // → t=900
    assert_eq!(
        active_claim_puts(&store),
        1,
        "failed probe re-degrades with a LONGER window; no claim at the old cadence"
    );

    // t=960: probe 2 passes, completes healthy at t=1020 → recovery;
    // the last shard is then claimed normally and the run finishes.
    tokio::time::sleep(Duration::from_secs(300)).await; // → t=1200
    assert_eq!(
        active_claim_puts(&store),
        3,
        "healthy probe reopens normal claiming (probe + recovered claims)"
    );

    worker.await.expect("worker task");
}
