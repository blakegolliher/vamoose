use super::*;
use crate::errors::{Error, Result};
use crate::events::EventLogConfig;
use crate::lease::{Identity, LeaseConfig};
use crate::schema::{EventEnvelope, EventKind, JobId, Phase, WorkerId};
use crate::store::{CoordStore, MemStore};
use chrono::{DateTime, Duration, TimeZone, Utc};
use std::sync::Arc;
use test_clock::FixedClock;

fn jid(s: &str) -> JobId {
    JobId::new(s).unwrap()
}

fn me(holder: &str) -> Identity {
    Identity {
        holder_id: holder.to_string(),
        host: "test-host".to_string(),
        pid: 1234,
    }
}

fn cfg_for_tests() -> RuntimeConfig {
    RuntimeConfig {
        lease: LeaseConfig {
            ttl: Duration::seconds(30),
            grace: Duration::seconds(5),
        },
        events: EventLogConfig {
            max_events_per_chunk: 100,
            max_chunk_age: Duration::seconds(60),
        },
        bus_capacity: 16,
        lease_retry_interval: std::time::Duration::from_millis(10),
        lease_retry_max_attempts: Some(3),
    }
}

fn job_created(job: &str) -> EventKind {
    EventKind::JobCreated {
        job_id: jid(job),
        name: format!("{job}-migration"),
        source: "nfs://src".into(),
        dest: "nfs://dst".into(),
        owner: "test".into(),
        config_hash: crate::schema::ConfigHash("ab".into()),
        total_files: 0,
        total_bytes: 0,
    }
}

fn at(secs: i64) -> DateTime<Utc> {
    Utc.with_ymd_and_hms(2026, 5, 29, 14, 32, 0).unwrap() + Duration::seconds(secs)
}

fn worker_joined(w: WorkerId, job: &str) -> EventKind {
    EventKind::WorkerJoined {
        worker_id: w,
        job_id: jid(job),
        host: "h".into(),
        pid: 42,
        start_time: at(0),
        version: "0.6".into(),
    }
}

fn worker_left(w: WorkerId) -> EventKind {
    EventKind::WorkerLeft {
        worker_id: w,
        reason: "drain".into(),
    }
}

async fn fresh_runtime() -> (CoordRuntime, Arc<FixedClock>, Arc<MemStore>) {
    // Keep an `Arc<MemStore>` for direct test access (peeks at
    // raw keys, asserts list/get outcomes) and hand the runtime
    // a coerced `Arc<dyn CoordStore>` pointing at the same
    // allocation. No casts, no unsafe.
    let mem = Arc::new(MemStore::new());
    let store: Arc<dyn CoordStore> = mem.clone();
    let clock = FixedClock::new(at(0));
    let rt = CoordRuntime::start(store, clock.clone(), me("A"), cfg_for_tests())
        .await
        .unwrap();
    (rt, clock, mem)
}

#[tokio::test]
async fn start_on_empty_bucket_yields_empty_state() {
    let (rt, _clock, _store) = fresh_runtime().await;
    let snap = rt.state().await;
    assert!(snap.jobs.is_empty());
    assert_eq!(rt.last_seq().await, 0);
}

#[tokio::test]
async fn ingest_assigns_monotonic_seq_and_mutates_state() {
    let (rt, _clock, _store) = fresh_runtime().await;

    let s1 = rt.ingest(job_created("bobby")).await.unwrap();
    let s2 = rt.ingest(job_created("mary")).await.unwrap();
    let s3 = rt
        .ingest(EventKind::ProgressDelta {
            job_id: jid("bobby"),
            worker_id: WorkerId::new(),
            files_delta: 10,
            bytes_delta: 1024,
            errors_delta: 0,
        })
        .await
        .unwrap();
    assert_eq!((s1, s2, s3), (1, 2, 3));
    let snap = rt.state().await;
    assert!(snap.jobs.contains_key(&jid("bobby")));
    assert!(snap.jobs.contains_key(&jid("mary")));
    assert_eq!(snap.jobs[&jid("bobby")].progress.files_done, 10);
    assert_eq!(snap.last_seq, 3);
}

#[tokio::test]
async fn ingest_stamps_at_from_clock() {
    let (rt, clock, _store) = fresh_runtime().await;
    clock.advance(Duration::seconds(5));
    let _ = rt.ingest(job_created("bobby")).await.unwrap();
    let mut sub = rt.subscribe();
    // No event in the subscriber for that ingest (subscribed
    // after the fact). Push another and observe.
    clock.advance(Duration::seconds(2));
    let seq = rt.ingest(job_created("mary")).await.unwrap();
    let env = sub.recv().await.unwrap();
    assert_eq!(env.seq, seq);
    assert_eq!(env.at, at(7));
}

#[tokio::test]
async fn subscribers_see_live_events() {
    let (rt, _clock, _store) = fresh_runtime().await;
    let mut sub_a = rt.subscribe();
    let mut sub_b = rt.subscribe();
    let _ = rt.ingest(job_created("bobby")).await.unwrap();
    let _ = rt.ingest(job_created("mary")).await.unwrap();

    let envs_a: Vec<_> = (0..2).map(|_| sub_a.try_recv().unwrap()).collect();
    let envs_b: Vec<_> = (0..2).map(|_| sub_b.try_recv().unwrap()).collect();
    assert_eq!(envs_a.len(), 2);
    assert_eq!(envs_b.len(), 2);
    // Same seqs to both subscribers.
    assert_eq!(envs_a[0].seq, envs_b[0].seq);
    assert_eq!(envs_a[1].seq, envs_b[1].seq);
}

#[tokio::test]
async fn restart_replays_prior_state() {
    // Run 1: ingest events, gracefully shut down (which writes
    // a snapshot and flushes the log). Includes a worker that
    // disconnected long ago (evicted at snapshot write, F24
    // residue 3b) and one that disconnected recently (kept).
    let store: Arc<dyn CoordStore> = Arc::new(MemStore::new());
    let clock = FixedClock::new(at(0));
    let w_stale = WorkerId::new();
    let w_fresh = WorkerId::new();
    let live_workers;
    {
        let rt = CoordRuntime::start(store.clone(), clock.clone(), me("A"), cfg_for_tests())
            .await
            .unwrap();
        rt.ingest(job_created("bobby")).await.unwrap();
        rt.ingest(EventKind::ProgressDelta {
            job_id: jid("bobby"),
            worker_id: WorkerId::new(),
            files_delta: 100,
            bytes_delta: 1024,
            errors_delta: 0,
        })
        .await
        .unwrap();
        rt.ingest(worker_joined(w_stale, "bobby")).await.unwrap();
        rt.ingest(worker_left(w_stale)).await.unwrap();
        clock.advance(Duration::seconds(WORKER_EVICT_AFTER_SECS + 60));
        rt.ingest(worker_joined(w_fresh, "bobby")).await.unwrap();
        rt.ingest(worker_left(w_fresh)).await.unwrap();
        rt.shutdown(3).await.unwrap();
        live_workers = rt.state().await.workers;
    }

    // Run 2: restart against the same bucket. State should
    // reconstitute exactly.
    clock.advance(Duration::seconds(60));
    let rt2 = CoordRuntime::start(store.clone(), clock.clone(), me("B"), cfg_for_tests())
        .await
        .unwrap();
    let snap = rt2.state().await;
    assert_eq!(snap.jobs[&jid("bobby")].progress.files_done, 100);
    // Progress flowed, so the reducer derives Copying from the first
    // ProgressDelta — identically live and on replay (that derivation
    // living in the shared reducer is what this test now also proves).
    assert_eq!(snap.jobs[&jid("bobby")].phase, Phase::Copying);
    // 3b replay equality: the stale Disconnected row was pruned
    // at snapshot write, the fresh one survives, and the replayed
    // worker table converges with the live coord's post-prune
    // state.
    assert!(
        !snap.workers.contains_key(&w_stale),
        "stale Disconnected row must not survive the restart",
    );
    assert!(
        snap.workers.contains_key(&w_fresh),
        "recently Disconnected row must survive the restart",
    );
    assert_eq!(
        snap.workers, live_workers,
        "replayed worker table must converge with live post-prune state",
    );
    assert_eq!(rt2.last_seq().await, 6);
    // Next ingest gets seq 7 — eviction never disturbs the seq
    // stream.
    let s7 = rt2.ingest(job_created("mary")).await.unwrap();
    assert_eq!(s7, 7);
}

#[tokio::test]
async fn ingest_returns_lease_lost_after_mark() {
    let (rt, _clock, _store) = fresh_runtime().await;
    rt.mark_lease_lost().await;
    let err = rt.ingest(job_created("bobby")).await.unwrap_err();
    assert!(matches!(err, Error::LeaseLost));
}

#[tokio::test]
async fn start_backs_off_when_lease_is_held() {
    let store: Arc<dyn CoordStore> = Arc::new(MemStore::new());
    let clock = FixedClock::new(at(0));
    // First coord acquires.
    let _rt_a = CoordRuntime::start(store.clone(), clock.clone(), me("A"), cfg_for_tests())
        .await
        .unwrap();
    // Second coord retries 3x with 10ms intervals then gives up.
    let err = CoordRuntime::start(store.clone(), clock.clone(), me("B"), cfg_for_tests())
        .await
        .unwrap_err();
    assert!(
        matches!(err, Error::LeaseHeld { .. }),
        "expected LeaseHeld after attempt cap, got {err:?}",
    );
}

#[tokio::test]
async fn shutdown_writes_snapshot_and_releases_lease() {
    let (rt, _clock, store) = fresh_runtime().await;
    rt.ingest(job_created("bobby")).await.unwrap();
    rt.shutdown(3).await.unwrap();

    // Snapshot exists.
    let snap = crate::snapshot::load(store.as_ref()).await.unwrap();
    assert!(snap.is_some());
    // Lease is gone.
    assert!(store.get(crate::layout::LEASE_KEY).await.unwrap().is_none());
}

#[tokio::test]
async fn write_snapshot_stamps_written_at_from_clock() {
    let (rt, clock, store) = fresh_runtime().await;
    clock.advance(Duration::seconds(123));
    rt.write_snapshot(3).await.unwrap();
    let snap = crate::snapshot::load(store.as_ref())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(snap.written_at, at(123));
}

// =========================================================
// F24 residue 3b — worker-row eviction at snapshot-write time
// only (COORD_RUNTIME_BATCH item 3b). Never wall-clock pruning
// of live state; the pruning decision is embodied in the
// durable snapshot, so replay = snapshot + events converges
// with the live coord.
// =========================================================

/// The snapshot writer omits rows that are Disconnected AND
/// stale (>= WORKER_EVICT_AFTER_SECS since last activity);
/// fresh Disconnected rows and Fenced rows (operator-relevant)
/// stay. On a successful write the live table drops the same
/// rows, bounding live memory by snapshot cadence.
#[tokio::test]
async fn snapshot_omits_stale_disconnected_workers() {
    let (rt, clock, store) = fresh_runtime().await;
    rt.ingest(job_created("bobby")).await.unwrap();
    let w_stale = WorkerId::new();
    let w_fenced = WorkerId::new();
    let w_fresh = WorkerId::new();
    rt.ingest(worker_joined(w_stale, "bobby")).await.unwrap();
    rt.ingest(worker_left(w_stale)).await.unwrap();
    rt.ingest(worker_joined(w_fenced, "bobby")).await.unwrap();
    rt.ingest(EventKind::WorkerFenced {
        worker_id: w_fenced,
        reason: "stuck".into(),
    })
    .await
    .unwrap();
    // Jump past the eviction window, then a recent disconnect.
    clock.advance(Duration::seconds(WORKER_EVICT_AFTER_SECS + 1));
    rt.ingest(worker_joined(w_fresh, "bobby")).await.unwrap();
    rt.ingest(worker_left(w_fresh)).await.unwrap();

    rt.write_snapshot(3).await.unwrap();

    let snap = crate::snapshot::load(store.as_ref())
        .await
        .unwrap()
        .unwrap();
    assert!(
        !snap.workers.contains_key(&w_stale),
        "stale Disconnected row must be omitted from the snapshot",
    );
    assert!(
        snap.workers.contains_key(&w_fenced),
        "Fenced rows are operator-relevant and must never be evicted",
    );
    assert!(
        snap.workers.contains_key(&w_fresh),
        "a fresh Disconnected row must be kept",
    );
    // Live state dropped the same row once the write succeeded.
    let live = rt.state().await;
    assert!(!live.workers.contains_key(&w_stale));
    assert!(live.workers.contains_key(&w_fenced));
    assert!(live.workers.contains_key(&w_fresh));
}

/// A WorkerJoined for an evicted id resurrects the row cleanly,
/// live and on replay.
#[tokio::test]
async fn worker_rejoin_after_eviction_resurrects() {
    let (rt, clock, store) = fresh_runtime().await;
    rt.ingest(job_created("bobby")).await.unwrap();
    let w = WorkerId::new();
    rt.ingest(worker_joined(w, "bobby")).await.unwrap();
    rt.ingest(worker_left(w)).await.unwrap();
    clock.advance(Duration::seconds(WORKER_EVICT_AFTER_SECS + 1));
    rt.write_snapshot(3).await.unwrap();
    assert!(
        !rt.state().await.workers.contains_key(&w),
        "test setup: the row must be evicted at snapshot write",
    );

    // The same id reappears in the log after the eviction.
    rt.ingest(worker_joined(w, "bobby")).await.unwrap();
    let live = rt.state().await;
    assert_eq!(
        live.workers[&w].state,
        crate::schema::WorkerState::Idle,
        "a rejoin must resurrect the evicted row cleanly",
    );

    // Replay from the pruned snapshot + post-snapshot events
    // converges with live.
    rt.flush_log().await.unwrap();
    let replayed = crate::state::replay(store.as_ref(), clock.now())
        .await
        .unwrap();
    assert_eq!(
        replayed.state.workers, live.workers,
        "replay must converge with live after eviction + rejoin",
    );
}

// =========================================================
// Lease-lost write fence (ledger F02) — every store-writing
// method must refuse to touch the store once the lease is
// observed lost, so a deposed coord cannot clobber the
// successor's chunks or snapshot.
// =========================================================

#[tokio::test]
async fn flush_log_after_lease_lost_writes_nothing() {
    let (rt, _clock, store) = fresh_runtime().await;
    // Buffer a few events (max_events_per_chunk = 100, so no
    // threshold flush happens).
    rt.ingest(job_created("bobby")).await.unwrap();
    rt.ingest(job_created("mary")).await.unwrap();
    rt.ingest(job_created("sue")).await.unwrap();

    let writes_before = store.write_count();
    rt.mark_lease_lost().await;

    let err = rt.flush_log().await.unwrap_err();
    assert!(
        matches!(err, Error::LeaseLost),
        "expected LeaseLost, got {err:?}",
    );
    assert_eq!(
        store.write_count(),
        writes_before,
        "flush_log after lease loss must not write to the store",
    );
}

#[tokio::test]
async fn flush_aged_after_lease_lost_writes_nothing() {
    let (rt, clock, store) = fresh_runtime().await;
    rt.ingest(job_created("bobby")).await.unwrap();
    // Age the open chunk well past max_chunk_age (60s) so
    // flush_aged WOULD flush it if the fence were absent.
    clock.advance(Duration::seconds(120));

    let writes_before = store.write_count();
    rt.mark_lease_lost().await;

    let err = rt.flush_aged().await.unwrap_err();
    assert!(
        matches!(err, Error::LeaseLost),
        "expected LeaseLost, got {err:?}",
    );
    assert_eq!(
        store.write_count(),
        writes_before,
        "flush_aged after lease loss must not write to the store",
    );
}

#[tokio::test]
async fn write_snapshot_after_lease_lost_writes_nothing() {
    let (rt, _clock, store) = fresh_runtime().await;
    rt.ingest(job_created("bobby")).await.unwrap();

    let writes_before = store.write_count();
    rt.mark_lease_lost().await;

    let err = rt.write_snapshot(3).await.unwrap_err();
    assert!(
        matches!(err, Error::LeaseLost),
        "expected LeaseLost, got {err:?}",
    );
    assert_eq!(
        store.write_count(),
        writes_before,
        "write_snapshot after lease loss must not write to the store",
    );
    // And nothing landed at the snapshot key.
    let snap = crate::snapshot::load(store.as_ref()).await.unwrap();
    assert!(snap.is_none(), "no snapshot may exist after fenced write");
}

#[tokio::test]
async fn shutdown_after_lease_lost_skips_flush_and_snapshot() {
    let (rt, _clock, store) = fresh_runtime().await;
    // Buffered events that a naive shutdown would flush.
    rt.ingest(job_created("bobby")).await.unwrap();
    rt.ingest(job_created("mary")).await.unwrap();

    let writes_before = store.write_count();
    rt.mark_lease_lost().await;

    // Must not panic; must be distinguishable from clean
    // shutdown so the CLI can log "buffered events NOT flushed".
    let err = rt.shutdown(3).await.unwrap_err();
    assert!(
        matches!(err, Error::LeaseLost),
        "expected LeaseLost, got {err:?}",
    );
    assert_eq!(
        store.write_count(),
        writes_before,
        "shutdown after lease loss must not write to the store",
    );
    // No event chunks flushed, no snapshot written.
    assert!(store.list("events/").await.unwrap().is_empty());
    let snap = crate::snapshot::load(store.as_ref()).await.unwrap();
    assert!(snap.is_none());
    // The successor's lease must not be touched either — release
    // is skipped entirely (the lease object we wrote at start is
    // still whatever the store holds).
    assert!(
        store.get(crate::layout::LEASE_KEY).await.unwrap().is_some(),
        "fenced shutdown must not attempt lease release",
    );
}

#[tokio::test]
async fn shutdown_with_lease_held_still_flushes() {
    let (rt, _clock, store) = fresh_runtime().await;
    rt.ingest(job_created("bobby")).await.unwrap();

    let writes_before = store.write_count();
    rt.shutdown(3).await.unwrap();

    assert!(
        store.write_count() > writes_before,
        "clean shutdown must flush the log and write a snapshot",
    );
    // Buffered chunk flushed.
    let chunks = store.list("events/bobby/").await.unwrap();
    assert_eq!(chunks.len(), 1);
    // Snapshot written.
    let snap = crate::snapshot::load(store.as_ref()).await.unwrap();
    assert!(snap.is_some());
    // Lease released.
    assert!(store.get(crate::layout::LEASE_KEY).await.unwrap().is_none());
}

// =========================================================
// Wire-cardinality caps (ledger F24, COORD_PLAN §3.3) — the
// SSE bus is rate-capped; state and the event log still see
// every event.
// =========================================================

fn progress_delta(job: &str, worker: WorkerId, files: u64) -> EventKind {
    EventKind::ProgressDelta {
        job_id: jid(job),
        worker_id: worker,
        files_delta: files,
        bytes_delta: 7,
        errors_delta: 0,
    }
}

fn drain_progress_frames(sub: &mut broadcast::Receiver<EventEnvelope>) -> usize {
    let mut n = 0;
    while let Ok(env) = sub.try_recv() {
        if matches!(env.kind, EventKind::ProgressDelta { .. }) {
            n += 1;
        }
    }
    n
}

#[tokio::test]
async fn progress_delta_coalesced_per_job_worker() {
    let (rt, clock, _store) = fresh_runtime().await;
    rt.ingest(job_created("bobby")).await.unwrap();
    let w = WorkerId::new();
    let mut sub = rt.subscribe();

    // Five deltas inside one injected-clock second.
    for _ in 0..5 {
        rt.ingest(progress_delta("bobby", w, 10)).await.unwrap();
    }
    // State folds every delta...
    let snap = rt.state().await;
    assert_eq!(snap.jobs[&jid("bobby")].progress.files_done, 50);
    // ...but the bus carried at most 1 Hz for this (job, worker).
    assert_eq!(
        drain_progress_frames(&mut sub),
        1,
        "five same-second deltas must coalesce to one broadcast",
    );

    // A different worker in the same second is its own key.
    let w2 = WorkerId::new();
    rt.ingest(progress_delta("bobby", w2, 1)).await.unwrap();
    assert_eq!(
        drain_progress_frames(&mut sub),
        1,
        "coalescing is per (job, worker), not global",
    );

    // The next second opens a new broadcast slot for w.
    clock.advance(Duration::seconds(1));
    rt.ingest(progress_delta("bobby", w, 1)).await.unwrap();
    assert_eq!(drain_progress_frames(&mut sub), 1);

    // The suppressed deltas still reached the event log.
    rt.flush_log().await.unwrap();
    let logged = crate::state::read_job_events(_store.as_ref(), &jid("bobby"), 0)
        .await
        .unwrap();
    let logged_deltas = logged
        .iter()
        .filter(|e| matches!(e.kind, EventKind::ProgressDelta { .. }))
        .count();
    assert_eq!(
        logged_deltas, 7,
        "the cap is bus-only; the log must carry every delta",
    );

    // ---- F24 residue 3a: trailing-edge flush ----
    // A fresh burst in a new second: the first delta broadcasts,
    // the burst's FINAL delta is suppressed. Leading-edge-only
    // coalescing left that final value invisible to live
    // subscribers until the next event arrived.
    clock.advance(Duration::seconds(1));
    rt.ingest(progress_delta("bobby", w, 100)).await.unwrap();
    assert_eq!(drain_progress_frames(&mut sub), 1);
    rt.ingest(progress_delta("bobby", w, 42)).await.unwrap();
    assert_eq!(
        drain_progress_frames(&mut sub),
        0,
        "the burst's final delta is suppressed by the cap",
    );
    let last_seq = rt.last_seq().await;
    let writes_before = _store.write_count();

    // One cap interval later the tick delivers the suppressed
    // final VALUES — as a re-broadcast of the already-ingested
    // envelope: same seq, no new event, no log write.
    clock.advance(Duration::seconds(1));
    assert_eq!(
        rt.flush_trailing_progress().await,
        1,
        "the retained trailing delta must re-broadcast on the tick",
    );
    let env = sub
        .try_recv()
        .expect("the trailing frame must reach the subscriber");
    match &env.kind {
        // Lossless coalescing: the trailing frame carries the SUM of
        // every suppressed-and-undelivered delta for the key — the 4
        // suppressed 10s from the first burst (40) plus this burst's
        // suppressed 42. Delivered totals then equal ingested totals
        // (10 + 1 + 100 + 82 = 50 + 1 + 100 + 42 = 193); latest-wins
        // retention dropped the difference on the floor and every
        // client-derived rate under-reported.
        EventKind::ProgressDelta { files_delta, .. } => assert_eq!(
            *files_delta, 82,
            "the trailing frame must carry the coalesced sum of all suppressed deltas",
        ),
        other => panic!("expected the trailing ProgressDelta, got {other:?}"),
    }
    assert_eq!(
        env.seq, last_seq,
        "a re-broadcast of the already-ingested envelope, not a new event",
    );
    assert_eq!(rt.last_seq().await, last_seq, "no new event may be minted");
    assert_eq!(
        _store.write_count(),
        writes_before,
        "the trailing flush is bus-only — no log write",
    );

    // Cleared after delivery: nothing re-broadcasts twice.
    clock.advance(Duration::seconds(1));
    assert_eq!(
        rt.flush_trailing_progress().await,
        0,
        "a delivered trailing frame must not repeat",
    );
    // And when the final delta WAS broadcast (nothing suppressed
    // behind it), the tick stays silent — no duplicate of a frame
    // that was already the last broadcast one.
    rt.ingest(progress_delta("bobby", w, 7)).await.unwrap();
    assert_eq!(drain_progress_frames(&mut sub), 1);
    clock.advance(Duration::seconds(2));
    assert_eq!(
        rt.flush_trailing_progress().await,
        0,
        "no trailing re-broadcast when the last frame was already broadcast",
    );
    assert_eq!(drain_progress_frames(&mut sub), 0);
}

/// COORD_PLAN §3.3: `WorkerHeartbeat` never streams. There is no
/// heartbeat event kind at all — heartbeats mutate worker state
/// directly. Pin that: no event, no log write, no broadcast.
#[tokio::test]
async fn worker_heartbeat_never_streams() {
    let (rt, _clock, store) = fresh_runtime().await;
    rt.ingest(job_created("bobby")).await.unwrap();
    let w = WorkerId::new();
    rt.ingest(EventKind::WorkerJoined {
        worker_id: w,
        job_id: jid("bobby"),
        host: "h".into(),
        pid: 42,
        start_time: at(0),
        version: "0.6".into(),
    })
    .await
    .unwrap();
    rt.flush_log().await.unwrap();

    let mut sub = rt.subscribe();
    let last_seq_before = rt.last_seq().await;
    let writes_before = store.write_count();

    let updated = rt
        .record_heartbeat(
            w,
            crate::schema::WorkerCounters {
                files_per_sec: 1.0,
                bytes_per_sec: 2.0,
                errors_per_min: 0.0,
            },
            crate::schema::WorkerState::Copying,
            3,
            4,
            None,
        )
        .await
        .unwrap();
    assert!(updated);

    // State updated...
    let snap = rt.state().await;
    assert_eq!(snap.workers[&w].state, crate::schema::WorkerState::Copying);
    assert_eq!(snap.workers[&w].queue_depth, 4);
    // ...but nothing was minted, logged, or streamed.
    assert_eq!(rt.last_seq().await, last_seq_before, "no event minted");
    assert_eq!(rt.buffered_event_count().await, 0, "nothing buffered");
    assert_eq!(store.write_count(), writes_before, "nothing written");
    assert!(
        matches!(
            sub.try_recv(),
            Err(tokio::sync::broadcast::error::TryRecvError::Empty),
        ),
        "heartbeats must not reach the bus",
    );
}

/// Regression for the caps: files/bytes totals and bucket counts
/// stay exact under coalescing and bucket capping — identity is
/// lossy, the counts are not.
#[tokio::test]
async fn caps_do_not_break_totals() {
    use crate::schema::{ErrorClass, ERROR_BUCKET_CAP};

    let (rt, _clock, _store) = fresh_runtime().await;
    rt.ingest(job_created("bobby")).await.unwrap();
    let w = WorkerId::new();

    // 30 same-second deltas (coalesced on the bus).
    for _ in 0..30 {
        rt.ingest(progress_delta("bobby", w, 3)).await.unwrap();
    }
    // More distinct error classes than the bucket cap.
    let total_errors = ERROR_BUCKET_CAP + 10;
    for i in 0..total_errors {
        rt.ingest(EventKind::ErrorEmitted {
            job_id: jid("bobby"),
            worker_id: w,
            class: ErrorClass::Other(format!("c{i}")),
            path: format!("/p/{i}"),
            retryable: false,
            message: "x".into(),
        })
        .await
        .unwrap();
    }

    let snap = rt.state().await;
    let p = &snap.jobs[&jid("bobby")].progress;
    assert_eq!(p.files_done, 90, "files total must be exact");
    assert_eq!(p.bytes_done, 210, "bytes total must be exact");
    let sum: u64 = snap.error_buckets[&jid("bobby")]
        .iter()
        .map(|b| b.count)
        .sum();
    assert_eq!(
        sum, total_errors as u64,
        "bucket counts must be exact under capping",
    );
}

// =========================================================
// F45b — event-chunk flush must not hold the runtime lock
// across S3 PUTs (COORD_RUNTIME_BATCH item 1).
//
// Store double: PUTs under `events/` report entry on an mpsc
// and then park on a watch-channel gate until the test opens
// it — the gated cousin of `FailEventPuts`
// (tests/worker_endpoints.rs) and `FailArchivePuts`
// (tests/archive_wiring.rs). Holding a flush in flight lets
// the tests probe reads, ingest, and a racing second flush.
// =========================================================

#[derive(Debug)]
struct GatedEventPuts {
    inner: Arc<MemStore>,
    gate: tokio::sync::watch::Receiver<bool>,
    entered_tx: tokio::sync::mpsc::UnboundedSender<String>,
}

impl GatedEventPuts {
    #[allow(clippy::type_complexity)]
    fn new() -> (
        Arc<Self>,
        Arc<MemStore>,
        tokio::sync::watch::Sender<bool>,
        tokio::sync::mpsc::UnboundedReceiver<String>,
    ) {
        let mem = Arc::new(MemStore::new());
        let (open_tx, open_rx) = tokio::sync::watch::channel(false);
        let (entered_tx, entered_rx) = tokio::sync::mpsc::unbounded_channel();
        (
            Arc::new(Self {
                inner: mem.clone(),
                gate: open_rx,
                entered_tx,
            }),
            mem,
            open_tx,
            entered_rx,
        )
    }
}

#[async_trait::async_trait]
impl CoordStore for GatedEventPuts {
    async fn get(&self, key: &str) -> Result<Option<(Vec<u8>, String)>> {
        self.inner.get(key).await
    }
    async fn head(&self, key: &str) -> Result<Option<String>> {
        self.inner.head(key).await
    }
    async fn put(&self, key: &str, body: Vec<u8>) -> Result<String> {
        if key.starts_with("events/") {
            let _ = self.entered_tx.send(key.to_string());
            let mut gate = self.gate.clone();
            while !*gate.borrow() {
                gate.changed()
                    .await
                    .map_err(|_| Error::Other(anyhow::anyhow!("gate sender dropped")))?;
            }
        }
        self.inner.put(key, body).await
    }
    async fn put_if_absent(&self, key: &str, body: Vec<u8>) -> Result<crate::store::PutOutcome> {
        self.inner.put_if_absent(key, body).await
    }
    async fn delete(&self, key: &str) -> Result<()> {
        self.inner.delete(key).await
    }
    async fn delete_if_match(
        &self,
        key: &str,
        etag: &str,
    ) -> Result<migration_core::claim::DeleteOutcome> {
        self.inner.delete_if_match(key, etag).await
    }
    async fn list(&self, prefix: &str) -> Result<Vec<crate::store::ListEntry>> {
        self.inner.list(prefix).await
    }
}

#[allow(clippy::type_complexity)]
async fn gated_runtime() -> (
    CoordRuntime,
    Arc<MemStore>,
    tokio::sync::watch::Sender<bool>,
    tokio::sync::mpsc::UnboundedReceiver<String>,
) {
    let (gated, mem, open, entered) = GatedEventPuts::new();
    let store: Arc<dyn CoordStore> = gated;
    let clock = FixedClock::new(at(0));
    let rt = CoordRuntime::start(store, clock, me("A"), cfg_for_tests())
        .await
        .unwrap();
    (rt, mem, open, entered)
}

/// F45b acceptance 1: a state read completes while a chunk PUT
/// is in flight. Red before the fix — `flush_log` held the
/// runtime lock across the PUT, so `state()` parked behind the
/// full S3 round-trip.
#[tokio::test]
async fn reads_do_not_block_on_inflight_flush() {
    let (rt, mem, open, mut entered) = gated_runtime().await;
    rt.ingest(job_created("bobby")).await.unwrap();
    rt.ingest(job_created("mary")).await.unwrap();

    let flusher = {
        let rt = rt.clone();
        tokio::spawn(async move { rt.flush_log().await })
    };
    entered
        .recv()
        .await
        .expect("flush must reach the store PUT");

    // The PUT is parked on the gate; reads must still complete.
    let snap = tokio::time::timeout(std::time::Duration::from_secs(1), rt.state())
        .await
        .expect("state() must not block on an in-flight chunk PUT");
    assert!(snap.jobs.contains_key(&jid("bobby")));
    let job = tokio::time::timeout(std::time::Duration::from_secs(1), rt.job_view(&jid("mary")))
        .await
        .expect("job_view() must not block on an in-flight chunk PUT");
    assert!(job.is_some());

    open.send(true).unwrap();
    flusher.await.unwrap().unwrap();
    // Flush-before-ack intact: the awaited flush left every
    // buffered chunk durable before returning.
    assert_eq!(mem.list("events/bobby/").await.unwrap().len(), 1);
    assert_eq!(mem.list("events/mary/").await.unwrap().len(), 1);
    assert_eq!(rt.buffered_event_count().await, 0);
}

/// F45b acceptance 2: an ingest completes (applies to state,
/// buffers) while a chunk PUT is in flight, and the new event
/// still becomes durable afterwards. Red before the fix.
#[tokio::test]
async fn ingest_does_not_block_on_inflight_flush() {
    let (rt, mem, open, mut entered) = gated_runtime().await;
    rt.ingest(job_created("bobby")).await.unwrap();

    let flusher = {
        let rt = rt.clone();
        tokio::spawn(async move { rt.flush_log().await })
    };
    entered
        .recv()
        .await
        .expect("flush must reach the store PUT");

    let seq = tokio::time::timeout(
        std::time::Duration::from_secs(1),
        rt.ingest(job_created("mary")),
    )
    .await
    .expect("ingest must not block on an in-flight chunk PUT")
    .unwrap();
    assert_eq!(seq, 2);
    // Applied to state and buffered while the PUT is pending.
    assert!(rt.state().await.jobs.contains_key(&jid("mary")));
    assert!(rt.buffered_event_count().await >= 1);

    open.send(true).unwrap();
    flusher.await.unwrap().unwrap();
    // The mid-flight ingest is not lost: the next flush lands it.
    rt.flush_log().await.unwrap();
    assert_eq!(mem.list("events/mary/").await.unwrap().len(), 1);
    assert_eq!(rt.buffered_event_count().await, 0);
}

/// F45b acceptance 3: two flush attempts racing around one gated
/// PUT (with ingests landing mid-flight in the same route). The
/// single-flusher token serializes them; the durable chunks must
/// cover contiguous, non-overlapping seq ranges with every event
/// exactly once.
#[tokio::test]
async fn concurrent_flushes_never_overlap_chunks() {
    let (rt, mem, open, mut entered) = gated_runtime().await;
    let w = WorkerId::new();
    rt.ingest(job_created("bobby")).await.unwrap(); // seq 1
    rt.ingest(progress_delta("bobby", w, 1)).await.unwrap(); // seq 2
    rt.ingest(progress_delta("bobby", w, 1)).await.unwrap(); // seq 3

    let flush_a = {
        let rt = rt.clone();
        tokio::spawn(async move { rt.flush_log().await })
    };
    entered
        .recv()
        .await
        .expect("first flush must reach the PUT");

    // Two more events land in the same route while the PUT for
    // seqs 1..=3 is in flight.
    for _ in 0..2 {
        tokio::time::timeout(
            std::time::Duration::from_secs(1),
            rt.ingest(progress_delta("bobby", w, 1)),
        )
        .await
        .expect("ingest must not block on the in-flight PUT")
        .unwrap();
    }
    // Second flusher parks on the flush token behind the first.
    let flush_b = {
        let rt = rt.clone();
        tokio::spawn(async move { rt.flush_log().await })
    };
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    open.send(true).unwrap();
    flush_a.await.unwrap().unwrap();
    flush_b.await.unwrap().unwrap();

    // Read every durable chunk back: in-chunk seqs contiguous,
    // cross-chunk ranges ascending, non-overlapping, contiguous,
    // covering 1..=5 exactly once.
    let chunks = mem.list("events/bobby/").await.unwrap();
    assert!(
        chunks.len() >= 2,
        "two flushes around the gate should leave at least two chunks: {:?}",
        chunks.iter().map(|c| &c.key).collect::<Vec<_>>(),
    );
    let mut ranges = Vec::new();
    for entry in &chunks {
        let envs = crate::events::read_chunk(mem.as_ref(), &entry.key)
            .await
            .unwrap();
        assert!(!envs.is_empty(), "empty chunk {}", entry.key);
        let seqs: Vec<u64> = envs.iter().map(|e| e.seq).collect();
        for pair in seqs.windows(2) {
            assert_eq!(
                pair[1],
                pair[0] + 1,
                "in-chunk seqs must be contiguous in {}: {seqs:?}",
                entry.key,
            );
        }
        ranges.push((seqs[0], *seqs.last().unwrap()));
    }
    ranges.sort_unstable();
    for pair in ranges.windows(2) {
        assert!(
            pair[0].1 < pair[1].0,
            "chunk seq ranges must not overlap: {ranges:?}",
        );
        assert_eq!(
            pair[1].0,
            pair[0].1 + 1,
            "chunk seq ranges must be contiguous: {ranges:?}",
        );
    }
    assert_eq!(
        (ranges[0].0, ranges.last().unwrap().1),
        (1, 5),
        "chunks must cover every ingested seq exactly once: {ranges:?}",
    );
    assert_eq!(rt.buffered_event_count().await, 0);
}

/// Sanity check: dropping the runtime does not panic even if
/// subscribers are still in scope. The broadcast channel
/// shutdowns gracefully.
#[tokio::test]
async fn drop_runtime_with_live_subscriber_is_safe() {
    let (rt, _clock, _store) = fresh_runtime().await;
    let mut sub = rt.subscribe();
    drop(rt);
    // Receiver sees Closed on next recv.
    let err = sub.try_recv().unwrap_err();
    assert!(matches!(
        err,
        tokio::sync::broadcast::error::TryRecvError::Closed,
    ));
}
