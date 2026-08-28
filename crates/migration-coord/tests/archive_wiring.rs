//! Acceptance tests for ledger F23 — archive-on-completion wiring
//! plus seq-aware event reads.
//!
//! Covers, per `docs/work-items/COORD_ARCHIVE_WIRING.md`:
//!
//! 1. `job_completed_triggers_archive` — the snapshot tick rolls a
//!    completed job's chunks to `archivelogs/`.
//! 2. `job_cancelled_triggers_archive` — same for cancellation.
//! 3. `archive_failure_does_not_lose_events` — a failed archive copy
//!    leaves chunks under `events/`, ingest keeps working, and the
//!    next tick retries.
//! 4. `replay_after_archive_reconstructs_terminal_job` — restart
//!    after archive still shows the job (terminal, from snapshot) and
//!    never reads `archivelogs/` on the live path.
//! 5. `read_all_events_since_skips_low_chunks` — seq-aware reads do
//!    not GET chunks that cannot contain `since`.
//! 6. `sse_reconnect_cost_bounded` — SSE catch-up with a high
//!    `Last-Event-ID` lists chunk keys but GETs only the tail chunk.

use chrono::{DateTime, TimeZone, Utc};
use futures::StreamExt;
use migration_coord::errors::Result as CoordResult;
use migration_coord::events::{read_all_events_since, EventLogConfig, EventLogWriter};
use migration_coord::layout::job_events_chunk_key;
use migration_coord::lease::{Identity, LeaseConfig};
use migration_coord::runtime::test_clock::FixedClock;
use migration_coord::runtime::{CoordRuntime, RuntimeConfig};
use migration_coord::schema::{
    ConfigHash, EventEnvelope, EventKind, JobId, Phase, WorkerId, SCHEMA_VERSION,
};
use migration_coord::server::stream::{sse_stream, JobFilter, StreamConfig, StreamFrame};
use migration_coord::store::{CoordStore, ListEntry, MemStore, PutOutcome};
use migration_coord::ticks::{snapshot_loop, TickerConfig};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio_util::sync::CancellationToken;

fn jid(s: &str) -> JobId {
    JobId::new(s).unwrap()
}

fn me(holder: &str) -> Identity {
    Identity {
        holder_id: holder.into(),
        host: "h".into(),
        pid: 1,
    }
}

fn at(secs: i64) -> DateTime<Utc> {
    Utc.with_ymd_and_hms(2026, 5, 29, 14, 32, 0).unwrap() + chrono::Duration::seconds(secs)
}

fn rt_cfg(max_events_per_chunk: usize) -> RuntimeConfig {
    RuntimeConfig {
        lease: LeaseConfig {
            ttl: chrono::Duration::seconds(30),
            grace: chrono::Duration::seconds(5),
        },
        events: EventLogConfig {
            max_events_per_chunk,
            max_chunk_age: chrono::Duration::seconds(60),
        },
        bus_capacity: 256,
        lease_retry_interval: Duration::from_millis(10),
        lease_retry_max_attempts: Some(3),
    }
}

/// Short-interval ticker; snapshot fires once `snapshot_events` have
/// been ingested since the loop's baseline.
fn ticker_cfg(snapshot_events: u64) -> TickerConfig {
    TickerConfig {
        lease_refresh_interval: Duration::from_millis(20),
        snapshot_check_interval: Duration::from_millis(20),
        snapshot_interval: Duration::from_secs(3600), // time threshold disabled
        snapshot_events,
        history_keep: 3,
        flush_check_interval: Duration::from_millis(20),
        worker_liveness_check_interval: Duration::from_millis(20),
        worker_liveness_timeout: Duration::from_secs(90),
        lease: LeaseConfig {
            ttl: chrono::Duration::seconds(30),
            grace: chrono::Duration::seconds(5),
        },
    }
}

fn job_created(job: &str) -> EventKind {
    EventKind::JobCreated {
        job_id: jid(job),
        name: format!("{job}-mig"),
        source: "nfs://src".into(),
        dest: "nfs://dst".into(),
        owner: "test".into(),
        config_hash: ConfigHash("ab".into()),
        total_files: 0,
        total_bytes: 0,
    }
}

fn progress_delta(job: &str) -> EventKind {
    EventKind::ProgressDelta {
        job_id: jid(job),
        worker_id: WorkerId::new(),
        files_delta: 1,
        bytes_delta: 0,
        errors_delta: 0,
    }
}

async fn start_runtime(
    store: Arc<dyn CoordStore>,
    holder: &str,
    max_events_per_chunk: usize,
) -> (CoordRuntime, Arc<FixedClock>) {
    let clock = FixedClock::new(at(0));
    let rt = CoordRuntime::start(
        store,
        clock.clone(),
        me(holder),
        rt_cfg(max_events_per_chunk),
    )
    .await
    .unwrap();
    (rt, clock)
}

/// Drive a full terminal lifecycle through the snapshot tick and wait
/// for it to fire. `terminal` is the closing event (JobCompleted /
/// JobCancelled).
async fn run_lifecycle_through_tick(rt: &CoordRuntime, job: &str, terminal: EventKind) {
    let shutdown = CancellationToken::new();
    // JobCreated + terminal = 2 events -> event threshold of 2 trips.
    let task = tokio::spawn(snapshot_loop(rt.clone(), ticker_cfg(2), shutdown.clone()));
    // Let the loop capture its baseline before we ingest (see the
    // sleep rationale in ticks.rs's own tests).
    tokio::time::sleep(Duration::from_millis(5)).await;

    rt.ingest(job_created(job)).await.unwrap();
    rt.ingest(terminal).await.unwrap();

    // Give the loop a few ticks: flush + snapshot + archive.
    tokio::time::sleep(Duration::from_millis(150)).await;
    shutdown.cancel();
    task.await.unwrap().unwrap();
}

// =============================================================================
// 1. JobCompleted triggers archive (red before fix)
// =============================================================================

#[tokio::test]
async fn job_completed_triggers_archive() {
    let mem = Arc::new(MemStore::new());
    let (rt, _clock) = start_runtime(mem.clone(), "A", 1000).await;

    run_lifecycle_through_tick(
        &rt,
        "bobby",
        EventKind::JobCompleted {
            job_id: jid("bobby"),
        },
    )
    .await;

    let archived = mem.list("archivelogs/bobby/").await.unwrap();
    assert!(
        !archived.is_empty(),
        "completed job's chunks must be rolled under archivelogs/",
    );
    assert!(
        mem.list("events/bobby/").await.unwrap().is_empty(),
        "archived chunks must be gone from events/",
    );
    // State is untouched by archive: the job is still known, terminal.
    assert_eq!(rt.state().await.jobs[&jid("bobby")].phase, Phase::Completed);
}

// =============================================================================
// 2. JobCancelled triggers archive (red before fix)
// =============================================================================

#[tokio::test]
async fn job_cancelled_triggers_archive() {
    let mem = Arc::new(MemStore::new());
    let (rt, _clock) = start_runtime(mem.clone(), "A", 1000).await;

    run_lifecycle_through_tick(
        &rt,
        "bobby",
        EventKind::JobCancelled {
            job_id: jid("bobby"),
            reason: "operator".into(),
        },
    )
    .await;

    let archived = mem.list("archivelogs/bobby/").await.unwrap();
    assert!(
        !archived.is_empty(),
        "cancelled job's chunks must be rolled under archivelogs/",
    );
    assert!(
        mem.list("events/bobby/").await.unwrap().is_empty(),
        "archived chunks must be gone from events/",
    );
    assert_eq!(rt.state().await.jobs[&jid("bobby")].phase, Phase::Cancelled);
}

// =============================================================================
// 3. Archive failure loses nothing — best-effort with natural retry
// =============================================================================

/// Store wrapper that fails PUTs under `archivelogs/` while the flag
/// is up. Everything else (including the ingest path's PUTs under
/// `events/`) passes through to the inner `MemStore`.
#[derive(Debug)]
struct FailArchivePuts {
    inner: Arc<MemStore>,
    fail: AtomicBool,
}

#[async_trait::async_trait]
impl CoordStore for FailArchivePuts {
    async fn get(&self, key: &str) -> CoordResult<Option<(Vec<u8>, String)>> {
        self.inner.get(key).await
    }
    async fn head(&self, key: &str) -> CoordResult<Option<String>> {
        self.inner.head(key).await
    }
    async fn put(&self, key: &str, body: Vec<u8>) -> CoordResult<String> {
        if self.fail.load(Ordering::SeqCst) && key.starts_with("archivelogs/") {
            return Err(migration_coord::Error::Other(anyhow::anyhow!(
                "injected archive copy failure for {key}"
            )));
        }
        self.inner.put(key, body).await
    }
    async fn put_if_absent(&self, key: &str, body: Vec<u8>) -> CoordResult<PutOutcome> {
        self.inner.put_if_absent(key, body).await
    }
    async fn delete(&self, key: &str) -> CoordResult<()> {
        self.inner.delete(key).await
    }
    async fn delete_if_match(
        &self,
        key: &str,
        etag: &str,
    ) -> CoordResult<migration_core::claim::DeleteOutcome> {
        self.inner.delete_if_match(key, etag).await
    }
    async fn list(&self, prefix: &str) -> CoordResult<Vec<ListEntry>> {
        self.inner.list(prefix).await
    }
}

#[tokio::test]
async fn archive_failure_does_not_lose_events() {
    let mem = Arc::new(MemStore::new());
    let failing = Arc::new(FailArchivePuts {
        inner: mem.clone(),
        fail: AtomicBool::new(true),
    });
    let store: Arc<dyn CoordStore> = failing.clone();
    let (rt, _clock) = start_runtime(store, "A", 1000).await;

    let shutdown = CancellationToken::new();
    let task = tokio::spawn(snapshot_loop(rt.clone(), ticker_cfg(2), shutdown.clone()));
    tokio::time::sleep(Duration::from_millis(5)).await;

    rt.ingest(job_created("bobby")).await.unwrap();
    rt.ingest(EventKind::JobCompleted {
        job_id: jid("bobby"),
    })
    .await
    .unwrap();

    // Several ticks fire; every archive attempt fails at the copy
    // step.
    tokio::time::sleep(Duration::from_millis(150)).await;

    // Chunks are still under events/ (retry-able), nothing landed
    // under archivelogs/, and ingest is unaffected.
    assert!(
        !mem.list("events/bobby/").await.unwrap().is_empty(),
        "failed archive must leave chunks under events/",
    );
    assert!(mem.list("archivelogs/bobby/").await.unwrap().is_empty());
    // Ingest keeps working while archive fails. A cluster-routed
    // event: appending to bobby's route here would (correctly) defer
    // bobby's archive until that tail is flushed, which is not what
    // this test is about.
    rt.ingest(EventKind::WorkerLeft {
        worker_id: WorkerId::new(),
        reason: "drain".into(),
    })
    .await
    .expect("ingest must keep working while archive fails");

    // Heal the store: the next tick retries and completes the move.
    failing.fail.store(false, Ordering::SeqCst);
    tokio::time::sleep(Duration::from_millis(150)).await;
    shutdown.cancel();
    task.await.unwrap().unwrap();

    assert!(
        !mem.list("archivelogs/bobby/").await.unwrap().is_empty(),
        "archive must succeed on a later tick once the store heals",
    );
    assert!(
        mem.list("events/bobby/").await.unwrap().is_empty(),
        "events/ must be drained after the retried archive",
    );
}

// =============================================================================
// 4. Replay after archive reconstructs the terminal job from the
//    snapshot; the live path never reads archivelogs/
// =============================================================================

#[tokio::test]
async fn replay_after_archive_reconstructs_terminal_job() {
    let mem = Arc::new(MemStore::new());
    {
        let (rt, _clock) = start_runtime(mem.clone(), "A", 1000).await;
        run_lifecycle_through_tick(
            &rt,
            "bobby",
            EventKind::JobCompleted {
                job_id: jid("bobby"),
            },
        )
        .await;
        // Archived before shutdown.
        assert!(mem.list("events/bobby/").await.unwrap().is_empty());
        rt.shutdown(3).await.unwrap();
    }

    // Restart: state must come back from the snapshot, terminal, and
    // nothing on the live path may touch archivelogs/.
    mem.clear_ops();
    let (rt2, _clock2) = start_runtime(mem.clone(), "B", 1000).await;
    let snap = rt2.state().await;
    assert_eq!(
        snap.jobs[&jid("bobby")].phase,
        Phase::Completed,
        "terminal job must reconstruct from the snapshot after archive",
    );
    for op in mem.ops() {
        assert!(
            !op.contains("archivelogs/"),
            "live-path replay must not touch archivelogs/: {op}",
        );
    }
}

// =============================================================================
// 5. Seq-aware reads skip chunks that cannot contain `since`
//    (red before fix)
// =============================================================================

/// Seed one flushed chunk per `start_seqs` batch for `job`, each
/// holding `n_per_chunk` consecutive seqs.
async fn seed_chunks(store: &MemStore, job: &str, starts: &[u64], n_per_chunk: u64) {
    let mut w = EventLogWriter::new(EventLogConfig {
        max_events_per_chunk: usize::MAX,
        max_chunk_age: chrono::Duration::seconds(3600),
    });
    for &start in starts {
        for seq in start..start + n_per_chunk {
            let env = EventEnvelope {
                seq,
                at: at(0),
                schema_version: SCHEMA_VERSION,
                worker_at: None,
                client_seq: None,
                from_worker: None,
                kind: EventKind::VerifyStarted { job_id: jid(job) },
            };
            w.append(store, env).await.unwrap();
        }
        w.flush_all(store).await.unwrap();
    }
}

#[tokio::test]
async fn read_all_events_since_skips_low_chunks() {
    let mem = MemStore::new();
    // Chunks starting at 1 / 1000 / 2000 (seq 0 is reserved — the
    // work item's "0/1000/2000" example is adapted accordingly).
    seed_chunks(&mem, "bobby", &[1, 1000, 2000], 3).await;
    mem.clear_ops();

    let since = 1500;
    let events = read_all_events_since(&mem, since).await.unwrap();

    // Results identical to the naive full read: everything > 1500.
    let seqs: Vec<u64> = events.iter().map(|e| e.seq).collect();
    assert_eq!(seqs, vec![2000, 2001, 2002]);

    let gets: Vec<String> = mem
        .ops()
        .into_iter()
        .filter(|op| op.starts_with("GET events/"))
        .collect();
    assert!(
        !gets.contains(&format!("GET {}", job_events_chunk_key("bobby", 1))),
        "since=1500 must not GET the low chunk; ops: {gets:?}",
    );
    // One chunk of slack: the 1000-chunk is still read (its
    // successor starts at 2000 > since), the 2000-chunk holds the
    // results.
    assert_eq!(
        gets,
        vec![
            format!("GET {}", job_events_chunk_key("bobby", 1000)),
            format!("GET {}", job_events_chunk_key("bobby", 2000)),
        ],
    );
}

// =============================================================================
// 6. SSE reconnect cost is bounded: lists chunk keys, GETs only the
//    tail chunk(s)
// =============================================================================

#[tokio::test]
async fn sse_reconnect_cost_bounded() {
    let mem = Arc::new(MemStore::new());
    // 10-event chunks; 100 events -> 10 flushed chunks starting at
    // 1, 11, ..., 91, nothing left buffered.
    let (rt, _clock) = start_runtime(mem.clone(), "A", 10).await;
    rt.ingest(job_created("bobby")).await.unwrap();
    for _ in 0..99 {
        rt.ingest(progress_delta("bobby")).await.unwrap();
    }
    assert_eq!(
        rt.buffered_event_count().await,
        0,
        "test setup: all flushed"
    );

    mem.clear_ops();
    // Reconnect with Last-Event-ID = 95 against the long history.
    let stream = sse_stream(
        rt.clone(),
        Some(95),
        JobFilter::All,
        StreamConfig {
            keepalive_interval: Duration::from_secs(60),
        },
    );
    tokio::pin!(stream);
    let mut seqs = Vec::new();
    for _ in 0..5 {
        match stream.next().await.unwrap().unwrap() {
            StreamFrame::Event(env) => seqs.push(env.seq),
            other => panic!("expected Event frame, got {other:?}"),
        }
    }
    assert_eq!(seqs, vec![96, 97, 98, 99, 100]);

    let ops = mem.ops();
    assert!(
        ops.iter().any(|op| op.starts_with("LIST events/")),
        "catch-up should list chunk keys; ops: {ops:?}",
    );
    let gets: Vec<&String> = ops
        .iter()
        .filter(|op| op.starts_with("GET events/"))
        .collect();
    assert_eq!(
        gets,
        vec![&format!("GET {}", job_events_chunk_key("bobby", 91))],
        "catch-up from seq 95 must GET only the tail chunk",
    );
}
