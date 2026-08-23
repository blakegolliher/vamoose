//! Coordinator replay orchestration over persisted snapshots and event chunks.
//!
//! The deterministic state transition itself is
//! [`migration_control_protocol::reducer`]. This module owns the I/O required to load
//! coordinator state, discover and merge event chunks, and feed ordered
//! envelopes through [`crate::schema::Snapshot::apply`].

#[cfg(any(test, feature = "test-helpers"))]
use crate::errors::Error;
use crate::errors::Result;
use crate::events::{list_chunks, read_chunk};
use crate::layout::EVENTS_PREFIX;
use crate::schema::{EventEnvelope, JobId, Snapshot};
use crate::snapshot;
use crate::store::CoordStore;
use chrono::{DateTime, Utc};

/// Outcome of a replay pass — useful for diagnostics and for the
/// runtime to know where to start its seq counter.
#[derive(Debug)]
pub struct ReplayResult {
    pub state: Snapshot,
    /// Seq the runtime should assign to the next event it appends.
    /// `state.last_seq + 1` if the log carried any events past the
    /// snapshot, otherwise `state.last_seq + 1` still — the +1 is
    /// always safe because seq 0 is reserved (snapshot's last_seq
    /// starts at 0 on a fresh bucket and we never emit seq 0).
    pub next_seq: u64,
    /// Number of events the reducer applied on top of the snapshot.
    pub events_applied: u64,
    /// `last_seq` from the snapshot before replay started. If a
    /// fresh bucket, this is 0.
    pub snapshot_last_seq: u64,
}

/// Discover every event-log chunk under `events/`, grouped by route
/// (`events/_cluster/`, `events/<job_id>/`). The LIST is lexical, so
/// each route's keys are ascending by start seq — the shape
/// `events::skip_chunks_below` wants.
///
/// Used by replay to walk every chunk on the bucket; individual route
/// listings live in `events::list_chunks` for callers that know which
/// route they want.
async fn discover_chunks_by_route(
    store: &dyn CoordStore,
) -> Result<std::collections::BTreeMap<String, Vec<String>>> {
    let entries = store.list(EVENTS_PREFIX).await?;
    let mut by_route = std::collections::BTreeMap::<String, Vec<String>>::new();
    for entry in entries {
        if !entry.key.ends_with(".jsonl") {
            continue;
        }
        let Some(slash) = entry.key.rfind('/') else {
            continue;
        };
        by_route
            .entry(entry.key[..=slash].to_string())
            .or_default()
            .push(entry.key);
    }
    Ok(by_route)
}

/// Load `state/snapshot.json` (defaulting to empty), then walk every
/// chunk under `events/`, merge by seq, and fold envelopes with `seq
/// > snapshot.last_seq` through the protocol reducer.
///
/// Memory footprint: O(events-since-snapshot). The production snapshot
/// cadence caps this at 1000 events or 5 minutes of normal operation. If
/// a future deployment needs streaming replay, this is the function
/// to revisit.
pub async fn replay(store: &dyn CoordStore, now: DateTime<Utc>) -> Result<ReplayResult> {
    let mut state = snapshot::load(store)
        .await?
        .unwrap_or_else(|| Snapshot::empty(now));
    let snapshot_last_seq = state.last_seq;

    let by_route = discover_chunks_by_route(store).await?;
    let mut pending: Vec<EventEnvelope> = Vec::new();
    for keys in by_route.values() {
        // Seq-aware: skip the leading chunks per route that cannot
        // contain events past the snapshot's last_seq.
        for key in crate::events::skip_chunks_below(keys, snapshot_last_seq) {
            for env in read_chunk(store, key).await? {
                if env.seq > state.last_seq {
                    pending.push(env);
                }
            }
        }
    }
    // Stable sort by seq — single-writer guarantees no duplicates,
    // but the lexical chunk-key walk visits routes in
    // (cluster, job-a, job-b, ...) order rather than global seq
    // order. Sort makes the application deterministic.
    pending.sort_by_key(|e| e.seq);

    let events_applied = pending.len() as u64;
    for env in &pending {
        state.apply(env);
    }
    let next_seq = snapshot_last_seq.max(state.last_seq) + 1;

    Ok(ReplayResult {
        state,
        next_seq,
        events_applied,
        snapshot_last_seq,
    })
}

// =============================================================================
// Per-job replay helper (used by /jobs/{id}/events REST endpoint)
// =============================================================================

/// Read every event for a job from the log, in seq order, with seq
/// strictly greater than `since`. Used by the REST endpoint
/// `GET /jobs/{id}/events?since=...` and by tests that need to assert
/// the per-job log contents.
pub async fn read_job_events(
    store: &dyn CoordStore,
    job_id: &JobId,
    since: u64,
) -> Result<Vec<EventEnvelope>> {
    let prefix = crate::layout::job_events_prefix(job_id.as_str());
    let chunks = list_chunks(store, &prefix).await?;
    let mut out = Vec::new();
    // Seq-aware: chunk keys embed their start seq; skip the leading
    // chunks that cannot contain `seq > since`.
    for key in crate::events::skip_chunks_below(&chunks, since) {
        for env in read_chunk(store, key).await? {
            if env.seq > since {
                out.push(env);
            }
        }
    }
    out.sort_by_key(|e| e.seq);
    Ok(out)
}

// Idempotency guard exposed to integration tests: replaying the same
// log twice produces the same final state. Worth pinning even though
// it's implicit in "reducer is pure".
//
// We expose it here rather than only in tests so the (future) coord
// `doctor` subcommand can run it as a self-check.
#[cfg(any(test, feature = "test-helpers"))]
pub async fn assert_replay_is_idempotent(store: &dyn CoordStore, now: DateTime<Utc>) -> Result<()> {
    let a = replay(store, now).await?;
    let b = replay(store, now).await?;
    if a.state != b.state {
        return Err(Error::Other(anyhow::anyhow!(
            "replay produced different state on second pass"
        )));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::events::{EventLogConfig, EventLogWriter};
    use crate::schema::{ConfigHash, ErrorClass, EventKind, Phase, WorkerId, SCHEMA_VERSION};
    use crate::store::MemStore;
    use chrono::{Duration, TimeZone};

    fn at(secs: i64) -> DateTime<Utc> {
        Utc.with_ymd_and_hms(2026, 5, 29, 14, 32, 0).unwrap() + Duration::seconds(secs)
    }

    fn jid(s: &str) -> JobId {
        JobId::new(s).unwrap()
    }

    fn env(seq: u64, secs: i64, kind: EventKind) -> EventEnvelope {
        EventEnvelope {
            seq,
            at: at(secs),
            schema_version: SCHEMA_VERSION,
            worker_at: None,
            client_seq: None,
            from_worker: None,
            kind,
        }
    }

    fn job_created(seq: u64, secs: i64, job: &str) -> EventEnvelope {
        env(
            seq,
            secs,
            EventKind::JobCreated {
                job_id: jid(job),
                name: format!("{job}-migration"),
                source: "nfs://src".into(),
                dest: "nfs://dst".into(),
                owner: "blake".into(),
                config_hash: ConfigHash("deadbeef".into()),
                total_files: 0,
                total_bytes: 0,
            },
        )
    }

    async fn write_log_and_snapshot(
        store: &MemStore,
        envs: &[EventEnvelope],
        snapshot_after: usize,
    ) -> Snapshot {
        let mut writer = EventLogWriter::new(EventLogConfig {
            max_events_per_chunk: 3,
            max_chunk_age: Duration::seconds(60),
        });
        let mut state = Snapshot::empty(at(0));
        for (i, env) in envs.iter().enumerate() {
            writer.append(store, env.clone()).await.unwrap();
            state.apply(env);
            if i + 1 == snapshot_after {
                writer.flush_all(store).await.unwrap();
                snapshot::write(store, &state, 5, env.at).await.unwrap();
            }
        }
        writer.flush_all(store).await.unwrap();
        state
    }

    #[tokio::test]
    async fn replay_from_empty_bucket_yields_empty_state() {
        let s = MemStore::new();
        let r = replay(&s, at(0)).await.unwrap();
        assert!(r.state.jobs.is_empty());
        assert!(r.state.workers.is_empty());
        assert_eq!(r.snapshot_last_seq, 0);
        assert_eq!(r.events_applied, 0);
        assert_eq!(r.next_seq, 1);
    }

    #[tokio::test]
    async fn replay_log_only_reconstitutes_state() {
        let s = MemStore::new();
        let envs = vec![
            job_created(1, 0, "bobby"),
            env(
                2,
                5,
                EventKind::JobPhaseChanged {
                    job_id: jid("bobby"),
                    from: Phase::Planned,
                    to: Phase::Copying,
                    reason: "go".into(),
                },
            ),
            env(
                3,
                6,
                EventKind::ProgressDelta {
                    job_id: jid("bobby"),
                    worker_id: WorkerId::new(),
                    files_delta: 10,
                    bytes_delta: 1024,
                    errors_delta: 0,
                },
            ),
        ];
        let single_pass = write_log_and_snapshot(&s, &envs, 0).await;
        let replayed = replay(&s, at(100)).await.unwrap();
        assert_eq!(replayed.state.jobs, single_pass.jobs);
        assert_eq!(replayed.events_applied, 3);
        assert_eq!(replayed.next_seq, 4);
    }

    #[tokio::test]
    async fn replay_snapshot_plus_log_matches_full_reduce() {
        let s = MemStore::new();
        let envs = vec![
            job_created(1, 0, "bobby"),
            env(
                2,
                5,
                EventKind::ProgressDelta {
                    job_id: jid("bobby"),
                    worker_id: WorkerId::new(),
                    files_delta: 100,
                    bytes_delta: 1_000_000,
                    errors_delta: 0,
                },
            ),
            env(
                3,
                10,
                EventKind::ProgressDelta {
                    job_id: jid("bobby"),
                    worker_id: WorkerId::new(),
                    files_delta: 50,
                    bytes_delta: 500_000,
                    errors_delta: 0,
                },
            ),
            env(
                4,
                15,
                EventKind::JobCompleted {
                    job_id: jid("bobby"),
                },
            ),
        ];
        let single_pass = write_log_and_snapshot(&s, &envs, 2).await;
        let replayed = replay(&s, at(100)).await.unwrap();
        assert_eq!(replayed.state.jobs, single_pass.jobs);
        assert_eq!(replayed.state.workers, single_pass.workers);
        assert_eq!(replayed.state.last_seq, 4);
        assert_eq!(replayed.snapshot_last_seq, 2);
        assert_eq!(replayed.events_applied, 2);
    }

    #[tokio::test]
    async fn replay_is_idempotent() {
        let s = MemStore::new();
        let envs = vec![
            job_created(1, 0, "bobby"),
            env(
                2,
                5,
                EventKind::WorkerJoined {
                    worker_id: WorkerId::new(),
                    job_id: jid("bobby"),
                    host: "h".into(),
                    pid: 1,
                    start_time: at(0),
                    version: "0.6".into(),
                },
            ),
            env(
                3,
                10,
                EventKind::ErrorEmitted {
                    job_id: jid("bobby"),
                    worker_id: WorkerId::new(),
                    class: ErrorClass::Nfs3Err(13),
                    path: "/a".into(),
                    retryable: true,
                    message: "x".into(),
                },
            ),
        ];
        let _ = write_log_and_snapshot(&s, &envs, 1).await;
        assert_replay_is_idempotent(&s, at(0)).await.unwrap();
    }

    #[tokio::test]
    async fn replay_handles_multiple_jobs_interleaved() {
        let s = MemStore::new();
        let envs = vec![
            job_created(1, 0, "bobby"),
            job_created(2, 0, "mary"),
            env(
                3,
                5,
                EventKind::ProgressDelta {
                    job_id: jid("bobby"),
                    worker_id: WorkerId::new(),
                    files_delta: 10,
                    bytes_delta: 1024,
                    errors_delta: 0,
                },
            ),
            env(
                4,
                5,
                EventKind::ProgressDelta {
                    job_id: jid("mary"),
                    worker_id: WorkerId::new(),
                    files_delta: 20,
                    bytes_delta: 2048,
                    errors_delta: 0,
                },
            ),
        ];
        let single_pass = write_log_and_snapshot(&s, &envs, 0).await;
        let replayed = replay(&s, at(100)).await.unwrap();
        assert_eq!(replayed.state.jobs, single_pass.jobs);
        assert_eq!(replayed.state.jobs[&jid("bobby")].progress.files_done, 10);
        assert_eq!(replayed.state.jobs[&jid("mary")].progress.files_done, 20);
    }
}
