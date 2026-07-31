//! In-memory coord state + reducer + replay.
//!
//! The reducer is a single function `Snapshot::apply(env)` that
//! mutates state in response to an event. Replay just loads the
//! latest snapshot, walks every event chunk in seq order, and feeds
//! envelopes with `seq > last_seq` through the reducer.
//!
//! ## Snapshot-as-state
//!
//! Rather than introducing a separate "in-memory state" type, the
//! runtime keeps a `Snapshot` value and mutates it directly. The
//! snapshot writer (`crate::snapshot::write`) re-stamps
//! `written_at` and `schema_version` before serializing; everything
//! else round-trips by construction.
//!
//! ## Determinism
//!
//! Events carry coord-assigned seq. Replay merges all available
//! chunks (cluster + per-job) into a single seq-ordered iterator
//! and applies them in order. The merge guarantees that two
//! independent replays of the same log produce byte-identical state
//! (modulo coord clock — the snapshot's `written_at` is set at
//! serialize time).
//!
//! ## What the reducer covers in Phase 1
//!
//! - Job lifecycle: created, phase changes, paused/resumed/cancelled/
//!   completed/failed (each updates phase + appends to phase_history).
//! - Worker lifecycle: joined, left, state-changed, fenced, recovered.
//! - Progress aggregation: ProgressDelta folds into Job.progress.
//! - Error aggregation: ErrorEmitted lands in the per-job error
//!   bucket vector (newest sample wins on overflow past
//!   ERROR_SAMPLE_CAP).
//! - Claim conflict count: ClaimConflictResolved bumps the per-job
//!   counter.
//!
//! Verify events are routed but produce no aggregate state change in
//! Phase 1 beyond Phase transitions. Phase 5 (TUI verify tab) will
//! grow more nuanced derived state from them; that's intentionally
//! deferred.

#[cfg(any(test, feature = "test-helpers"))]
use crate::errors::Error;
use crate::errors::Result;
use crate::events::{list_chunks, read_chunk};
use crate::layout::EVENTS_PREFIX;
use crate::schema::{
    ErrorBucket, ErrorClass, EventEnvelope, EventKind, Job, JobConfig, JobId, Phase,
    PhaseTransition, Progress, Snapshot, Worker, WorkerCounters, WorkerState, ERROR_BUCKET_CAP,
    ERROR_OVERFLOW_CLASS, ERROR_SAMPLE_CAP, PHASE_HISTORY_CAP,
};
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

impl Snapshot {
    /// Apply an event envelope. Pure on `(state, env)`; the reducer
    /// only reads `env.kind` (and the seq it's about to install).
    /// Caller updates `self.last_seq` to `env.seq` after — keeping
    /// that bookkeeping out of the match keeps the reducer easier to
    /// audit.
    pub fn apply(&mut self, env: &EventEnvelope) {
        // Per-worker client_seq high-water mark (ledger F20, D4).
        // Runs in the reducer — not the ingest handler — so replay
        // reconstructs the mark from the durable stream and the
        // snapshot carries it like every other piece of state. Only
        // envelopes with worker attribution can advance it; the
        // `max` keeps replay idempotent and tolerates historical
        // out-of-order logs without regressing the mark.
        //
        // Attribution prefers the envelope's `from_worker` caller
        // stamp (F20 residue — set by the worker events route, so
        // stamped kinds whose payload carries only conflict roles
        // still advance the mark) and falls back to the kind's
        // payload attribution for envelopes from older coords, whose
        // HWM behavior stays byte-identical.
        let attributed = env.from_worker.or_else(|| env.kind.attributed_worker());
        if let (Some(cs), Some(worker)) = (env.client_seq, attributed) {
            let hwm = self.last_client_seq.entry(worker).or_insert(0);
            *hwm = (*hwm).max(cs);
        }

        match &env.kind {
            EventKind::JobCreated {
                job_id,
                name,
                source,
                dest,
                owner,
                config_hash,
            } => {
                // Synthesizing JobConfig from event fields is the
                // event-creator's responsibility for now; the event
                // carries no config payload (it's referenced by hash).
                // For replay we install a minimal Job with the config
                // we already have on disk if available. Phase 1 reads
                // jobs/{id}/config.json on demand from the REST
                // handlers (Phase 2); the in-memory Job carries a
                // sentinel config that the REST handler refreshes.
                self.jobs.insert(
                    job_id.clone(),
                    Job {
                        id: job_id.clone(),
                        name: name.clone(),
                        source: source.clone(),
                        dest: dest.clone(),
                        owner: owner.clone(),
                        created_at: env.at,
                        config_hash: config_hash.clone(),
                        // Sentinel: Phase 2 REST overlays this from
                        // jobs/{id}/config.json. Fields the replay
                        // reducer touches (phase, progress, etc.)
                        // are not on JobConfig, so the sentinel is
                        // safe to carry through replay.
                        config: sentinel_config(source, dest),
                        phase: Phase::Planned,
                        phase_history: Vec::new(),
                        progress: Progress::default(),
                        throughput: Default::default(),
                        eta: Default::default(),
                        health: Default::default(),
                        assigned_workers: Vec::new(),
                    },
                );
            }

            EventKind::JobPhaseChanged {
                job_id,
                from: _,
                to,
                reason,
                // The event's `from` is advisory; transition_phase
                // derives the real one from state and the legality
                // guard (F25) rejects anything the current phase
                // does not allow.
            } => transition_phase(self, job_id, *to, reason, env.at),

            EventKind::JobPaused { job_id, reason } => {
                transition_phase(self, job_id, Phase::Paused, reason, env.at);
            }

            EventKind::JobResumed { job_id, reason } => {
                // Resume is only legal from Paused (F25) — checked
                // here explicitly because for a non-paused job the
                // derived target below could otherwise look like a
                // legal forward transition (e.g. Planned -> Scanning
                // on empty history).
                let current = self.jobs.get(job_id).map(|j| j.phase);
                if current == Some(Phase::Paused) {
                    // Resume returns to the last non-paused phase, or
                    // Scanning if there's no history (a paused-at-
                    // creation job).
                    let resume_to = self
                        .jobs
                        .get(job_id)
                        .map(|j| {
                            j.phase_history
                                .iter()
                                .rev()
                                .find(|t| t.from != Phase::Paused)
                                .map(|t| t.from)
                                .unwrap_or(Phase::Scanning)
                        })
                        .expect("job exists: current phase was read above");
                    transition_phase(self, job_id, resume_to, reason, env.at);
                } else if let Some(current) = current {
                    tracing::warn!(
                        job = %job_id,
                        phase = ?current,
                        reason,
                        "ignoring JobResumed for a job that is not paused",
                    );
                }
            }

            EventKind::JobCancelled { job_id, reason } => {
                transition_phase(self, job_id, Phase::Cancelled, reason, env.at);
            }

            EventKind::JobCompleted { job_id } => {
                transition_phase(self, job_id, Phase::Completed, "ok", env.at);
            }

            EventKind::JobFailed { job_id, reason } => {
                transition_phase(self, job_id, Phase::Failed, reason, env.at);
            }

            EventKind::WorkerJoined {
                worker_id,
                job_id,
                host,
                pid,
                start_time,
                version,
            } => {
                self.workers.insert(
                    *worker_id,
                    Worker {
                        id: *worker_id,
                        job_id: job_id.clone(),
                        host: host.clone(),
                        pid: *pid,
                        start_time: *start_time,
                        version: version.clone(),
                        joined_at: env.at,
                        last_heartbeat: env.at,
                        state: WorkerState::Idle,
                        assigned_shard: None,
                        queue_depth: 0,
                        inflight_ops: 0,
                        counters: WorkerCounters::default(),
                        last_error: None,
                        fence_reason: None,
                    },
                );
                if let Some(j) = self.jobs.get_mut(job_id) {
                    if !j.assigned_workers.contains(worker_id) {
                        j.assigned_workers.push(*worker_id);
                    }
                }
            }

            EventKind::WorkerLeft { worker_id, reason } => {
                if let Some(w) = self.workers.get_mut(worker_id) {
                    w.state = WorkerState::Disconnected;
                    w.last_error = Some(reason.clone());
                }
            }

            EventKind::WorkerStateChanged {
                worker_id,
                from: _,
                to,
            } => {
                if let Some(w) = self.workers.get_mut(worker_id) {
                    w.state = *to;
                }
            }

            EventKind::WorkerFenced { worker_id, reason } => {
                if let Some(w) = self.workers.get_mut(worker_id) {
                    w.state = WorkerState::Fenced;
                    w.fence_reason = Some(reason.clone());
                }
            }

            EventKind::WorkerRecovered { worker_id } => {
                if let Some(w) = self.workers.get_mut(worker_id) {
                    w.state = WorkerState::Idle;
                    w.fence_reason = None;
                }
            }

            EventKind::ProgressDelta {
                job_id,
                worker_id: _,
                files_delta,
                bytes_delta,
                errors_delta,
            } => {
                if let Some(j) = self.jobs.get_mut(job_id) {
                    j.progress.files_done = j.progress.files_done.saturating_add(*files_delta);
                    j.progress.bytes_done = j.progress.bytes_done.saturating_add(*bytes_delta);
                    j.progress.errors_total = j.progress.errors_total.saturating_add(*errors_delta);
                }
            }

            EventKind::ErrorEmitted {
                job_id,
                worker_id: _,
                class,
                path,
                retryable,
                message: _,
            } => {
                let buckets = self.error_buckets.entry(job_id.clone()).or_default();
                // Cardinality cap (ledger F24): `Other(String)` is
                // free-form, so distinct classes are unbounded. Once
                // the job has ERROR_BUCKET_CAP buckets, a NEW class
                // folds into the catch-all bucket instead — its
                // identity is dropped, its count is not. The vector
                // therefore holds at most ERROR_BUCKET_CAP + 1
                // entries (the cap plus the catch-all).
                let effective_class = if buckets.len() < ERROR_BUCKET_CAP
                    || buckets.iter().any(|b| &b.class == class)
                {
                    class.clone()
                } else {
                    ErrorClass::Other(ERROR_OVERFLOW_CLASS.to_string())
                };
                if let Some(b) = buckets.iter_mut().find(|b| b.class == effective_class) {
                    b.count = b.count.saturating_add(1);
                    b.last_seen = env.at;
                    push_sample(&mut b.sample_paths, path);
                    // retryable can flip if the same class is seen
                    // with different retryability — preserve the
                    // *most recent* signal, which is operationally
                    // what an operator wants for "is the latest
                    // error retryable?".
                    b.retryable = *retryable;
                } else {
                    buckets.push(ErrorBucket {
                        class: effective_class,
                        count: 1,
                        first_seen: env.at,
                        last_seen: env.at,
                        sample_paths: vec![path.clone()],
                        retryable: *retryable,
                    });
                }
            }

            EventKind::ClaimConflictDetected { .. } => {
                // No state change on detection — the resolution is
                // what we count.
            }

            EventKind::ClaimConflictResolved {
                job_id,
                shard_id: _,
                winner: _,
            } => {
                if let Some(j) = self.jobs.get_mut(job_id) {
                    j.progress.conflicts_resolved = j.progress.conflicts_resolved.saturating_add(1);
                }
            }

            EventKind::VerifyStarted { job_id } => {
                transition_phase(self, job_id, Phase::Verifying, "verify start", env.at);
            }

            EventKind::VerifyCompleted { job_id, mismatches } => {
                let to = if *mismatches == 0 {
                    Phase::Cutover
                } else {
                    Phase::Failed
                };
                let reason = if *mismatches == 0 {
                    "verify ok".to_string()
                } else {
                    format!("verify: {mismatches} mismatches")
                };
                transition_phase(self, job_id, to, &reason, env.at);
            }

            EventKind::VerifyFileMismatch { .. } => {
                // Streams to SSE but no aggregate state change in
                // Phase 1.
            }
        }

        self.last_seq = env.seq;
    }
}

fn sentinel_config(source: &str, dest: &str) -> JobConfig {
    JobConfig {
        source: source.to_string(),
        dest: dest.to_string(),
        claim_version: 2,
        conflict_policy: crate::schema::ConflictPolicy::Fail,
        exclusions: Vec::new(),
        parallelism: Default::default(),
        rate_caps: Default::default(),
        acl_handling: crate::schema::AclHandling::PosixOnly,
        verify_mode: crate::schema::VerifyMode::Stat,
    }
}

fn transition_phase(
    state: &mut Snapshot,
    job_id: &JobId,
    to: Phase,
    reason: &str,
    at: DateTime<Utc>,
) {
    if let Some(j) = state.jobs.get_mut(job_id) {
        // `from` is always the job's actual phase — events that carry
        // their own `from` (JobPhaseChanged) may disagree with state,
        // and the history should record what really happened.
        let from = j.phase;
        // Drop no-op transitions (Paused→Paused etc.) — the operator
        // sees no value in them and they would clutter phase_history.
        if from == to {
            return;
        }
        // Legality guard (ledger F25). Replay compatibility: the log
        // may carry historical illegal events (pre-guard writers) —
        // warn and skip, never panic, never apply.
        if !crate::schema::phase_transition_allowed(from, to) {
            tracing::warn!(
                job = %job_id,
                ?from,
                ?to,
                reason,
                "ignoring illegal phase transition event",
            );
            return;
        }
        j.phase = to;
        j.phase_history.push(PhaseTransition {
            from,
            to,
            at,
            reason: reason.to_string(),
        });
        // Bound history growth (ledger F24): trim the oldest entry.
        // Resume-target derivation reads from the tail, so this is
        // safe for the JobResumed arm.
        if j.phase_history.len() > PHASE_HISTORY_CAP {
            j.phase_history.remove(0);
        }
    }
}

fn push_sample(samples: &mut Vec<String>, new: &str) {
    if samples.iter().any(|s| s == new) {
        return;
    }
    samples.push(new.to_string());
    if samples.len() > ERROR_SAMPLE_CAP {
        // Newest-wins on overflow: drop the oldest entry.
        samples.remove(0);
    }
}

// =============================================================================
// Replay
// =============================================================================

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
/// > snapshot.last_seq` through the reducer.
///
/// Memory footprint: O(events-since-snapshot). For Phase 1 that is
/// acceptable (snapshots cap at 1000 events or 5 minutes apart). If
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
/// strictly greater than `since`. Used by the (Phase 2) REST endpoint
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
    use crate::schema::{ConfigHash, ErrorClass, EventKind, WorkerId, SCHEMA_VERSION};
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
            },
        )
    }

    #[test]
    fn job_created_installs_planned_job() {
        let mut s = Snapshot::empty(at(0));
        s.apply(&job_created(1, 0, "bobby"));
        let j = s.jobs.get(&jid("bobby")).unwrap();
        assert_eq!(j.phase, Phase::Planned);
        assert_eq!(j.owner, "blake");
        assert_eq!(s.last_seq, 1);
    }

    #[test]
    fn phase_transitions_appended_to_history() {
        let mut s = Snapshot::empty(at(0));
        s.apply(&job_created(1, 0, "bobby"));
        s.apply(&env(
            2,
            10,
            EventKind::JobPhaseChanged {
                job_id: jid("bobby"),
                from: Phase::Planned,
                to: Phase::Scanning,
                reason: "scan start".into(),
            },
        ));
        let j = s.jobs.get(&jid("bobby")).unwrap();
        assert_eq!(j.phase, Phase::Scanning);
        assert_eq!(j.phase_history.len(), 1);
        assert_eq!(j.phase_history[0].to, Phase::Scanning);
    }

    #[test]
    fn pause_then_resume_returns_to_prior_phase() {
        let mut s = Snapshot::empty(at(0));
        s.apply(&job_created(1, 0, "bobby"));
        s.apply(&env(
            2,
            10,
            EventKind::JobPhaseChanged {
                job_id: jid("bobby"),
                from: Phase::Planned,
                to: Phase::Copying,
                reason: "x".into(),
            },
        ));
        s.apply(&env(
            3,
            20,
            EventKind::JobPaused {
                job_id: jid("bobby"),
                reason: "op".into(),
            },
        ));
        assert_eq!(s.jobs[&jid("bobby")].phase, Phase::Paused);

        s.apply(&env(
            4,
            30,
            EventKind::JobResumed {
                job_id: jid("bobby"),
                reason: "op".into(),
            },
        ));
        assert_eq!(s.jobs[&jid("bobby")].phase, Phase::Copying);
    }

    #[test]
    fn progress_delta_folds_into_counters() {
        let mut s = Snapshot::empty(at(0));
        s.apply(&job_created(1, 0, "bobby"));
        s.apply(&env(
            2,
            10,
            EventKind::ProgressDelta {
                job_id: jid("bobby"),
                worker_id: WorkerId::new(),
                files_delta: 100,
                bytes_delta: 1_000_000,
                errors_delta: 2,
            },
        ));
        s.apply(&env(
            3,
            11,
            EventKind::ProgressDelta {
                job_id: jid("bobby"),
                worker_id: WorkerId::new(),
                files_delta: 50,
                bytes_delta: 500_000,
                errors_delta: 0,
            },
        ));
        let p = &s.jobs[&jid("bobby")].progress;
        assert_eq!(p.files_done, 150);
        assert_eq!(p.bytes_done, 1_500_000);
        assert_eq!(p.errors_total, 2);
    }

    #[test]
    fn error_emitted_aggregates_per_class() {
        let mut s = Snapshot::empty(at(0));
        s.apply(&job_created(1, 0, "bobby"));
        for i in 0..3 {
            s.apply(&env(
                2 + i,
                10 + i as i64,
                EventKind::ErrorEmitted {
                    job_id: jid("bobby"),
                    worker_id: WorkerId::new(),
                    class: ErrorClass::Nfs3Err(13),
                    path: format!("/a/b/{i}"),
                    retryable: true,
                    message: "EACCES".into(),
                },
            ));
        }
        let buckets = &s.error_buckets[&jid("bobby")];
        assert_eq!(buckets.len(), 1);
        assert_eq!(buckets[0].count, 3);
        assert_eq!(buckets[0].sample_paths.len(), 3);
    }

    #[test]
    fn error_sample_cap_evicts_oldest() {
        let mut s = Snapshot::empty(at(0));
        s.apply(&job_created(1, 0, "bobby"));
        for i in 0..(ERROR_SAMPLE_CAP + 5) {
            s.apply(&env(
                2 + i as u64,
                10 + i as i64,
                EventKind::ErrorEmitted {
                    job_id: jid("bobby"),
                    worker_id: WorkerId::new(),
                    class: ErrorClass::Permission,
                    path: format!("/a/b/{i}"),
                    retryable: false,
                    message: "EPERM".into(),
                },
            ));
        }
        let buckets = &s.error_buckets[&jid("bobby")];
        assert_eq!(buckets[0].sample_paths.len(), ERROR_SAMPLE_CAP);
        // Oldest path (/a/b/0) was evicted; newest (/a/b/14) is present.
        assert!(!buckets[0].sample_paths.contains(&"/a/b/0".to_string()));
        assert!(buckets[0]
            .sample_paths
            .contains(&format!("/a/b/{}", ERROR_SAMPLE_CAP + 4)));
    }

    /// Ledger F24: `ErrorClass::Other(String)` is free-form, so the
    /// per-job bucket vector must stop growing at `ERROR_BUCKET_CAP`;
    /// overflow folds into a catch-all bucket. Identities past the
    /// cap are lossy, counts are not.
    #[test]
    fn error_bucket_count_capped_per_job() {
        use crate::schema::{ERROR_BUCKET_CAP, ERROR_OVERFLOW_CLASS};

        let mut s = Snapshot::empty(at(0));
        s.apply(&job_created(1, 0, "bobby"));
        let extra = 25usize;
        let total = ERROR_BUCKET_CAP + extra;
        for i in 0..total {
            s.apply(&env(
                2 + i as u64,
                10 + i as i64,
                EventKind::ErrorEmitted {
                    job_id: jid("bobby"),
                    worker_id: WorkerId::new(),
                    class: ErrorClass::Other(format!("weird-{i}")),
                    path: format!("/p/{i}"),
                    retryable: false,
                    message: "x".into(),
                },
            ));
        }

        let buckets = &s.error_buckets[&jid("bobby")];
        assert!(
            buckets.len() <= ERROR_BUCKET_CAP + 1,
            "bucket vec must stop growing at the cap (+1 catch-all), got {}",
            buckets.len(),
        );
        let overflow = buckets
            .iter()
            .find(|b| b.class == ErrorClass::Other(ERROR_OVERFLOW_CLASS.to_string()))
            .expect("overflow must fold into the catch-all bucket");
        assert_eq!(
            overflow.count, extra as u64,
            "catch-all bucket must count every folded record",
        );
        // Counts are exact even though identities are dropped.
        let sum: u64 = buckets.iter().map(|b| b.count).sum();
        assert_eq!(sum, total as u64);

        // And it really has stopped growing.
        let len_before = buckets.len();
        for i in 0..10u64 {
            s.apply(&env(
                2 + total as u64 + i,
                1000 + i as i64,
                EventKind::ErrorEmitted {
                    job_id: jid("bobby"),
                    worker_id: WorkerId::new(),
                    class: ErrorClass::Other(format!("more-{i}")),
                    path: "/q".into(),
                    retryable: false,
                    message: "x".into(),
                },
            ));
        }
        assert_eq!(s.error_buckets[&jid("bobby")].len(), len_before);
    }

    /// Ledger F24 (cheap-cap note): `phase_history` grows per
    /// transition; unbounded pause/resume cycles must not bloat
    /// state and snapshots forever. Oldest entries are trimmed;
    /// resume still returns to the prior phase because derivation
    /// reads from the tail.
    #[test]
    fn phase_history_capped_and_resume_still_works() {
        use crate::schema::PHASE_HISTORY_CAP;

        let mut s = Snapshot::empty(at(0));
        s.apply(&job_created(1, 0, "bobby"));
        s.apply(&env(
            2,
            1,
            EventKind::JobPhaseChanged {
                job_id: jid("bobby"),
                from: Phase::Planned,
                to: Phase::Copying,
                reason: "go".into(),
            },
        ));
        let mut seq = 3u64;
        for i in 0..(PHASE_HISTORY_CAP as i64) {
            s.apply(&env(
                seq,
                10 + 2 * i,
                EventKind::JobPaused {
                    job_id: jid("bobby"),
                    reason: "op".into(),
                },
            ));
            seq += 1;
            s.apply(&env(
                seq,
                11 + 2 * i,
                EventKind::JobResumed {
                    job_id: jid("bobby"),
                    reason: "op".into(),
                },
            ));
            seq += 1;
        }
        let j = &s.jobs[&jid("bobby")];
        assert_eq!(
            j.phase_history.len(),
            PHASE_HISTORY_CAP,
            "history must be capped",
        );
        // The final resume still landed back on Copying.
        assert_eq!(j.phase, Phase::Copying);
    }

    #[test]
    fn worker_lifecycle_drives_state_changes() {
        let mut s = Snapshot::empty(at(0));
        s.apply(&job_created(1, 0, "bobby"));
        let w = WorkerId::new();
        s.apply(&env(
            2,
            10,
            EventKind::WorkerJoined {
                worker_id: w,
                job_id: jid("bobby"),
                host: "h".into(),
                pid: 42,
                start_time: at(0),
                version: "0.6".into(),
            },
        ));
        assert_eq!(s.workers[&w].state, WorkerState::Idle);

        s.apply(&env(
            3,
            11,
            EventKind::WorkerStateChanged {
                worker_id: w,
                from: WorkerState::Idle,
                to: WorkerState::Copying,
            },
        ));
        assert_eq!(s.workers[&w].state, WorkerState::Copying);

        s.apply(&env(
            4,
            12,
            EventKind::WorkerFenced {
                worker_id: w,
                reason: "self-fence R7".into(),
            },
        ));
        assert_eq!(s.workers[&w].state, WorkerState::Fenced);
        assert_eq!(s.workers[&w].fence_reason.as_deref(), Some("self-fence R7"),);

        s.apply(&env(5, 13, EventKind::WorkerRecovered { worker_id: w }));
        assert_eq!(s.workers[&w].state, WorkerState::Idle);
        assert!(s.workers[&w].fence_reason.is_none());

        // assigned_workers was populated by WorkerJoined.
        assert_eq!(s.jobs[&jid("bobby")].assigned_workers, vec![w]);

        // Legacy envelopes (this fixture carries no caller
        // attribution — the pre-F20-residue wire shape) keep their
        // HWM behavior byte-identical: a stamped envelope of an
        // attributed kind advances the mark via attributed_worker()…
        let mut legacy = env(
            6,
            14,
            EventKind::WorkerStateChanged {
                worker_id: w,
                from: WorkerState::Idle,
                to: WorkerState::Copying,
            },
        );
        legacy.client_seq = Some(7);
        s.apply(&legacy);
        assert_eq!(
            s.last_client_seq.get(&w).copied(),
            Some(7),
            "legacy stamped attributed envelope must advance the HWM",
        );
        // …and a stamped envelope of a non-attributed kind does NOT —
        // its winner field is a conflict role, and without a caller
        // stamp the reducer must not guess.
        let mut legacy_tail = env(
            7,
            15,
            EventKind::ClaimConflictResolved {
                job_id: jid("bobby"),
                shard_id: crate::schema::ShardId("s1".into()),
                winner: w,
            },
        );
        legacy_tail.client_seq = Some(9);
        s.apply(&legacy_tail);
        assert_eq!(
            s.last_client_seq.get(&w).copied(),
            Some(7),
            "a legacy stamped envelope without attribution must not \
             advance the mark — byte-identical legacy behavior",
        );
    }

    #[test]
    fn claim_conflict_resolved_increments_counter() {
        let mut s = Snapshot::empty(at(0));
        s.apply(&job_created(1, 0, "bobby"));
        s.apply(&env(
            2,
            5,
            EventKind::ClaimConflictResolved {
                job_id: jid("bobby"),
                shard_id: crate::schema::ShardId("part-1".into()),
                winner: WorkerId::new(),
            },
        ));
        assert_eq!(s.jobs[&jid("bobby")].progress.conflicts_resolved, 1);
    }

    #[test]
    fn verify_completed_routes_phase_by_mismatch_count() {
        let mut s = Snapshot::empty(at(0));
        s.apply(&job_created(1, 0, "bobby"));
        s.apply(&env(
            2,
            5,
            EventKind::VerifyStarted {
                job_id: jid("bobby"),
            },
        ));
        assert_eq!(s.jobs[&jid("bobby")].phase, Phase::Verifying);

        s.apply(&env(
            3,
            6,
            EventKind::VerifyCompleted {
                job_id: jid("bobby"),
                mismatches: 0,
            },
        ));
        assert_eq!(s.jobs[&jid("bobby")].phase, Phase::Cutover);

        // Second job, with mismatches.
        s.apply(&job_created(4, 0, "mary"));
        s.apply(&env(
            5,
            6,
            EventKind::VerifyStarted {
                job_id: jid("mary"),
            },
        ));
        s.apply(&env(
            6,
            7,
            EventKind::VerifyCompleted {
                job_id: jid("mary"),
                mismatches: 3,
            },
        ));
        assert_eq!(s.jobs[&jid("mary")].phase, Phase::Failed);
    }

    // =========================================================================
    // Phase legality in the reducer (ledger F25) — defense in depth
    // for replay: historical or buggy-writer events carrying illegal
    // transitions must be warn+no-op, never applied, never a panic.
    // =========================================================================

    #[test]
    fn reducer_ignores_illegal_transition_events() {
        let mut s = Snapshot::empty(at(0));

        // JobPaused against a Cancelled job: Cancelled -> Paused must
        // not happen.
        s.apply(&job_created(1, 0, "bobby"));
        s.apply(&env(
            2,
            5,
            EventKind::JobCancelled {
                job_id: jid("bobby"),
                reason: "op".into(),
            },
        ));
        assert_eq!(s.jobs[&jid("bobby")].phase, Phase::Cancelled);
        let history_before = s.jobs[&jid("bobby")].phase_history.clone();
        s.apply(&env(
            3,
            10,
            EventKind::JobPaused {
                job_id: jid("bobby"),
                reason: "stray".into(),
            },
        ));
        assert_eq!(
            s.jobs[&jid("bobby")].phase,
            Phase::Cancelled,
            "JobPaused on a cancelled job must be a no-op",
        );
        assert_eq!(s.jobs[&jid("bobby")].phase_history, history_before);
        // Bookkeeping still advances — the event was consumed, not applied.
        assert_eq!(s.last_seq, 3);

        // JobResumed against a non-Paused job: the rewind bug. A
        // Copying job must not be driven back to Planned (or to
        // Scanning on empty history).
        s.apply(&job_created(4, 0, "mary"));
        s.apply(&env(
            5,
            15,
            EventKind::JobPhaseChanged {
                job_id: jid("mary"),
                from: Phase::Planned,
                to: Phase::Copying,
                reason: "go".into(),
            },
        ));
        let history_before = s.jobs[&jid("mary")].phase_history.clone();
        s.apply(&env(
            6,
            20,
            EventKind::JobResumed {
                job_id: jid("mary"),
                reason: "stray".into(),
            },
        ));
        assert_eq!(
            s.jobs[&jid("mary")].phase,
            Phase::Copying,
            "JobResumed on a non-paused job must be a no-op",
        );
        assert_eq!(s.jobs[&jid("mary")].phase_history, history_before);

        // Terminal states are absorbing: no event moves a job out of
        // Completed/Failed/Cancelled — not even another terminal event.
        s.apply(&job_created(7, 0, "sue"));
        s.apply(&env(8, 25, EventKind::JobCompleted { job_id: jid("sue") }));
        for (seq, kind) in [
            (
                9,
                EventKind::JobPhaseChanged {
                    job_id: jid("sue"),
                    from: Phase::Completed,
                    to: Phase::Scanning,
                    reason: "stray".into(),
                },
            ),
            (
                10,
                EventKind::JobResumed {
                    job_id: jid("sue"),
                    reason: "stray".into(),
                },
            ),
            (
                11,
                EventKind::JobPaused {
                    job_id: jid("sue"),
                    reason: "stray".into(),
                },
            ),
            (
                12,
                EventKind::JobFailed {
                    job_id: jid("sue"),
                    reason: "stray".into(),
                },
            ),
            (
                13,
                EventKind::JobCancelled {
                    job_id: jid("sue"),
                    reason: "stray".into(),
                },
            ),
        ] {
            s.apply(&env(seq, 30, kind));
            assert_eq!(
                s.jobs[&jid("sue")].phase,
                Phase::Completed,
                "terminal phases are absorbing (event seq {seq})",
            );
        }
    }

    // =========================================================================
    // Replay round-trips
    // =========================================================================

    async fn write_log_and_snapshot(
        store: &MemStore,
        envs: &[EventEnvelope],
        snapshot_after: usize,
    ) -> Snapshot {
        // Replay-from-events into a fresh state, snapshot at boundary,
        // append remaining events. Mirrors what the coord runtime does.
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
        // No snapshot, just an event log. Replay must reach the same
        // state as a single-pass reduce.
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
        // 0 = snapshot before any events
        let single_pass = write_log_and_snapshot(&s, &envs, 0).await;
        // Replace the snapshot with nothing — the run we want is "no
        // snapshot, just the log".
        // (write_log_and_snapshot at snapshot_after=0 never snapshots,
        // because the condition `i + 1 == 0` is never satisfied.)
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
        // Equal *state* — replay's written_at field may differ, so
        // compare the data fields.
        assert_eq!(replayed.state.jobs, single_pass.jobs);
        assert_eq!(replayed.state.workers, single_pass.workers);
        assert_eq!(replayed.state.last_seq, 4);
        // Snapshot was at seq 2; events 3 and 4 should have replayed.
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
