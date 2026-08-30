//! Deterministic event-to-snapshot reducer.
//!
//! The reducer is a single function `Snapshot::apply(env)` that
//! mutates state in response to an event. Persistence and replay
//! orchestration are intentionally outside this crate.
//!
//! ## Snapshot-as-state
//!
//! Rather than introducing a separate "in-memory state" type, the
//! coordinator runtime keeps a `Snapshot` value and mutates it
//! directly. Snapshot storage re-stamps `written_at` and
//! `schema_version` before serializing; everything else round-trips
//! by construction.
//!
//! ## Determinism
//!
//! Events carry coordinator-assigned sequence numbers. Applying the
//! same ordered envelopes to the same snapshot produces the same
//! state.
//!
//! ## Covered transitions
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
//! Verify start/completion events transition job phase. Individual mismatch
//! events remain stream-visible but do not mutate aggregate snapshot state;
//! clients may maintain their own bounded mismatch history.

use crate::schema::{
    ErrorBucket, ErrorClass, EventEnvelope, EventKind, Job, JobConfig, JobId, Phase,
    PhaseTransition, Progress, Snapshot, Worker, WorkerCounters, WorkerState, ERROR_BUCKET_CAP,
    ERROR_OVERFLOW_CLASS, ERROR_SAMPLE_CAP, PHASE_HISTORY_CAP,
};
use chrono::{DateTime, Utc};

impl Snapshot {
    /// Apply an event envelope. Pure on `(state, env)`; the reducer reads the
    /// event kind and records `env.seq` as `last_seq` after the transition.
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
                total_files,
                total_bytes,
            } => {
                // JobCreated carries the endpoints and config hash, not the
                // full JobConfig. The reducer therefore installs the stable
                // protocol defaults around those endpoints.
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
                        // The full configuration is not an event payload, so
                        // replay deterministically reconstructs this value.
                        config: sentinel_config(source, dest),
                        phase: Phase::Planned,
                        phase_history: Vec::new(),
                        progress: Progress {
                            files_total: *total_files,
                            bytes_total: *total_bytes,
                            ..Progress::default()
                        },
                        throughput: Default::default(),
                        eta: Default::default(),
                        health: Default::default(),
                        assigned_workers: Vec::new(),
                    },
                );
            }

            EventKind::JobTotalsSet {
                job_id,
                total_files,
                total_bytes,
            } => {
                if let Some(j) = self.jobs.get_mut(job_id) {
                    j.progress.files_total = *total_files;
                    j.progress.bytes_total = *total_bytes;
                }
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
                        files_done: 0,
                        bytes_done: 0,
                        assigned_shard: None,
                        queue_depth: 0,
                        inflight_ops: 0,
                        counters: WorkerCounters::default(),
                        last_error: None,
                        fence_reason: None,
                        latency: None,
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

            EventKind::ProgressSync {
                job_id,
                files_done,
                bytes_done,
                workers,
            } => {
                // Floor-merge: sync carries absolutes captured from
                // this same reducer's state at emit time, so on live
                // application it is a no-op; on replay (or for a
                // client that missed capped delta frames) it heals
                // counters upward. Never moves anything backward.
                if let Some(j) = self.jobs.get_mut(job_id) {
                    j.progress.files_done = j.progress.files_done.max(*files_done);
                    j.progress.bytes_done = j.progress.bytes_done.max(*bytes_done);
                }
                for wc in workers {
                    if let Some(w) = self.workers.get_mut(&wc.worker_id) {
                        w.files_done = w.files_done.max(wc.files_done);
                        w.bytes_done = w.bytes_done.max(wc.bytes_done);
                    }
                }
            }

            EventKind::ProgressDelta {
                job_id,
                worker_id,
                files_delta,
                bytes_delta,
                errors_delta,
            } => {
                if let Some(w) = self.workers.get_mut(worker_id) {
                    w.files_done = w.files_done.saturating_add(*files_delta);
                    w.bytes_done = w.bytes_done.saturating_add(*bytes_delta);
                }
                let was_planned = if let Some(j) = self.jobs.get_mut(job_id) {
                    j.progress.files_done = j.progress.files_done.saturating_add(*files_delta);
                    j.progress.bytes_done = j.progress.bytes_done.saturating_add(*bytes_delta);
                    j.progress.errors_total = j.progress.errors_total.saturating_add(*errors_delta);
                    j.phase == Phase::Planned
                } else {
                    false
                };
                // A job with work flowing is not "Planned" — derive
                // Copying on the first delta. Deterministic on replay
                // (same events, same transition), and legal per the
                // F25 guard (Planned -> Copying skips forward).
                if was_planned {
                    transition_phase(self, job_id, Phase::Copying, "first progress delta", env.at);
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
                // Streams to SSE but does not change aggregate snapshot state.
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::schema::{ConfigHash, ErrorClass, EventKind, WorkerId, SCHEMA_VERSION};
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
}
