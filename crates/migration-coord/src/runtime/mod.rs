//! Coord runtime — the long-running data plane.
//!
//! Owns the lease handle, the in-memory state ([`Snapshot`]), the
//! monotonic seq counter, the [`EventLogWriter`], and a tokio
//! broadcast channel that fans out [`EventEnvelope`]s to SSE
//! subscribers.
//!
//! ## Concurrency model
//!
//! A single `tokio::sync::Mutex<RuntimeInner>` serializes every
//! mutation. The critical section is short and never does store
//! I/O for the event log:
//!
//! 1. Assign seq, stamp `at`.
//! 2. Build the envelope.
//! 3. Apply the reducer.
//! 4. Buffer into the log writer (pure bookkeeping, no PUT).
//! 5. Drop the lock.
//! 6. If the route crossed `max_events_per_chunk`, flush it (see
//!    below) — still on the ingest call path, so a failed PUT
//!    fails the ingest exactly as before.
//! 7. `broadcast::send` to SSE subscribers (non-blocking).
//!
//! ## Chunk flush (F45b): PUT outside the state lock
//!
//! Chunk flushes (`flush_log`, `flush_aged`, the threshold flush
//! in step 6) run the S3 PUT *outside* the state mutex so ingest
//! and every read (`state()`, `job_view()`, ...) proceed while a
//! PUT is in flight. The invariants:
//!
//! - **Single flusher**: a dedicated flush token (a second mutex,
//!   held across the PUTs) admits at most one chunk PUT at a
//!   time. A concurrent flush request parks on the token; once
//!   the in-flight PUT completes it observes the covered seqs
//!   gone from the buffer and flushes only what remains — two
//!   racing flushes can never produce overlapping or
//!   out-of-order chunk keys.
//! - **Flush-before-ack (F03)**: `flush_log` still returns only
//!   after every event buffered at its start is durable (or an
//!   error) — the PUT moved off the lock, not off the request
//!   path.
//! - **Failure atomicity**: buffered envelopes leave the writer
//!   only *after* their PUT succeeded. A failed PUT leaves the
//!   buffer intact (retried by the next flush) and fails the
//!   caller — today's ack semantics unchanged.
//!
//! Reads (REST handlers, SSE catch-up reads) acquire the state
//! lock and clone what they need. Snapshot size at deployment
//! scale (~hundreds of jobs, ~thousands of workers) keeps clone
//! cost in microseconds; we revisit if that ever isn't true.
//!
//! ## Clock
//!
//! `at` is stamped from a `Clock` — `SystemClock` in production,
//! `FixedClock` in tests. The clock is injected at construction
//! so tests can drive lease expiry, snapshot cadence, and event
//! ordering deterministically.
//!
//! ## Lease ownership
//!
//! The runtime *owns* the lease handle. Lease refresh and
//! takeover-on-loss live in [`crate::ticks`] (Phase 2.4). The
//! runtime exposes `lease_lost()` so the ticks task can signal
//! shutdown when refresh sees [`Error::LeaseLost`].
//!
//! ## What this module deliberately does *not* do
//!
//! - No HTTP — that's [`crate::server`].
//! - No worker registration or heartbeat — Phase 3 wires those.
//! - No background ticks — Phase 2.4.

use crate::errors::{Error, Result};
use crate::events::{EventLogConfig, EventLogWriter, FlushScope, PendingFlush};
use crate::lease::{self, AcquireOutcome, Identity, LeaseConfig, LeaseHandle};
use crate::schema::{EventEnvelope, EventKind, Snapshot, WorkerId, SCHEMA_VERSION};
use crate::state;
use crate::store::CoordStore;
use chrono::{DateTime, Utc};
use std::sync::Arc;
use tokio::sync::{broadcast, Mutex};

// =============================================================================
// Clock — injectable so tests are deterministic
// =============================================================================

/// Source of wall-clock `at` timestamps for events the runtime
/// ingests. Production uses [`SystemClock`]; tests use
/// `FixedClock` (the test module) to drive event timing
/// deterministically.
pub trait Clock: Send + Sync + std::fmt::Debug {
    fn now(&self) -> DateTime<Utc>;
}

#[derive(Debug, Default, Clone, Copy)]
pub struct SystemClock;

impl Clock for SystemClock {
    fn now(&self) -> DateTime<Utc> {
        Utc::now()
    }
}

// =============================================================================
// Config
// =============================================================================

#[derive(Debug, Clone)]
pub struct RuntimeConfig {
    pub lease: LeaseConfig,
    pub events: EventLogConfig,
    /// Capacity of the SSE broadcast channel. Each subscriber sees
    /// the most recent N events queued for it; once full, the
    /// subscriber sees `RecvError::Lagged(skipped)` and the SSE
    /// handler emits a synthetic `Resync` event telling the client
    /// to re-fetch the snapshot. 1024 is a reasonable starting
    /// point — sized for "one slow subscriber falls behind a
    /// 10k-events-per-second burst for ~100ms before resync".
    pub bus_capacity: usize,
    /// How long [`CoordRuntime::start`] backs off between failed lease-acquire
    /// attempts. Tests inject a short value; production defaults
    /// to 2s.
    pub lease_retry_interval: std::time::Duration,
    /// Maximum attempts before [`CoordRuntime::start`] gives up. `None` means
    /// retry forever — the production default. Tests pin a finite
    /// cap.
    pub lease_retry_max_attempts: Option<u32>,
}

impl RuntimeConfig {
    pub fn default_for_prod() -> Self {
        Self {
            lease: LeaseConfig::default_for_prod(),
            events: EventLogConfig::default(),
            bus_capacity: 1024,
            lease_retry_interval: std::time::Duration::from_secs(2),
            lease_retry_max_attempts: None,
        }
    }
}

// =============================================================================
// Runtime
// =============================================================================

/// Upper bound on `put_if_absent` probes when allocating an audit
/// key (see [`CoordRuntime::record_audit`]). Collisions only happen
/// when a restart rewound the per-day counter, so the free slot is
/// at most one crash window's worth of audit rows ahead — operator
/// commands number in the tens per day, so this bound is generous.
const MAX_AUDIT_SEQ_PROBES: u64 = 10_000;

/// Snapshot-write-time eviction window for worker rows (ledger F24
/// residue): a worker whose state is `Disconnected` and whose last
/// activity (`last_heartbeat` — stamped by the join event and every
/// heartbeat) is at least this old at snapshot-write time is omitted
/// from the written snapshot, and dropped from live state once that
/// write succeeds. `Fenced` rows are never evicted — a fence is
/// operator-relevant until acted on. Eviction happens ONLY at
/// snapshot write (never wall-clock pruning of live state on a
/// tick), so replay stays deterministic: the pruning decision is
/// embodied in the durable snapshot both sides share, and replay =
/// pruned snapshot + events(seq > last_seq) converges with the live
/// coord. 24 hours keeps a full operator day of disconnected rows
/// visible for debugging while bounding live-state growth by the
/// snapshot cadence.
pub const WORKER_EVICT_AFTER_SECS: i64 = 24 * 60 * 60;

/// Wire-cardinality caps (ledger F24, COORD_PLAN §3.3). Decides, at
/// the ingest boundary, whether an event goes out on the SSE bus.
/// The caps are **bus-only**: state applies every event and the
/// event log carries every event, so durability and replay are
/// untouched — only what live subscribers see is rate-limited.
///
/// - `ProgressDelta`: at most 1 Hz per (job, worker) — a suppressed
///   delta is still folded into `Job.progress`; clients see the
///   next delta (or refetch) for the latest values.
/// - `ErrorEmitted`: at most [`crate::schema::ERROR_STREAM_MAX_PER_SEC`]
///   per class per second — excess folds into `ErrorBucket.count`
///   via the reducer as always.
/// - Everything else streams unconditionally.
///
/// `progress_last` grows with the set of (job, worker) pairs seen —
/// the same cardinality as the workers table, which is bounded by
/// the snapshot-write-time row eviction ([`WORKER_EVICT_AFTER_SECS`]).
///
/// Trailing edge (F24 residue, 3a): a suppressed `ProgressDelta` is
/// retained per (job, worker), latest wins; the flush tick calls
/// [`StreamCaps::take_due_trailing`] to re-broadcast retained deltas
/// once the cap interval has passed, so a quieting burst's final
/// values reach live subscribers. Retention is bus-only bookkeeping
/// — the envelope was already applied and logged at ingest.
#[derive(Debug, Default)]
struct StreamCaps {
    progress_last:
        std::collections::HashMap<(crate::schema::JobId, crate::schema::WorkerId), DateTime<Utc>>,
    /// Latest SUPPRESSED delta per (job, worker) — the trailing edge
    /// the tick re-broadcasts. Cleared whenever a fresh delta for
    /// the key broadcasts (the retained frame is then stale) and on
    /// delivery.
    retained_progress:
        std::collections::HashMap<(crate::schema::JobId, crate::schema::WorkerId), EventEnvelope>,
    error_window_start: Option<DateTime<Utc>>,
    error_counts: std::collections::HashMap<crate::schema::ErrorClass, u32>,
}

impl StreamCaps {
    /// True if `env` may be broadcast at `now`. Mutates the cap
    /// bookkeeping (including trailing-edge retention); call exactly
    /// once per ingested event, under the runtime lock.
    fn should_broadcast(&mut self, env: &EventEnvelope, now: DateTime<Utc>) -> bool {
        match &env.kind {
            EventKind::ProgressDelta {
                job_id, worker_id, ..
            } => {
                let min_interval =
                    chrono::Duration::milliseconds(crate::schema::PROGRESS_STREAM_MIN_INTERVAL_MS);
                let key = (job_id.clone(), *worker_id);
                match self.progress_last.get(&key) {
                    Some(last) if now.signed_duration_since(*last) < min_interval => {
                        // Suppressed: retain the trailing edge so the
                        // flush tick can deliver the burst's final
                        // values (latest wins).
                        self.retained_progress.insert(key, env.clone());
                        false
                    }
                    _ => {
                        // A fresh broadcast supersedes any retained
                        // older frame for this key.
                        self.retained_progress.remove(&key);
                        self.progress_last.insert(key, now);
                        true
                    }
                }
            }
            EventKind::ErrorEmitted { class, .. } => {
                // Tumbling one-second window shared across classes;
                // per-class token count within it.
                let stale = match self.error_window_start {
                    Some(start) => now.signed_duration_since(start) >= chrono::Duration::seconds(1),
                    None => true,
                };
                if stale {
                    self.error_window_start = Some(now);
                    self.error_counts.clear();
                }
                let n = self.error_counts.entry(class.clone()).or_insert(0);
                if *n < crate::schema::ERROR_STREAM_MAX_PER_SEC {
                    *n += 1;
                    true
                } else {
                    false
                }
            }
            _ => true,
        }
    }

    /// Remove and return every retained trailing delta whose key has
    /// gone at least the cap interval without a broadcast. Each
    /// delivery counts as that key's broadcast (its `progress_last`
    /// advances to `now`), so the tick never exceeds the 1 Hz cap.
    fn take_due_trailing(&mut self, now: DateTime<Utc>) -> Vec<EventEnvelope> {
        let min_interval =
            chrono::Duration::milliseconds(crate::schema::PROGRESS_STREAM_MIN_INTERVAL_MS);
        let due: Vec<_> = self
            .retained_progress
            .keys()
            .filter(|key| match self.progress_last.get(*key) {
                Some(last) => now.signed_duration_since(*last) >= min_interval,
                // Unreachable (retention implies a prior broadcast),
                // but deliver rather than leak if it ever happens.
                None => true,
            })
            .cloned()
            .collect();
        let mut out = Vec::with_capacity(due.len());
        for key in due {
            if let Some(env) = self.retained_progress.remove(&key) {
                self.progress_last.insert(key, now);
                out.push(env);
            }
        }
        // Deterministic delivery order for multi-key ticks.
        out.sort_by_key(|e| e.seq);
        out
    }
}

struct RuntimeInner {
    state: Snapshot,
    next_seq: u64,
    writer: EventLogWriter,
    lease: LeaseHandle,
    /// True once the lease has been observed lost. Subsequent
    /// `ingest` calls return [`Error::LeaseLost`] without touching
    /// the store, so a partial-state coord cannot keep writing.
    lease_lost: bool,
    /// Terminal jobs whose phase is covered by a successfully written
    /// snapshot and that are therefore safe to archive: replay after
    /// the archive reconstructs them from that snapshot even though
    /// their `events/` chunks are gone. Populated by
    /// [`CoordRuntime::write_snapshot`], drained by
    /// [`CoordRuntime::archive_terminal_jobs`]. Never persisted — a
    /// restart rebuilds it from the next snapshot write (re-archiving
    /// an already-archived job is a cheap no-op).
    archive_eligible: std::collections::BTreeSet<crate::schema::JobId>,
    /// Jobs already archived in this process's lifetime — keeps the
    /// archive tick from re-LISTing every historical terminal job on
    /// every snapshot.
    archived_jobs: std::collections::BTreeSet<crate::schema::JobId>,
    /// Bus-only rate caps for high-cardinality event kinds (ledger
    /// F24). See [`StreamCaps`].
    stream_caps: StreamCaps,
}

/// One page of the jobs view, returned from
/// [`CoordRuntime::jobs_view`]. `next_cursor` is the `JobId` of the
/// first job that would have been in the *next* page, or `None` if
/// the current page contained the last job.
#[derive(Debug, Clone)]
pub struct JobsPage {
    pub jobs: Vec<crate::schema::Job>,
    pub next_cursor: Option<crate::schema::JobId>,
}

/// The runtime handle. Clones share the same inner state via `Arc`.
///
/// Construct via [`CoordRuntime::start`]. Drop the handle to release
/// the broadcast channel; the lease and writer are released on
/// [`CoordRuntime::shutdown`] (which the operator should always
/// call before drop).
#[derive(Clone)]
pub struct CoordRuntime {
    inner: Arc<Mutex<RuntimeInner>>,
    /// Single-flusher token (F45b): held across event-chunk PUTs,
    /// distinct from the state mutex so ingest and reads proceed
    /// while a PUT is in flight. A concurrent flush request parks
    /// here and, once the in-flight PUT completes, flushes only the
    /// seqs still buffered — chunk keys can never overlap.
    flush_token: Arc<Mutex<()>>,
    bus: broadcast::Sender<EventEnvelope>,
    store: Arc<dyn CoordStore>,
    clock: Arc<dyn Clock>,
    cfg: RuntimeConfig,
}

impl std::fmt::Debug for CoordRuntime {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CoordRuntime")
            .field("bus_capacity", &self.cfg.bus_capacity)
            .finish()
    }
}

impl CoordRuntime {
    /// Bring up the runtime: acquire the lease (backing off on
    /// `Held`), replay state from snapshot + log, open a fresh
    /// event-log writer, and seed the broadcast channel.
    pub async fn start(
        store: Arc<dyn CoordStore>,
        clock: Arc<dyn Clock>,
        me: Identity,
        cfg: RuntimeConfig,
    ) -> Result<Self> {
        let lease = acquire_with_backoff(store.as_ref(), &me, &cfg, clock.as_ref()).await?;
        let replay = state::replay(store.as_ref(), clock.now()).await?;
        let writer = EventLogWriter::new(cfg.events);
        let (bus, _) = broadcast::channel(cfg.bus_capacity);

        Ok(Self {
            inner: Arc::new(Mutex::new(RuntimeInner {
                state: replay.state,
                next_seq: replay.next_seq,
                writer,
                lease,
                lease_lost: false,
                archive_eligible: Default::default(),
                archived_jobs: Default::default(),
                stream_caps: Default::default(),
            })),
            flush_token: Arc::new(Mutex::new(())),
            bus,
            store,
            clock,
            cfg,
        })
    }

    /// Ingest one event. Assigns the next seq, stamps `at`, applies
    /// the reducer, appends to the log, then broadcasts to SSE
    /// subscribers (subject to the bus-only rate caps — see
    /// `StreamCaps`). Returns the assigned seq.
    pub async fn ingest(&self, kind: EventKind) -> Result<u64> {
        self.ingest_inner(kind, None, None, None).await
    }

    /// Ingest an event whose payload was constructed by a worker
    /// (carrying its own `worker_at`). Same semantics as `ingest`
    /// but preserves the worker's local timestamp on the envelope
    /// for diagnostic correlation.
    pub async fn ingest_with_worker_at(
        &self,
        kind: EventKind,
        worker_at: DateTime<Utc>,
    ) -> Result<u64> {
        self.ingest_inner(kind, Some(worker_at), None, None).await
    }

    /// Ingest an event submitted on the worker events route: optional
    /// worker-local timestamp plus the optional per-worker
    /// `client_seq` idempotency stamp (ledger F20, D4). The stamp is
    /// carried on the envelope into the durable log, so the reducer —
    /// live and on replay — maintains the per-worker high-water mark
    /// the events handler dedups against.
    ///
    /// `caller` is the registered worker id from the URL — already
    /// validated by the route's trust boundary — stamped onto the
    /// envelope as `from_worker` (F20 residue) so stamped kinds
    /// without payload attribution can still advance the mark.
    /// Admin/internal ingest paths ([`Self::ingest`],
    /// [`Self::ingest_with_worker_at`]) leave it `None`.
    pub async fn ingest_worker_event(
        &self,
        kind: EventKind,
        worker_at: Option<DateTime<Utc>>,
        client_seq: Option<u64>,
        caller: WorkerId,
    ) -> Result<u64> {
        self.ingest_inner(kind, worker_at, client_seq, Some(caller))
            .await
    }

    async fn ingest_inner(
        &self,
        kind: EventKind,
        worker_at: Option<DateTime<Utc>>,
        client_seq: Option<u64>,
        from_worker: Option<WorkerId>,
    ) -> Result<u64> {
        let (env, broadcast, threshold_route) = {
            let mut guard = self.inner.lock().await;
            if guard.lease_lost {
                return Err(Error::LeaseLost);
            }
            let seq = guard.next_seq;
            guard.next_seq += 1;
            let env = EventEnvelope {
                seq,
                at: self.clock.now(),
                schema_version: SCHEMA_VERSION,
                worker_at,
                client_seq,
                from_worker,
                kind,
            };
            guard.state.apply(&env);
            let broadcast = guard.stream_caps.should_broadcast(&env, env.at);
            // Pure bookkeeping under the lock; the PUT (if the route
            // crossed the chunk threshold) runs below, outside it.
            let out = guard.writer.buffer(env.clone());
            (env, broadcast, out.threshold_reached.then_some(out.route))
        };
        // Threshold flush — still on the ingest call path (a failed
        // PUT fails this ingest, exactly as when the flush lived
        // inside the lock), but no longer under the state mutex.
        if let Some(route) = threshold_route {
            self.flush_scope(FlushScope::Route(route)).await?;
        }
        // Broadcast outside the lock — `send` is non-blocking; if
        // there are no subscribers it returns an error we ignore.
        let seq = env.seq;
        if broadcast {
            let _ = self.bus.send(env);
        }
        Ok(seq)
    }

    /// F45b flush core: snapshot the chunks selected by `scope`
    /// under the state lock, release it, PUT each chunk while
    /// holding only the single-flusher token, and reacquire the
    /// state lock briefly per chunk to record the result.
    ///
    /// A failed PUT returns the error with the buffer intact — the
    /// envelopes leave the writer only on success — so the ack path
    /// fails exactly as before and the next flush retries the same
    /// seqs. Fenced on the lease before the snapshot and re-checked
    /// before every PUT (the lease can drop mid-loop).
    async fn flush_scope(&self, scope: FlushScope) -> Result<()> {
        let _token = self.flush_token.lock().await;
        let pending: Vec<PendingFlush> = {
            let guard = self.inner.lock().await;
            if guard.lease_lost {
                return Err(Error::LeaseLost);
            }
            guard.writer.pending_flushes(&scope)?
        };
        for p in pending {
            if self.lease_lost().await {
                return Err(Error::LeaseLost);
            }
            self.store.put(&p.key, p.body).await?;
            let mut guard = self.inner.lock().await;
            guard.writer.complete_flush(&p.route, p.end_seq);
        }
        Ok(())
    }

    /// Per-worker `client_seq` high-water mark from reducer state
    /// (ledger F20, D4). `0` for a worker that has never submitted a
    /// stamped event — every real stamp starts at 1, so `0` never
    /// masks one.
    pub async fn client_seq_hwm(&self, worker_id: crate::schema::WorkerId) -> u64 {
        self.inner
            .lock()
            .await
            .state
            .last_client_seq
            .get(&worker_id)
            .copied()
            .unwrap_or(0)
    }

    /// Clone the current state. Cheap at deployment scale; if it
    /// stops being cheap, the snapshot grows a copy-on-write
    /// indirection.
    pub async fn state(&self) -> Snapshot {
        self.inner.lock().await.state.clone()
    }

    /// Highest seq ingested so far. `0` on a fresh bucket.
    pub async fn last_seq(&self) -> u64 {
        self.inner.lock().await.next_seq.saturating_sub(1)
    }

    /// Clone a single job's view. Returns `None` if the job has
    /// not been created yet.
    pub async fn job_view(&self, id: &crate::schema::JobId) -> Option<crate::schema::Job> {
        self.inner.lock().await.state.jobs.get(id).cloned()
    }

    /// Page of jobs sorted lexically by [`crate::schema::JobId`].
    /// `cursor` is the JobId of the last entry the client saw
    /// (excluded from the next page). `limit` caps the page size.
    pub async fn jobs_view(&self, cursor: Option<&crate::schema::JobId>, limit: usize) -> JobsPage {
        let guard = self.inner.lock().await;
        // Walk the BTreeMap from the cursor (excluded) forward.
        let iter: Box<dyn Iterator<Item = (&crate::schema::JobId, &crate::schema::Job)>> =
            match cursor {
                Some(c) => Box::new(guard.state.jobs.range((
                    std::ops::Bound::Excluded(c.clone()),
                    std::ops::Bound::Unbounded,
                ))),
                None => Box::new(guard.state.jobs.iter()),
            };

        let mut jobs = Vec::with_capacity(limit);
        let mut has_more = false;
        for (_id, job) in iter {
            if jobs.len() == limit {
                has_more = true;
                break;
            }
            jobs.push(job.clone());
        }
        // Cursor is the LAST RETURNED job's id — the next request
        // passes it as `cursor=` and gets everything strictly after.
        // If there isn't a next page, no cursor.
        let next_cursor = if has_more {
            jobs.last().map(|j| j.id.clone())
        } else {
            None
        };
        JobsPage { jobs, next_cursor }
    }

    /// All workers assigned to a job, in the order they joined.
    /// Returns `None` if the job has not been created yet.
    pub async fn workers_view_for_job(
        &self,
        id: &crate::schema::JobId,
    ) -> Option<Vec<crate::schema::Worker>> {
        let guard = self.inner.lock().await;
        guard.state.jobs.get(id).map(|job| {
            job.assigned_workers
                .iter()
                .filter_map(|wid| guard.state.workers.get(wid).cloned())
                .collect()
        })
    }

    /// Compute the [`ControlMode`](crate::schema::ControlMode) the coord wants this worker to
    /// observe, plus the current `last_seq` and the coord's wall
    /// clock. Used by the heartbeat handler to build the response
    /// envelope.
    ///
    /// Returns `None` if the worker is unknown — the caller raises
    /// 404. Returns `Some(ControlMode::Cancel)` for a worker whose
    /// job no longer exists (shouldn't happen, defensive).
    pub async fn control_for_worker(
        &self,
        worker_id: crate::schema::WorkerId,
    ) -> Option<(
        crate::schema::ControlMode,
        u64,
        chrono::DateTime<chrono::Utc>,
    )> {
        let guard = self.inner.lock().await;
        let worker = guard.state.workers.get(&worker_id)?;
        let mode = guard
            .state
            .jobs
            .get(&worker.job_id)
            .map(|j| crate::schema::ControlMode::for_phase(j.phase))
            .unwrap_or(crate::schema::ControlMode::Cancel);
        Some((mode, guard.state.last_seq, self.clock.now()))
    }

    /// Find WorkerIds on `(job_id, host)` whose `(pid, start_time)`
    /// does NOT match the incoming `(pid, start_time)` and that are
    /// still in a live state. The register handler emits a
    /// `WorkerLeft{reason:"reregister"}` for each returned id before
    /// minting the new `WorkerId`.
    ///
    /// Already-Disconnected workers are skipped — re-emitting
    /// `WorkerLeft` for a worker that already left is noise without
    /// information.
    pub async fn stale_workers_for_register(
        &self,
        job_id: &crate::schema::JobId,
        host: &str,
        pid: u32,
        start_time: chrono::DateTime<chrono::Utc>,
    ) -> Vec<crate::schema::WorkerId> {
        let guard = self.inner.lock().await;
        let Some(job) = guard.state.jobs.get(job_id) else {
            return Vec::new();
        };
        job.assigned_workers
            .iter()
            .filter_map(|wid| {
                let w = guard.state.workers.get(wid)?;
                if w.host != host {
                    return None;
                }
                if w.pid == pid && w.start_time == start_time {
                    return None;
                }
                if w.state == crate::schema::WorkerState::Disconnected {
                    return None;
                }
                Some(*wid)
            })
            .collect()
    }

    /// Error buckets for a job. Returns `Some(vec![])` for a job
    /// with no errors yet, `None` for an unknown job.
    pub async fn errors_view_for_job(
        &self,
        id: &crate::schema::JobId,
    ) -> Option<Vec<crate::schema::ErrorBucket>> {
        let guard = self.inner.lock().await;
        // A job that exists but has no errors yet returns an empty
        // vec; a job that does not exist returns None.
        if !guard.state.jobs.contains_key(id) {
            return None;
        }
        Some(
            guard
                .state
                .error_buckets
                .get(id)
                .cloned()
                .unwrap_or_default(),
        )
    }

    /// Write one audit entry. The caller has already established
    /// authorization; this method assigns the per-day seq, writes
    /// the JSON line to `audit/<YYYY-MM-DD>/<seq:020>.jsonl`, and
    /// returns the assigned `command_id` (UUID v4).
    ///
    /// On UTC date rollover the counter resets. The snapshot
    /// persists `(audit_seq_today, audit_seq_date)`, but that only
    /// covers numbering up to the last snapshot — a crash inside
    /// the window rewinds the in-memory counter (to zero on a day
    /// with no snapshot). Durability therefore does NOT depend on
    /// the counter: rows are written with `put_if_absent`, and a
    /// collision (a prior generation already used the number) just
    /// advances the seq and retries. Existing rows are never
    /// overwritten (ledger F22).
    ///
    /// The inner lock is held across the conditional PUTs so
    /// concurrent audits cannot double-allocate a seq. Audit rows
    /// are operator-cadence (tens per day), so this path was left
    /// out of the F45b flush-outside-the-lock restructure — the
    /// event-chunk paths were the hot ones.
    pub async fn record_audit(
        &self,
        token_label: impl Into<String>,
        action: impl Into<String>,
        target: impl Into<String>,
        args: serde_json::Value,
        result: crate::schema::AuditResult,
    ) -> Result<String> {
        let now = self.clock.now();
        let date = now.format("%Y-%m-%d").to_string();
        let command_id = uuid::Uuid::new_v4().to_string();

        let mut guard = self.inner.lock().await;
        if guard.lease_lost {
            return Err(Error::LeaseLost);
        }
        // Rollover: reset on a new UTC day.
        if guard.state.audit_seq_date != date {
            guard.state.audit_seq_date = date.clone();
            guard.state.audit_seq_today = 0;
        }

        let entry = crate::schema::AuditEntry {
            at: now,
            command_id: command_id.clone(),
            token_label: token_label.into(),
            action: action.into(),
            target: target.into(),
            args,
            result,
        };
        let mut body = serde_json::to_vec(&entry)?;
        body.push(b'\n');

        // Self-healing key allocation: a collision means a prior
        // coord generation used the number during the crash window;
        // the next free slot is at most that window's row count
        // ahead. Bounded so a pathological store cannot spin forever.
        for _ in 0..MAX_AUDIT_SEQ_PROBES {
            guard.state.audit_seq_today += 1;
            let key = crate::layout::audit_chunk_key(&date, guard.state.audit_seq_today);
            match self.store.put_if_absent(&key, body.clone()).await? {
                crate::store::PutOutcome::Created(_) => return Ok(command_id),
                crate::store::PutOutcome::AlreadyExists => continue,
            }
        }
        Err(Error::Other(anyhow::anyhow!(
            "audit key allocation exhausted {MAX_AUDIT_SEQ_PROBES} probes \
             for {date} — audit/ prefix is unexpectedly dense",
        )))
    }

    /// Update a worker's heartbeat-only fields (counters,
    /// transient state, queue depth, inflight ops) without writing
    /// an event. Heartbeats are intentionally absent from the event
    /// log per the build prompt — they drive state derivation but
    /// would flood SSE consumers if streamed.
    ///
    /// Returns `true` if the worker existed and was updated,
    /// `false` if it was unknown (caller responds with 404).
    pub async fn record_heartbeat(
        &self,
        worker_id: crate::schema::WorkerId,
        counters: crate::schema::WorkerCounters,
        state: crate::schema::WorkerState,
        inflight_ops: u32,
        queue_depth: u32,
    ) -> Result<bool> {
        let now = self.clock.now();
        let mut guard = self.inner.lock().await;
        if guard.lease_lost {
            return Err(Error::LeaseLost);
        }
        match guard.state.workers.get_mut(&worker_id) {
            Some(w) => {
                w.counters = counters;
                w.state = state;
                w.inflight_ops = inflight_ops;
                w.queue_depth = queue_depth;
                w.last_heartbeat = now;
                Ok(true)
            }
            None => Ok(false),
        }
    }

    /// Trailing-edge flush for the ProgressDelta wire cap (ledger
    /// F24 residue, item 3a): re-broadcast the latest SUPPRESSED
    /// delta per (job, worker) once the cap interval has passed
    /// since that key's last broadcast, so a quieting burst's final
    /// values reach live subscribers within ~2×
    /// [`crate::schema::PROGRESS_STREAM_MIN_INTERVAL_MS`] (cap
    /// interval + tick cadence) instead of hanging stale until the
    /// next event. Bus-only: the frame is a re-broadcast of an
    /// already-ingested, already-logged envelope — same seq, no new
    /// event, no log write; log and replay are untouched. Driven by
    /// the flush tick ([`crate::ticks::flush_aged_loop`]). Returns
    /// the number of frames re-broadcast.
    pub async fn flush_trailing_progress(&self) -> usize {
        let now = self.clock.now();
        let due = {
            let mut guard = self.inner.lock().await;
            guard.stream_caps.take_due_trailing(now)
        };
        let n = due.len();
        // Send outside the lock, like every other broadcast.
        for env in due {
            let _ = self.bus.send(env);
        }
        n
    }

    /// Subscribe to live events. Returns a `broadcast::Receiver`;
    /// the SSE handler typically wraps it in a stream and emits
    /// each envelope as a wire frame.
    pub fn subscribe(&self) -> broadcast::Receiver<EventEnvelope> {
        self.bus.subscribe()
    }

    /// Read-only accessor for the event-log writer's unflushed
    /// in-memory tail. The SSE catch-up path holds this instead of
    /// a full `CoordRuntime` clone: it shares the inner state but
    /// NOT the broadcast sender, so a long-lived stream body does
    /// not keep the event bus alive (dropping every runtime handle
    /// still closes the channel and ends the stream).
    pub fn tail_reader(&self) -> EventTailReader {
        EventTailReader {
            inner: Arc::clone(&self.inner),
        }
    }

    /// Subscriber count. Useful for `/healthz` and tests.
    pub fn subscriber_count(&self) -> usize {
        self.bus.receiver_count()
    }

    /// Borrow the store (so background ticks and audit can share
    /// the runtime's S3 client).
    pub fn store(&self) -> &Arc<dyn CoordStore> {
        &self.store
    }

    /// Borrow the clock.
    pub fn clock(&self) -> &Arc<dyn Clock> {
        &self.clock
    }

    /// Runtime config (read-only).
    pub fn config(&self) -> &RuntimeConfig {
        &self.cfg
    }

    /// Replace the current lease handle (after a refresh).
    /// Background-tick-only API; HTTP handlers do not call this.
    pub async fn update_lease(&self, new_handle: LeaseHandle) {
        let mut guard = self.inner.lock().await;
        guard.lease = new_handle;
    }

    /// Mark the lease as lost. Subsequent `ingest` calls return
    /// [`Error::LeaseLost`]. Idempotent.
    pub async fn mark_lease_lost(&self) {
        self.inner.lock().await.lease_lost = true;
    }

    pub async fn lease_lost(&self) -> bool {
        self.inner.lock().await.lease_lost
    }

    /// Snapshot the current lease handle (for diagnostics and the
    /// next refresh tick).
    pub async fn lease_handle(&self) -> LeaseHandle {
        self.inner.lock().await.lease.clone()
    }

    /// Number of events currently buffered in open (unflushed)
    /// event-log chunks. The CLI reports this when a lease-lost
    /// shutdown drops the buffer instead of flushing it.
    pub async fn buffered_event_count(&self) -> usize {
        self.inner.lock().await.writer.buffered_events()
    }

    /// Force-flush every open event-log chunk. Called from the
    /// snapshot tick (so the snapshot reflects a clean log
    /// boundary), from graceful shutdown, and from the
    /// flush-before-ack seam on the worker endpoints (F03) — it
    /// returns only once every event buffered at its start is
    /// durable, or an error. The PUT runs outside the state lock
    /// under the single-flusher token (F45b).
    ///
    /// Fenced on the lease: once the lease is observed lost this
    /// returns [`Error::LeaseLost`] without touching the store — a
    /// deposed coord's buffered chunks could otherwise clobber the
    /// successor's (chunk keys carry no lease epoch).
    pub async fn flush_log(&self) -> Result<()> {
        self.flush_scope(FlushScope::All).await
    }

    /// Flush only chunks aged past `max_chunk_age`. Driven by the
    /// snapshot tick at a lower cadence than `flush_log`. Fenced on
    /// the lease like [`CoordRuntime::flush_log`], and runs its
    /// PUTs outside the state lock the same way.
    pub async fn flush_aged(&self) -> Result<()> {
        let now = self.clock.now();
        self.flush_scope(FlushScope::Aged(now)).await
    }

    /// Persist the current state to `state/snapshot.json`. Called
    /// from the snapshot tick. Fenced on the lease: a deposed coord
    /// must not overwrite the successor's snapshot with a plain PUT.
    ///
    /// A successful write also marks every terminal job in the
    /// persisted state as archive-eligible (see
    /// [`CoordRuntime::archive_terminal_jobs`]): once the terminal
    /// phase is durable in the snapshot, replay no longer needs the
    /// job's `events/` chunks.
    pub async fn write_snapshot(&self, history_keep: usize) -> Result<()> {
        let now = self.clock.now();
        let snap = {
            let mut guard = self.inner.lock().await;
            if guard.lease_lost {
                return Err(Error::LeaseLost);
            }
            // Stamp written_at and schema_version; the rest is
            // already populated by the reducer.
            guard.state.written_at = now;
            guard.state.schema_version = SCHEMA_VERSION;
            let mut snap = guard.state.clone();
            // Worker-row eviction (ledger F24 residue): omit rows
            // that are Disconnected and stale — see
            // [`WORKER_EVICT_AFTER_SECS`] for why this happens only
            // here. Replay soundness: any event at seq <=
            // snap.last_seq referencing an omitted row is below the
            // replay cut; any later event either no-ops on the
            // missing row (the reducer's get_mut arms) or is a
            // WorkerJoined, which resurrects it identically on both
            // sides.
            snap.workers.retain(|_, w| !worker_evictable(w, now));
            snap
        };
        crate::snapshot::write(self.store.as_ref(), &snap, history_keep, now).await?;

        let mut guard = self.inner.lock().await;
        // Drop the same rows from live state only now that the
        // pruned snapshot is durable — a failed write leaves live
        // state untouched. The criteria are re-evaluated on the
        // current rows: a worker that rejoined or changed state
        // while the write was in flight stays live, and the event
        // that changed it has seq > snap.last_seq, so replay
        // re-applies it on top of the pruned snapshot and both
        // sides converge.
        guard.state.workers.retain(|_, w| !worker_evictable(w, now));
        for (id, job) in &snap.jobs {
            if job.phase.is_terminal() && !guard.archived_jobs.contains(id) {
                guard.archive_eligible.insert(id.clone());
            }
        }
        Ok(())
    }

    /// Archive every job that is eligible (terminal phase covered by
    /// a written snapshot — see [`CoordRuntime::write_snapshot`]) and
    /// whose event chunks are fully flushed. Called from the snapshot
    /// tick, never inline in ingest or command paths.
    ///
    /// Best-effort: a job whose archive fails (or that still has
    /// buffered events) stays eligible and is retried on the next
    /// tick; its chunks remain under `events/` untouched by design
    /// (`archive_job` copies before it deletes).
    ///
    /// Fenced on the lease like every other store-writing method —
    /// archive DELETEs from `events/`, and a deposed coord must not
    /// delete chunks the successor is replaying from.
    pub async fn archive_terminal_jobs(
        &self,
    ) -> Result<Vec<(crate::schema::JobId, crate::archive::ArchiveOutcome)>> {
        let candidates: Vec<crate::schema::JobId> = {
            let guard = self.inner.lock().await;
            if guard.lease_lost {
                return Err(Error::LeaseLost);
            }
            guard
                .archive_eligible
                .iter()
                .filter(|id| !guard.writer.has_buffered_for_job(id))
                .cloned()
                .collect()
        };

        let mut archived = Vec::new();
        for id in candidates {
            // Re-check the fence per job: the loop does store I/O
            // between candidates and the lease can drop mid-pass.
            if self.lease_lost().await {
                return Err(Error::LeaseLost);
            }
            match crate::archive::archive_job(self.store.as_ref(), &id).await {
                Ok(outcome) => {
                    tracing::info!(
                        job = %id,
                        chunks = outcome.chunks_moved,
                        bytes = outcome.bytes_moved,
                        "archived terminal job's event chunks",
                    );
                    let mut guard = self.inner.lock().await;
                    guard.archive_eligible.remove(&id);
                    guard.archived_jobs.insert(id.clone());
                    drop(guard);
                    archived.push((id, outcome));
                }
                Err(e) => {
                    tracing::warn!(
                        job = %id,
                        error = %e,
                        "archive failed; chunks stay under events/ and \
                         the next tick retries",
                    );
                }
            }
        }
        Ok(archived)
    }

    /// Graceful shutdown: flush the log, write a final snapshot,
    /// release the lease. Returns the [`crate::lease::release`]
    /// outcome implicitly (release is idempotent — see lease docs).
    ///
    /// Short-circuits with [`Error::LeaseLost`] before any store
    /// write (or the lease release) once the lease is observed
    /// lost: a successor coord has already taken over and replayed;
    /// flushing our buffer or snapshotting our state would corrupt
    /// its log. The caller decides how loudly to surface the drop.
    pub async fn shutdown(&self, history_keep: usize) -> Result<()> {
        if self.lease_lost().await {
            return Err(Error::LeaseLost);
        }
        self.flush_log().await?;
        self.write_snapshot(history_keep).await?;
        let handle = self.lease_handle().await;
        lease::release(self.store.as_ref(), &handle).await?;
        Ok(())
    }
}

/// See [`CoordRuntime::tail_reader`]. Clones share the runtime's
/// inner state; none of them hold the broadcast sender.
#[derive(Clone)]
pub struct EventTailReader {
    inner: Arc<Mutex<RuntimeInner>>,
}

impl EventTailReader {
    /// Snapshot every buffered (not-yet-flushed) envelope with
    /// `seq > since`, ascending by seq.
    pub async fn unflushed_since(&self, since: u64) -> Vec<EventEnvelope> {
        self.inner.lock().await.writer.unflushed_since(since)
    }
}

impl std::fmt::Debug for EventTailReader {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("EventTailReader").finish_non_exhaustive()
    }
}

/// Eviction predicate for [`CoordRuntime::write_snapshot`]: only
/// Disconnected rows past [`WORKER_EVICT_AFTER_SECS`] since their
/// last activity qualify. Fenced (and every other) state never does.
fn worker_evictable(w: &crate::schema::Worker, now: DateTime<Utc>) -> bool {
    w.state == crate::schema::WorkerState::Disconnected
        && now.signed_duration_since(w.last_heartbeat)
            >= chrono::Duration::seconds(WORKER_EVICT_AFTER_SECS)
}

async fn acquire_with_backoff(
    store: &dyn CoordStore,
    me: &Identity,
    cfg: &RuntimeConfig,
    clock: &dyn Clock,
) -> Result<LeaseHandle> {
    let mut attempts = 0u32;
    loop {
        match lease::try_acquire(store, me, cfg.lease, clock.now()).await? {
            AcquireOutcome::Acquired(h) => return Ok(h),
            AcquireOutcome::Held {
                holder_id,
                expires_at,
            } => {
                tracing::warn!(
                    holder = %holder_id,
                    %expires_at,
                    attempt = attempts,
                    "lease held; backing off",
                );
                attempts = attempts.saturating_add(1);
                if let Some(max) = cfg.lease_retry_max_attempts {
                    if attempts >= max {
                        return Err(Error::LeaseHeld {
                            holder: holder_id,
                            expires_at,
                        });
                    }
                }
                tokio::time::sleep(cfg.lease_retry_interval).await;
            }
        }
    }
}

// Test clock is always available so integration tests can use it without a feature.
pub mod test_clock;

#[cfg(test)]
mod tests;
