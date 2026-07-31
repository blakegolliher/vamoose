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
use crate::schema::{EventEnvelope, EventKind, Snapshot, SCHEMA_VERSION};
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
/// [`FixedClock`] (the test module) to drive event timing
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
    /// How long [`start`] backs off between failed lease-acquire
    /// attempts. Tests inject a short value; production defaults
    /// to 2s.
    pub lease_retry_interval: std::time::Duration,
    /// Maximum attempts before [`start`] gives up. `None` means
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
/// the same cardinality as the workers table, which is itself
/// unbounded today (noted in the F24 ledger row as a follow-up).
#[derive(Debug, Default)]
struct StreamCaps {
    progress_last:
        std::collections::HashMap<(crate::schema::JobId, crate::schema::WorkerId), DateTime<Utc>>,
    error_window_start: Option<DateTime<Utc>>,
    error_counts: std::collections::HashMap<crate::schema::ErrorClass, u32>,
}

impl StreamCaps {
    /// True if `kind` may be broadcast at `now`. Mutates the cap
    /// bookkeeping; call exactly once per ingested event, under the
    /// runtime lock.
    fn should_broadcast(&mut self, kind: &EventKind, now: DateTime<Utc>) -> bool {
        match kind {
            EventKind::ProgressDelta {
                job_id, worker_id, ..
            } => {
                let min_interval =
                    chrono::Duration::milliseconds(crate::schema::PROGRESS_STREAM_MIN_INTERVAL_MS);
                let key = (job_id.clone(), *worker_id);
                match self.progress_last.get(&key) {
                    Some(last) if now.signed_duration_since(*last) < min_interval => false,
                    _ => {
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
    /// [`StreamCaps`]). Returns the assigned seq.
    pub async fn ingest(&self, kind: EventKind) -> Result<u64> {
        self.ingest_inner(kind, None, None).await
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
        self.ingest_inner(kind, Some(worker_at), None).await
    }

    /// Ingest an event submitted on the worker events route: optional
    /// worker-local timestamp plus the optional per-worker
    /// `client_seq` idempotency stamp (ledger F20, D4). The stamp is
    /// carried on the envelope into the durable log, so the reducer —
    /// live and on replay — maintains the per-worker high-water mark
    /// the events handler dedups against.
    pub async fn ingest_worker_event(
        &self,
        kind: EventKind,
        worker_at: Option<DateTime<Utc>>,
        client_seq: Option<u64>,
    ) -> Result<u64> {
        self.ingest_inner(kind, worker_at, client_seq).await
    }

    async fn ingest_inner(
        &self,
        kind: EventKind,
        worker_at: Option<DateTime<Utc>>,
        client_seq: Option<u64>,
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
                kind,
            };
            guard.state.apply(&env);
            let broadcast = guard.stream_caps.should_broadcast(&env.kind, env.at);
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

    /// Compute the [`ControlMode`] the coord wants this worker to
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
            guard.state.clone()
        };
        crate::snapshot::write(self.store.as_ref(), &snap, history_keep, now).await?;

        let mut guard = self.inner.lock().await;
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

// =============================================================================
// Test clock — always available so integration tests in tests/ can
// use it without depending on the test-helpers feature.
// =============================================================================

pub mod test_clock {
    use super::*;
    use std::sync::Mutex;

    /// Wall-clock that returns a fixed instant; tests bump it
    /// explicitly between `ingest` calls to control event `at`
    /// timestamps.
    #[derive(Debug)]
    pub struct FixedClock {
        now: Mutex<DateTime<Utc>>,
    }

    impl FixedClock {
        pub fn new(at: DateTime<Utc>) -> Arc<Self> {
            Arc::new(Self {
                now: Mutex::new(at),
            })
        }

        pub fn set(&self, at: DateTime<Utc>) {
            *self.now.lock().unwrap() = at;
        }

        pub fn advance(&self, by: chrono::Duration) {
            let mut n = self.now.lock().unwrap();
            *n += by;
        }
    }

    impl Clock for FixedClock {
        fn now(&self) -> DateTime<Utc> {
            *self.now.lock().unwrap()
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::schema::{JobId, Phase, WorkerId};
    use crate::store::MemStore;
    use chrono::{Duration, TimeZone};
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
        }
    }

    fn at(secs: i64) -> DateTime<Utc> {
        Utc.with_ymd_and_hms(2026, 5, 29, 14, 32, 0).unwrap() + Duration::seconds(secs)
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
        // a snapshot and flushes the log).
        let store: Arc<dyn CoordStore> = Arc::new(MemStore::new());
        let clock = FixedClock::new(at(0));
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
            rt.shutdown(3).await.unwrap();
        }

        // Run 2: restart against the same bucket. State should
        // reconstitute exactly.
        clock.advance(Duration::seconds(60));
        let rt2 = CoordRuntime::start(store.clone(), clock.clone(), me("B"), cfg_for_tests())
            .await
            .unwrap();
        let snap = rt2.state().await;
        assert_eq!(snap.jobs[&jid("bobby")].progress.files_done, 100);
        assert_eq!(snap.jobs[&jid("bobby")].phase, Phase::Planned);
        assert_eq!(rt2.last_seq().await, 2);
        // Next ingest gets seq 3.
        let s3 = rt2.ingest(job_created("mary")).await.unwrap();
        assert_eq!(s3, 3);
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
        async fn put_if_absent(
            &self,
            key: &str,
            body: Vec<u8>,
        ) -> Result<crate::store::PutOutcome> {
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
        let job =
            tokio::time::timeout(std::time::Duration::from_secs(1), rt.job_view(&jid("mary")))
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
}
