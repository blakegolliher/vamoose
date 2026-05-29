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
//! mutation. The critical section is short:
//!
//! 1. Assign seq, stamp `at`.
//! 2. Build the envelope.
//! 3. Apply the reducer.
//! 4. Append to the log writer (may flush; `.await` inside the
//!    lock).
//! 5. Drop the lock.
//! 6. `broadcast::send` to SSE subscribers (non-blocking).
//!
//! The flush in step 4 is the long pole — an `S3 PUT` of an
//! event chunk runs every 1000 events or 5 minutes. Holding the
//! lock through that PUT keeps ingest serialized and avoids
//! state-vs-log drift. If a future deployment finds ingest
//! latency a problem, the flush can move to a background task
//! consuming a bounded queue; the seq/state/log triple still
//! stays inside the lock.
//!
//! Reads (REST handlers, SSE catch-up reads) acquire the same
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
use crate::events::{EventLogConfig, EventLogWriter};
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

struct RuntimeInner {
    state: Snapshot,
    next_seq: u64,
    writer: EventLogWriter,
    lease: LeaseHandle,
    /// True once the lease has been observed lost. Subsequent
    /// `ingest` calls return [`Error::LeaseLost`] without touching
    /// the store, so a partial-state coord cannot keep writing.
    lease_lost: bool,
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
            })),
            bus,
            store,
            clock,
            cfg,
        })
    }

    /// Ingest one event. Assigns the next seq, stamps `at`, applies
    /// the reducer, appends to the log, then broadcasts to SSE
    /// subscribers. Returns the assigned seq.
    pub async fn ingest(&self, kind: EventKind) -> Result<u64> {
        let env = {
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
                worker_at: None,
                kind,
            };
            guard.state.apply(&env);
            guard
                .writer
                .append(self.store.as_ref(), env.clone())
                .await?;
            env
        };
        // Broadcast outside the lock — `send` is non-blocking; if
        // there are no subscribers it returns an error we ignore.
        let _ = self.bus.send(env.clone());
        Ok(env.seq)
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
        let env = {
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
                worker_at: Some(worker_at),
                kind,
            };
            guard.state.apply(&env);
            guard
                .writer
                .append(self.store.as_ref(), env.clone())
                .await?;
            env
        };
        let _ = self.bus.send(env.clone());
        Ok(env.seq)
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
    /// On UTC date rollover the counter resets — the snapshot
    /// persists `(audit_seq_today, audit_seq_date)` so a coord
    /// restart on the same day continues the day's numbering
    /// rather than racing previously-written keys.
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

        let (key, body) = {
            let mut guard = self.inner.lock().await;
            if guard.lease_lost {
                return Err(Error::LeaseLost);
            }
            // Rollover: reset on a new UTC day.
            if guard.state.audit_seq_date != date {
                guard.state.audit_seq_date = date.clone();
                guard.state.audit_seq_today = 0;
            }
            guard.state.audit_seq_today += 1;
            let seq = guard.state.audit_seq_today;
            let key = crate::layout::audit_chunk_key(&date, seq);

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
            (key, body)
        };

        self.store.put(&key, body).await?;
        Ok(command_id)
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

    /// Force-flush every open event-log chunk. Called from the
    /// snapshot tick (so the snapshot reflects a clean log
    /// boundary) and from graceful shutdown.
    pub async fn flush_log(&self) -> Result<()> {
        let mut guard = self.inner.lock().await;
        guard.writer.flush_all(self.store.as_ref()).await
    }

    /// Flush only chunks aged past `max_chunk_age`. Driven by the
    /// snapshot tick at a lower cadence than `flush_log`.
    pub async fn flush_aged(&self) -> Result<()> {
        let mut guard = self.inner.lock().await;
        let now = self.clock.now();
        guard.writer.flush_aged(self.store.as_ref(), now).await
    }

    /// Persist the current state to `state/snapshot.json`. Called
    /// from the snapshot tick.
    pub async fn write_snapshot(&self, history_keep: usize) -> Result<()> {
        let now = self.clock.now();
        let snap = {
            let mut guard = self.inner.lock().await;
            // Stamp written_at and schema_version; the rest is
            // already populated by the reducer.
            guard.state.written_at = now;
            guard.state.schema_version = SCHEMA_VERSION;
            guard.state.clone()
        };
        crate::snapshot::write(self.store.as_ref(), &snap, history_keep, now).await
    }

    /// Graceful shutdown: flush the log, write a final snapshot,
    /// release the lease. Returns the [`crate::lease::release`]
    /// outcome implicitly (release is idempotent — see lease docs).
    pub async fn shutdown(&self, history_keep: usize) -> Result<()> {
        self.flush_log().await?;
        self.write_snapshot(history_keep).await?;
        let handle = self.lease_handle().await;
        lease::release(self.store.as_ref(), &handle).await?;
        Ok(())
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
