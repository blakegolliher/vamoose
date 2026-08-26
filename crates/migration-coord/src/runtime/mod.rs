//! Coord runtime — the long-running control-plane state engine.
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
//! refresh cadence and shutdown-on-loss live in [`crate::ticks`]. The
//! runtime exposes `lease_lost()` so the ticks task can signal
//! shutdown when refresh sees [`crate::Error::LeaseLost`].
//!
//! ## Internal layout
//!
//! Configuration and clocks live in `config`; construction and replay
//! startup live in `startup`; mutation, broadcast caps, and heartbeats live
//! in `ingest`; read-oriented APIs live in `query`; durable writes live in
//! `persistence`; and lease/shutdown handling lives in `lifecycle`.
//!
//! ## What this module deliberately does *not* do
//!
//! - No HTTP — request handling stays in [`crate::server`].
//! - No background ticks — orchestration stays in [`crate::ticks`].

mod config;
mod ingest;
mod lifecycle;
mod persistence;
mod query;
mod startup;

pub use config::{Clock, RuntimeConfig, SystemClock};
pub use persistence::WORKER_EVICT_AFTER_SECS;
pub use query::{EventTailReader, JobsPage};

use crate::events::EventLogWriter;
use crate::lease::LeaseHandle;
use crate::schema::{EventEnvelope, Snapshot};
use crate::store::CoordStore;
use ingest::StreamCaps;
use std::sync::Arc;
use tokio::sync::{broadcast, Mutex};

struct RuntimeInner {
    state: Snapshot,
    /// Latest `prepare/progress.json` read from the bucket, with the
    /// coord-clock time it was read. Not an event and never persisted:
    /// it describes the stage before the job exists.
    prepare: Option<(
        crate::schema::PrepareProgress,
        chrono::DateTime<chrono::Utc>,
    )>,
    next_seq: u64,
    writer: EventLogWriter,
    lease: LeaseHandle,
    /// True once the lease has been observed lost. Subsequent
    /// `ingest` calls return [`crate::Error::LeaseLost`] without touching
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

// Test clock is always available so integration tests can use it without a feature.
pub mod test_clock;

#[cfg(test)]
mod tests;
