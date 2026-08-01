use super::CoordRuntime;
use crate::errors::{Error, Result};
use crate::events::FlushScope;
use crate::schema::{EventEnvelope, EventKind, WorkerId, SCHEMA_VERSION};
use chrono::{DateTime, Utc};

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
pub(super) struct StreamCaps {
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

impl CoordRuntime {
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
}
