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
    /// Accounting (observability): total files_delta ingested through
    /// the cap, broadcast on the leading edge, and drained via the
    /// trailing tick. ingested == leading + drained + currently
    /// retained, at all times — logged by the flush tick so wire
    /// losslessness is verifiable in production.
    /// Last emitted (files_done, bytes_done) per job — dedups
    /// per-tick ProgressSync emission for idle jobs.
    pub(super) last_sync: std::collections::HashMap<crate::schema::JobId, (u64, u64)>,
    pub(super) acct_ingested_files: u64,
    pub(super) acct_leading_files: u64,
    pub(super) acct_drained_files: u64,
    progress_last:
        std::collections::HashMap<(crate::schema::JobId, crate::schema::WorkerId), DateTime<Utc>>,
    /// Latest SUPPRESSED delta per (job, worker) — the trailing edge
    /// the tick re-broadcasts. Cleared whenever a fresh delta for
    /// the key broadcasts (the retained frame is then stale) and on
    /// delivery.
    retained_progress: std::collections::HashMap<
        (crate::schema::JobId, crate::schema::WorkerId),
        (EventEnvelope, DateTime<Utc>),
    >,
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
                job_id,
                worker_id,
                files_delta,
                ..
            } => {
                self.acct_ingested_files = self.acct_ingested_files.saturating_add(*files_delta);
                let files_delta_now = *files_delta;
                let min_interval =
                    chrono::Duration::milliseconds(crate::schema::PROGRESS_STREAM_MIN_INTERVAL_MS);
                let key = (job_id.clone(), *worker_id);
                match self.progress_last.get(&key) {
                    Some(last) if now.signed_duration_since(*last) < min_interval => {
                        // Suppressed: COALESCE into the retained frame
                        // (sum the deltas, keep the newest envelope
                        // shell). Latest-wins retention dropped the
                        // in-between counts from the wire, so every
                        // client-derived rate under-reported — the
                        // 600M-rig TUI read ~60% of the true files/s.
                        match self.retained_progress.get_mut(&key) {
                            Some((retained, _first_at)) => merge_progress_delta(retained, env),
                            None => {
                                self.retained_progress.insert(key, (env.clone(), now));
                            }
                        }
                        false
                    }
                    _ => {
                        // Fresh broadcast. Any retained (suppressed)
                        // sums stay retained — their counts have not
                        // reached subscribers yet; the flush tick
                        // delivers them. Removing them here silently
                        // dropped their counts.
                        self.acct_leading_files =
                            self.acct_leading_files.saturating_add(files_delta_now);
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
        // Due-ness is the RETAINED frame's age, not time since the
        // key's last broadcast: under a steady event flow a fresh
        // frame broadcasts every interval, which would keep resetting
        // a broadcast-based clock and the retained sums would
        // accumulate for the entire run (observed on the 600M rig:
        // only each batch's leading event reached the wire — clients
        // saw ~67% of the counts). Draining by age bounds the wire at
        // ≤2 frames per interval per key: the leading edge plus one
        // coalesced trailing sum.
        let due: Vec<_> = self
            .retained_progress
            .iter()
            .filter(|(_, (_, first_at))| now.signed_duration_since(*first_at) >= min_interval)
            .map(|(key, _)| key.clone())
            .collect();
        let mut out = Vec::with_capacity(due.len());
        for key in due {
            if let Some((env, _)) = self.retained_progress.remove(&key) {
                if let EventKind::ProgressDelta { files_delta, .. } = &env.kind {
                    self.acct_drained_files = self.acct_drained_files.saturating_add(*files_delta);
                }
                out.push(env);
            }
        }
        // Deterministic delivery order for multi-key ticks.
        out.sort_by_key(|e| e.seq);
        out
    }
}

/// Fold `newer`'s ProgressDelta counts into `retained` (also a
/// ProgressDelta for the same (job, worker)), keeping `newer`'s
/// envelope identity (seq / at / client_seq) so resume cursors stay
/// monotonic. The merged frame then represents the SUM of every
/// suppressed event up to its seq — a client folding it reaches the
/// same totals as one that saw each original.
fn merge_progress_delta(retained: &mut EventEnvelope, newer: &EventEnvelope) {
    let (
        EventKind::ProgressDelta {
            files_delta: rf,
            bytes_delta: rb,
            errors_delta: re,
            ..
        },
        EventKind::ProgressDelta {
            files_delta: nf,
            bytes_delta: nb,
            errors_delta: ne,
            ..
        },
    ) = (&retained.kind, &newer.kind)
    else {
        return;
    };
    let (sf, sb, se) = (
        rf.saturating_add(*nf),
        rb.saturating_add(*nb),
        re.saturating_add(*ne),
    );
    let mut merged = newer.clone();
    if let EventKind::ProgressDelta {
        files_delta,
        bytes_delta,
        errors_delta,
        ..
    } = &mut merged.kind
    {
        *files_delta = sf;
        *bytes_delta = sb;
        *errors_delta = se;
    }
    *retained = merged;
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
        latency: Option<crate::schema::LatencySummary>,
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
                // A heartbeat without a window (older worker, or the
                // first tick) keeps the last one it sent.
                if latency.is_some() {
                    w.latency = latency;
                }
                w.last_heartbeat = now;
                Ok(true)
            }
            None => Ok(false),
        }
    }

    /// Is this WorkerId known to the coord at all (any state)?
    pub async fn worker_is_registered(&self, worker_id: &crate::schema::WorkerId) -> bool {
        let guard = self.inner.lock().await;
        guard.state.workers.contains_key(worker_id)
    }

    /// Liveness sweep: every worker still in a live state whose last
    /// heartbeat is older than `timeout` gets a
    /// `WorkerLeft{reason: "no heartbeat for Ns"}`. Heartbeats are
    /// the only liveness signal — a worker that died hard (SIGKILL,
    /// node loss, self-fence that could not reach the coord) never
    /// says goodbye, and before this sweep it stayed `Copying` in
    /// every view forever. Returns the ids swept.
    pub async fn sweep_stale_workers(
        &self,
        timeout: chrono::Duration,
    ) -> Result<Vec<crate::schema::WorkerId>> {
        let now = self.clock.now();
        let stale: Vec<(crate::schema::WorkerId, i64)> = {
            let guard = self.inner.lock().await;
            if guard.lease_lost {
                return Err(Error::LeaseLost);
            }
            guard
                .state
                .workers
                .values()
                .filter(|w| w.state != crate::schema::WorkerState::Disconnected)
                .filter_map(|w| {
                    let age = now.signed_duration_since(w.last_heartbeat);
                    (age > timeout).then_some((w.id, age.num_seconds()))
                })
                .collect()
        };
        let mut swept = Vec::with_capacity(stale.len());
        for (worker_id, secs) in stale {
            tracing::warn!(%worker_id, no_heartbeat_secs = secs, "worker missed its liveness timeout; marking Disconnected");
            self.ingest(EventKind::WorkerLeft {
                worker_id,
                reason: format!("no heartbeat for {secs}s"),
            })
            .await?;
            swept.push(worker_id);
        }
        Ok(swept)
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
    /// Emit one authoritative `ProgressSync` per active (Copying /
    /// Scanning / Verifying) job, built from reducer state. Rates and
    /// progress derived from these absolutes are immune to the
    /// ProgressDelta stream cap; ~1 event/s/job of log growth. Skips
    /// jobs whose counters haven't moved since the last sync so idle
    /// jobs cost nothing.
    pub async fn emit_progress_syncs(&self) -> Result<usize> {
        let pending: Vec<EventKind> = {
            let guard = self.inner.lock().await;
            guard
                .state
                .jobs
                .values()
                .filter(|j| j.phase.is_active() && !j.phase.is_terminal())
                .map(|j| {
                    let workers = guard
                        .state
                        .workers
                        .values()
                        .filter(|w| w.job_id == j.id)
                        .map(|w| crate::schema::WorkerCum {
                            worker_id: w.id,
                            files_done: w.files_done,
                            bytes_done: w.bytes_done,
                        })
                        .collect();
                    EventKind::ProgressSync {
                        job_id: j.id.clone(),
                        files_done: j.progress.files_done,
                        bytes_done: j.progress.bytes_done,
                        workers,
                    }
                })
                .collect()
        };
        let mut n = 0;
        for kind in pending {
            // Dedup: don't log a sync identical to the previous one
            // for this job (idle job, nothing moved).
            let fresh = {
                let mut guard = self.inner.lock().await;
                let key = kind.job_id().cloned();
                let sig = match &kind {
                    EventKind::ProgressSync {
                        files_done,
                        bytes_done,
                        ..
                    } => (*files_done, *bytes_done),
                    _ => (0, 0),
                };
                match key {
                    Some(k) => guard.stream_caps.last_sync.insert(k, sig) != Some(sig),
                    None => false,
                }
            };
            if fresh {
                self.ingest(kind).await?;
                n += 1;
            }
        }
        Ok(n)
    }

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
        {
            let guard = self.inner.lock().await;
            let c = &guard.stream_caps;
            tracing::debug!(
                ingested = c.acct_ingested_files,
                leading = c.acct_leading_files,
                drained = c.acct_drained_files,
                retained_keys = c.retained_progress.len(),
                "progress stream cap accounting",
            );
        }
        n
    }
}
