use super::CoordRuntime;
use crate::errors::{Error, Result};
use crate::events::{FlushScope, PendingFlush};
use crate::schema::SCHEMA_VERSION;
use chrono::{DateTime, Utc};

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

impl CoordRuntime {
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
    pub(super) async fn flush_scope(&self, scope: FlushScope) -> Result<()> {
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
}

/// Eviction predicate for [`CoordRuntime::write_snapshot`]: only
/// Disconnected rows past [`WORKER_EVICT_AFTER_SECS`] since their
/// last activity qualify. Fenced (and every other) state never does.
fn worker_evictable(w: &crate::schema::Worker, now: DateTime<Utc>) -> bool {
    w.state == crate::schema::WorkerState::Disconnected
        && now.signed_duration_since(w.last_heartbeat)
            >= chrono::Duration::seconds(WORKER_EVICT_AFTER_SECS)
}
