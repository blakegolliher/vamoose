use super::{Clock, CoordRuntime, RuntimeConfig, RuntimeInner};
use crate::schema::{EventEnvelope, Snapshot};
use crate::store::CoordStore;
use std::sync::Arc;
use tokio::sync::{broadcast, Mutex};

/// One page of the jobs view, returned from
/// [`CoordRuntime::jobs_view`]. `next_cursor` is the `JobId` of the
/// first job that would have been in the *next* page, or `None` if
/// the current page contained the last job.
#[derive(Debug, Clone)]
pub struct JobsPage {
    pub jobs: Vec<crate::schema::Job>,
    pub next_cursor: Option<crate::schema::JobId>,
}

impl CoordRuntime {
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

    /// Remember the progress object `vamoose prepare` last wrote to
    /// the bucket (`None` clears it), stamped with the coord's clock.
    pub async fn set_prepare_progress(&self, progress: Option<crate::schema::PrepareProgress>) {
        let now = self.clock.now();
        self.inner.lock().await.prepare = progress.map(|p| (p, now));
    }

    /// `GET /prepare`: the latest prepare progress and its age by the
    /// preparing host's own `updated_utc` against the coord's clock.
    pub async fn prepare_view(&self) -> crate::schema::PrepareResponse {
        let now = self.clock.now();
        let guard = self.inner.lock().await;
        match &guard.prepare {
            Some((p, _read_at)) => crate::schema::PrepareResponse {
                age_secs: Some((now - p.updated_utc).num_seconds().max(0) as u64),
                progress: Some(p.clone()),
            },
            None => crate::schema::PrepareResponse {
                progress: None,
                age_secs: None,
            },
        }
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
                if !same_machine(&w.host, w.pid, host, pid) {
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

/// Do two registrations come from the same machine? The worker's
/// default host id is `<hostname>-<pid>`, so a restarted worker
/// never carries the same host string as the one it replaces; a
/// literal comparison made the supersede path dead on every real
/// deployment (the 600M retest ended with eleven dead workers still
/// "Copying"). Strip a trailing `-<pid>` when it matches the
/// registration's own pid, then compare what's left — a custom host
/// id without the suffix still compares literally.
pub(crate) fn same_machine(host_a: &str, pid_a: u32, host_b: &str, pid_b: u32) -> bool {
    machine_name(host_a, pid_a) == machine_name(host_b, pid_b)
}

fn machine_name(host: &str, pid: u32) -> &str {
    let suffix = format!("-{pid}");
    host.strip_suffix(suffix.as_str()).unwrap_or(host)
}

#[cfg(test)]
mod machine_tests {
    use super::same_machine;

    #[test]
    fn pid_suffixed_host_ids_from_one_machine_match() {
        assert!(same_machine(
            "k8s-se-2-1203425",
            1203425,
            "k8s-se-2-2620823",
            2620823
        ));
        assert!(same_machine("k8s-se-2", 7, "k8s-se-2", 8));
        assert!(same_machine("k8s-se-2-1203425", 1203425, "k8s-se-2", 8));
    }

    #[test]
    fn different_machines_or_unrelated_suffixes_do_not_match() {
        assert!(!same_machine(
            "k8s-se-2-1203425",
            1203425,
            "k8s-se-1-214989",
            214989
        ));
        // A suffix that is not this registration's pid is part of the name.
        assert!(!same_machine("host-a-100", 5, "host-a", 5));
        assert!(!same_machine("host-a", 1, "host-b", 1));
    }
}
