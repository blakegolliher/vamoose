//! Client-side state for the TUI.
//!
//! The data model is the coord's own
//! [`migration_coord::schema::Snapshot`] — we reuse its
//! [`Snapshot::apply`] reducer verbatim so the client and the server
//! agree on the wire semantics of every event variant. The wrapper
//! adds three things the coord itself does not need:
//!
//! - [`ConnectionStatus`] — observability of the SSE link health
//!   for the connection banner.
//! - [`AppState::last_seen_seq`] — the highest seq we've observed,
//!   used as the `Last-Event-ID` value on reconnect.
//! - lightweight UI-only state (selected job id, filter string) so
//!   the render layer can drop in without re-deriving anything.
//!
//! The reducer is single-threaded; the TUI owns the state on the
//! render-loop task. Use [`AppState::apply_envelope`] for every
//! envelope received from the SSE stream — it routes through
//! `Snapshot::apply` and updates `last_seen_seq`.

use chrono::{DateTime, Utc};
use migration_coord::schema::{ErrorBucket, EventEnvelope, Job, JobId, Snapshot, Worker, WorkerId};

/// Health of the SSE link.
///
/// - `Connected` — last event or keepalive received within the
///   liveness window; the banner is green.
/// - `Reconnecting { since }` — link dropped; the resume loop is
///   sleeping with exponential backoff. Banner is yellow.
/// - `Disconnected { reason }` — terminal — usually a non-retryable
///   4xx (bad token, unknown URL). Banner is red. The TUI surfaces
///   the reason to the operator.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ConnectionStatus {
    Connected {
        /// Wall-clock of the most recent event or keepalive frame.
        /// The render layer compares against `now()` to fade the
        /// banner if no traffic has arrived in N seconds.
        last_traffic: DateTime<Utc>,
    },
    Reconnecting {
        /// When the most recent disconnect started.
        since: DateTime<Utc>,
        /// Operator-readable error from the latest connect attempt.
        last_error: String,
    },
    Disconnected {
        reason: String,
    },
}

impl ConnectionStatus {
    pub fn connected_now(now: DateTime<Utc>) -> Self {
        Self::Connected { last_traffic: now }
    }
}

/// UI-only state. Survives reconnects.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct UiState {
    /// Currently-selected job id in the jobs list. None = no
    /// selection (e.g. empty list, or operator hasn't moved the
    /// cursor yet).
    pub selected_job: Option<JobId>,
    /// Case-insensitive substring filter over job name. Empty
    /// matches everything.
    pub filter: String,
    /// Sort criterion for the jobs list. The render layer applies
    /// this to the filtered set.
    pub sort: JobSort,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum JobSort {
    /// Alphabetical by job id. Stable default; renders sort the
    /// same way the operator would type the list.
    #[default]
    ById,
    /// Phase-grouped (active phases first, terminal last), then by
    /// id within each phase.
    ByPhase,
    /// Highest progress fraction first.
    ByProgressDesc,
    /// Highest error count first.
    ByErrorsDesc,
}

/// Whole client-side state. `Snapshot` is the derived data model
/// (jobs, workers, errors, last_seq); the rest is TUI-only.
#[derive(Debug, Clone)]
pub struct AppState {
    pub snapshot: Snapshot,
    pub connection: ConnectionStatus,
    pub ui: UiState,
    /// Highest seq observed from the SSE stream. Always >= the
    /// snapshot's `last_seq` (the reducer also updates that field
    /// in-place). The resume cursor on reconnect is this value.
    pub last_seen_seq: u64,
}

impl AppState {
    /// Construct an empty state, before any traffic has arrived.
    /// `now` is captured as the snapshot's `written_at` so a fresh
    /// TUI doesn't look like it was loaded from an ancient
    /// persisted file.
    pub fn empty(now: DateTime<Utc>) -> Self {
        Self {
            snapshot: Snapshot::empty(now),
            connection: ConnectionStatus::Reconnecting {
                since: now,
                last_error: "not yet connected".into(),
            },
            ui: UiState::default(),
            last_seen_seq: 0,
        }
    }

    /// Apply one event envelope received from the SSE stream.
    ///
    /// Events arriving with `seq <= last_seen_seq` are silently
    /// discarded — they have already been folded into state on a
    /// prior pass (this happens when a reconnect overlaps with a
    /// late frame still in flight from the prior connection, or
    /// when the resume cursor is set slightly too low).
    ///
    /// Returns `true` if the event advanced state, `false` if it
    /// was discarded as a duplicate / stale.
    pub fn apply_envelope(&mut self, envelope: &EventEnvelope) -> bool {
        if envelope.seq <= self.last_seen_seq {
            return false;
        }
        self.snapshot.apply(envelope);
        self.last_seen_seq = envelope.seq;
        true
    }

    /// Replace the entire derived state with a freshly-fetched
    /// snapshot (e.g. after a forced full re-bootstrap via REST).
    /// The connection status and UI state are preserved.
    pub fn replace_snapshot(&mut self, snapshot: Snapshot) {
        self.last_seen_seq = self.last_seen_seq.max(snapshot.last_seq);
        self.snapshot = snapshot;
    }

    /// Mark the SSE link as connected (called by the connection
    /// driver on a successful response). Refreshes `last_traffic`
    /// to `now`.
    pub fn mark_connected(&mut self, now: DateTime<Utc>) {
        self.connection = ConnectionStatus::connected_now(now);
    }

    /// Refresh the connected-state `last_traffic` timestamp without
    /// transitioning to a different state — called on every SSE
    /// frame (event OR keepalive). No-op if not Connected (the
    /// connection driver hasn't recorded a successful connect yet).
    pub fn mark_traffic(&mut self, now: DateTime<Utc>) {
        if let ConnectionStatus::Connected { last_traffic } = &mut self.connection {
            *last_traffic = now;
        }
    }

    /// Mark the SSE link as reconnecting. Stamps `since` with `now`
    /// and surfaces `last_error` so the banner can show it.
    pub fn mark_reconnecting(&mut self, now: DateTime<Utc>, last_error: impl Into<String>) {
        self.connection = ConnectionStatus::Reconnecting {
            since: now,
            last_error: last_error.into(),
        };
    }

    /// Terminal disconnect. The banner goes red and the resume loop
    /// stops trying (used for non-retryable 4xx, e.g. bad token).
    pub fn mark_disconnected(&mut self, reason: impl Into<String>) {
        self.connection = ConnectionStatus::Disconnected {
            reason: reason.into(),
        };
    }

    // ----- Convenience accessors over the derived snapshot -----

    pub fn jobs_iter(&self) -> impl Iterator<Item = &Job> {
        self.snapshot.jobs.values()
    }

    pub fn job(&self, id: &JobId) -> Option<&Job> {
        self.snapshot.jobs.get(id)
    }

    pub fn workers_for_job(&self, id: &JobId) -> Vec<&Worker> {
        let job = match self.snapshot.jobs.get(id) {
            Some(j) => j,
            None => return Vec::new(),
        };
        job.assigned_workers
            .iter()
            .filter_map(|wid| self.snapshot.workers.get(wid))
            .collect()
    }

    pub fn worker(&self, id: &WorkerId) -> Option<&Worker> {
        self.snapshot.workers.get(id)
    }

    pub fn errors_for_job(&self, id: &JobId) -> &[ErrorBucket] {
        self.snapshot
            .error_buckets
            .get(id)
            .map(|v| v.as_slice())
            .unwrap_or(&[])
    }

    pub fn last_seq(&self) -> u64 {
        self.last_seen_seq
    }
}

// =============================================================================
// Tests
// =============================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::TimeZone;
    use migration_coord::schema::{ConfigHash, EventKind, SCHEMA_VERSION};

    fn at(secs: i64) -> DateTime<Utc> {
        Utc.timestamp_opt(secs, 0).unwrap()
    }

    fn jid(s: &str) -> JobId {
        JobId::new(s).unwrap()
    }

    fn job_created(seq: u64, job: &str) -> EventEnvelope {
        EventEnvelope {
            seq,
            at: at(seq as i64),
            schema_version: SCHEMA_VERSION,
            worker_at: None,
            kind: EventKind::JobCreated {
                job_id: jid(job),
                name: format!("{job}-mig"),
                source: "nfs://src".into(),
                dest: "nfs://dst".into(),
                owner: "test".into(),
                config_hash: ConfigHash("ab".into()),
            },
        }
    }

    fn progress(seq: u64, job: &str, files: u64, bytes: u64) -> EventEnvelope {
        EventEnvelope {
            seq,
            at: at(seq as i64),
            schema_version: SCHEMA_VERSION,
            worker_at: None,
            kind: EventKind::ProgressDelta {
                job_id: jid(job),
                worker_id: WorkerId::new(),
                files_delta: files,
                bytes_delta: bytes,
                errors_delta: 0,
            },
        }
    }

    #[test]
    fn empty_state_is_pre_connect() {
        let s = AppState::empty(at(0));
        assert_eq!(s.last_seen_seq, 0);
        assert!(s.snapshot.jobs.is_empty());
        assert!(matches!(
            s.connection,
            ConnectionStatus::Reconnecting { .. }
        ));
    }

    #[test]
    fn apply_envelope_advances_state_and_seq() {
        let mut s = AppState::empty(at(0));
        assert!(s.apply_envelope(&job_created(1, "bobby")));
        assert_eq!(s.last_seen_seq, 1);
        assert!(s.snapshot.jobs.contains_key(&jid("bobby")));
        assert!(s.apply_envelope(&progress(2, "bobby", 5, 1024)));
        assert_eq!(s.last_seen_seq, 2);
        let j = s.job(&jid("bobby")).unwrap();
        assert_eq!(j.progress.files_done, 5);
        assert_eq!(j.progress.bytes_done, 1024);
    }

    #[test]
    fn duplicate_or_stale_envelope_is_dropped() {
        let mut s = AppState::empty(at(0));
        s.apply_envelope(&job_created(1, "bobby"));
        s.apply_envelope(&progress(2, "bobby", 5, 1024));
        // Re-applying seq 2 must not double-count.
        assert!(!s.apply_envelope(&progress(2, "bobby", 5, 1024)));
        // Out-of-order older seq is also dropped.
        assert!(!s.apply_envelope(&progress(1, "bobby", 99, 99)));
        let j = s.job(&jid("bobby")).unwrap();
        assert_eq!(j.progress.files_done, 5);
        assert_eq!(j.progress.bytes_done, 1024);
        assert_eq!(s.last_seen_seq, 2);
    }

    #[test]
    fn replace_snapshot_keeps_last_seen_when_snapshot_is_older() {
        let mut s = AppState::empty(at(0));
        s.apply_envelope(&job_created(1, "bobby"));
        s.apply_envelope(&progress(2, "bobby", 5, 1024));
        assert_eq!(s.last_seen_seq, 2);

        let mut older = Snapshot::empty(at(100));
        older.last_seq = 1;
        s.replace_snapshot(older);
        // We had already seen seq 2; do not regress.
        assert_eq!(s.last_seen_seq, 2);
        // But the data is now whatever the (older, empty) snapshot
        // says — replace is a hard replace.
        assert!(s.snapshot.jobs.is_empty());
    }

    #[test]
    fn connection_status_transitions() {
        let mut s = AppState::empty(at(0));
        // Empty -> Reconnecting by construction.
        s.mark_connected(at(1));
        assert!(matches!(s.connection, ConnectionStatus::Connected { .. }));
        s.mark_traffic(at(5));
        if let ConnectionStatus::Connected { last_traffic } = &s.connection {
            assert_eq!(*last_traffic, at(5));
        } else {
            panic!("must be Connected");
        }
        s.mark_reconnecting(at(10), "connection reset");
        match &s.connection {
            ConnectionStatus::Reconnecting { last_error, .. } => {
                assert_eq!(last_error, "connection reset");
            }
            _ => panic!("must be Reconnecting"),
        }
        s.mark_disconnected("bad token");
        match &s.connection {
            ConnectionStatus::Disconnected { reason } => assert_eq!(reason, "bad token"),
            _ => panic!("must be Disconnected"),
        }
    }

    #[test]
    fn mark_traffic_is_noop_when_not_connected() {
        let mut s = AppState::empty(at(0));
        // empty() leaves connection in Reconnecting; mark_traffic must
        // NOT promote that to Connected.
        s.mark_traffic(at(10));
        assert!(matches!(
            s.connection,
            ConnectionStatus::Reconnecting { .. }
        ));
    }

    #[test]
    fn workers_for_job_filters_to_assigned() {
        let mut s = AppState::empty(at(0));
        s.apply_envelope(&job_created(1, "bobby"));
        let w1 = WorkerId::new();
        s.apply_envelope(&EventEnvelope {
            seq: 2,
            at: at(2),
            schema_version: SCHEMA_VERSION,
            worker_at: None,
            kind: EventKind::WorkerJoined {
                worker_id: w1,
                job_id: jid("bobby"),
                host: "h".into(),
                pid: 1,
                start_time: at(0),
                version: "0.6".into(),
            },
        });
        let workers = s.workers_for_job(&jid("bobby"));
        assert_eq!(workers.len(), 1);
        assert_eq!(workers[0].id, w1);
        assert_eq!(s.worker(&w1).map(|w| &w.host[..]), Some("h"));
    }
}
