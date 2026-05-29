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

use chrono::{DateTime, Duration as ChronoDuration, Utc};
use migration_coord::schema::{
    ErrorBucket, EventEnvelope, EventKind, Job, JobId, Snapshot, Worker, WorkerId,
};
use std::collections::{HashMap, VecDeque};

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
    /// Case-insensitive substring filter over job id + name. Empty
    /// matches everything. Updated live while in
    /// [`InputMode::Filter`] so the visible table tracks the typed
    /// buffer; committed to here permanently on Enter.
    pub filter: String,
    /// Sort criterion for the jobs list. The render layer applies
    /// this to the filtered set.
    pub sort: JobSort,
    /// What the next keypress means. `Normal` is operator
    /// navigation; `Filter` captures typed characters into the
    /// filter buffer.
    pub input_mode: InputMode,
    /// Top-level view. Defaults to `List`; the operator presses
    /// Enter on a job row to switch to `Detail`. The render layer
    /// dispatches on this; the event loop reads it to decide which
    /// keybindings are active.
    pub view: View,
}

/// Top-level view dispatcher. Phase 4 had only the jobs list;
/// Phase 5 adds a per-job detail view with five tabs.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum View {
    List,
    Detail {
        /// Job currently being inspected. The render layer fetches
        /// the [`Job`] from the snapshot on every draw, so the
        /// detail view tracks live changes automatically.
        job_id: JobId,
        /// Which tab is currently active. `Tab` cycle methods are
        /// hooked to Tab / Shift-Tab in the event loop.
        tab: Tab,
    },
}

impl Default for View {
    fn default() -> Self {
        Self::List
    }
}

/// Five-tab dispatcher for the per-job detail view.
///
/// Order is the on-screen order in the tab bar; `cycle_next` /
/// `cycle_prev` walk this sequence with wrap-around.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Tab {
    Overview,
    Workers,
    Errors,
    Plan,
    Verify,
}

impl Tab {
    pub fn all() -> [Tab; 5] {
        [
            Tab::Overview,
            Tab::Workers,
            Tab::Errors,
            Tab::Plan,
            Tab::Verify,
        ]
    }

    /// Short label rendered in the tab bar.
    pub fn label(self) -> &'static str {
        match self {
            Tab::Overview => "Overview",
            Tab::Workers => "Workers",
            Tab::Errors => "Errors",
            Tab::Plan => "Plan",
            Tab::Verify => "Verify",
        }
    }

    /// Cycle to the next tab (wraps from Verify back to Overview).
    /// Hooked to the Tab key.
    pub fn cycle_next(self) -> Self {
        match self {
            Tab::Overview => Tab::Workers,
            Tab::Workers => Tab::Errors,
            Tab::Errors => Tab::Plan,
            Tab::Plan => Tab::Verify,
            Tab::Verify => Tab::Overview,
        }
    }

    /// Cycle to the previous tab (wraps from Overview back to
    /// Verify). Hooked to Shift-Tab / BackTab.
    pub fn cycle_prev(self) -> Self {
        match self {
            Tab::Overview => Tab::Verify,
            Tab::Workers => Tab::Overview,
            Tab::Errors => Tab::Workers,
            Tab::Plan => Tab::Errors,
            Tab::Verify => Tab::Plan,
        }
    }
}

/// Modal dispatcher for keypresses. Normal = jobs-list navigation;
/// Filter = capturing typed characters into `filter`. The render
/// layer reads this to know whether to show the live buffer (with
/// a cursor glyph) in the banner.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum InputMode {
    Normal,
    Filter {
        /// Live edit buffer; appended on Char, popped on Backspace.
        /// The `UiState.filter` field is updated to match this on
        /// every keypress so the visible jobs list tracks the
        /// typed string in real time.
        buffer: String,
        /// Snapshot of `UiState.filter` from when the operator
        /// pressed `/`. Esc restores this; Enter discards it.
        prior: String,
    },
}

impl Default for InputMode {
    fn default() -> Self {
        Self::Normal
    }
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

impl JobSort {
    /// Cycle to the next sort criterion. Hooked to the `s` key.
    pub fn cycle(self) -> Self {
        match self {
            Self::ById => Self::ByPhase,
            Self::ByPhase => Self::ByProgressDesc,
            Self::ByProgressDesc => Self::ByErrorsDesc,
            Self::ByErrorsDesc => Self::ById,
        }
    }

    /// Single-word label shown in the banner so the operator can
    /// see at a glance which sort is active.
    pub fn label(self) -> &'static str {
        match self {
            Self::ById => "id",
            Self::ByPhase => "phase",
            Self::ByProgressDesc => "progress",
            Self::ByErrorsDesc => "errors",
        }
    }
}

// =============================================================================
// Rolling per-job throughput windows
// =============================================================================

/// Longest window the TUI tracks. Samples older than this are
/// pruned on every push so the buffer stays bounded.
const MAX_WINDOW_SECS: i64 = 5 * 60;

/// Per-job rolling samples of `ProgressDelta` events for client-
/// side throughput estimation. The TUI computes rolling 1s / 1m /
/// 5m windows from this rather than having the coord ship a wider
/// per-tick payload.
#[derive(Debug, Clone, Default)]
pub struct ProgressDeltaHistory {
    /// Bounded ring of `(wall_clock, bytes_delta)`. Sorted oldest-
    /// first so pruning is a single `pop_front` per stale entry.
    samples: VecDeque<(DateTime<Utc>, u64)>,
}

impl ProgressDeltaHistory {
    /// Record one ProgressDelta. Prunes anything older than the
    /// longest window the TUI maintains.
    pub fn push(&mut self, at: DateTime<Utc>, bytes_delta: u64) {
        let cutoff = at - ChronoDuration::seconds(MAX_WINDOW_SECS);
        while let Some(&(t, _)) = self.samples.front() {
            if t < cutoff {
                self.samples.pop_front();
            } else {
                break;
            }
        }
        self.samples.push_back((at, bytes_delta));
    }

    /// Bytes-per-second over the most recent `window_secs`. Returns
    /// 0.0 when the window is empty (no samples yet, or all pruned
    /// because the worker stopped emitting).
    pub fn bytes_per_sec(&self, window_secs: i64, now: DateTime<Utc>) -> f64 {
        if window_secs <= 0 {
            return 0.0;
        }
        let cutoff = now - ChronoDuration::seconds(window_secs);
        let total: u64 = self
            .samples
            .iter()
            .filter(|(t, _)| *t >= cutoff)
            .map(|(_, b)| *b)
            .sum();
        total as f64 / window_secs as f64
    }

    /// Sample count (mostly for tests and diagnostics).
    pub fn len(&self) -> usize {
        self.samples.len()
    }

    pub fn is_empty(&self) -> bool {
        self.samples.is_empty()
    }
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
    /// Per-job rolling throughput history derived from
    /// `ProgressDelta` events. Pruned to 5 min on every push.
    pub progress_windows: HashMap<JobId, ProgressDeltaHistory>,
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
            progress_windows: HashMap::new(),
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
        // Side-effect: feed the per-job throughput window if this
        // was a ProgressDelta. Pruning happens inside `push` so the
        // sample VecDeque stays bounded by the 5-min window.
        if let EventKind::ProgressDelta {
            job_id,
            bytes_delta,
            ..
        } = &envelope.kind
        {
            self.progress_windows
                .entry(job_id.clone())
                .or_default()
                .push(envelope.at, *bytes_delta);
        }
        true
    }

    /// Total bytes-per-second across all jobs over the given
    /// rolling `window_secs`. Used by the banner's aggregate line.
    pub fn total_bytes_per_sec(&self, window_secs: i64, now: DateTime<Utc>) -> f64 {
        self.progress_windows
            .values()
            .map(|h| h.bytes_per_sec(window_secs, now))
            .sum()
    }

    /// Per-job bytes-per-second over `window_secs`. None means no
    /// samples yet (most likely a job that hasn't started
    /// transferring; the render layer shows ` -- ` instead of 0
    /// for visual clarity).
    pub fn job_bytes_per_sec(
        &self,
        id: &JobId,
        window_secs: i64,
        now: DateTime<Utc>,
    ) -> Option<f64> {
        self.progress_windows
            .get(id)
            .filter(|h| !h.is_empty())
            .map(|h| h.bytes_per_sec(window_secs, now))
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

    // ----- JobSort cycle + label -----

    #[test]
    fn jobsort_cycle_visits_all_four_then_wraps() {
        let order = [
            JobSort::ById,
            JobSort::ByPhase,
            JobSort::ByProgressDesc,
            JobSort::ByErrorsDesc,
        ];
        let mut cur = order[0];
        for next in order.iter().skip(1) {
            cur = cur.cycle();
            assert_eq!(cur, *next);
        }
        cur = cur.cycle();
        assert_eq!(cur, JobSort::ById, "must wrap from last back to first");
    }

    #[test]
    fn jobsort_labels_are_distinct_single_words() {
        let labels = [
            JobSort::ById.label(),
            JobSort::ByPhase.label(),
            JobSort::ByProgressDesc.label(),
            JobSort::ByErrorsDesc.label(),
        ];
        // No duplicates.
        let mut sorted: Vec<&str> = labels.to_vec();
        sorted.sort();
        sorted.dedup();
        assert_eq!(sorted.len(), 4);
        // Each fits within ~10 characters for the banner.
        for l in labels {
            assert!(l.len() <= 10, "label too long: {l:?}");
            assert!(!l.contains(' '), "label has spaces: {l:?}");
        }
    }

    // ----- ProgressDeltaHistory + AppState wiring -----

    fn prog_at(seq: u64, secs: i64, job: &str, bytes: u64) -> EventEnvelope {
        EventEnvelope {
            seq,
            at: at(secs),
            schema_version: migration_coord::schema::SCHEMA_VERSION,
            worker_at: None,
            kind: EventKind::ProgressDelta {
                job_id: jid(job),
                worker_id: WorkerId::new(),
                files_delta: 1,
                bytes_delta: bytes,
                errors_delta: 0,
            },
        }
    }

    #[test]
    fn history_push_then_bytes_per_sec_sums_within_window() {
        let mut h = ProgressDeltaHistory::default();
        h.push(at(0), 1000);
        h.push(at(1), 2000);
        h.push(at(2), 3000);
        // 6000 bytes over the last 10 seconds → 600 B/s.
        assert!((h.bytes_per_sec(10, at(10)) - 600.0).abs() < 0.0001);
        // Inclusive window: at now=at(2), the 2-second window is
        // [at(0), at(2)] — all three samples fall in it. 6000 B
        // over 2 s = 3000 B/s.
        let v = h.bytes_per_sec(2, at(2));
        assert!((v - 3000.0).abs() < 0.0001, "got {v}");
        // A tighter 1-second window at now=at(2) includes only
        // samples at t ≥ at(1): 2000 + 3000 = 5000 over 1 s.
        let v = h.bytes_per_sec(1, at(2));
        assert!((v - 5000.0).abs() < 0.0001, "got {v}");
    }

    #[test]
    fn history_prunes_entries_older_than_max_window() {
        let mut h = ProgressDeltaHistory::default();
        // First entry far in the past.
        h.push(at(0), 100);
        // Push enough later that the first is dropped.
        h.push(at(MAX_WINDOW_SECS + 1), 200);
        assert_eq!(h.len(), 1, "history pruned to 1 entry");
        // Only the surviving entry contributes to the window.
        assert!((h.bytes_per_sec(10, at(MAX_WINDOW_SECS + 1)) - 20.0).abs() < 0.0001);
    }

    #[test]
    fn history_empty_window_yields_zero() {
        let h = ProgressDeltaHistory::default();
        assert_eq!(h.bytes_per_sec(10, at(0)), 0.0);
        assert_eq!(h.bytes_per_sec(60, at(0)), 0.0);
    }

    #[test]
    fn history_window_secs_zero_or_negative_yields_zero() {
        let mut h = ProgressDeltaHistory::default();
        h.push(at(0), 1000);
        assert_eq!(h.bytes_per_sec(0, at(0)), 0.0);
        assert_eq!(h.bytes_per_sec(-5, at(0)), 0.0);
    }

    #[test]
    fn appstate_apply_envelope_feeds_progress_window() {
        let mut s = AppState::empty(at(0));
        s.apply_envelope(&job_created(1, "bobby"));
        // No progress yet — no window for this job.
        assert!(s.progress_windows.get(&jid("bobby")).is_none());
        s.apply_envelope(&prog_at(2, 1, "bobby", 1024));
        s.apply_envelope(&prog_at(3, 2, "bobby", 2048));
        // Window now exists with 2 samples, totaling 3072 bytes.
        let h = s.progress_windows.get(&jid("bobby")).expect("window");
        assert_eq!(h.len(), 2);
        let rate = s.job_bytes_per_sec(&jid("bobby"), 10, at(10));
        assert!(rate.is_some());
        assert!((rate.unwrap() - 307.2).abs() < 0.01);
    }

    #[test]
    fn appstate_duplicate_envelope_does_not_double_count_window() {
        let mut s = AppState::empty(at(0));
        s.apply_envelope(&job_created(1, "bobby"));
        s.apply_envelope(&prog_at(2, 1, "bobby", 1024));
        // Re-applying same seq must be dropped at apply_envelope
        // level — window must not gain a second sample.
        s.apply_envelope(&prog_at(2, 1, "bobby", 1024));
        let h = s.progress_windows.get(&jid("bobby")).expect("window");
        assert_eq!(h.len(), 1);
    }

    #[test]
    fn appstate_total_bytes_per_sec_sums_across_jobs() {
        let mut s = AppState::empty(at(0));
        s.apply_envelope(&job_created(1, "alpha"));
        s.apply_envelope(&job_created(2, "bravo"));
        s.apply_envelope(&prog_at(3, 0, "alpha", 1000));
        s.apply_envelope(&prog_at(4, 0, "bravo", 2000));
        // Over the last 10 seconds, alpha=100, bravo=200 → total=300.
        let total = s.total_bytes_per_sec(10, at(10));
        assert!((total - 300.0).abs() < 0.0001, "got {total}");
    }

    // ----- InputMode default -----

    #[test]
    fn default_input_mode_is_normal() {
        let ui = UiState::default();
        assert!(matches!(ui.input_mode, InputMode::Normal));
    }

    // ----- View / Tab (Phase 5a) -----

    #[test]
    fn default_view_is_list() {
        let ui = UiState::default();
        assert!(matches!(ui.view, View::List));
    }

    #[test]
    fn tab_cycle_next_walks_all_five_then_wraps() {
        let order = [
            Tab::Overview,
            Tab::Workers,
            Tab::Errors,
            Tab::Plan,
            Tab::Verify,
        ];
        let mut cur = order[0];
        for next in order.iter().skip(1) {
            cur = cur.cycle_next();
            assert_eq!(cur, *next);
        }
        cur = cur.cycle_next();
        assert_eq!(cur, Tab::Overview, "Verify must wrap to Overview");
    }

    #[test]
    fn tab_cycle_prev_walks_all_five_in_reverse_and_wraps() {
        let mut cur = Tab::Overview;
        let reverse = [
            Tab::Verify,
            Tab::Plan,
            Tab::Errors,
            Tab::Workers,
            Tab::Overview,
        ];
        for expected in reverse {
            cur = cur.cycle_prev();
            assert_eq!(cur, expected);
        }
    }

    #[test]
    fn tab_labels_are_distinct_and_human_readable() {
        let labels: Vec<&'static str> = Tab::all().iter().map(|t| t.label()).collect();
        let mut sorted = labels.clone();
        sorted.sort();
        sorted.dedup();
        assert_eq!(sorted.len(), 5, "labels must be unique");
        for l in labels {
            assert!(!l.is_empty());
            // Tab bar real estate is tight — keep labels ≤ 10 chars.
            assert!(l.len() <= 10, "label too long: {l:?}");
        }
    }

    #[test]
    fn tab_all_returns_canonical_order() {
        let all = Tab::all();
        assert_eq!(
            all,
            [
                Tab::Overview,
                Tab::Workers,
                Tab::Errors,
                Tab::Plan,
                Tab::Verify,
            ]
        );
    }
}
