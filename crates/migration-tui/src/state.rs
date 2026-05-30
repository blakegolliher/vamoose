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
    ErrorBucket, ErrorClass, EventEnvelope, EventKind, Job, JobId, Snapshot, Worker, WorkerId,
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
    /// Sort criterion for the Workers tab of the detail view.
    /// `s` cycles this when the active tab is Workers (List view's
    /// `s` continues to cycle `sort` instead — different tabs,
    /// different sorts).
    pub worker_sort: WorkerSort,
    /// Selected worker id in the Workers tab. Independent of
    /// `selected_job` so leaving and re-entering Detail preserves
    /// each cursor.
    pub selected_worker: Option<WorkerId>,
    /// Active modal overlay, if any. When `Some`, the event loop
    /// intercepts input (Esc closes, q quits, else no-op) and the
    /// render layer paints the modal over whatever's underneath.
    pub modal: Option<Modal>,
}

/// Modal overlay variants.
///
/// - `WorkerDetail` — drill-down invoked from the Workers tab.
/// - `ConfirmCommand` — y/n gate that the palette injects in front
///   of destructive verbs (cancel, drain, retry-failed). On `y`
///   the event loop dispatches the command; on `n`/Esc it closes
///   without sending anything.
/// - `Help` — reference card listing every keybinding. Opened with
///   `?` from any Normal-mode view; closed with Esc.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Modal {
    WorkerDetail {
        worker_id: WorkerId,
    },
    ConfirmCommand {
        /// The fully-parsed command sitting behind the prompt.
        /// Reusing [`crate::palette::PaletteCommand`] keeps the
        /// modal and the actual dispatch path looking at the same
        /// value — no risk of "the modal said X but Enter ran Y".
        command: crate::palette::PaletteCommand,
        /// Human-readable summary line — what the operator sees in
        /// the modal title. The renderer derives this from
        /// `command` at construction time so subsequent state
        /// transitions can't desync the title from the action.
        summary: String,
    },
    Help,
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

/// Modal dispatcher for keypresses.
///
/// - `Normal` — jobs-list navigation (or detail-view nav).
/// - `Filter` — capturing typed characters into `UiState.filter`
///   (entered via `/` from List view).
/// - `Palette` — command palette (entered via `:`). The buffer
///   feeds [`crate::palette::parse`] on Enter.
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
    /// Command-palette input. Captures whatever the operator types
    /// after `:`. Enter parses and dispatches (with confirm-modal
    /// gating for destructive verbs); Esc closes without action.
    Palette {
        /// Live edit buffer. The render layer shows it on the
        /// bottom-hints row with a cursor glyph and the current
        /// completion in dim text.
        buffer: String,
        /// Cycle index into the most recent
        /// [`crate::palette::complete`] suggestion list. Tab
        /// advances; the renderer surfaces `suggestions[idx]` as
        /// the ghost-text completion.
        completion_idx: usize,
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

/// Sort criterion for the Workers tab of the detail view.
///
/// Default is `ByMbpsDesc` — operators ranking workers usually
/// want the throughput leaderboard up top so a degraded node is
/// visible at a glance (it sinks to the bottom).
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum WorkerSort {
    /// Highest bytes-per-second first. Default.
    #[default]
    ByMbpsDesc,
    /// Highest files-per-second first.
    ByFilesDesc,
    /// Highest errors-per-minute first — surface degraded workers.
    ByErrorsDesc,
    /// Alphabetical by host. Stable fallback.
    ByHost,
}

impl WorkerSort {
    pub fn cycle(self) -> Self {
        match self {
            Self::ByMbpsDesc => Self::ByFilesDesc,
            Self::ByFilesDesc => Self::ByErrorsDesc,
            Self::ByErrorsDesc => Self::ByHost,
            Self::ByHost => Self::ByMbpsDesc,
        }
    }

    pub fn label(self) -> &'static str {
        match self {
            Self::ByMbpsDesc => "mb/s",
            Self::ByFilesDesc => "files/s",
            Self::ByErrorsDesc => "errors",
            Self::ByHost => "host",
        }
    }
}

// =============================================================================
// Per-job recent-errors tail
// =============================================================================

/// How many recent errors per job the TUI keeps. The coord's
/// [`ErrorBucket`] aggregation captures totals + sample paths, but
/// the Errors tab needs a chronological tail for the operator to
/// scan — and the tail can't be reconstructed from the bucket
/// (`sample_paths` is unordered, deduped, and capped). So the
/// client maintains its own.
pub const RECENT_ERRORS_PER_JOB: usize = 50;

/// One captured `ErrorEmitted` event, materialized client-side so
/// the Errors tab's recent tail has the full payload without
/// re-querying the coord.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RecentError {
    pub at: DateTime<Utc>,
    pub worker_id: WorkerId,
    pub class: ErrorClass,
    pub path: String,
    pub retryable: bool,
    pub message: String,
}

/// Bounded ring of [`RecentError`]s, oldest first. Pushes that
/// would exceed [`RECENT_ERRORS_PER_JOB`] drop the head.
#[derive(Debug, Clone, Default)]
pub struct RecentErrors {
    entries: VecDeque<RecentError>,
}

/// One captured `VerifyFileMismatch` event. The coord doesn't keep
/// these on the snapshot (Phase 1 reducer just streams them) — the
/// TUI keeps a chronological ring so the Verify tab can show the
/// recent picture.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RecentVerifyMismatch {
    pub at: DateTime<Utc>,
    pub path: String,
    pub expected: String,
    pub got: String,
}

/// Per-job verify mismatches ring. Same bounded shape as
/// [`RecentErrors`]; oldest pushed out at the cap.
pub const RECENT_VERIFY_MISMATCHES_PER_JOB: usize = 50;

#[derive(Debug, Clone, Default)]
pub struct RecentVerifyMismatches {
    entries: VecDeque<RecentVerifyMismatch>,
}

impl RecentVerifyMismatches {
    pub fn push(&mut self, e: RecentVerifyMismatch) {
        if self.entries.len() >= RECENT_VERIFY_MISMATCHES_PER_JOB {
            self.entries.pop_front();
        }
        self.entries.push_back(e);
    }
    pub fn tail(&self, n: usize) -> impl Iterator<Item = &RecentVerifyMismatch> {
        let skip = self.entries.len().saturating_sub(n);
        self.entries.iter().skip(skip)
    }
    pub fn len(&self) -> usize {
        self.entries.len()
    }
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }
}

/// Last observed verify lifecycle for a job. The coord drives the
/// phase transition via `VerifyCompleted`'s mismatch count but
/// doesn't keep the timestamps + counts on the snapshot — those
/// are captured here so the Verify tab can show "started Xs ago,
/// completed with N mismatches".
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct VerifyStatus {
    pub last_started: Option<DateTime<Utc>>,
    pub last_completed: Option<DateTime<Utc>>,
    pub last_mismatches: Option<u64>,
}

impl RecentErrors {
    pub fn push(&mut self, e: RecentError) {
        if self.entries.len() >= RECENT_ERRORS_PER_JOB {
            self.entries.pop_front();
        }
        self.entries.push_back(e);
    }

    /// Tail of `n` most-recent entries, newest LAST (matches the
    /// internal order so callers can iterate normally and have the
    /// most recent at the bottom of the table).
    pub fn tail(&self, n: usize) -> impl Iterator<Item = &RecentError> {
        let skip = self.entries.len().saturating_sub(n);
        self.entries.iter().skip(skip)
    }

    pub fn len(&self) -> usize {
        self.entries.len()
    }

    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
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
    /// Per-job recent-errors tail. The coord aggregates totals into
    /// `ErrorBucket`s on the snapshot; the TUI keeps a chronological
    /// ring so the Errors tab can show recent activity.
    pub recent_errors: HashMap<JobId, RecentErrors>,
    /// Per-job tail of `VerifyFileMismatch` events. The coord's
    /// Phase 1 reducer just streams these; the TUI captures them so
    /// the Verify tab has a recent-activity table.
    pub recent_verify_mismatches: HashMap<JobId, RecentVerifyMismatches>,
    /// Per-job verify lifecycle status (last start / complete / count).
    pub verify_status: HashMap<JobId, VerifyStatus>,
    /// Most recent palette-command result. Shown as a banner toast
    /// for a few seconds, then auto-cleared by the render layer
    /// (the renderer checks `now - at >= COMMAND_STATUS_TTL` and
    /// drops the field on render).
    pub command_status: Option<CommandStatus>,
}

/// Toast surfaced in the top banner after a palette command runs.
///
/// `kind` selects the color (green ok / red error). The render
/// layer also displays a short message; on auto-clear it's wiped
/// from `AppState.command_status` at the next render tick.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CommandStatus {
    pub kind: CommandStatusKind,
    pub message: String,
    pub at: DateTime<Utc>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CommandStatusKind {
    Ok,
    Error,
}

/// How long a command-status toast stays on screen before the
/// renderer auto-clears it. 5 seconds is long enough for the
/// operator to read but short enough that it doesn't linger past
/// the next likely command.
pub const COMMAND_STATUS_TTL_SECS: i64 = 5;

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
            recent_errors: HashMap::new(),
            recent_verify_mismatches: HashMap::new(),
            verify_status: HashMap::new(),
            command_status: None,
        }
    }

    /// Record a successful command-status toast. The renderer
    /// auto-clears after [`COMMAND_STATUS_TTL_SECS`] seconds.
    pub fn set_command_ok(&mut self, message: impl Into<String>, now: DateTime<Utc>) {
        self.command_status = Some(CommandStatus {
            kind: CommandStatusKind::Ok,
            message: message.into(),
            at: now,
        });
    }

    /// Record an error command-status toast.
    pub fn set_command_error(&mut self, message: impl Into<String>, now: DateTime<Utc>) {
        self.command_status = Some(CommandStatus {
            kind: CommandStatusKind::Error,
            message: message.into(),
            at: now,
        });
    }

    /// Called by the render layer before drawing the banner. Drops
    /// the toast when it has been on screen longer than the TTL.
    pub fn tick_command_status(&mut self, now: DateTime<Utc>) {
        if let Some(s) = &self.command_status {
            let elapsed = now.signed_duration_since(s.at).num_seconds();
            if elapsed >= COMMAND_STATUS_TTL_SECS {
                self.command_status = None;
            }
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
        // Side-effect: append to the per-job recent-errors ring when
        // this was an `ErrorEmitted`. The dedup is implicit — we
        // only reach this branch after the early-return seq guard,
        // so a replayed event will never double-push.
        if let EventKind::ErrorEmitted {
            job_id,
            worker_id,
            class,
            path,
            retryable,
            message,
        } = &envelope.kind
        {
            self.recent_errors
                .entry(job_id.clone())
                .or_default()
                .push(RecentError {
                    at: envelope.at,
                    worker_id: *worker_id,
                    class: class.clone(),
                    path: path.clone(),
                    retryable: *retryable,
                    message: message.clone(),
                });
        }
        // Verify lifecycle bookkeeping. The coord drives phase
        // transitions but doesn't keep timestamps / counts on the
        // snapshot — those are captured here so the Verify tab can
        // narrate "started Xs ago, completed with N mismatches".
        match &envelope.kind {
            EventKind::VerifyStarted { job_id } => {
                self.verify_status
                    .entry(job_id.clone())
                    .or_default()
                    .last_started = Some(envelope.at);
            }
            EventKind::VerifyCompleted { job_id, mismatches } => {
                let s = self.verify_status.entry(job_id.clone()).or_default();
                s.last_completed = Some(envelope.at);
                s.last_mismatches = Some(*mismatches);
            }
            EventKind::VerifyFileMismatch {
                job_id,
                path,
                expected,
                got,
            } => {
                self.recent_verify_mismatches
                    .entry(job_id.clone())
                    .or_default()
                    .push(RecentVerifyMismatch {
                        at: envelope.at,
                        path: path.clone(),
                        expected: expected.clone(),
                        got: got.clone(),
                    });
            }
            _ => {}
        }
        true
    }

    /// Per-job recent-errors ring. Returns `None` for a job that has
    /// never emitted an error.
    pub fn recent_errors_for_job(&self, id: &JobId) -> Option<&RecentErrors> {
        self.recent_errors.get(id)
    }

    /// Per-job recent verify mismatches ring. `None` when no
    /// VerifyFileMismatch has streamed for the job yet.
    pub fn recent_verify_mismatches_for_job(&self, id: &JobId) -> Option<&RecentVerifyMismatches> {
        self.recent_verify_mismatches.get(id)
    }

    /// Per-job verify lifecycle status (last start, last complete,
    /// mismatch count from the last completion). Returns a zero-
    /// initialized `VerifyStatus` when no verify events have
    /// touched the job — the renderer treats absence and
    /// all-None the same.
    pub fn verify_status_for_job(&self, id: &JobId) -> VerifyStatus {
        self.verify_status.get(id).cloned().unwrap_or_default()
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

    // ----- WorkerSort (Phase 5b) -----

    #[test]
    fn worker_sort_default_is_mbps_desc() {
        // Operator-leaderboard default — degraded nodes sink so a
        // quick glance at the Workers tab surfaces the laggard.
        let s = WorkerSort::default();
        assert_eq!(s, WorkerSort::ByMbpsDesc);
    }

    #[test]
    fn worker_sort_cycle_walks_all_four_then_wraps() {
        let order = [
            WorkerSort::ByMbpsDesc,
            WorkerSort::ByFilesDesc,
            WorkerSort::ByErrorsDesc,
            WorkerSort::ByHost,
        ];
        let mut cur = order[0];
        for next in order.iter().skip(1) {
            cur = cur.cycle();
            assert_eq!(cur, *next);
        }
        cur = cur.cycle();
        assert_eq!(cur, WorkerSort::ByMbpsDesc, "must wrap");
    }

    #[test]
    fn worker_sort_labels_are_distinct_and_short() {
        let labels: Vec<&'static str> = [
            WorkerSort::ByMbpsDesc.label(),
            WorkerSort::ByFilesDesc.label(),
            WorkerSort::ByErrorsDesc.label(),
            WorkerSort::ByHost.label(),
        ]
        .to_vec();
        let mut sorted = labels.clone();
        sorted.sort();
        sorted.dedup();
        assert_eq!(sorted.len(), 4);
        for l in labels {
            assert!(l.len() <= 10);
            assert!(!l.is_empty());
        }
    }

    #[test]
    fn modal_default_is_none() {
        let ui = UiState::default();
        assert!(ui.modal.is_none());
    }

    // ----- RecentErrors ring (Phase 5c) -----

    fn err_evt(seq: u64, job: &str, path: &str, message: &str) -> EventEnvelope {
        EventEnvelope {
            seq,
            at: at(seq as i64),
            schema_version: migration_coord::schema::SCHEMA_VERSION,
            worker_at: None,
            kind: EventKind::ErrorEmitted {
                job_id: jid(job),
                worker_id: WorkerId::new(),
                class: ErrorClass::Permission,
                path: path.into(),
                retryable: false,
                message: message.into(),
            },
        }
    }

    #[test]
    fn recent_errors_push_caps_at_limit() {
        let mut r = RecentErrors::default();
        for i in 0..(RECENT_ERRORS_PER_JOB + 10) {
            r.push(RecentError {
                at: at(i as i64),
                worker_id: WorkerId::new(),
                class: ErrorClass::Other(format!("k{i}")),
                path: format!("/p/{i}"),
                retryable: false,
                message: format!("m{i}"),
            });
        }
        assert_eq!(r.len(), RECENT_ERRORS_PER_JOB);
        // Oldest (entries 0..9) should have dropped; the FIRST item
        // in the tail is now entry 10.
        let tail: Vec<_> = r.tail(usize::MAX).collect();
        assert_eq!(tail.first().unwrap().path, "/p/10");
        assert_eq!(
            tail.last().unwrap().path,
            format!("/p/{}", RECENT_ERRORS_PER_JOB + 9)
        );
    }

    #[test]
    fn recent_errors_tail_n_returns_last_n_newest_last() {
        let mut r = RecentErrors::default();
        for i in 0..5 {
            r.push(RecentError {
                at: at(i),
                worker_id: WorkerId::new(),
                class: ErrorClass::Permission,
                path: format!("/p/{i}"),
                retryable: false,
                message: "m".into(),
            });
        }
        let last3: Vec<_> = r.tail(3).collect();
        assert_eq!(last3.len(), 3);
        assert_eq!(last3[0].path, "/p/2");
        assert_eq!(last3[2].path, "/p/4");
    }

    #[test]
    fn appstate_apply_envelope_feeds_recent_errors() {
        let mut s = AppState::empty(at(0));
        s.apply_envelope(&job_created(1, "alpha"));
        s.apply_envelope(&err_evt(2, "alpha", "/path/a", "perm denied"));
        s.apply_envelope(&err_evt(3, "alpha", "/path/b", "another"));
        let r = s.recent_errors_for_job(&jid("alpha")).expect("ring");
        assert_eq!(r.len(), 2);
        let tail: Vec<_> = r.tail(usize::MAX).collect();
        assert_eq!(tail[0].path, "/path/a");
        assert_eq!(tail[0].message, "perm denied");
        assert_eq!(tail[1].path, "/path/b");
    }

    #[test]
    fn appstate_duplicate_envelope_does_not_double_push_recent_errors() {
        let mut s = AppState::empty(at(0));
        s.apply_envelope(&job_created(1, "alpha"));
        s.apply_envelope(&err_evt(2, "alpha", "/p/x", "boom"));
        // Replay same seq — dedup at apply_envelope blocks it.
        s.apply_envelope(&err_evt(2, "alpha", "/p/x", "boom"));
        let r = s.recent_errors_for_job(&jid("alpha")).expect("ring");
        assert_eq!(r.len(), 1);
    }

    #[test]
    fn appstate_recent_errors_for_job_returns_none_when_unseen() {
        let s = AppState::empty(at(0));
        assert!(s.recent_errors_for_job(&jid("alpha")).is_none());
    }

    // ----- Verify lifecycle + mismatches ring (Phase 5d) -----

    fn verify_mismatch_evt(
        seq: u64,
        secs: i64,
        job: &str,
        path: &str,
        expected: &str,
        got: &str,
    ) -> EventEnvelope {
        EventEnvelope {
            seq,
            at: at(secs),
            schema_version: migration_coord::schema::SCHEMA_VERSION,
            worker_at: None,
            kind: EventKind::VerifyFileMismatch {
                job_id: jid(job),
                path: path.into(),
                expected: expected.into(),
                got: got.into(),
            },
        }
    }

    #[test]
    fn appstate_verify_started_then_completed_records_timestamps_and_count() {
        let mut s = AppState::empty(at(0));
        s.apply_envelope(&job_created(1, "alpha"));
        s.apply_envelope(&EventEnvelope {
            seq: 2,
            at: at(100),
            schema_version: migration_coord::schema::SCHEMA_VERSION,
            worker_at: None,
            kind: EventKind::VerifyStarted {
                job_id: jid("alpha"),
            },
        });
        s.apply_envelope(&EventEnvelope {
            seq: 3,
            at: at(200),
            schema_version: migration_coord::schema::SCHEMA_VERSION,
            worker_at: None,
            kind: EventKind::VerifyCompleted {
                job_id: jid("alpha"),
                mismatches: 7,
            },
        });
        let st = s.verify_status_for_job(&jid("alpha"));
        assert_eq!(st.last_started, Some(at(100)));
        assert_eq!(st.last_completed, Some(at(200)));
        assert_eq!(st.last_mismatches, Some(7));
    }

    #[test]
    fn appstate_verify_mismatch_events_feed_ring() {
        let mut s = AppState::empty(at(0));
        s.apply_envelope(&job_created(1, "alpha"));
        s.apply_envelope(&verify_mismatch_evt(
            2, 10, "alpha", "/p/a", "size:100", "size:101",
        ));
        s.apply_envelope(&verify_mismatch_evt(
            3, 20, "alpha", "/p/b", "size:200", "size:0",
        ));
        let r = s
            .recent_verify_mismatches_for_job(&jid("alpha"))
            .expect("ring");
        assert_eq!(r.len(), 2);
        let tail: Vec<_> = r.tail(usize::MAX).collect();
        assert_eq!(tail[0].path, "/p/a");
        assert_eq!(tail[0].expected, "size:100");
        assert_eq!(tail[1].path, "/p/b");
        assert_eq!(tail[1].got, "size:0");
    }

    #[test]
    fn recent_verify_mismatches_ring_caps_at_limit() {
        let mut r = RecentVerifyMismatches::default();
        for i in 0..(RECENT_VERIFY_MISMATCHES_PER_JOB + 5) {
            r.push(RecentVerifyMismatch {
                at: at(i as i64),
                path: format!("/p/{i}"),
                expected: "x".into(),
                got: "y".into(),
            });
        }
        assert_eq!(r.len(), RECENT_VERIFY_MISMATCHES_PER_JOB);
        // Oldest 5 dropped → first surviving entry is index 5.
        let tail: Vec<_> = r.tail(usize::MAX).collect();
        assert_eq!(tail.first().unwrap().path, "/p/5");
    }

    #[test]
    fn verify_status_for_unseen_job_is_all_none() {
        let s = AppState::empty(at(0));
        let st = s.verify_status_for_job(&jid("alpha"));
        assert_eq!(st, VerifyStatus::default());
    }

    #[test]
    fn duplicate_verify_envelope_does_not_double_push_ring() {
        let mut s = AppState::empty(at(0));
        s.apply_envelope(&job_created(1, "alpha"));
        s.apply_envelope(&verify_mismatch_evt(2, 10, "alpha", "/p/x", "e", "g"));
        // Replay same seq.
        s.apply_envelope(&verify_mismatch_evt(2, 10, "alpha", "/p/x", "e", "g"));
        let r = s
            .recent_verify_mismatches_for_job(&jid("alpha"))
            .expect("ring");
        assert_eq!(r.len(), 1);
    }

    // ----- Command status toast (Phase 6a) -----

    #[test]
    fn set_command_ok_records_a_green_toast() {
        let mut s = AppState::empty(at(0));
        s.set_command_ok("pause 'alpha' ok", at(100));
        let cs = s.command_status.expect("set");
        assert_eq!(cs.kind, CommandStatusKind::Ok);
        assert_eq!(cs.message, "pause 'alpha' ok");
        assert_eq!(cs.at, at(100));
    }

    #[test]
    fn set_command_error_records_a_red_toast() {
        let mut s = AppState::empty(at(0));
        s.set_command_error("pause failed: 401", at(100));
        let cs = s.command_status.expect("set");
        assert_eq!(cs.kind, CommandStatusKind::Error);
    }

    #[test]
    fn tick_command_status_clears_after_ttl() {
        let mut s = AppState::empty(at(0));
        s.set_command_ok("ok", at(100));
        // 1s after — still present.
        s.tick_command_status(at(101));
        assert!(s.command_status.is_some());
        // 5s exactly — TTL hit, cleared.
        s.tick_command_status(at(100 + COMMAND_STATUS_TTL_SECS));
        assert!(s.command_status.is_none());
    }

    #[test]
    fn tick_command_status_idempotent_when_unset() {
        let mut s = AppState::empty(at(0));
        s.tick_command_status(at(100));
        assert!(s.command_status.is_none());
    }
}
