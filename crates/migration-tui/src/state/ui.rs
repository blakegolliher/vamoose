use chrono::{DateTime, Utc};
use migration_control_protocol::schema::{JobId, WorkerId};

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
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub enum View {
    #[default]
    List,
    Detail {
        /// Job currently being inspected. The render layer fetches
        /// the [`migration_control_protocol::schema::Job`] from the snapshot on every draw, so the
        /// detail view tracks live changes automatically.
        job_id: JobId,
        /// Which tab is currently active. `Tab` cycle methods are
        /// hooked to Tab / Shift-Tab in the event loop.
        tab: Tab,
    },
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
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub enum InputMode {
    #[default]
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
