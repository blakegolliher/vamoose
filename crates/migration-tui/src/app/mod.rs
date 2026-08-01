//! Event loop + terminal lifecycle for the TUI.
//!
//! The loop is split into two layers so the interesting state
//! transitions can be unit-tested without standing up a terminal
//! or a real coord:
//!
//! - **Pure reducer** ([`Input`], [`AppAction`], [`handle_input`])
//!   — every state mutation goes through this. Tests drive it
//!   directly to verify keyboard handling, selection movement, and
//!   SSE-frame application.
//!
//! - **IO shell** ([`run`]) — sets up crossterm raw mode, spawns
//!   the SSE driver task with reconnect-and-backoff, drives a
//!   render tick at ≤ 20 fps, and feeds keyboard + SSE events into
//!   the reducer.
//!
//! State acquisition contract (COORD_PLAN §3.4, §3.7 P4): the
//! driver bootstraps over REST — `GET /healthz` for the resume
//! cursor, `GET /jobs` (+ per-job detail) for the data — before the
//! first `GET /stream`, and again whenever the coord emits `Resync`
//! (its per-subscriber bus overflowed and events were dropped that
//! will never be re-streamed). A seq-0 replay is NOT equivalent:
//! archived terminal jobs' event chunks are deleted from `events/`
//! (F23), so they only exist in the REST view. The reducer never
//! does I/O — [`handle_input`] returns [`AppAction::Resync`] and the
//! driver owns drop-stream → bootstrap → re-stream ([`sse_driver`]).

use crate::client::{Client, ClientError, SseFrame};
use crate::render;
use crate::state::{AppState, InputMode, Modal, Tab, View};
use chrono::{DateTime, Utc};
use crossterm::event::{KeyCode, KeyEvent, KeyEventKind};
use migration_control_protocol::schema::JobId;
use migration_control_protocol::schema::WorkerId;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{mpsc, Mutex};
use tokio_util::sync::CancellationToken;

// =============================================================================
// Pure-logic core — Input enum, handle_input, handle_key
// =============================================================================

/// Every event the loop reacts to. Funnelling SSE frames, key
/// presses, and the periodic tick through one enum lets the loop
/// be a single `select!` and the reducer a single `match`.
#[derive(Debug)]
pub enum Input {
    /// SSE driver successfully (re)connected — coord is live.
    SseConnected,
    /// SSE driver dropped — connection lost, driver will retry.
    /// `last_error` is operator-visible in the banner.
    SseDisconnected(String),
    /// Non-retryable failure (bad auth, etc). Driver has stopped.
    SseFatal(String),
    /// One frame from the SSE consumer. `Event` envelopes get
    /// applied via [`AppState::apply_envelope`]; `Keepalive` just
    /// refreshes the connection's `last_traffic` timestamp.
    SseFrame(SseFrame),
    /// Fresh REST snapshot from the driver's bootstrap path (initial
    /// connect and Resync recovery — F26). Applied via
    /// [`AppState::replace_snapshot`] so every state mutation stays
    /// on the reducer.
    Snapshot(Box<migration_control_protocol::schema::Snapshot>),
    /// Crossterm key event from the input task.
    Key(KeyEvent),
    /// Periodic render tick — used to advance elapsed-time labels
    /// in the banner even when no events are arriving.
    Tick,
    /// Result of a palette-dispatched command, surfaced as a
    /// banner toast. `ok=true` colors green; `ok=false` colors red.
    CommandResult { ok: bool, message: String },
}

/// Whether the loop should keep running after handling an input,
/// plus an `Execute` arm the main loop reacts to by spawning an
/// HTTP POST against the coord. The handler stays pure — the
/// network call lives in the event loop's `select!` branch.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AppAction {
    Continue,
    Quit,
    Execute(crate::palette::PaletteCommand),
    /// The coord requested a Resync (SSE bus overflow — dropped
    /// events will never be re-streamed). The reducer has flipped
    /// the banner; the caller that owns the stream must drop it,
    /// re-bootstrap over REST, and resume ([`sse_driver`] does this
    /// itself when it forwards the frame — the main loop treats
    /// this like `Continue`).
    Resync,
}

/// Apply one [`Input`] to `state`. The render layer treats `now`
/// as the current wall clock (passed in so tests are deterministic).
pub fn handle_input(state: &mut AppState, input: Input, now: DateTime<Utc>) -> AppAction {
    match input {
        Input::CommandResult { ok, message } => {
            if ok {
                state.set_command_ok(message, now);
            } else {
                state.set_command_error(message, now);
            }
            AppAction::Continue
        }
        Input::SseConnected => {
            state.mark_connected(now);
            AppAction::Continue
        }
        Input::SseDisconnected(err) => {
            state.mark_reconnecting(now, err);
            AppAction::Continue
        }
        Input::SseFatal(err) => {
            state.mark_disconnected(err);
            AppAction::Continue
        }
        Input::SseFrame(frame) => handle_sse_frame(state, frame, now),
        Input::Snapshot(snapshot) => {
            // Bootstrap / Resync recovery (F26): hard-replace the
            // derived state with the REST view; the resume cursor
            // only ever moves forward (replace_snapshot keeps
            // max(last_seen_seq, snapshot.last_seq)).
            state.replace_snapshot(*snapshot);
            auto_select_first_job(state);
            AppAction::Continue
        }
        Input::Key(key) => handle_key(state, key),
        Input::Tick => AppAction::Continue,
    }
}

/// Auto-select the first visible job once we have any — saves the
/// operator one keypress on first boot. Idempotent; no-op while a
/// selection exists.
fn auto_select_first_job(state: &mut AppState) {
    if state.ui.selected_job.is_none() {
        if let Some(j) = state
            .snapshot
            .jobs
            .values()
            .min_by_key(|j| j.id.as_str().to_string())
        {
            state.ui.selected_job = Some(j.id.clone());
        }
    }
}

fn handle_sse_frame(state: &mut AppState, frame: SseFrame, now: DateTime<Utc>) -> AppAction {
    match frame {
        SseFrame::Event { envelope, .. } => {
            state.apply_envelope(&envelope);
            state.mark_traffic(now);
            auto_select_first_job(state);
        }
        SseFrame::UnknownEvent { seq, kind } => {
            // F38: a coord newer than this build streamed a kind we
            // have no variant for. Skip it but advance the resume
            // cursor (dropping the frame without the advance would
            // replay it on every reconnect, forever) and count it
            // for the banner. The payload is unrecoverable — the
            // envelope can't deserialize past the unknown tag.
            if state.note_unknown_event(seq) {
                tracing::debug!(seq, kind = %kind, "skipped unknown event kind from newer coord");
            }
            state.mark_traffic(now);
        }
        SseFrame::Resync => {
            // Server told us our SSE subscriber overflowed and
            // dropped events — they will never be re-streamed
            // (COORD_PLAN §3.4). Flip the banner and demand a
            // driver-side recovery: drop the stream, re-fetch the
            // REST snapshot, resume from the new cursor. The
            // reducer does no I/O itself.
            state.mark_reconnecting(now, "server requested Resync — re-bootstrapping");
            return AppAction::Resync;
        }
        SseFrame::Keepalive => {
            state.mark_traffic(now);
        }
    }
    AppAction::Continue
}

fn handle_key(state: &mut AppState, key: KeyEvent) -> AppAction {
    // Ignore key-release events on platforms that emit them
    // (Windows). We only react to press / repeat.
    if matches!(key.kind, KeyEventKind::Release) {
        return AppAction::Continue;
    }
    // The confirm-command modal interceptor runs FIRST so y/n
    // don't fall through to the underlying view's bindings. The
    // worker-detail modal is handled later in handle_key_detail
    // because it's view-scoped.
    if let Some(Modal::ConfirmCommand { .. }) = &state.ui.modal {
        return handle_key_confirm_command(state, key);
    }
    // Help modal: same "intercept first" treatment so its Esc
    // closes itself and doesn't bubble up to "back / quit".
    if let Some(Modal::Help) = &state.ui.modal {
        return handle_key_help_modal(state, key);
    }
    match &state.ui.input_mode {
        InputMode::Normal => handle_key_normal(state, key),
        InputMode::Filter { .. } => handle_key_filter(state, key),
        InputMode::Palette { .. } => handle_key_palette(state, key),
    }
}

/// Confirm-command modal handler. Intercepts BEFORE view dispatch
/// so y/n can't accidentally activate underlying tab navigation.
fn handle_key_help_modal(state: &mut AppState, key: KeyEvent) -> AppAction {
    match key.code {
        // `q` still quits — emergency-exit ergonomics.
        KeyCode::Char('q') | KeyCode::Char('Q') => AppAction::Quit,
        KeyCode::Esc | KeyCode::Backspace | KeyCode::Char('?') => {
            state.ui.modal = None;
            AppAction::Continue
        }
        _ => AppAction::Continue,
    }
}

fn handle_key_confirm_command(state: &mut AppState, key: KeyEvent) -> AppAction {
    // Pull the command out so we can fire it on confirm without
    // borrowing state through the match guard.
    let cmd = if let Some(Modal::ConfirmCommand { command, .. }) = &state.ui.modal {
        command.clone()
    } else {
        return AppAction::Continue;
    };
    match key.code {
        KeyCode::Char('y') | KeyCode::Char('Y') | KeyCode::Enter => {
            state.ui.modal = None;
            AppAction::Execute(cmd)
        }
        KeyCode::Char('n') | KeyCode::Char('N') | KeyCode::Esc | KeyCode::Backspace => {
            state.ui.modal = None;
            AppAction::Continue
        }
        _ => AppAction::Continue,
    }
}

fn handle_key_palette(state: &mut AppState, key: KeyEvent) -> AppAction {
    // Take the mode out so we can mutate the buffer in place.
    let mode = std::mem::replace(&mut state.ui.input_mode, InputMode::Normal);
    let InputMode::Palette {
        mut buffer,
        mut completion_idx,
    } = mode
    else {
        return AppAction::Continue;
    };
    match key.code {
        KeyCode::Esc => {
            // Bail out — operator changed their mind. Drop the
            // mode and the buffer.
            AppAction::Continue
        }
        KeyCode::Enter => match parse_palette_buffer(state, &buffer) {
            Ok(cmd) => match cmd {
                crate::palette::PaletteCommand::Quit => AppAction::Quit,
                _ if cmd.is_destructive() => {
                    state.ui.modal = Some(Modal::ConfirmCommand {
                        summary: confirm_summary(&cmd),
                        command: cmd,
                    });
                    AppAction::Continue
                }
                _ => AppAction::Execute(cmd),
            },
            Err(err) => {
                // Surface the parse error as a toast; leave palette
                // closed so the operator can immediately retry.
                state.set_command_error(err.to_string(), chrono::Utc::now());
                AppAction::Continue
            }
        },
        KeyCode::Tab => {
            // Cycle through completion suggestions. First Tab on a
            // fresh buffer picks suggestion 0; subsequent Tabs
            // advance only when the buffer matches the previously-
            // selected suggestion (so typing after a Tab and
            // pressing Tab again starts a fresh round, not "skip
            // one").
            let suggestions = palette_suggestions(state, &buffer);
            if !suggestions.is_empty() {
                let current_idx = suggestions.iter().position(|s| s == &buffer);
                let next = match current_idx {
                    Some(i) => (i + 1) % suggestions.len(),
                    None => 0,
                };
                buffer = suggestions[next].clone();
                completion_idx = next;
            }
            state.ui.input_mode = InputMode::Palette {
                buffer,
                completion_idx,
            };
            AppAction::Continue
        }
        KeyCode::Backspace => {
            buffer.pop();
            completion_idx = 0;
            state.ui.input_mode = InputMode::Palette {
                buffer,
                completion_idx,
            };
            AppAction::Continue
        }
        KeyCode::Char(c) => {
            buffer.push(c);
            completion_idx = 0;
            state.ui.input_mode = InputMode::Palette {
                buffer,
                completion_idx,
            };
            AppAction::Continue
        }
        _ => {
            // Unknown key in palette mode — stay in mode, no edits.
            state.ui.input_mode = InputMode::Palette {
                buffer,
                completion_idx,
            };
            AppAction::Continue
        }
    }
}

fn parse_palette_buffer(
    state: &AppState,
    buffer: &str,
) -> Result<crate::palette::PaletteCommand, crate::palette::ParseError> {
    let default = default_palette_job_id(state);
    crate::palette::parse(buffer, default.as_ref())
}

fn palette_suggestions(state: &AppState, buffer: &str) -> Vec<String> {
    let ids: Vec<String> = state
        .snapshot
        .jobs
        .values()
        .map(|j| j.id.as_str().to_string())
        .collect();
    palette_tab_cycle_list(buffer, &ids)
}

/// Suggestion list for Tab cycling. Differs from
/// [`crate::palette::complete`] when the buffer is already a fully-
/// typed verb or "verb <full-job-id>": in those cases we return
/// the full verb / job list (Tab cycles through ALL of them) rather
/// than the single prefix match the strict completer would return.
fn palette_tab_cycle_list(buffer: &str, visible_job_ids: &[String]) -> Vec<String> {
    let space = buffer.find(char::is_whitespace);
    match space {
        None => {
            if crate::palette::VERBS.contains(&buffer) {
                crate::palette::VERBS
                    .iter()
                    .map(|v| v.to_string())
                    .collect()
            } else {
                crate::palette::complete(buffer, visible_job_ids)
            }
        }
        Some(idx) => {
            let verb = &buffer[..idx];
            let after = buffer[idx..].trim_start();
            if visible_job_ids.iter().any(|j| j == after) {
                visible_job_ids
                    .iter()
                    .map(|j| format!("{verb} {j}"))
                    .collect()
            } else {
                crate::palette::complete(buffer, visible_job_ids)
            }
        }
    }
}

fn default_palette_job_id(state: &AppState) -> Option<JobId> {
    match &state.ui.view {
        View::Detail { job_id, .. } => Some(job_id.clone()),
        View::List => state.ui.selected_job.clone(),
    }
}

fn confirm_summary(cmd: &crate::palette::PaletteCommand) -> String {
    let verb = cmd.verb();
    match cmd.job_id() {
        Some(j) => format!("{verb} job '{}'?", j.as_str()),
        None => format!("{verb}?"),
    }
}

/// Normal-mode dispatcher: routes by [`View`]. `Filter` mode is
/// List-view-only (entered via `/` from List Normal), so it does
/// not appear here.
fn handle_key_normal(state: &mut AppState, key: KeyEvent) -> AppAction {
    match &state.ui.view {
        View::List => handle_key_list_normal(state, key),
        View::Detail { .. } => handle_key_detail(state, key),
    }
}

fn handle_key_list_normal(state: &mut AppState, key: KeyEvent) -> AppAction {
    match key.code {
        KeyCode::Char('q') | KeyCode::Char('Q') | KeyCode::Esc => AppAction::Quit,
        KeyCode::Up => {
            move_selection(state, -1);
            AppAction::Continue
        }
        KeyCode::Down => {
            move_selection(state, 1);
            AppAction::Continue
        }
        KeyCode::Home => {
            if let Some(first) = visible_ids(state).into_iter().next() {
                state.ui.selected_job = Some(first);
            }
            AppAction::Continue
        }
        KeyCode::End => {
            if let Some(last) = visible_ids(state).into_iter().last() {
                state.ui.selected_job = Some(last);
            }
            AppAction::Continue
        }
        KeyCode::Enter => {
            // Open the detail view for the selected job. No-op when
            // nothing is selected (would be misleading to flip to a
            // detail view with no job to inspect).
            if let Some(id) = state.ui.selected_job.clone() {
                state.ui.view = View::Detail {
                    job_id: id,
                    tab: Tab::Overview,
                };
            }
            AppAction::Continue
        }
        KeyCode::Char('/') => {
            // Enter filter-input mode. Capture the current filter as
            // `prior` so Esc reverts cleanly.
            let prior = state.ui.filter.clone();
            state.ui.input_mode = InputMode::Filter {
                buffer: state.ui.filter.clone(),
                prior,
            };
            AppAction::Continue
        }
        KeyCode::Char(':') => {
            state.ui.input_mode = InputMode::Palette {
                buffer: String::new(),
                completion_idx: 0,
            };
            AppAction::Continue
        }
        KeyCode::Char('?') => {
            state.ui.modal = Some(Modal::Help);
            AppAction::Continue
        }
        KeyCode::Char('s') => {
            state.ui.sort = state.ui.sort.cycle();
            AppAction::Continue
        }
        _ => AppAction::Continue,
    }
}

/// Detail-view dispatcher with two interposed layers:
///
/// 1. Modal takes priority. When a modal is open the Workers-tab
///    selection / sort bindings would mislead the operator (Up/Down
///    looks like it should scroll the modal, not move the table
///    cursor underneath), so the modal intercepts input first —
///    only Esc/Backspace close it and `q` still quits.
///
/// 2. Workers tab gets its own sub-dispatcher (Up/Down/Home/End for
///    row selection, `s` cycles `worker_sort`, Enter opens the
///    modal). Falls through to the base Detail dispatcher for
///    keys the Workers tab doesn't claim (Esc/Tab/etc).
///
/// 3. Base Detail handler — Esc/Backspace returns to List, Tab
///    cycles tab forward (wraps), BackTab cycles back, q quits.
fn handle_key_detail(state: &mut AppState, key: KeyEvent) -> AppAction {
    if state.ui.modal.is_some() {
        return handle_key_modal(state, key);
    }
    if let View::Detail {
        tab: Tab::Workers, ..
    } = state.ui.view
    {
        return handle_key_workers_tab(state, key);
    }
    handle_key_detail_base(state, key)
}

fn handle_key_modal(state: &mut AppState, key: KeyEvent) -> AppAction {
    match key.code {
        KeyCode::Char('q') | KeyCode::Char('Q') => AppAction::Quit,
        KeyCode::Esc | KeyCode::Backspace => {
            state.ui.modal = None;
            AppAction::Continue
        }
        _ => AppAction::Continue,
    }
}

fn handle_key_workers_tab(state: &mut AppState, key: KeyEvent) -> AppAction {
    match key.code {
        KeyCode::Up => {
            move_worker_selection(state, -1);
            AppAction::Continue
        }
        KeyCode::Down => {
            move_worker_selection(state, 1);
            AppAction::Continue
        }
        KeyCode::Home => {
            let ids = visible_worker_ids(state);
            state.ui.selected_worker = ids.into_iter().next();
            AppAction::Continue
        }
        KeyCode::End => {
            let ids = visible_worker_ids(state);
            state.ui.selected_worker = ids.into_iter().last();
            AppAction::Continue
        }
        KeyCode::Char('s') => {
            state.ui.worker_sort = state.ui.worker_sort.cycle();
            AppAction::Continue
        }
        KeyCode::Enter => {
            // Drill-down on the selected worker. Auto-select the
            // first visible if the operator hasn't moved the
            // cursor yet — Enter on a tab with workers should
            // always do something.
            let target = state
                .ui
                .selected_worker
                .or_else(|| visible_worker_ids(state).into_iter().next());
            if let Some(wid) = target {
                state.ui.modal = Some(Modal::WorkerDetail { worker_id: wid });
                state.ui.selected_worker = Some(wid);
            }
            AppAction::Continue
        }
        // Anything else flows through to Esc/Backspace/Tab/BackTab/q
        // in the base Detail handler.
        _ => handle_key_detail_base(state, key),
    }
}

fn handle_key_detail_base(state: &mut AppState, key: KeyEvent) -> AppAction {
    match key.code {
        KeyCode::Char('q') | KeyCode::Char('Q') => AppAction::Quit,
        KeyCode::Esc | KeyCode::Backspace => {
            state.ui.view = View::List;
            AppAction::Continue
        }
        KeyCode::Tab => {
            if let View::Detail { job_id, tab } = state.ui.view.clone() {
                state.ui.view = View::Detail {
                    job_id,
                    tab: tab.cycle_next(),
                };
            }
            AppAction::Continue
        }
        KeyCode::BackTab => {
            if let View::Detail { job_id, tab } = state.ui.view.clone() {
                state.ui.view = View::Detail {
                    job_id,
                    tab: tab.cycle_prev(),
                };
            }
            AppAction::Continue
        }
        KeyCode::Char(':') => {
            state.ui.input_mode = InputMode::Palette {
                buffer: String::new(),
                completion_idx: 0,
            };
            AppAction::Continue
        }
        KeyCode::Char('?') => {
            state.ui.modal = Some(Modal::Help);
            AppAction::Continue
        }
        _ => AppAction::Continue,
    }
}

fn visible_worker_ids(state: &AppState) -> Vec<WorkerId> {
    let job_id = match &state.ui.view {
        View::Detail { job_id, .. } => job_id.clone(),
        _ => return Vec::new(),
    };
    render::visible_workers(state, &job_id)
        .iter()
        .map(|w| w.id)
        .collect()
}

fn move_worker_selection(state: &mut AppState, delta: isize) {
    let ids = visible_worker_ids(state);
    if ids.is_empty() {
        state.ui.selected_worker = None;
        return;
    }
    let len = ids.len() as isize;
    let current = state
        .ui
        .selected_worker
        .as_ref()
        .and_then(|id| ids.iter().position(|i| i == id));
    let next = match current {
        Some(i) => ((i as isize + delta).rem_euclid(len)) as usize,
        None => {
            if delta >= 0 {
                0
            } else {
                (len - 1) as usize
            }
        }
    };
    state.ui.selected_worker = Some(ids[next]);
}

fn handle_key_filter(state: &mut AppState, key: KeyEvent) -> AppAction {
    // Take the mode out so we can mutate the buffer in place
    // without holding two `&mut state` borrows.
    let mode = std::mem::replace(&mut state.ui.input_mode, InputMode::Normal);
    let InputMode::Filter { mut buffer, prior } = mode else {
        // Shouldn't happen — the dispatcher already matched Filter.
        return AppAction::Continue;
    };
    match key.code {
        KeyCode::Enter => {
            // Commit: filter already tracks buffer (we update it
            // live below), so just leave Normal mode.
            state.ui.filter = buffer;
            // Reset selection if the previously-selected job no
            // longer matches the filter — otherwise the operator
            // sees an empty highlight.
            if let Some(sel) = state.ui.selected_job.clone() {
                let still_visible = render::visible_jobs(state).iter().any(|j| j.id == sel);
                if !still_visible {
                    state.ui.selected_job =
                        render::visible_jobs(state).first().map(|j| j.id.clone());
                }
            }
        }
        KeyCode::Esc => {
            // Cancel: restore the filter that was in effect before
            // the operator pressed `/`.
            state.ui.filter = prior;
        }
        KeyCode::Backspace => {
            buffer.pop();
            state.ui.filter = buffer.clone();
            state.ui.input_mode = InputMode::Filter { buffer, prior };
        }
        KeyCode::Char(c) => {
            buffer.push(c);
            state.ui.filter = buffer.clone();
            state.ui.input_mode = InputMode::Filter { buffer, prior };
        }
        _ => {
            // Unknown key in filter mode — stay in mode, no edits.
            state.ui.input_mode = InputMode::Filter { buffer, prior };
        }
    }
    AppAction::Continue
}

fn visible_ids(state: &AppState) -> Vec<JobId> {
    render::visible_jobs(state)
        .iter()
        .map(|j| j.id.clone())
        .collect()
}

fn move_selection(state: &mut AppState, delta: isize) {
    let ids = visible_ids(state);
    if ids.is_empty() {
        state.ui.selected_job = None;
        return;
    }
    let len = ids.len() as isize;
    let current = state
        .ui
        .selected_job
        .as_ref()
        .and_then(|id| ids.iter().position(|i| i == id));
    let next = match current {
        Some(i) => ((i as isize + delta).rem_euclid(len)) as usize,
        None => {
            if delta >= 0 {
                0
            } else {
                (len - 1) as usize
            }
        }
    };
    state.ui.selected_job = Some(ids[next].clone());
}

// =============================================================================
// IO shell — run(client, opts)
// =============================================================================

/// Options for [`run`]. Kept narrow: the CLI maps its argument
/// surface onto this struct, and tests can build it directly.
#[derive(Debug, Clone)]
pub struct RunOpts {
    /// Render tick interval. Defaults to 50 ms (20 fps cap).
    pub render_tick: Duration,
    /// Initial backoff for SSE reconnect attempts.
    pub reconnect_initial: Duration,
    /// Maximum backoff between SSE reconnect attempts.
    pub reconnect_max: Duration,
}

impl Default for RunOpts {
    fn default() -> Self {
        Self {
            render_tick: Duration::from_millis(50),
            reconnect_initial: Duration::from_secs(1),
            reconnect_max: Duration::from_secs(30),
        }
    }
}

/// Run the TUI against `client` until the user presses `q`/`Esc`
/// or the SSE driver hits a non-retryable error. Owns the terminal
/// for the duration; raw mode and the alt screen are restored on
/// any exit path (including panics, via the RAII guard).
pub async fn run(client: Client, opts: RunOpts) -> anyhow::Result<()> {
    use ratatui::backend::CrosstermBackend;
    use ratatui::Terminal;

    // F27: restore-on-panic must not depend on Drop (which never runs
    // under release panic = "abort"). Install before entering raw mode
    // so there is no window where a panic leaves the terminal raw.
    install_panic_hook();

    let mut stdout = std::io::stdout();
    crossterm::terminal::enable_raw_mode()?;
    crossterm::execute!(stdout, crossterm::terminal::EnterAlternateScreen)?;
    let _restore = TerminalGuard;

    // Hidden manual-verification hook for F27; see `panic_after_ms`.
    if let Some(ms) = panic_after_ms(std::env::var("VAMOOSE_TUI_PANIC_AFTER_MS").ok().as_deref()) {
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(ms)).await;
            panic!("VAMOOSE_TUI_PANIC_AFTER_MS={ms} elapsed — deliberate F27 test panic");
        });
    }
    let backend = CrosstermBackend::new(stdout);
    let mut terminal = Terminal::new(backend)?;

    // Pick up theme from NO_COLOR / VAMOOSE_THEME at app start so
    // `NO_COLOR=1 vamoose tui ...` is a single-flag toggle.
    let theme = crate::theme::Theme::from_env();
    let app_state = Arc::new(Mutex::new(AppState::empty(Utc::now()).with_theme(theme)));
    let cancel = CancellationToken::new();
    let (tx, mut rx) = mpsc::channel::<Input>(256);

    // SSE driver task — reconnect/backoff/forward to channel.
    let sse_state = Arc::clone(&app_state);
    let sse_cancel = cancel.clone();
    let sse_tx = tx.clone();
    let sse_opts = opts.clone();
    // `client` stays in scope so the main loop can clone it for
    // each palette-dispatched command. The SSE driver gets its own
    // clone via the spawned task.
    let sse_client = client.clone();
    let sse_handle = tokio::spawn(async move {
        sse_driver(sse_client, sse_state, sse_tx, sse_cancel, sse_opts).await
    });

    // Key event task — async stream from crossterm.
    let key_cancel = cancel.clone();
    let key_tx = tx.clone();
    let key_handle = tokio::spawn(async move {
        key_event_loop(key_tx, key_cancel).await;
    });

    // Render tick.
    let mut tick = tokio::time::interval(opts.render_tick);
    tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

    // Draw once before the first event so the operator sees the
    // empty banner immediately rather than a blank screen.
    {
        let guard = app_state.lock().await;
        terminal.draw(|f| render::render(f, &guard, Utc::now()))?;
    }

    loop {
        tokio::select! {
            biased;
            input = rx.recv() => {
                let Some(input) = input else { break };
                let mut state = app_state.lock().await;
                let action = handle_input(&mut state, input, Utc::now());
                match action {
                    AppAction::Quit => break,
                    AppAction::Execute(cmd) => {
                        // Drop the state lock BEFORE spawning the
                        // dispatch task so the HTTP call doesn't
                        // hold the render-loop's lock during a
                        // network round trip.
                        drop(state);
                        spawn_command_dispatch(
                            client.clone(),
                            cmd,
                            tx.clone(),
                            cancel.clone(),
                        );
                        let mut state = app_state.lock().await;
                        let now = Utc::now();
                        state.tick_command_status(now);
                        terminal.draw(|f| render::render(f, &state, now))?;
                    }
                    // Resync recovery is owned by the SSE driver —
                    // it saw the same frame before forwarding it.
                    // The loop just redraws the flipped banner.
                    AppAction::Continue | AppAction::Resync => {
                        let now = Utc::now();
                        state.tick_command_status(now);
                        terminal.draw(|f| render::render(f, &state, now))?;
                    }
                }
            }
            _ = tick.tick() => {
                let mut state = app_state.lock().await;
                let now = Utc::now();
                state.tick_command_status(now);
                terminal.draw(|f| render::render(f, &state, now))?;
            }
        }
    }

    // Tear down: cancel background tasks, drop the terminal guard
    // (raw mode + alt screen restored), join the handles.
    cancel.cancel();
    drop(tx); // close the channel so any sender await wakes
    let _ = sse_handle.await;
    let _ = key_handle.await;
    Ok(())
}

/// Restore the terminal: raw mode off, back to the main screen.
///
/// Single source of truth for restore (F27): both [`TerminalGuard`]'s
/// Drop and the panic hook installed by [`install_panic_hook`] call
/// this fn and nothing else. Best-effort and idempotent — safe to run
/// twice (guard after hook), on a non-tty, or mid-panic.
pub(crate) fn restore_terminal() {
    let _ = crossterm::terminal::disable_raw_mode();
    let _ = crossterm::execute!(std::io::stdout(), crossterm::terminal::LeaveAlternateScreen);
}

/// Install a panic hook that restores the terminal, then delegates to
/// the previously installed hook (so the panic message still prints —
/// now onto the operator's real screen instead of the vanished alt
/// screen).
///
/// Why a hook and not just the guard: `TerminalGuard` is Drop-based,
/// and Drop never runs under release `panic = "abort"` — a panic would
/// leave the operator's terminal raw in the alternate screen. Panic
/// hooks run before the abort, so restore still happens.
///
/// Idempotent: only the first call installs; returns whether this call
/// did the installing. Re-wrapping on every call would chain a restore
/// per install and re-entrantly grow the hook.
pub(crate) fn install_panic_hook() -> bool {
    use std::sync::atomic::{AtomicBool, Ordering};
    static INSTALLED: AtomicBool = AtomicBool::new(false);
    if INSTALLED.swap(true, Ordering::SeqCst) {
        return false;
    }
    let prev = std::panic::take_hook();
    std::panic::set_hook(Box::new(move |info| {
        restore_terminal();
        prev(info);
    }));
    true
}

/// Parse the hidden `VAMOOSE_TUI_PANIC_AFTER_MS` test hook value.
/// When set to a millisecond count, [`run`] spawns a task that panics
/// after the delay — the only practical way to verify panic-path
/// terminal restore in a real terminal (`kill -SEGV` is not a panic).
/// See the ignored test `manual_panic_abort_restore_via_env_hook` for
/// the manual recipe. Non-numeric values are ignored.
fn panic_after_ms(raw: Option<&str>) -> Option<u64> {
    raw?.trim().parse().ok()
}

/// Restore the terminal on Drop. Bypasses anyhow — restore should
/// happen even if the user kills the process via Ctrl-C and main
/// is unwound.
struct TerminalGuard;
impl Drop for TerminalGuard {
    fn drop(&mut self) {
        restore_terminal();
    }
}

/// Non-retryable client error: a 4xx-class HTTP response (bad
/// token, unknown route). 5xx and transport errors stay retryable.
fn is_fatal(e: &ClientError) -> bool {
    matches!(e, ClientError::Http { status, .. } if !status.is_server_error() && !status.is_success())
}

/// Fetch the coord's authoritative view over REST (F26 bootstrap,
/// COORD_PLAN §3.4 / §3.7 P4): `GET /healthz` for the resume
/// cursor, then every `GET /jobs` page plus each job's `/workers`
/// and `/errors`, shaped by [`crate::state::snapshot_from_rest`].
/// Required on first connect and after every `Resync` — a seq-0 SSE
/// replay cannot substitute since archived terminal jobs' event
/// chunks are deleted from `events/` (F23).
///
/// Consistency: the cursor is re-read after the walk; if events
/// landed mid-fetch the walk retries (bounded), and a walk that never
/// sees a quiescent coord is a **retryable error** — the driver's
/// normal backoff loop tries again. There is no safe cursor for a
/// torn walk: the pieces were read at different seqs, so a pre-walk
/// cursor replays events whose effects are already baked into
/// later-read pieces (permanently double-counting aggregates like
/// error buckets — envelope-seq dedup cannot see into the snapshot),
/// and a post-walk cursor permanently skips increments missing from
/// earlier-read pieces. Only a clean pass (same cursor before and
/// after) is sound.
pub async fn fetch_bootstrap_snapshot(
    client: &Client,
    now: DateTime<Utc>,
) -> crate::client::Result<migration_control_protocol::schema::Snapshot> {
    const CONSISTENT_ATTEMPTS: usize = 3;
    let mut cursor_seq = client.healthz().await?.last_seq;
    for attempt in 1..=CONSISTENT_ATTEMPTS {
        let mut jobs = Vec::new();
        let mut workers = Vec::new();
        let mut buckets = Vec::new();
        let mut page_cursor: Option<String> = None;
        loop {
            let page = client.get_jobs(page_cursor.as_deref(), None).await?;
            for job in &page.jobs {
                workers.extend(client.get_workers(job.id.as_str()).await?.workers);
                let b = client.get_errors(job.id.as_str()).await?.buckets;
                if !b.is_empty() {
                    buckets.push((job.id.clone(), b));
                }
            }
            jobs.extend(page.jobs);
            match page.next_cursor {
                Some(c) => page_cursor = Some(c),
                None => break,
            }
        }
        let after = client.healthz().await?.last_seq;
        if after == cursor_seq {
            return Ok(crate::state::snapshot_from_rest(
                jobs, workers, buckets, cursor_seq, now,
            ));
        }
        tracing::debug!(
            before = cursor_seq,
            after,
            attempt,
            "bootstrap walk torn by concurrent writes; retrying"
        );
        cursor_seq = after;
    }
    Err(crate::client::ClientError::BootstrapTorn {
        attempts: CONSISTENT_ATTEMPTS,
    })
}

/// SSE driver task: REST-bootstrap → open the stream from the
/// bootstrap cursor → forward frames → on drop, mark reconnecting +
/// sleep with exponential backoff → reconnect from the highest seq
/// seen. On a server-sent `Resync` the stream is dropped and the
/// bootstrap runs again — the dropped events are only recoverable
/// through the REST view (COORD_PLAN §3.4).
///
/// Public so the integration suite can run the real
/// bootstrap/stream/recovery machinery against an in-process coord
/// without standing up a terminal.
pub async fn sse_driver(
    client: Client,
    state: Arc<Mutex<AppState>>,
    tx: mpsc::Sender<Input>,
    cancel: CancellationToken,
    opts: RunOpts,
) {
    use futures::StreamExt;
    let mut backoff = opts.reconnect_initial;
    // True on the first connect and again after every Resync.
    let mut needs_bootstrap = true;
    // Cursor from the latest bootstrap. The reducer owns
    // last_seen_seq, but the Input::Snapshot we just sent may not
    // have been folded in yet when the stream opens — take the max.
    let mut bootstrap_seq: u64 = 0;
    loop {
        if needs_bootstrap {
            match fetch_bootstrap_snapshot(&client, Utc::now()).await {
                Ok(snapshot) => {
                    bootstrap_seq = bootstrap_seq.max(snapshot.last_seq);
                    if tx.send(Input::Snapshot(Box::new(snapshot))).await.is_err() {
                        return;
                    }
                    needs_bootstrap = false;
                }
                Err(e) if is_fatal(&e) => {
                    let _ = tx.send(Input::SseFatal(e.to_string())).await;
                    return;
                }
                Err(e) => {
                    let _ = tx.send(Input::SseDisconnected(e.to_string())).await;
                    tokio::select! {
                        biased;
                        _ = cancel.cancelled() => return,
                        _ = tokio::time::sleep(backoff) => {}
                    }
                    backoff = (backoff.saturating_mul(2)).min(opts.reconnect_max);
                    continue;
                }
            }
        }
        let resume_from = state.lock().await.last_seq().max(bootstrap_seq);
        let cursor = Some(resume_from);
        let stream_result = client.stream(cursor, None).await;
        match stream_result {
            Ok(stream) => {
                // Connected!
                if tx.send(Input::SseConnected).await.is_err() {
                    return;
                }
                backoff = opts.reconnect_initial;
                tokio::pin!(stream);
                loop {
                    tokio::select! {
                        biased;
                        _ = cancel.cancelled() => return,
                        next = stream.next() => {
                            match next {
                                Some(Ok(frame)) => {
                                    let resync = matches!(frame, SseFrame::Resync);
                                    if tx.send(Input::SseFrame(frame)).await.is_err() {
                                        return;
                                    }
                                    if resync {
                                        // The coord dropped events we
                                        // will never see on this or any
                                        // stream. Drop it and recover
                                        // through REST (the reducer only
                                        // flips the banner — recovery is
                                        // owned here).
                                        needs_bootstrap = true;
                                        break;
                                    }
                                }
                                Some(Err(e)) => {
                                    let _ = tx
                                        .send(Input::SseDisconnected(e.to_string()))
                                        .await;
                                    break;
                                }
                                None => {
                                    let _ = tx
                                        .send(Input::SseDisconnected(
                                            "SSE stream closed by server".into(),
                                        ))
                                        .await;
                                    break;
                                }
                            }
                        }
                    }
                }
                if needs_bootstrap {
                    // Resync recovery: the coord is healthy, it just
                    // dropped our tail — re-bootstrap immediately,
                    // no backoff.
                    continue;
                }
            }
            Err(e) if is_fatal(&e) => {
                // 4xx-class error — non-retryable. Surface it and
                // stop the driver so the operator sees the banner
                // turn red.
                let _ = tx.send(Input::SseFatal(e.to_string())).await;
                return;
            }
            Err(e) => {
                let _ = tx.send(Input::SseDisconnected(e.to_string())).await;
            }
        }
        // Sleep before the next attempt unless cancelled.
        tokio::select! {
            biased;
            _ = cancel.cancelled() => return,
            _ = tokio::time::sleep(backoff) => {}
        }
        backoff = (backoff.saturating_mul(2)).min(opts.reconnect_max);
    }
}

/// Key event task: pulls Crossterm events asynchronously and
/// Fire-and-forget HTTP dispatch for a parsed [`PaletteCommand`].
/// Runs in its own tokio task so the render loop never blocks on a
/// slow coord. On completion sends `Input::CommandResult` back
/// through the main channel so the operator gets a banner toast.
fn spawn_command_dispatch(
    client: Client,
    cmd: crate::palette::PaletteCommand,
    tx: mpsc::Sender<Input>,
    cancel: CancellationToken,
) {
    tokio::spawn(async move {
        let result = run_command(&client, &cmd, &cancel).await;
        let _ = tx.send(result).await;
    });
}

async fn run_command(
    client: &Client,
    cmd: &crate::palette::PaletteCommand,
    cancel: &CancellationToken,
) -> Input {
    use crate::palette::PaletteCommand as PC;
    let dispatch = async {
        let resp = match cmd {
            PC::Pause { job_id } => client.pause(job_id.as_str(), None).await,
            PC::Resume { job_id } => client.resume(job_id.as_str(), None).await,
            PC::Cancel { job_id } => client.cancel(job_id.as_str(), None).await,
            PC::Drain { job_id } => client.drain(job_id.as_str(), None).await,
            PC::RetryFailed { job_id } => client.retry_failed(job_id.as_str(), None).await,
            // Help / Quit are handled by the event loop, not here —
            // mark as an internal bug if they reach run_command.
            PC::Help => {
                return Input::CommandResult {
                    ok: false,
                    message: "':help' is local; this should not have dispatched".into(),
                };
            }
            PC::Quit => {
                return Input::CommandResult {
                    ok: false,
                    message: "':quit' is local; this should not have dispatched".into(),
                };
            }
        };
        match resp {
            Ok(_) => {
                let verb = cmd.verb();
                let target = cmd
                    .job_id()
                    .map(|j| format!(" '{}'", j.as_str()))
                    .unwrap_or_default();
                Input::CommandResult {
                    ok: true,
                    message: format!("{verb}{target} ok"),
                }
            }
            Err(e) => Input::CommandResult {
                ok: false,
                message: format!("{}: {e}", cmd.verb()),
            },
        }
    };
    tokio::select! {
        biased;
        _ = cancel.cancelled() => Input::CommandResult {
            ok: false,
            message: format!("{}: cancelled", cmd.verb()),
        },
        out = dispatch => out,
    }
}

/// forwards Key events to the main loop. Resize / mouse / focus
/// events are dropped on the floor for now — the render layer
/// re-lays-out on every tick regardless.
async fn key_event_loop(tx: mpsc::Sender<Input>, cancel: CancellationToken) {
    use crossterm::event::{Event, EventStream};
    use futures::StreamExt;
    let mut stream = EventStream::new();
    loop {
        tokio::select! {
            biased;
            _ = cancel.cancelled() => return,
            ev = stream.next() => {
                match ev {
                    Some(Ok(Event::Key(key))) => {
                        if tx.send(Input::Key(key)).await.is_err() {
                            return;
                        }
                    }
                    Some(Ok(_)) => { /* drop resize/mouse/focus */ }
                    Some(Err(e)) => {
                        tracing::warn!(error = %e, "crossterm event read failed");
                    }
                    None => return,
                }
            }
        }
    }
}

// =============================================================================
// Tests
// =============================================================================

#[cfg(test)]
mod tests;
