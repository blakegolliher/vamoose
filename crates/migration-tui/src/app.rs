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

use crate::client::{Client, ClientError, SseFrame};
use crate::render;
use crate::state::{AppState, InputMode, Modal, Tab, View};
use chrono::{DateTime, Utc};
use crossterm::event::{KeyCode, KeyEvent, KeyEventKind};
use migration_coord::schema::JobId;
use migration_coord::schema::WorkerId;
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
        Input::SseFrame(frame) => {
            handle_sse_frame(state, frame, now);
            AppAction::Continue
        }
        Input::Key(key) => handle_key(state, key),
        Input::Tick => AppAction::Continue,
    }
}

fn handle_sse_frame(state: &mut AppState, frame: SseFrame, now: DateTime<Utc>) {
    match frame {
        SseFrame::Event { envelope, .. } => {
            state.apply_envelope(&envelope);
            state.mark_traffic(now);
            if state.ui.selected_job.is_none() {
                // Auto-select the first visible job once we have
                // any — saves the operator one keypress on first
                // boot. Idempotent for subsequent frames.
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
        SseFrame::Resync => {
            // Server told us our SSE subscriber overflowed and
            // dropped events. Re-bootstrap by treating it as a
            // soft reconnect — the driver will resume from the
            // current last_seen_seq on its next attempt.
            state.mark_reconnecting(now, "server requested Resync — re-bootstrapping");
        }
        SseFrame::Keepalive => {
            state.mark_traffic(now);
        }
    }
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

    let mut stdout = std::io::stdout();
    crossterm::terminal::enable_raw_mode()?;
    crossterm::execute!(stdout, crossterm::terminal::EnterAlternateScreen)?;
    let _restore = TerminalGuard;
    let backend = CrosstermBackend::new(stdout);
    let mut terminal = Terminal::new(backend)?;

    let app_state = Arc::new(Mutex::new(AppState::empty(Utc::now())));
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
                    AppAction::Continue => {
                        {
                    let now = Utc::now();
                    state.tick_command_status(now);
                    terminal.draw(|f| render::render(f, &state, now))?;
                }
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

/// Restore the terminal on Drop. Bypasses anyhow — restore should
/// happen even if the user kills the process via Ctrl-C and main
/// is unwound.
struct TerminalGuard;
impl Drop for TerminalGuard {
    fn drop(&mut self) {
        let _ = crossterm::terminal::disable_raw_mode();
        let _ = crossterm::execute!(std::io::stdout(), crossterm::terminal::LeaveAlternateScreen);
    }
}

/// SSE driver task: connect → forward frames → on drop, mark
/// reconnecting + sleep with exponential backoff → reconnect.
/// Reads the current `last_seen_seq` from `state` on every connect
/// so a backlog from before the disconnect is resumed correctly.
async fn sse_driver(
    client: Client,
    state: Arc<Mutex<AppState>>,
    tx: mpsc::Sender<Input>,
    cancel: CancellationToken,
    opts: RunOpts,
) {
    use futures::StreamExt;
    let mut backoff = opts.reconnect_initial;
    loop {
        let resume_from = state.lock().await.last_seq();
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
                                    if tx.send(Input::SseFrame(frame)).await.is_err() {
                                        return;
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
            }
            Err(e) if matches!(e, ClientError::Http { status, .. } if !status.is_server_error() && !status.is_success()) =>
            {
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
mod tests {
    use super::*;
    use chrono::TimeZone;
    use crossterm::event::{KeyEvent, KeyEventKind, KeyModifiers};
    use migration_coord::schema::{ConfigHash, EventEnvelope, EventKind, SCHEMA_VERSION};

    fn at(s: i64) -> DateTime<Utc> {
        Utc.timestamp_opt(s, 0).unwrap()
    }
    fn jid(s: &str) -> JobId {
        JobId::new(s).unwrap()
    }
    fn key(code: KeyCode) -> KeyEvent {
        KeyEvent {
            code,
            modifiers: KeyModifiers::NONE,
            kind: KeyEventKind::Press,
            state: crossterm::event::KeyEventState::NONE,
        }
    }
    fn release(code: KeyCode) -> KeyEvent {
        KeyEvent {
            code,
            modifiers: KeyModifiers::NONE,
            kind: KeyEventKind::Release,
            state: crossterm::event::KeyEventState::NONE,
        }
    }
    fn job_created(seq: u64, j: &str) -> EventEnvelope {
        EventEnvelope {
            seq,
            at: at(seq as i64),
            schema_version: SCHEMA_VERSION,
            worker_at: None,
            kind: EventKind::JobCreated {
                job_id: jid(j),
                name: format!("{j}-mig"),
                source: "nfs://src".into(),
                dest: "nfs://dst".into(),
                owner: "test".into(),
                config_hash: ConfigHash("ab".into()),
            },
        }
    }

    // ----- handle_input branches -----

    #[test]
    fn quit_on_q_and_esc() {
        let mut s = AppState::empty(at(0));
        assert_eq!(
            handle_input(&mut s, Input::Key(key(KeyCode::Char('q'))), at(0)),
            AppAction::Quit
        );
        assert_eq!(
            handle_input(&mut s, Input::Key(key(KeyCode::Esc)), at(0)),
            AppAction::Quit
        );
        // Uppercase Q also quits (some terminals send shifted form).
        assert_eq!(
            handle_input(&mut s, Input::Key(key(KeyCode::Char('Q'))), at(0)),
            AppAction::Quit
        );
    }

    #[test]
    fn key_release_does_not_quit() {
        let mut s = AppState::empty(at(0));
        assert_eq!(
            handle_input(&mut s, Input::Key(release(KeyCode::Char('q'))), at(0)),
            AppAction::Continue
        );
    }

    #[test]
    fn sse_connected_marks_state() {
        let mut s = AppState::empty(at(0));
        assert_eq!(
            handle_input(&mut s, Input::SseConnected, at(10)),
            AppAction::Continue
        );
        assert!(matches!(
            s.connection,
            crate::state::ConnectionStatus::Connected { .. }
        ));
    }

    #[test]
    fn sse_disconnected_flips_to_reconnecting() {
        let mut s = AppState::empty(at(0));
        s.mark_connected(at(0));
        handle_input(&mut s, Input::SseDisconnected("reset".into()), at(5));
        match s.connection {
            crate::state::ConnectionStatus::Reconnecting { last_error, .. } => {
                assert_eq!(last_error, "reset");
            }
            _ => panic!("expected Reconnecting"),
        }
    }

    #[test]
    fn sse_fatal_marks_disconnected_terminal() {
        let mut s = AppState::empty(at(0));
        handle_input(&mut s, Input::SseFatal("bad auth".into()), at(0));
        match s.connection {
            crate::state::ConnectionStatus::Disconnected { reason } => {
                assert_eq!(reason, "bad auth");
            }
            _ => panic!("expected Disconnected"),
        }
    }

    #[test]
    fn sse_frame_event_applies_envelope_and_marks_traffic() {
        let mut s = AppState::empty(at(0));
        s.mark_connected(at(0));
        let env = job_created(1, "alpha");
        handle_input(
            &mut s,
            Input::SseFrame(SseFrame::Event {
                seq: 1,
                envelope: env,
            }),
            at(7),
        );
        assert!(s.job(&jid("alpha")).is_some());
        assert_eq!(s.last_seq(), 1);
        // mark_traffic refreshed last_traffic.
        if let crate::state::ConnectionStatus::Connected { last_traffic } = &s.connection {
            assert_eq!(*last_traffic, at(7));
        } else {
            panic!("expected Connected");
        }
    }

    #[test]
    fn sse_frame_event_auto_selects_first_job() {
        let mut s = AppState::empty(at(0));
        s.mark_connected(at(0));
        assert!(s.ui.selected_job.is_none());
        handle_input(
            &mut s,
            Input::SseFrame(SseFrame::Event {
                seq: 1,
                envelope: job_created(1, "alpha"),
            }),
            at(0),
        );
        assert_eq!(s.ui.selected_job, Some(jid("alpha")));
        // A second job arriving does NOT change the selection.
        handle_input(
            &mut s,
            Input::SseFrame(SseFrame::Event {
                seq: 2,
                envelope: job_created(2, "bravo"),
            }),
            at(0),
        );
        assert_eq!(s.ui.selected_job, Some(jid("alpha")));
    }

    #[test]
    fn sse_frame_keepalive_only_refreshes_traffic() {
        let mut s = AppState::empty(at(0));
        s.mark_connected(at(0));
        let before = s.last_seq();
        handle_input(&mut s, Input::SseFrame(SseFrame::Keepalive), at(3));
        assert_eq!(s.last_seq(), before);
        if let crate::state::ConnectionStatus::Connected { last_traffic } = &s.connection {
            assert_eq!(*last_traffic, at(3));
        } else {
            panic!("expected Connected");
        }
    }

    #[test]
    fn sse_frame_resync_marks_reconnecting() {
        let mut s = AppState::empty(at(0));
        s.mark_connected(at(0));
        handle_input(&mut s, Input::SseFrame(SseFrame::Resync), at(5));
        assert!(matches!(
            s.connection,
            crate::state::ConnectionStatus::Reconnecting { .. }
        ));
    }

    // ----- selection movement -----

    #[test]
    fn move_selection_with_no_jobs_is_a_noop() {
        let mut s = AppState::empty(at(0));
        handle_input(&mut s, Input::Key(key(KeyCode::Down)), at(0));
        assert!(s.ui.selected_job.is_none());
    }

    #[test]
    fn move_selection_wraps_around_top_and_bottom() {
        let mut s = AppState::empty(at(0));
        s.apply_envelope(&job_created(1, "alpha"));
        s.apply_envelope(&job_created(2, "bravo"));
        s.apply_envelope(&job_created(3, "charlie"));
        s.ui.selected_job = Some(jid("alpha"));

        handle_input(&mut s, Input::Key(key(KeyCode::Down)), at(0));
        assert_eq!(s.ui.selected_job, Some(jid("bravo")));
        handle_input(&mut s, Input::Key(key(KeyCode::Down)), at(0));
        assert_eq!(s.ui.selected_job, Some(jid("charlie")));
        handle_input(&mut s, Input::Key(key(KeyCode::Down)), at(0));
        assert_eq!(
            s.ui.selected_job,
            Some(jid("alpha")),
            "wraps from last back to first"
        );
        handle_input(&mut s, Input::Key(key(KeyCode::Up)), at(0));
        assert_eq!(
            s.ui.selected_job,
            Some(jid("charlie")),
            "wraps from first back to last"
        );
    }

    #[test]
    fn home_and_end_jump_to_extremes() {
        let mut s = AppState::empty(at(0));
        s.apply_envelope(&job_created(1, "alpha"));
        s.apply_envelope(&job_created(2, "bravo"));
        s.apply_envelope(&job_created(3, "charlie"));
        s.ui.selected_job = Some(jid("bravo"));

        handle_input(&mut s, Input::Key(key(KeyCode::End)), at(0));
        assert_eq!(s.ui.selected_job, Some(jid("charlie")));
        handle_input(&mut s, Input::Key(key(KeyCode::Home)), at(0));
        assert_eq!(s.ui.selected_job, Some(jid("alpha")));
    }

    #[test]
    fn tick_does_not_quit() {
        let mut s = AppState::empty(at(0));
        assert_eq!(
            handle_input(&mut s, Input::Tick, at(0)),
            AppAction::Continue
        );
    }

    // ----- sort cycling -----

    #[test]
    fn s_key_cycles_sort_in_normal_mode() {
        let mut s = AppState::empty(at(0));
        assert_eq!(s.ui.sort, crate::state::JobSort::ById);
        handle_input(&mut s, Input::Key(key(KeyCode::Char('s'))), at(0));
        assert_eq!(s.ui.sort, crate::state::JobSort::ByPhase);
        handle_input(&mut s, Input::Key(key(KeyCode::Char('s'))), at(0));
        assert_eq!(s.ui.sort, crate::state::JobSort::ByProgressDesc);
        handle_input(&mut s, Input::Key(key(KeyCode::Char('s'))), at(0));
        assert_eq!(s.ui.sort, crate::state::JobSort::ByErrorsDesc);
        handle_input(&mut s, Input::Key(key(KeyCode::Char('s'))), at(0));
        assert_eq!(s.ui.sort, crate::state::JobSort::ById, "wraps");
    }

    // ----- filter mode UX -----

    fn assert_normal(s: &AppState) {
        assert!(matches!(s.ui.input_mode, InputMode::Normal));
    }

    fn assert_filter_buffer(s: &AppState, expected: &str) {
        match &s.ui.input_mode {
            InputMode::Filter { buffer, .. } => assert_eq!(buffer, expected),
            _ => panic!("expected Filter mode, got {:?}", s.ui.input_mode),
        }
    }

    #[test]
    fn slash_enters_filter_mode_and_seeds_buffer_from_current_filter() {
        let mut s = AppState::empty(at(0));
        s.ui.filter = "prod".into();
        handle_input(&mut s, Input::Key(key(KeyCode::Char('/'))), at(0));
        assert_filter_buffer(&s, "prod");
    }

    #[test]
    fn filter_mode_appends_chars_and_updates_live_filter() {
        let mut s = AppState::empty(at(0));
        handle_input(&mut s, Input::Key(key(KeyCode::Char('/'))), at(0));
        handle_input(&mut s, Input::Key(key(KeyCode::Char('a'))), at(0));
        handle_input(&mut s, Input::Key(key(KeyCode::Char('b'))), at(0));
        assert_filter_buffer(&s, "ab");
        // Live update — visible jobs apply this NOW, no Enter needed.
        assert_eq!(s.ui.filter, "ab");
    }

    #[test]
    fn filter_mode_backspace_pops_last_char() {
        let mut s = AppState::empty(at(0));
        handle_input(&mut s, Input::Key(key(KeyCode::Char('/'))), at(0));
        for c in ['a', 'b', 'c'] {
            handle_input(&mut s, Input::Key(key(KeyCode::Char(c))), at(0));
        }
        handle_input(&mut s, Input::Key(key(KeyCode::Backspace)), at(0));
        assert_filter_buffer(&s, "ab");
        assert_eq!(s.ui.filter, "ab");
        // Backspace on empty is a no-op.
        for _ in 0..5 {
            handle_input(&mut s, Input::Key(key(KeyCode::Backspace)), at(0));
        }
        assert_filter_buffer(&s, "");
        assert_eq!(s.ui.filter, "");
    }

    #[test]
    fn filter_mode_enter_commits_and_returns_to_normal() {
        let mut s = AppState::empty(at(0));
        handle_input(&mut s, Input::Key(key(KeyCode::Char('/'))), at(0));
        for c in ['p', 'r', 'o', 'd'] {
            handle_input(&mut s, Input::Key(key(KeyCode::Char(c))), at(0));
        }
        handle_input(&mut s, Input::Key(key(KeyCode::Enter)), at(0));
        assert_normal(&s);
        assert_eq!(s.ui.filter, "prod");
    }

    #[test]
    fn filter_mode_esc_reverts_filter_to_prior() {
        let mut s = AppState::empty(at(0));
        s.ui.filter = "alpha".into();
        handle_input(&mut s, Input::Key(key(KeyCode::Char('/'))), at(0));
        // Live edit — filter changes as we type.
        for c in ['b', 'r', 'a', 'v', 'o'] {
            handle_input(&mut s, Input::Key(key(KeyCode::Char(c))), at(0));
        }
        assert_eq!(s.ui.filter, "alphabravo");
        handle_input(&mut s, Input::Key(key(KeyCode::Esc)), at(0));
        // Esc must NOT quit in filter mode — it cancels.
        assert_normal(&s);
        assert_eq!(s.ui.filter, "alpha", "Esc restores prior filter");
    }

    #[test]
    fn q_in_filter_mode_is_a_literal_q_not_quit() {
        let mut s = AppState::empty(at(0));
        handle_input(&mut s, Input::Key(key(KeyCode::Char('/'))), at(0));
        // 'q' should append to the buffer, not return Quit.
        let action = handle_input(&mut s, Input::Key(key(KeyCode::Char('q'))), at(0));
        assert_eq!(action, AppAction::Continue);
        assert_filter_buffer(&s, "q");
    }

    #[test]
    fn filter_commit_resets_selection_when_selected_falls_out_of_view() {
        let mut s = AppState::empty(at(0));
        s.apply_envelope(&job_created(1, "alpha"));
        s.apply_envelope(&job_created(2, "bravo"));
        s.apply_envelope(&job_created(3, "charlie"));
        s.ui.selected_job = Some(jid("charlie"));
        // Filter to "br" — only bravo matches; charlie does NOT
        // contain that substring (charlie does contain 'a' so a
        // single-letter filter like "a" would still match it).
        handle_input(&mut s, Input::Key(key(KeyCode::Char('/'))), at(0));
        handle_input(&mut s, Input::Key(key(KeyCode::Char('b'))), at(0));
        handle_input(&mut s, Input::Key(key(KeyCode::Char('r'))), at(0));
        handle_input(&mut s, Input::Key(key(KeyCode::Enter)), at(0));
        // selected_job must have moved off charlie onto the now-
        // visible bravo.
        assert_eq!(s.ui.selected_job, Some(jid("bravo")));
    }

    // ----- Detail view navigation (Phase 5a) -----

    fn assert_list(s: &AppState) {
        assert!(matches!(s.ui.view, View::List), "expected View::List");
    }

    fn assert_detail(s: &AppState, want_job: &str, want_tab: Tab) {
        match &s.ui.view {
            View::Detail { job_id, tab } => {
                assert_eq!(job_id.as_str(), want_job);
                assert_eq!(*tab, want_tab);
            }
            View::List => panic!("expected View::Detail, got List"),
        }
    }

    fn seed_two_jobs() -> AppState {
        let mut s = AppState::empty(at(0));
        s.apply_envelope(&job_created(1, "alpha"));
        s.apply_envelope(&job_created(2, "bravo"));
        s
    }

    #[test]
    fn enter_on_selected_job_opens_detail_with_overview_tab() {
        let mut s = seed_two_jobs();
        s.ui.selected_job = Some(jid("bravo"));
        handle_input(&mut s, Input::Key(key(KeyCode::Enter)), at(0));
        assert_detail(&s, "bravo", Tab::Overview);
    }

    #[test]
    fn enter_with_no_selection_does_not_open_detail() {
        let mut s = seed_two_jobs();
        s.ui.selected_job = None;
        handle_input(&mut s, Input::Key(key(KeyCode::Enter)), at(0));
        assert_list(&s);
    }

    #[test]
    fn esc_in_detail_returns_to_list_not_quit() {
        let mut s = seed_two_jobs();
        s.ui.selected_job = Some(jid("alpha"));
        handle_input(&mut s, Input::Key(key(KeyCode::Enter)), at(0));
        assert_detail(&s, "alpha", Tab::Overview);
        let action = handle_input(&mut s, Input::Key(key(KeyCode::Esc)), at(0));
        assert_eq!(action, AppAction::Continue, "Esc in Detail must NOT quit");
        assert_list(&s);
    }

    #[test]
    fn backspace_in_detail_also_returns_to_list() {
        let mut s = seed_two_jobs();
        s.ui.selected_job = Some(jid("alpha"));
        handle_input(&mut s, Input::Key(key(KeyCode::Enter)), at(0));
        handle_input(&mut s, Input::Key(key(KeyCode::Backspace)), at(0));
        assert_list(&s);
    }

    #[test]
    fn q_in_detail_still_quits() {
        let mut s = seed_two_jobs();
        s.ui.selected_job = Some(jid("alpha"));
        handle_input(&mut s, Input::Key(key(KeyCode::Enter)), at(0));
        let action = handle_input(&mut s, Input::Key(key(KeyCode::Char('q'))), at(0));
        assert_eq!(action, AppAction::Quit);
    }

    #[test]
    fn tab_key_cycles_tabs_forward_and_wraps() {
        let mut s = seed_two_jobs();
        s.ui.selected_job = Some(jid("alpha"));
        handle_input(&mut s, Input::Key(key(KeyCode::Enter)), at(0));
        // Overview → Workers → Errors → Plan → Verify → Overview
        let order = [
            Tab::Workers,
            Tab::Errors,
            Tab::Plan,
            Tab::Verify,
            Tab::Overview,
        ];
        for expected in order {
            handle_input(&mut s, Input::Key(key(KeyCode::Tab)), at(0));
            assert_detail(&s, "alpha", expected);
        }
    }

    #[test]
    fn backtab_key_cycles_tabs_backward_and_wraps() {
        let mut s = seed_two_jobs();
        s.ui.selected_job = Some(jid("alpha"));
        handle_input(&mut s, Input::Key(key(KeyCode::Enter)), at(0));
        // Overview ← Verify ← Plan ← Errors ← Workers ← Overview
        let order = [
            Tab::Verify,
            Tab::Plan,
            Tab::Errors,
            Tab::Workers,
            Tab::Overview,
        ];
        for expected in order {
            handle_input(&mut s, Input::Key(key(KeyCode::BackTab)), at(0));
            assert_detail(&s, "alpha", expected);
        }
    }

    #[test]
    fn list_view_keys_inert_after_entering_detail() {
        // / and s do navigation in List, but in Detail they're
        // operator typos — must NOT open filter mode or cycle sort.
        let mut s = seed_two_jobs();
        s.ui.selected_job = Some(jid("alpha"));
        handle_input(&mut s, Input::Key(key(KeyCode::Enter)), at(0));
        let prior_sort = s.ui.sort;
        handle_input(&mut s, Input::Key(key(KeyCode::Char('/'))), at(0));
        // input_mode must stay Normal — filter mode is List-only.
        assert!(matches!(s.ui.input_mode, InputMode::Normal));
        handle_input(&mut s, Input::Key(key(KeyCode::Char('s'))), at(0));
        // sort criterion unchanged.
        assert_eq!(s.ui.sort, prior_sort);
    }

    // ----- Workers tab navigation + modal (Phase 5b) -----

    use crate::state::WorkerSort;
    use migration_coord::schema::WorkerId as TestWorkerId;

    fn worker_joined(seq: u64, job: &str, wid: TestWorkerId, host: &str) -> EventEnvelope {
        EventEnvelope {
            seq,
            at: at(seq as i64),
            schema_version: SCHEMA_VERSION,
            worker_at: None,
            kind: EventKind::WorkerJoined {
                worker_id: wid,
                job_id: jid(job),
                host: host.into(),
                pid: 1000 + (seq as u32),
                start_time: at(0),
                version: "0.6.0".into(),
            },
        }
    }

    fn seed_job_with_workers() -> (AppState, [TestWorkerId; 3]) {
        let mut s = AppState::empty(at(0));
        s.apply_envelope(&job_created(1, "alpha"));
        let w1 = TestWorkerId::new();
        let w2 = TestWorkerId::new();
        let w3 = TestWorkerId::new();
        s.apply_envelope(&worker_joined(2, "alpha", w1, "host-1"));
        s.apply_envelope(&worker_joined(3, "alpha", w2, "host-2"));
        s.apply_envelope(&worker_joined(4, "alpha", w3, "host-3"));
        s.ui.selected_job = Some(jid("alpha"));
        // Enter detail and switch to Workers tab.
        handle_input(&mut s, Input::Key(key(KeyCode::Enter)), at(0));
        handle_input(&mut s, Input::Key(key(KeyCode::Tab)), at(0));
        (s, [w1, w2, w3])
    }

    fn assert_workers_tab(s: &AppState) {
        match &s.ui.view {
            View::Detail { tab, .. } => assert_eq!(*tab, Tab::Workers),
            View::List => panic!("expected Detail view"),
        }
    }

    #[test]
    fn down_arrow_in_workers_tab_selects_from_none_then_moves() {
        let (mut s, _wids) = seed_job_with_workers();
        assert_workers_tab(&s);
        assert!(s.ui.selected_worker.is_none());
        handle_input(&mut s, Input::Key(key(KeyCode::Down)), at(0));
        assert!(s.ui.selected_worker.is_some(), "Down must select a worker");
    }

    #[test]
    fn worker_selection_wraps_around_top_and_bottom() {
        let (mut s, _) = seed_job_with_workers();
        handle_input(&mut s, Input::Key(key(KeyCode::Home)), at(0));
        let first = s.ui.selected_worker.unwrap();
        handle_input(&mut s, Input::Key(key(KeyCode::End)), at(0));
        let last = s.ui.selected_worker.unwrap();
        assert_ne!(first, last);
        // Down from last → wraps to first.
        handle_input(&mut s, Input::Key(key(KeyCode::Down)), at(0));
        assert_eq!(s.ui.selected_worker, Some(first));
        // Up from first → wraps to last.
        handle_input(&mut s, Input::Key(key(KeyCode::Up)), at(0));
        assert_eq!(s.ui.selected_worker, Some(last));
    }

    #[test]
    fn s_in_workers_tab_cycles_worker_sort_not_job_sort() {
        let (mut s, _) = seed_job_with_workers();
        let prior_job_sort = s.ui.sort;
        assert_eq!(s.ui.worker_sort, WorkerSort::ByMbpsDesc);
        handle_input(&mut s, Input::Key(key(KeyCode::Char('s'))), at(0));
        assert_eq!(s.ui.worker_sort, WorkerSort::ByFilesDesc);
        assert_eq!(s.ui.sort, prior_job_sort, "job sort must NOT change");
    }

    #[test]
    fn enter_in_workers_tab_opens_modal_on_selected_worker() {
        let (mut s, _) = seed_job_with_workers();
        // Auto-select first via Home.
        handle_input(&mut s, Input::Key(key(KeyCode::Home)), at(0));
        let sel = s.ui.selected_worker.unwrap();
        handle_input(&mut s, Input::Key(key(KeyCode::Enter)), at(0));
        match &s.ui.modal {
            Some(Modal::WorkerDetail { worker_id }) => assert_eq!(*worker_id, sel),
            Some(other) => panic!("unexpected modal variant: {other:?}"),
            None => panic!("modal not opened"),
        }
    }

    #[test]
    fn enter_with_no_selection_auto_selects_first_then_opens_modal() {
        let (mut s, _) = seed_job_with_workers();
        assert!(s.ui.selected_worker.is_none());
        handle_input(&mut s, Input::Key(key(KeyCode::Enter)), at(0));
        // Both modal AND selection should now be set.
        assert!(s.ui.selected_worker.is_some());
        assert!(s.ui.modal.is_some());
    }

    #[test]
    fn esc_with_modal_open_closes_modal_not_view() {
        let (mut s, _) = seed_job_with_workers();
        handle_input(&mut s, Input::Key(key(KeyCode::Home)), at(0));
        handle_input(&mut s, Input::Key(key(KeyCode::Enter)), at(0));
        assert!(s.ui.modal.is_some());
        let action = handle_input(&mut s, Input::Key(key(KeyCode::Esc)), at(0));
        assert_eq!(action, AppAction::Continue);
        assert!(s.ui.modal.is_none(), "Esc must close modal");
        assert_workers_tab(&s);
    }

    #[test]
    fn navigation_keys_inert_when_modal_open() {
        // Up/Down/Tab/'s' must not affect anything while a modal is
        // up — the modal owns the input.
        let (mut s, _) = seed_job_with_workers();
        handle_input(&mut s, Input::Key(key(KeyCode::Home)), at(0));
        let pre_selection = s.ui.selected_worker;
        let pre_sort = s.ui.worker_sort;
        handle_input(&mut s, Input::Key(key(KeyCode::Enter)), at(0));
        assert!(s.ui.modal.is_some());

        for code in [
            KeyCode::Up,
            KeyCode::Down,
            KeyCode::Tab,
            KeyCode::BackTab,
            KeyCode::Char('s'),
            KeyCode::Char('/'),
        ] {
            handle_input(&mut s, Input::Key(key(code)), at(0));
        }
        // Modal still open, no state change.
        assert!(s.ui.modal.is_some(), "modal must stay open across navs");
        assert_eq!(s.ui.selected_worker, pre_selection);
        assert_eq!(s.ui.worker_sort, pre_sort);
        assert_workers_tab(&s);
    }

    #[test]
    fn q_with_modal_open_still_quits() {
        let (mut s, _) = seed_job_with_workers();
        handle_input(&mut s, Input::Key(key(KeyCode::Enter)), at(0));
        let action = handle_input(&mut s, Input::Key(key(KeyCode::Char('q'))), at(0));
        assert_eq!(action, AppAction::Quit);
    }

    // ----- Palette + confirm modal (Phase 6a) -----

    use crate::palette::PaletteCommand;

    fn assert_palette(s: &AppState) {
        assert!(
            matches!(s.ui.input_mode, InputMode::Palette { .. }),
            "expected Palette mode, got {:?}",
            s.ui.input_mode
        );
    }

    fn palette_buffer(s: &AppState) -> String {
        match &s.ui.input_mode {
            InputMode::Palette { buffer, .. } => buffer.clone(),
            _ => panic!("not in palette mode"),
        }
    }

    #[test]
    fn colon_opens_palette_from_list() {
        let mut s = seed_two_jobs();
        handle_input(&mut s, Input::Key(key(KeyCode::Char(':'))), at(0));
        assert_palette(&s);
        assert_eq!(palette_buffer(&s), "");
    }

    #[test]
    fn colon_opens_palette_from_detail() {
        let mut s = seed_two_jobs();
        s.ui.selected_job = Some(jid("alpha"));
        handle_input(&mut s, Input::Key(key(KeyCode::Enter)), at(0));
        // Now in Detail.
        handle_input(&mut s, Input::Key(key(KeyCode::Char(':'))), at(0));
        assert_palette(&s);
    }

    #[test]
    fn palette_chars_build_buffer() {
        let mut s = seed_two_jobs();
        handle_input(&mut s, Input::Key(key(KeyCode::Char(':'))), at(0));
        for c in ['p', 'a', 'u', 's', 'e'] {
            handle_input(&mut s, Input::Key(key(KeyCode::Char(c))), at(0));
        }
        assert_eq!(palette_buffer(&s), "pause");
    }

    #[test]
    fn palette_backspace_pops_chars() {
        let mut s = seed_two_jobs();
        handle_input(&mut s, Input::Key(key(KeyCode::Char(':'))), at(0));
        for c in ['p', 'a', 'u'] {
            handle_input(&mut s, Input::Key(key(KeyCode::Char(c))), at(0));
        }
        handle_input(&mut s, Input::Key(key(KeyCode::Backspace)), at(0));
        assert_eq!(palette_buffer(&s), "pa");
    }

    #[test]
    fn palette_tab_cycles_completion_from_empty_buffer() {
        let mut s = seed_two_jobs();
        handle_input(&mut s, Input::Key(key(KeyCode::Char(':'))), at(0));
        // First Tab → first verb in canonical order = "pause".
        handle_input(&mut s, Input::Key(key(KeyCode::Tab)), at(0));
        assert_eq!(palette_buffer(&s), "pause");
        // Next Tab → "resume".
        handle_input(&mut s, Input::Key(key(KeyCode::Tab)), at(0));
        assert_eq!(palette_buffer(&s), "resume");
    }

    #[test]
    fn palette_esc_cancels_back_to_normal() {
        let mut s = seed_two_jobs();
        handle_input(&mut s, Input::Key(key(KeyCode::Char(':'))), at(0));
        let action = handle_input(&mut s, Input::Key(key(KeyCode::Esc)), at(0));
        assert_eq!(action, AppAction::Continue);
        assert!(matches!(s.ui.input_mode, InputMode::Normal));
    }

    #[test]
    fn palette_enter_on_pause_with_default_dispatches_execute() {
        let mut s = seed_two_jobs();
        s.ui.selected_job = Some(jid("alpha"));
        handle_input(&mut s, Input::Key(key(KeyCode::Char(':'))), at(0));
        for c in ['p', 'a', 'u', 's', 'e'] {
            handle_input(&mut s, Input::Key(key(KeyCode::Char(c))), at(0));
        }
        let action = handle_input(&mut s, Input::Key(key(KeyCode::Enter)), at(0));
        match action {
            AppAction::Execute(PaletteCommand::Pause { job_id }) => {
                assert_eq!(job_id, jid("alpha"));
            }
            other => panic!("expected Execute(Pause), got {other:?}"),
        }
    }

    #[test]
    fn palette_enter_on_cancel_opens_confirm_modal() {
        let mut s = seed_two_jobs();
        s.ui.selected_job = Some(jid("alpha"));
        handle_input(&mut s, Input::Key(key(KeyCode::Char(':'))), at(0));
        for c in ['c', 'a', 'n', 'c', 'e', 'l'] {
            handle_input(&mut s, Input::Key(key(KeyCode::Char(c))), at(0));
        }
        let action = handle_input(&mut s, Input::Key(key(KeyCode::Enter)), at(0));
        assert_eq!(action, AppAction::Continue);
        match &s.ui.modal {
            Some(Modal::ConfirmCommand { command, .. }) => {
                assert!(matches!(command, PaletteCommand::Cancel { .. }));
            }
            other => panic!("expected ConfirmCommand modal, got {other:?}"),
        }
    }

    #[test]
    fn palette_enter_on_quit_returns_quit() {
        let mut s = seed_two_jobs();
        handle_input(&mut s, Input::Key(key(KeyCode::Char(':'))), at(0));
        for c in ['q', 'u', 'i', 't'] {
            handle_input(&mut s, Input::Key(key(KeyCode::Char(c))), at(0));
        }
        let action = handle_input(&mut s, Input::Key(key(KeyCode::Enter)), at(0));
        assert_eq!(action, AppAction::Quit);
    }

    #[test]
    fn palette_parse_error_surfaces_as_command_status() {
        let mut s = seed_two_jobs();
        handle_input(&mut s, Input::Key(key(KeyCode::Char(':'))), at(0));
        for c in ['n', 'u', 'k', 'e'] {
            handle_input(&mut s, Input::Key(key(KeyCode::Char(c))), at(0));
        }
        let action = handle_input(&mut s, Input::Key(key(KeyCode::Enter)), at(0));
        assert_eq!(action, AppAction::Continue);
        let cs = s.command_status.as_ref().expect("status set");
        assert_eq!(cs.kind, crate::state::CommandStatusKind::Error);
        assert!(cs.message.contains("unknown command"));
    }

    #[test]
    fn confirm_modal_y_executes_command() {
        let mut s = seed_two_jobs();
        s.ui.modal = Some(Modal::ConfirmCommand {
            command: PaletteCommand::Cancel {
                job_id: jid("alpha"),
            },
            summary: "cancel job 'alpha'?".into(),
        });
        let action = handle_input(&mut s, Input::Key(key(KeyCode::Char('y'))), at(0));
        match action {
            AppAction::Execute(PaletteCommand::Cancel { job_id }) => {
                assert_eq!(job_id, jid("alpha"));
            }
            other => panic!("expected Execute, got {other:?}"),
        }
        assert!(s.ui.modal.is_none(), "modal must close on confirm");
    }

    #[test]
    fn confirm_modal_n_cancels_without_execute() {
        let mut s = seed_two_jobs();
        s.ui.modal = Some(Modal::ConfirmCommand {
            command: PaletteCommand::Cancel {
                job_id: jid("alpha"),
            },
            summary: "x".into(),
        });
        let action = handle_input(&mut s, Input::Key(key(KeyCode::Char('n'))), at(0));
        assert_eq!(action, AppAction::Continue);
        assert!(s.ui.modal.is_none());
    }

    #[test]
    fn confirm_modal_esc_also_cancels() {
        let mut s = seed_two_jobs();
        s.ui.modal = Some(Modal::ConfirmCommand {
            command: PaletteCommand::Drain {
                job_id: jid("alpha"),
            },
            summary: "x".into(),
        });
        handle_input(&mut s, Input::Key(key(KeyCode::Esc)), at(0));
        assert!(s.ui.modal.is_none());
    }

    // ----- Help overlay (Phase 6b) -----

    #[test]
    fn question_mark_opens_help_modal_from_list() {
        let mut s = seed_two_jobs();
        handle_input(&mut s, Input::Key(key(KeyCode::Char('?'))), at(0));
        assert!(matches!(s.ui.modal, Some(Modal::Help)));
    }

    #[test]
    fn question_mark_opens_help_modal_from_detail() {
        let mut s = seed_two_jobs();
        s.ui.selected_job = Some(jid("alpha"));
        handle_input(&mut s, Input::Key(key(KeyCode::Enter)), at(0));
        handle_input(&mut s, Input::Key(key(KeyCode::Char('?'))), at(0));
        assert!(matches!(s.ui.modal, Some(Modal::Help)));
    }

    #[test]
    fn esc_closes_help_modal_without_quitting() {
        let mut s = seed_two_jobs();
        s.ui.modal = Some(Modal::Help);
        let action = handle_input(&mut s, Input::Key(key(KeyCode::Esc)), at(0));
        assert_eq!(action, AppAction::Continue);
        assert!(s.ui.modal.is_none());
    }

    #[test]
    fn question_mark_in_help_also_closes_it() {
        let mut s = seed_two_jobs();
        s.ui.modal = Some(Modal::Help);
        handle_input(&mut s, Input::Key(key(KeyCode::Char('?'))), at(0));
        assert!(s.ui.modal.is_none());
    }

    #[test]
    fn q_with_help_modal_still_quits() {
        let mut s = seed_two_jobs();
        s.ui.modal = Some(Modal::Help);
        let action = handle_input(&mut s, Input::Key(key(KeyCode::Char('q'))), at(0));
        assert_eq!(action, AppAction::Quit);
    }

    #[test]
    fn nav_keys_inert_when_help_modal_open() {
        let mut s = seed_two_jobs();
        s.ui.modal = Some(Modal::Help);
        s.ui.selected_job = Some(jid("alpha"));
        let before = s.ui.selected_job.clone();
        handle_input(&mut s, Input::Key(key(KeyCode::Down)), at(0));
        handle_input(&mut s, Input::Key(key(KeyCode::Tab)), at(0));
        handle_input(&mut s, Input::Key(key(KeyCode::Char('s'))), at(0));
        // Selection / sort / view untouched.
        assert_eq!(s.ui.selected_job, before);
        assert!(matches!(s.ui.view, View::List));
        assert!(s.ui.modal.is_some(), "help modal must stay open");
    }

    #[test]
    fn command_result_input_updates_banner_toast() {
        let mut s = seed_two_jobs();
        handle_input(
            &mut s,
            Input::CommandResult {
                ok: true,
                message: "pause 'alpha' ok".into(),
            },
            at(0),
        );
        let cs = s.command_status.as_ref().expect("status");
        assert_eq!(cs.kind, crate::state::CommandStatusKind::Ok);
        assert_eq!(cs.message, "pause 'alpha' ok");
    }
}
