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
use crate::state::{AppState, InputMode, Tab, View};
use chrono::{DateTime, Utc};
use crossterm::event::{KeyCode, KeyEvent, KeyEventKind};
use migration_coord::schema::JobId;
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
}

/// Whether the loop should keep running after handling an input.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AppAction {
    Continue,
    Quit,
}

/// Apply one [`Input`] to `state`. The render layer treats `now`
/// as the current wall clock (passed in so tests are deterministic).
pub fn handle_input(state: &mut AppState, input: Input, now: DateTime<Utc>) -> AppAction {
    match input {
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
    match &state.ui.input_mode {
        InputMode::Normal => handle_key_normal(state, key),
        InputMode::Filter { .. } => handle_key_filter(state, key),
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
        KeyCode::Char('s') => {
            state.ui.sort = state.ui.sort.cycle();
            AppAction::Continue
        }
        _ => AppAction::Continue,
    }
}

/// Detail-view dispatcher. Operator keys:
/// - `Esc` / `Backspace` → back to the jobs list. Convention: Esc
///   in TUI nav means "back", so it does NOT quit from Detail.
/// - `q` / `Q` → still quits the app from any view.
/// - `Tab` → next tab (wraps). `BackTab` / Shift-Tab → previous tab.
/// - everything else: no-op for now. Per-tab keybindings land in
///   the step that fleshes out each tab body.
fn handle_key_detail(state: &mut AppState, key: KeyEvent) -> AppAction {
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
        _ => AppAction::Continue,
    }
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
    let sse_handle =
        tokio::spawn(
            async move { sse_driver(client, sse_state, sse_tx, sse_cancel, sse_opts).await },
        );

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
                if handle_input(&mut state, input, Utc::now()) == AppAction::Quit {
                    break;
                }
                terminal.draw(|f| render::render(f, &state, Utc::now()))?;
            }
            _ = tick.tick() => {
                let state = app_state.lock().await;
                terminal.draw(|f| render::render(f, &state, Utc::now()))?;
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
}
