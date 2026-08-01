use super::reducer::AppAction;
use crate::render;
use crate::state::{AppState, InputMode, Modal, Tab, View};
use crossterm::event::{KeyCode, KeyEvent, KeyEventKind};
use migration_control_protocol::schema::{JobId, WorkerId};

pub(super) fn handle_key(state: &mut AppState, key: KeyEvent) -> AppAction {
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
