use super::keys::handle_key;
use crate::client::SseFrame;
use crate::state::AppState;
use chrono::{DateTime, Utc};
use crossterm::event::KeyEvent;

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
