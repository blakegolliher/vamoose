use super::reducer::Input;
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

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
        super::restore_terminal();
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
pub(super) fn panic_after_ms(raw: Option<&str>) -> Option<u64> {
    raw?.trim().parse().ok()
}

/// Restore the terminal on Drop. Bypasses anyhow — restore should
/// happen even if the user kills the process via Ctrl-C and main
/// is unwound.
pub(super) struct TerminalGuard;
impl Drop for TerminalGuard {
    fn drop(&mut self) {
        super::restore_terminal();
    }
}

/// Key event task: pulls Crossterm events asynchronously and
/// forwards Key events to the main loop. Resize / mouse / focus
/// events are dropped on the floor for now — the render layer
/// re-lays-out on every tick regardless.
pub(super) async fn key_event_loop(tx: mpsc::Sender<Input>, cancel: CancellationToken) {
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
