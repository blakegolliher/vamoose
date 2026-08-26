use super::commands::spawn_command_dispatch;
use super::install_panic_hook;
use super::reducer::{handle_input, AppAction, Input};
use super::stream::sse_driver;
use super::terminal::{key_event_loop, panic_after_ms, TerminalGuard};
use crate::client::Client;
use crate::render;
use crate::state::AppState;
use chrono::Utc;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{mpsc, Mutex};
use tokio_util::sync::CancellationToken;

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

    // Prepare-progress poll: while there is no job to show, ask the
    // coord what `vamoose prepare` is doing.
    let prep_client = client.clone();
    let prep_state = Arc::clone(&app_state);
    let prep_tx = tx.clone();
    let prep_cancel = cancel.clone();
    let prep_handle = tokio::spawn(async move {
        prepare_poll_loop(prep_client, prep_state, prep_tx, prep_cancel).await;
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
    let _ = prep_handle.await;
    Ok(())
}

/// How often the list view re-asks the coord for `GET /prepare` while
/// it has no job to show. The reporter writes every ~5 s; the coord
/// re-reads the bucket every 15 s.
const PREPARE_POLL: Duration = Duration::from_secs(5);

/// Poll `GET /prepare` until cancelled, only while the snapshot has
/// no jobs. A failed request (older coord without the route, a
/// transport blip) keeps the last view rather than blanking it.
async fn prepare_poll_loop(
    client: Client,
    state: Arc<Mutex<AppState>>,
    tx: mpsc::Sender<Input>,
    cancel: CancellationToken,
) {
    let mut interval = tokio::time::interval(PREPARE_POLL);
    interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    loop {
        tokio::select! {
            biased;
            _ = cancel.cancelled() => return,
            _ = interval.tick() => {}
        }
        if !state.lock().await.snapshot.jobs.is_empty() {
            continue;
        }
        if let Ok(resp) = client.get_prepare().await {
            if tx.send(Input::Prepare(Box::new(resp))).await.is_err() {
                return;
            }
        }
    }
}
