use super::reducer::Input;
use crate::client::Client;
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

/// Fire-and-forget HTTP dispatch for a parsed [`PaletteCommand`].
/// Runs in its own tokio task so the render loop never blocks on a
/// slow coord. On completion sends `Input::CommandResult` back
/// through the main channel so the operator gets a banner toast.
pub(super) fn spawn_command_dispatch(
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
