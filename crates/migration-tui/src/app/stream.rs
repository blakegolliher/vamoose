use super::reducer::Input;
use super::runtime::RunOpts;
use crate::client::{Client, ClientError, SseFrame};
use crate::state::AppState;
use chrono::{DateTime, Utc};
use std::sync::Arc;
use tokio::sync::{mpsc, Mutex};
use tokio_util::sync::CancellationToken;

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
