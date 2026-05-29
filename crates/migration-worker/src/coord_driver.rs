//! Driver task that owns the worker → coord HTTP session.
//!
//! `coord_driver::spawn` starts a background tokio task that:
//!
//! 1. Registers with the coord, retrying with exponential backoff
//!    until the coord accepts. A non-retryable error (4xx other
//!    than 5xx → see [`crate::coord_client::CoordError::is_retryable`])
//!    surfaces as the task's error result and the worker bails out.
//!
//! 2. Heartbeats at the configured cadence. Each successful
//!    heartbeat updates [`RunControl`] from the coord-supplied
//!    `control.mode`. Heartbeat failures fall back to the [`Backoff`]
//!    schedule between attempts; the heartbeat interval resumes
//!    after the first success.
//!
//! 3. Honors the `CancellationToken` on shutdown — both during
//!    register-backoff sleeps and between heartbeats.
//!
//! Step 4a deliberately does NOT plumb the outbound event channel
//! or the fence POST — those land in step 4b. The driver still
//! gives the orchestrator everything it needs to gate the claim loop
//! on coord-driven `Pause`/`Drain`/`Cancel`.

use crate::config::CoordCfg;
use crate::coord_client::{Backoff, CoordClient};
use crate::heartbeat::ProgressState;
use crate::run_control::RunControl;
use crate::throughput::ThroughputCounter;
use anyhow::Context;
use chrono::{DateTime, Utc};
use migration_coord::schema::{JobId, WorkerId, WorkerState};
use migration_coord::server::worker::HeartbeatBody;
use migration_core::fence::Fence;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{watch, RwLock};
use tokio_util::sync::CancellationToken;

/// Handle returned by [`spawn`]. The orchestrator clones the
/// `RunControl` once and gates its claim loop on it; the
/// `worker_id_rx` is mostly for diagnostics and step 4b (event
/// emission needs the worker id).
pub struct CoordDriverHandle {
    pub run_control: RunControl,
    pub worker_id: watch::Receiver<Option<WorkerId>>,
    pub task: tokio::task::JoinHandle<anyhow::Result<()>>,
}

/// Read-only inputs the driver samples for each heartbeat. The
/// driver does NOT mutate these — orchestration and shard processing
/// own the writes.
///
/// `fence_rx` is the consumer end of the heartbeat task's
/// `coord_fence` channel — when the heartbeat raises a self-fence
/// at one of its R6/R7/R8 sites it forwards the reason here, and
/// the driver loop POSTs `/workers/{id}/fence` with that reason
/// before shutting down. `None` when the worker runs in legacy
/// S3-only mode.
pub struct DriverInputs {
    pub progress: Arc<RwLock<ProgressState>>,
    pub throughput: ThroughputCounter,
    pub fence: Fence,
    pub fence_rx: Option<tokio::sync::mpsc::Receiver<String>>,
}

/// Build the [`CoordClient`] from config (resolving the cluster
/// secret from its env var, if configured) and spawn the driver
/// task. Validation errors (bad URL, missing env var) are returned
/// synchronously so the worker can fail fast at startup; runtime
/// HTTP errors are handled inside the task with backoff.
pub fn spawn(
    cfg: &CoordCfg,
    host_id: String,
    pid: u32,
    process_start: DateTime<Utc>,
    version: String,
    inputs: DriverInputs,
    cancel: CancellationToken,
) -> anyhow::Result<CoordDriverHandle> {
    let secret = if let Some(var) = &cfg.cluster_secret_env {
        let v = std::env::var(var)
            .with_context(|| format!("read coord cluster secret from env ${var}"))?;
        if v.is_empty() {
            anyhow::bail!("coord cluster_secret_env ${var} is set but empty");
        }
        Some(v)
    } else {
        None
    };

    let client = CoordClient::new(
        &cfg.url,
        secret.as_deref(),
        cfg.verify_tls,
        Duration::from_secs(cfg.request_timeout_sec),
    )
    .context("build CoordClient")?;

    let job_id = JobId::new(cfg.job_id.clone())
        .map_err(|e| anyhow::anyhow!("invalid coord.job_id {:?}: {e}", cfg.job_id))?;

    let run_control = RunControl::new();
    let (worker_id_tx, worker_id_rx) = watch::channel(None);

    let task = tokio::spawn(driver_loop(
        client,
        DriverParams {
            job_id,
            host_id,
            pid,
            process_start,
            version,
            heartbeat: Duration::from_secs(cfg.heartbeat_sec),
        },
        inputs,
        run_control.clone(),
        worker_id_tx,
        cancel,
    ));

    Ok(CoordDriverHandle {
        run_control,
        worker_id: worker_id_rx,
        task,
    })
}

struct DriverParams {
    job_id: JobId,
    host_id: String,
    pid: u32,
    process_start: DateTime<Utc>,
    version: String,
    heartbeat: Duration,
}

async fn driver_loop(
    client: CoordClient,
    params: DriverParams,
    mut inputs: DriverInputs,
    run_control: RunControl,
    worker_id_tx: watch::Sender<Option<WorkerId>>,
    cancel: CancellationToken,
) -> anyhow::Result<()> {
    // ----------------------------------------------------------------
    // Phase A — register, backing off on transport / 5xx failures.
    // ----------------------------------------------------------------
    let worker_id = match register_with_backoff(&client, &params, &cancel).await? {
        Some(id) => id,
        None => {
            // Cancelled before we got a WorkerId.
            tracing::info!("coord_driver: cancelled before register; exiting");
            return Ok(());
        }
    };
    let _ = worker_id_tx.send(Some(worker_id));
    tracing::info!(worker_id = %worker_id, "coord_driver: registered");

    // ----------------------------------------------------------------
    // Phase B — heartbeat loop. Sleep `heartbeat` on success;
    // sleep backoff.current() and step() on failure. Both sleeps are
    // cancellable. A signal on `fence_rx` (set when the heartbeat
    // task tripped a self-fence) wins over the sleep — we POST
    // `/workers/{id}/fence` immediately, then exit. The orchestrator
    // sees the fence trip via its own `fence.is_valid()` check on the
    // next loop iteration; the coord POST is purely informational.
    // ----------------------------------------------------------------
    let mut backoff = Backoff::default_schedule();
    let mut consecutive_failures: u64 = 0;
    loop {
        let delay = if consecutive_failures == 0 {
            params.heartbeat
        } else {
            backoff.current()
        };
        // Take ownership of fence_rx via Option::as_mut so the
        // select! can await it. When fence_rx is None we still need
        // a future to put in the arm; a `pending()` future that
        // never resolves is the simplest way to disable the arm
        // without restructuring the select.
        let fence_fut = async {
            match inputs.fence_rx.as_mut() {
                Some(rx) => rx.recv().await,
                None => std::future::pending::<Option<String>>().await,
            }
        };
        tokio::select! {
            biased;
            _ = cancel.cancelled() => {
                tracing::info!("coord_driver: cancelled; exiting heartbeat loop");
                return Ok(());
            }
            reason_opt = fence_fut => {
                // None means the heartbeat task's sender was dropped
                // without sending — treat as a regular shutdown.
                let Some(reason) = reason_opt else {
                    tracing::info!("coord_driver: fence channel closed without trip; exiting");
                    return Ok(());
                };
                tracing::warn!(reason = %reason,
                    "coord_driver: heartbeat self-fenced; posting /fence");
                match client.fence(worker_id, reason.clone()).await {
                    Ok(resp) => {
                        tracing::info!(seq = resp.seq,
                            "coord_driver: /fence accepted");
                    }
                    Err(e) => {
                        // Fence is already in effect locally; coord
                        // missing the announcement is non-fatal —
                        // the operator may need to mark the worker
                        // disconnected manually but the worker
                        // itself is safely fenced regardless.
                        tracing::warn!(error = %e,
                            "coord_driver: /fence POST failed (non-fatal); exiting");
                    }
                }
                return Ok(());
            }
            _ = tokio::time::sleep(delay) => {}
        }

        let body = sample_heartbeat(&inputs, &run_control).await;
        match client.heartbeat(worker_id, body).await {
            Ok(resp) => {
                let prev = run_control.mode();
                run_control.set(resp.control.mode);
                if prev != resp.control.mode {
                    tracing::info!(
                        from = ?prev,
                        to = ?resp.control.mode,
                        "coord_driver: control mode changed",
                    );
                }
                consecutive_failures = 0;
                backoff.reset();
            }
            Err(e) if !e.is_retryable() => {
                tracing::error!(error = %e, "coord_driver: non-retryable heartbeat error; exiting");
                anyhow::bail!("coord heartbeat failed (non-retryable): {e}");
            }
            Err(e) => {
                consecutive_failures += 1;
                let next = backoff.step();
                tracing::warn!(
                    error = %e,
                    consecutive_failures,
                    next_retry_ms = next.as_millis() as u64,
                    "coord_driver: heartbeat failed; will retry",
                );
            }
        }
    }
}

async fn register_with_backoff(
    client: &CoordClient,
    params: &DriverParams,
    cancel: &CancellationToken,
) -> anyhow::Result<Option<WorkerId>> {
    let mut backoff = Backoff::default_schedule();
    loop {
        tokio::select! {
            biased;
            _ = cancel.cancelled() => return Ok(None),
            res = client.register(
                &params.job_id,
                params.host_id.clone(),
                params.pid,
                params.process_start,
                params.version.clone(),
            ) => {
                match res {
                    Ok(reg) => return Ok(Some(reg.worker_id)),
                    Err(e) if !e.is_retryable() => {
                        anyhow::bail!("coord register failed (non-retryable): {e}");
                    }
                    Err(e) => {
                        let delay = backoff.current();
                        tracing::warn!(
                            error = %e,
                            delay_ms = delay.as_millis() as u64,
                            "coord_driver: register failed; backing off",
                        );
                        backoff.step();
                        tokio::select! {
                            biased;
                            _ = cancel.cancelled() => return Ok(None),
                            _ = tokio::time::sleep(delay) => {}
                        }
                    }
                }
            }
        }
    }
}

/// Build a [`HeartbeatBody`] from the worker's current internal
/// state. Derives [`WorkerState`] from `fence`, `run_control`, and
/// whether a shard is currently held:
///
/// - `fence.is_valid() == false` → `Fenced` (R7/R8 in effect)
/// - `run_control.is_terminating()` → `Draining`
/// - `current_shard.is_some()` → `Copying`
/// - else → `Idle`
async fn sample_heartbeat(inputs: &DriverInputs, run_control: &RunControl) -> HeartbeatBody {
    let progress = inputs.progress.read().await;
    let state = if !inputs.fence.is_valid() {
        WorkerState::Fenced
    } else if run_control.is_terminating() {
        WorkerState::Draining
    } else if progress.current_shard.is_some() {
        WorkerState::Copying
    } else {
        WorkerState::Idle
    };
    // Sample the rolling throughput on every tick — same 60s window
    // the legacy S3 progress writer uses, for shape parity.
    let bytes_per_sec = inputs.throughput.sample_mb_s(60) * 1024.0 * 1024.0;
    // files_per_sec and errors_per_min derivation needs a rolling
    // window we don't maintain on the worker side today. Phase 3.5
    // surfaces that; for now report 0 and let the coord show the
    // counters that DO flow through the event log instead.
    HeartbeatBody {
        state,
        files_per_sec: 0.0,
        bytes_per_sec,
        errors_per_min: 0.0,
        inflight_ops: 0,
        queue_depth: 0,
    }
}

// =============================================================================
// Tests — driver-loop-shape only. The end-to-end happy path is
// covered by tests/coord_driver_integration.rs (step 5).
// =============================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use crate::heartbeat::ProgressState;

    #[tokio::test]
    async fn sample_heartbeat_reports_fenced_when_fence_is_tripped() {
        let progress = Arc::new(RwLock::new(ProgressState::new()));
        let throughput = ThroughputCounter::new();
        let fence = Fence::new();
        fence.trip("test-trip");
        let inputs = DriverInputs {
            progress,
            throughput,
            fence,
            fence_rx: None,
        };
        let rc = RunControl::new();
        let body = sample_heartbeat(&inputs, &rc).await;
        assert!(matches!(body.state, WorkerState::Fenced));
    }

    #[tokio::test]
    async fn sample_heartbeat_reports_draining_on_terminate_mode() {
        let progress = Arc::new(RwLock::new(ProgressState::new()));
        let throughput = ThroughputCounter::new();
        let fence = Fence::new();
        let inputs = DriverInputs {
            progress,
            throughput,
            fence,
            fence_rx: None,
        };
        let rc = RunControl::with_mode(crate::coord_client::ControlMode::Drain);
        let body = sample_heartbeat(&inputs, &rc).await;
        assert!(matches!(body.state, WorkerState::Draining));
        let rc2 = RunControl::with_mode(crate::coord_client::ControlMode::Cancel);
        let body2 = sample_heartbeat(&inputs, &rc2).await;
        assert!(matches!(body2.state, WorkerState::Draining));
    }

    #[tokio::test]
    async fn sample_heartbeat_reports_copying_when_shard_held() {
        let mut p = ProgressState::new();
        p.current_shard = Some("part-0042".to_string());
        let progress = Arc::new(RwLock::new(p));
        let throughput = ThroughputCounter::new();
        let fence = Fence::new();
        let inputs = DriverInputs {
            progress,
            throughput,
            fence,
            fence_rx: None,
        };
        let rc = RunControl::new();
        let body = sample_heartbeat(&inputs, &rc).await;
        assert!(matches!(body.state, WorkerState::Copying));
    }

    #[tokio::test]
    async fn sample_heartbeat_reports_idle_by_default() {
        let progress = Arc::new(RwLock::new(ProgressState::new()));
        let throughput = ThroughputCounter::new();
        let fence = Fence::new();
        let inputs = DriverInputs {
            progress,
            throughput,
            fence,
            fence_rx: None,
        };
        let rc = RunControl::new();
        let body = sample_heartbeat(&inputs, &rc).await;
        assert!(matches!(body.state, WorkerState::Idle));
    }
}
