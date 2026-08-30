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
//! 3. Drains coalesced worker progress events to the coordinator and reports a
//!    local self-fence through the worker fence endpoint.
//! 4. Honors the `CancellationToken` during registration, heartbeat, event
//!    draining, and shutdown.

use crate::config::CoordCfg;
use crate::coord_client::{Backoff, CoordClient};
use crate::heartbeat::ProgressState;
use crate::run_control::RunControl;
use crate::throughput::ThroughputCounter;
use anyhow::Context;
use chrono::{DateTime, Utc};
use migration_control_protocol::schema::HeartbeatBody;
use migration_control_protocol::schema::{JobId, WorkerId, WorkerState};
use migration_core::fence::Fence;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{watch, RwLock};
use tokio_util::sync::CancellationToken;

/// Handle returned by [`spawn`]. The orchestrator clones the
/// `RunControl` once and gates its claim loop on it; the
/// `worker_id` exposes successful registration for diagnostics and tests.
pub struct CoordDriverHandle {
    pub run_control: RunControl,
    pub worker_id: watch::Receiver<Option<WorkerId>>,
    pub task: tokio::task::JoinHandle<anyhow::Result<()>>,
}

/// Read-only inputs the driver samples for each heartbeat. The
/// driver does NOT mutate these — orchestration and shard processing
/// own the writes.
///
/// - `fence_rx` is the consumer end of the heartbeat task's
///   `coord_fence` channel — when the heartbeat raises a self-fence
///   at one of its R6/R7/R8 sites it forwards the reason here, and
///   the driver loop POSTs `/workers/{id}/fence` with that reason
///   before shutting down. `None` when the worker runs in legacy
///   S3-only mode.
///
/// - `events_rx` is the consumer end of the in-process events
///   channel. The shard processor pushes [`WorkerEventDraft`]s here
///   as files succeed / fail / get fenced; the driver coalesces them
///   over a 1s window and POSTs to `/workers/{id}/events`. `None`
///   when the worker runs in legacy mode.
pub struct DriverInputs {
    pub progress: Arc<RwLock<ProgressState>>,
    /// Live per-row counters (see `heartbeat::LivePending`); sampled
    /// for the heartbeat's `inflight_ops` gauge (rows started minus
    /// rows recorded — floors at 0 during phases that don't stamp
    /// starts, never reads high).
    pub live: Arc<crate::heartbeat::LivePending>,
    pub throughput: ThroughputCounter,
    pub fence: Fence,
    pub fence_rx: Option<tokio::sync::mpsc::Receiver<String>>,
    pub events_rx: Option<tokio::sync::mpsc::Receiver<WorkerEventDraft>>,
}

/// In-process event the shard processor (and any future caller)
/// hands to the coord_driver for forwarding to the coord. The
/// processor does NOT carry `worker_id` — it isn't known at
/// processor-construction time (register completes asynchronously).
/// The driver materializes the full
/// [`EventKind`](migration_control_protocol::schema::EventKind) envelope by
/// attaching `worker_id` and `job_id` at POST time.
///
/// v1 only emits per-file progress counts (Ok / Failed / Fenced).
/// Richer detail (`ErrorEmitted` with class + path) can be added in
/// a follow-on step without changing the channel shape — just add a
/// new variant.
#[derive(Debug, Clone)]
pub enum WorkerEventDraft {
    /// One file completed successfully — fold into the next
    /// coalesced `ProgressDelta`.
    ProgressOk { bytes: u64 },
    /// One file failed (per-file failure, not a fence trip). Folded
    /// into `errors_delta` on the next ProgressDelta. The full
    /// `ErrorEmitted` event (class, path) is a future enhancement.
    ProgressFailed,
    /// One file bailed out at the mover's R8 fence check. Surfaced
    /// to coord as `errors_delta` for visibility but not flagged as
    /// a worker failure — the next reclaimer copies the row.
    ProgressFenced,
}

/// Sender-side handle wrapping `Option<Sender<WorkerEventDraft>>`.
/// Construct with [`EventEmitter::disabled`] for legacy / no-coord
/// mode — every call becomes a no-op. Construct via
/// [`EventEmitter::from_sender`] when the orchestrator has wired the
/// channel.
///
/// All methods use `try_send` so the caller (shard processor) is
/// never blocked. On full channel the draft is dropped and a counter
/// could be bumped (deferred to a future revision — for now the
/// coord just sees lower progress numbers).
#[derive(Debug, Clone)]
pub struct EventEmitter {
    tx: Option<tokio::sync::mpsc::Sender<WorkerEventDraft>>,
}

impl EventEmitter {
    pub fn disabled() -> Self {
        Self { tx: None }
    }

    pub fn from_sender(tx: tokio::sync::mpsc::Sender<WorkerEventDraft>) -> Self {
        Self { tx: Some(tx) }
    }

    pub fn is_enabled(&self) -> bool {
        self.tx.is_some()
    }

    pub fn progress_ok(&self, bytes: u64) {
        if let Some(tx) = &self.tx {
            let _ = tx.try_send(WorkerEventDraft::ProgressOk { bytes });
        }
    }

    pub fn progress_failed(&self) {
        if let Some(tx) = &self.tx {
            let _ = tx.try_send(WorkerEventDraft::ProgressFailed);
        }
    }

    pub fn progress_fenced(&self) {
        if let Some(tx) = &self.tx {
            let _ = tx.try_send(WorkerEventDraft::ProgressFenced);
        }
    }
}

impl Default for EventEmitter {
    fn default() -> Self {
        Self::disabled()
    }
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

    // The orchestrator resolves an omitted `[coord] job_id` to the
    // manifest's run id before spawning (see `CoordCfg::job_id`); by
    // the time we get here the id must be concrete.
    let raw_job_id = cfg
        .job_id
        .as_deref()
        .ok_or_else(|| anyhow::anyhow!("coord.job_id is unresolved (no manifest run id)"))?;
    let job_id = JobId::new(raw_job_id.to_string())
        .map_err(|e| anyhow::anyhow!("invalid coord.job_id {raw_job_id:?}: {e}"))?;

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
            events_flush: Duration::from_secs(cfg.events_flush_sec.max(1)),
            buffer_max_bytes: cfg.buffer_max_bytes as usize,
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
    events_flush: Duration,
    buffer_max_bytes: usize,
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
    // Phase A.5 — spawn the events drainer if a channel is wired.
    //
    // The drainer has its own cancellation token so the heartbeat
    // loop's exit path can stop it independently of the outer
    // `cancel` (which the orchestrator may not have tripped yet on
    // a fence-driven shutdown). Heartbeat-loop exit ALWAYS cancels
    // and joins the drainer via the cleanup block at the end.
    // ----------------------------------------------------------------
    let drainer_cancel = CancellationToken::new();
    let drainer_task: Option<tokio::task::JoinHandle<anyhow::Result<()>>> =
        match inputs.events_rx.take() {
            Some(events_rx) => Some(tokio::spawn(events_drainer(
                client.clone(),
                worker_id,
                params.job_id.clone(),
                events_rx,
                params.events_flush,
                params.buffer_max_bytes,
                drainer_cancel.clone(),
            ))),
            None => None,
        };

    // ----------------------------------------------------------------
    // Phase B — heartbeat loop. Sleep `heartbeat` on success;
    // sleep backoff.current() and step() on failure. Both sleeps are
    // cancellable. A signal on `fence_rx` (set when the heartbeat
    // task tripped a self-fence) wins over the sleep — we POST
    // `/workers/{id}/fence` immediately, then exit. The orchestrator
    // sees the fence trip via its own `fence.is_valid()` check on the
    // next loop iteration; the coord POST is purely informational.
    // ----------------------------------------------------------------
    let outcome = heartbeat_loop(
        &client,
        worker_id,
        &params,
        &mut inputs,
        &run_control,
        &cancel,
    )
    .await;

    // ----------------------------------------------------------------
    // Phase C — tear down the drainer. Cancel its token (so it
    // stops accepting new ticks) and bound the join so a wedged
    // HTTP connection cannot hold the driver task open forever.
    // The drainer still drains anything already-buffered before
    // observing the cancel, so per-file progress emitted right
    // before exit still has a chance to land at the coord.
    // ----------------------------------------------------------------
    drainer_cancel.cancel();
    if let Some(t) = drainer_task {
        match tokio::time::timeout(Duration::from_secs(5), t).await {
            Ok(Ok(Ok(()))) => tracing::debug!("events_drainer: clean exit"),
            Ok(Ok(Err(e))) => tracing::warn!(error = %e, "events_drainer returned error"),
            Ok(Err(e)) => tracing::warn!(join_error = %e, "events_drainer join failed"),
            Err(_) => tracing::warn!("events_drainer did not exit within 5s"),
        }
    }
    outcome
}

async fn heartbeat_loop(
    client: &CoordClient,
    worker_id: WorkerId,
    params: &DriverParams,
    inputs: &mut DriverInputs,
    run_control: &RunControl,
    cancel: &CancellationToken,
) -> anyhow::Result<()> {
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
                // Orderly exit: tell the coord so we read as
                // Disconnected at once. Bounded and best-effort — the
                // liveness sweep covers us if this never lands.
                match tokio::time::timeout(
                    Duration::from_secs(3),
                    client.leave(worker_id, "worker exiting".into()),
                )
                .await
                {
                    Ok(Ok(resp)) => tracing::info!(seq = resp.seq, "coord_driver: /leave accepted"),
                    Ok(Err(e)) => tracing::warn!(error = %e, "coord_driver: /leave POST failed (non-fatal)"),
                    Err(_) => tracing::warn!("coord_driver: /leave POST timed out (non-fatal)"),
                }
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

        let body = sample_heartbeat(inputs, run_control).await;
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

// =============================================================================
// Events drainer — coalesce ProgressDelta drafts into ~1Hz POSTs.
// =============================================================================

/// Per-window accumulator. Folds incoming drafts into running sums;
/// `flush` materializes the running sums as one `EventEnvelope`
/// (or `None` if no drafts arrived since the last flush).
#[derive(Debug, Default)]
struct ProgressAccum {
    files_delta: u64,
    bytes_delta: u64,
    errors_delta: u64,
}

impl ProgressAccum {
    fn absorb(&mut self, draft: WorkerEventDraft) {
        match draft {
            WorkerEventDraft::ProgressOk { bytes } => {
                self.files_delta = self.files_delta.saturating_add(1);
                self.bytes_delta = self.bytes_delta.saturating_add(bytes);
            }
            WorkerEventDraft::ProgressFailed | WorkerEventDraft::ProgressFenced => {
                self.errors_delta = self.errors_delta.saturating_add(1);
            }
        }
    }

    fn is_empty(&self) -> bool {
        self.files_delta == 0 && self.bytes_delta == 0 && self.errors_delta == 0
    }

    fn take_envelope(
        &mut self,
        job_id: JobId,
        worker_id: WorkerId,
        at: DateTime<Utc>,
    ) -> Option<migration_control_protocol::schema::EventEnvelope> {
        if self.is_empty() {
            return None;
        }
        let env = migration_control_protocol::schema::EventEnvelope {
            seq: 0, // coord assigns
            at,
            schema_version: migration_control_protocol::schema::SCHEMA_VERSION,
            worker_at: Some(at),
            client_seq: None,
            from_worker: None,
            kind: migration_control_protocol::schema::EventKind::ProgressDelta {
                job_id,
                worker_id,
                files_delta: self.files_delta,
                bytes_delta: self.bytes_delta,
                errors_delta: self.errors_delta,
            },
        };
        *self = Self::default();
        Some(env)
    }
}

/// Drainer task body. Owns `events_rx`, `buffer`, and the periodic
/// flusher. Exits cleanly when `cancel` fires OR the sender is
/// dropped (orchestrator hands the EventEmitter to ShardProcessor;
/// when that drops, the channel closes here and we drain the rest).
async fn events_drainer(
    client: CoordClient,
    worker_id: WorkerId,
    job_id: JobId,
    mut events_rx: tokio::sync::mpsc::Receiver<WorkerEventDraft>,
    flush_interval: Duration,
    buffer_max_bytes: usize,
    cancel: CancellationToken,
) -> anyhow::Result<()> {
    let mut buffer = crate::coord_client::EventBuffer::new(buffer_max_bytes);
    let mut accum = ProgressAccum::default();
    let mut ticker = tokio::time::interval(flush_interval);
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    // Skip the first immediate tick that interval() fires — we want
    // the first flush to wait the full interval after start.
    ticker.tick().await;

    let mut channel_closed = false;
    loop {
        tokio::select! {
            biased;
            _ = cancel.cancelled() => {
                tracing::debug!("events_drainer: cancelled");
                break;
            }
            maybe = events_rx.recv(), if !channel_closed => {
                match maybe {
                    Some(draft) => {
                        accum.absorb(draft);
                        // Drain anything else immediately available so a
                        // burst from the processor doesn't require many
                        // round-trips through select!.
                        while let Ok(d) = events_rx.try_recv() {
                            accum.absorb(d);
                        }
                    }
                    None => {
                        // Sender side dropped — no more drafts will
                        // arrive. Disable this arm and keep ticking
                        // until the buffer drains, then exit.
                        channel_closed = true;
                        tracing::debug!("events_drainer: channel closed; draining remaining");
                    }
                }
            }
            _ = ticker.tick() => {
                if let Some(env) = accum.take_envelope(job_id.clone(), worker_id, chrono::Utc::now()) {
                    buffer.push(env);
                }
                if !buffer.is_empty() {
                    drain_one_batch(&client, worker_id, &mut buffer).await;
                }
                if channel_closed && buffer.is_empty() {
                    break;
                }
            }
        }
    }

    // Final flush — absorb any drafts still in the channel, materialize
    // the accumulator, and try to ship one more batch. Best-effort: a
    // wedged coord here doesn't block the bounded outer join.
    while let Ok(d) = events_rx.try_recv() {
        accum.absorb(d);
    }
    if let Some(env) = accum.take_envelope(job_id, worker_id, chrono::Utc::now()) {
        buffer.push(env);
    }
    if !buffer.is_empty() {
        drain_one_batch(&client, worker_id, &mut buffer).await;
    }
    Ok(())
}

/// POST one batch of events. On failure, requeue the batch to the
/// front of the buffer so order is preserved for the next attempt.
/// Caller is responsible for the ticker-driven cadence between calls.
async fn drain_one_batch(
    client: &CoordClient,
    worker_id: WorkerId,
    buffer: &mut crate::coord_client::EventBuffer,
) {
    const MAX_BATCH: usize = 64;
    let batch = buffer.drain_batch(MAX_BATCH);
    if batch.is_empty() {
        return;
    }
    match client.events_batch(worker_id, batch.clone()).await {
        Ok(seqs) => {
            tracing::debug!(
                n = seqs.len(),
                first_seq = seqs.first().copied().unwrap_or(0),
                last_seq = seqs.last().copied().unwrap_or(0),
                "events_drainer: batch posted",
            );
        }
        Err(e) => {
            tracing::warn!(
                error = %e,
                n = batch.len(),
                "events_drainer: POST failed; requeuing for next tick",
            );
            buffer.requeue_front(batch);
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
                    Err(e) if e.is_job_not_found() => {
                        // The coord is up but has not seeded this job
                        // yet — normal when workers are enabled at
                        // install time and the coord seeds from a
                        // manifest that appears later. Keep waiting at
                        // the slowest backoff step; a wrong job_id
                        // shows up as this warning repeating forever.
                        backoff.saturate();
                        let delay = backoff.current();
                        tracing::warn!(
                            job_id = %params.job_id,
                            delay_ms = delay.as_millis() as u64,
                            "coord_driver: job not found on coord; waiting for it to be \
                             seeded (check [coord] job_id if this persists)",
                        );
                        tokio::select! {
                            biased;
                            _ = cancel.cancelled() => return Ok(None),
                            _ = tokio::time::sleep(delay) => {}
                        }
                    }
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
    // files_per_sec and errors_per_min derivation needs a rolling window we do
    // not maintain on the worker side. Report 0 and let the coord show the
    // counters that do flow through the event log instead.
    HeartbeatBody {
        state,
        files_per_sec: 0.0,
        bytes_per_sec,
        errors_per_min: 0.0,
        inflight_ops: inputs.live.inflight() as u32,
        // No real queue concept in the worker: batches admit rows
        // straight into the inflight window. Reported as 0 honestly
        // rather than inventing a number.
        queue_depth: 0,
        latency: migration_core::latency::latest().map(latency_to_wire),
    }
}

/// `migration_core::latency::Summary` → the control-protocol mirror
/// (field for field; the protocol crate does not depend on core).
fn latency_to_wire(
    s: migration_core::latency::Summary,
) -> migration_control_protocol::schema::LatencySummary {
    migration_control_protocol::schema::LatencySummary {
        window_secs: s.window_secs,
        pairs: s.pairs,
        src_busy_pct: s.src_busy_pct,
        dst_busy_pct: s.dst_busy_pct,
        s3_wait_pct: s.s3_wait_pct,
        ops: s
            .ops
            .into_iter()
            .map(|o| migration_control_protocol::schema::OpLatency {
                side: o.side,
                op: o.op,
                count: o.count,
                mean_us: o.mean_us,
                p50_us: o.p50_us,
                p95_us: o.p95_us,
                p99_us: o.p99_us,
                max_us: o.max_us,
                total_us: o.total_us,
            })
            .collect(),
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
            live: Arc::new(crate::heartbeat::LivePending::default()),
            progress,
            throughput,
            fence,
            fence_rx: None,
            events_rx: None,
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
            live: Arc::new(crate::heartbeat::LivePending::default()),
            progress,
            throughput,
            fence,
            fence_rx: None,
            events_rx: None,
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
            live: Arc::new(crate::heartbeat::LivePending::default()),
            progress,
            throughput,
            fence,
            fence_rx: None,
            events_rx: None,
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
            live: Arc::new(crate::heartbeat::LivePending::default()),
            progress,
            throughput,
            fence,
            fence_rx: None,
            events_rx: None,
        };
        let rc = RunControl::new();
        let body = sample_heartbeat(&inputs, &rc).await;
        assert!(matches!(body.state, WorkerState::Idle));
    }

    // =========================================================================
    // EventEmitter — disabled / enabled / drop behavior
    // =========================================================================

    #[test]
    fn disabled_emitter_silently_swallows_drafts() {
        let e = EventEmitter::disabled();
        assert!(!e.is_enabled());
        // None of these should panic or block.
        e.progress_ok(1024);
        e.progress_failed();
        e.progress_fenced();
    }

    #[tokio::test]
    async fn enabled_emitter_pushes_drafts_to_channel() {
        let (tx, mut rx) = tokio::sync::mpsc::channel(16);
        let e = EventEmitter::from_sender(tx);
        assert!(e.is_enabled());
        e.progress_ok(1024);
        e.progress_ok(2048);
        e.progress_failed();
        e.progress_fenced();

        let mut got = Vec::new();
        while let Ok(d) = rx.try_recv() {
            got.push(d);
        }
        assert_eq!(got.len(), 4);
        assert!(matches!(
            got[0],
            WorkerEventDraft::ProgressOk { bytes: 1024 }
        ));
        assert!(matches!(
            got[1],
            WorkerEventDraft::ProgressOk { bytes: 2048 }
        ));
        assert!(matches!(got[2], WorkerEventDraft::ProgressFailed));
        assert!(matches!(got[3], WorkerEventDraft::ProgressFenced));
    }

    #[tokio::test]
    async fn full_channel_drops_drafts_without_blocking() {
        let (tx, mut rx) = tokio::sync::mpsc::channel(2);
        let e = EventEmitter::from_sender(tx);
        // Fill the channel.
        e.progress_ok(1);
        e.progress_ok(2);
        // These should silently drop.
        e.progress_ok(3);
        e.progress_ok(4);

        let mut got = 0;
        while rx.try_recv().is_ok() {
            got += 1;
        }
        assert_eq!(got, 2, "exactly the channel capacity should land");
    }

    // =========================================================================
    // ProgressAccum — coalesce semantics
    // =========================================================================

    fn at(secs: i64) -> chrono::DateTime<chrono::Utc> {
        chrono::TimeZone::timestamp_opt(&chrono::Utc, secs, 0).unwrap()
    }

    fn jid(s: &str) -> JobId {
        JobId::new(s).unwrap()
    }

    #[test]
    fn empty_accum_yields_no_envelope() {
        let mut a = ProgressAccum::default();
        assert!(a.is_empty());
        assert!(a
            .take_envelope(jid("bobby"), WorkerId::new(), at(0))
            .is_none());
    }

    #[test]
    fn accum_sums_drafts_and_resets_on_take() {
        let mut a = ProgressAccum::default();
        a.absorb(WorkerEventDraft::ProgressOk { bytes: 1000 });
        a.absorb(WorkerEventDraft::ProgressOk { bytes: 2000 });
        a.absorb(WorkerEventDraft::ProgressOk { bytes: 500 });
        a.absorb(WorkerEventDraft::ProgressFailed);
        a.absorb(WorkerEventDraft::ProgressFenced);
        assert_eq!(a.files_delta, 3);
        assert_eq!(a.bytes_delta, 3500);
        assert_eq!(a.errors_delta, 2);

        let wid = WorkerId::new();
        let env = a
            .take_envelope(jid("bobby"), wid, at(42))
            .expect("envelope");
        match env.kind {
            migration_control_protocol::schema::EventKind::ProgressDelta {
                job_id,
                worker_id,
                files_delta,
                bytes_delta,
                errors_delta,
            } => {
                assert_eq!(job_id, jid("bobby"));
                assert_eq!(worker_id, wid);
                assert_eq!(files_delta, 3);
                assert_eq!(bytes_delta, 3500);
                assert_eq!(errors_delta, 2);
            }
            other => panic!("unexpected event kind: {other:?}"),
        }
        // Accumulator must reset after take.
        assert!(a.is_empty());
    }

    #[test]
    fn accum_saturating_add_does_not_panic_on_overflow() {
        let mut a = ProgressAccum {
            files_delta: u64::MAX,
            bytes_delta: u64::MAX - 100,
            errors_delta: 0,
        };
        // Overflow path — should saturate, not panic.
        a.absorb(WorkerEventDraft::ProgressOk { bytes: 500 });
        assert_eq!(a.files_delta, u64::MAX);
        assert_eq!(a.bytes_delta, u64::MAX);
    }
}
