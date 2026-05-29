//! Worker-facing REST endpoints.
//!
//! These sit behind the cluster-secret auth in Phase 2.8. Until then
//! they are reachable directly — the integration tests exercise the
//! handlers in that mode.
//!
//! - **POST /workers/register** — bootstrap. Body: `{job_id, host,
//!   pid, start_time, version}`. Coord mints a fresh `WorkerId`,
//!   walks `(job_id, host)` for any prior worker whose `(pid,
//!   start_time)` differs and emits `WorkerLeft{reason:"reregister"}`
//!   for each, then emits `WorkerJoined`. Returns
//!   `{worker_id, superseded}`. `superseded` is the list of stale
//!   WorkerIds the coord just disconnected.
//!
//! - **POST /workers/{id}/heartbeat** — counters + state. Body:
//!   `{state, files_per_sec, bytes_per_sec, errors_per_min,
//!   inflight_ops, queue_depth}`. Coord calls
//!   [`crate::runtime::CoordRuntime::record_heartbeat`] — NO event
//!   ingested (heartbeats deliberately stay out of the event log
//!   per the build prompt). Response: `{control: {mode}, last_seq,
//!   server_time}`. The worker reads `control.mode` and flips its
//!   local `RunControl` on every heartbeat (Phase 3.5). 404 if
//!   `worker_id` is unknown.
//!
//! - **POST /workers/{id}/events** — batched event submission.
//!   Body: `{events: [{kind, ...payload, worker_at?}]}`. Each entry
//!   is ingested individually (coord assigns seq); the response
//!   reports the assigned seqs.
//!
//! - **POST /workers/{id}/fence** — self-fence. Body: `{reason}`.
//!   Coord emits `WorkerFenced`, which the reducer routes to the
//!   worker's state.

use super::{ApiError, AppState};
use crate::schema::{ControlMode, EventKind, JobId, WorkerCounters, WorkerId, WorkerState};
use axum::extract::{Path, State};
use axum::Json;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

// =============================================================================
// POST /workers/register
// =============================================================================

#[derive(Debug, Serialize, Deserialize)]
pub struct RegisterBody {
    pub job_id: String,
    pub host: String,
    pub pid: u32,
    /// Worker-process start time. Coord pairs `(host, pid, start_time)`
    /// to detect stale registrations across restarts.
    pub start_time: DateTime<Utc>,
    pub version: String,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct RegisterResponse {
    pub worker_id: WorkerId,
    /// WorkerIds the coord marked Disconnected as a side effect of
    /// this register — any prior workers on `(job_id, host)` whose
    /// `(pid, start_time)` differs from the new registration. The
    /// caller can use this for debugging; clients ignore it.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub superseded: Vec<WorkerId>,
}

pub async fn register(
    State(state): State<AppState>,
    Json(body): Json<RegisterBody>,
) -> Result<Json<RegisterResponse>, ApiError> {
    let job_id = JobId::new(body.job_id)
        .map_err(|e| ApiError::bad_request("invalid_job_id", e.to_string()))?;
    if state.runtime.job_view(&job_id).await.is_none() {
        return Err(ApiError::not_found(
            "job_not_found",
            format!("no such job: {job_id}"),
        ));
    }

    // Dedup: any prior worker on (job_id, host) whose (pid, start_time)
    // differs from the new registration is stale. Mark each one
    // Disconnected before emitting the new WorkerJoined so the order
    // on the wire is "old leaves, then new joins".
    let stale = state
        .runtime
        .stale_workers_for_register(&job_id, &body.host, body.pid, body.start_time)
        .await;
    let mut superseded = Vec::with_capacity(stale.len());
    for prior in stale {
        state
            .runtime
            .ingest(EventKind::WorkerLeft {
                worker_id: prior,
                reason: "reregister".into(),
            })
            .await
            .map_err(ApiError::storage)?;
        superseded.push(prior);
    }

    let worker_id = WorkerId::new();
    state
        .runtime
        .ingest(EventKind::WorkerJoined {
            worker_id,
            job_id,
            host: body.host,
            pid: body.pid,
            start_time: body.start_time,
            version: body.version,
        })
        .await
        .map_err(ApiError::storage)?;
    Ok(Json(RegisterResponse {
        worker_id,
        superseded,
    }))
}

// =============================================================================
// POST /workers/{id}/heartbeat
// =============================================================================

#[derive(Debug, Serialize, Deserialize)]
pub struct HeartbeatBody {
    pub state: WorkerState,
    #[serde(default)]
    pub files_per_sec: f64,
    #[serde(default)]
    pub bytes_per_sec: f64,
    #[serde(default)]
    pub errors_per_min: f64,
    #[serde(default)]
    pub inflight_ops: u32,
    #[serde(default)]
    pub queue_depth: u32,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct ControlEnvelope {
    pub mode: ControlMode,
}

/// Body of the heartbeat response. The worker reads `control.mode`
/// and flips its local `RunControl` to match on every heartbeat.
/// `last_seq` lets the worker spot a coord restart (a backwards jump
/// is the trigger to flush its event buffer). `server_time` is
/// echoed for clock-skew diagnostics — workers never use it for
/// fence decisions.
#[derive(Debug, Serialize, Deserialize)]
pub struct HeartbeatResponse {
    pub control: ControlEnvelope,
    pub last_seq: u64,
    pub server_time: DateTime<Utc>,
}

fn parse_worker_id(raw: String) -> Result<WorkerId, ApiError> {
    uuid::Uuid::parse_str(&raw)
        .map(WorkerId)
        .map_err(|e| ApiError::bad_request("invalid_worker_id", e.to_string()))
}

pub async fn heartbeat(
    State(state): State<AppState>,
    Path(id): Path<String>,
    Json(body): Json<HeartbeatBody>,
) -> Result<Json<HeartbeatResponse>, ApiError> {
    let worker_id = parse_worker_id(id)?;
    let counters = WorkerCounters {
        files_per_sec: body.files_per_sec,
        bytes_per_sec: body.bytes_per_sec,
        errors_per_min: body.errors_per_min,
    };
    let updated = state
        .runtime
        .record_heartbeat(
            worker_id,
            counters,
            body.state,
            body.inflight_ops,
            body.queue_depth,
        )
        .await
        .map_err(ApiError::storage)?;
    if !updated {
        return Err(ApiError::not_found(
            "worker_not_found",
            format!("no such worker: {worker_id}"),
        ));
    }
    // record_heartbeat already verified the worker exists; the lookup
    // here is the same guard one more time so a worker that vanished
    // between record_heartbeat and the control read (impossible today,
    // but cheap insurance) does not return a stale Cancel.
    let (mode, last_seq, server_time) = state
        .runtime
        .control_for_worker(worker_id)
        .await
        .ok_or_else(|| {
            ApiError::not_found("worker_not_found", format!("no such worker: {worker_id}"))
        })?;
    Ok(Json(HeartbeatResponse {
        control: ControlEnvelope { mode },
        last_seq,
        server_time,
    }))
}

// =============================================================================
// POST /workers/{id}/events
// =============================================================================

#[derive(Debug, Serialize, Deserialize)]
pub struct WorkerEventEntry {
    #[serde(flatten)]
    pub kind: EventKind,
    #[serde(default)]
    pub worker_at: Option<DateTime<Utc>>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct EventsBatchBody {
    pub events: Vec<WorkerEventEntry>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct EventsBatchResponse {
    /// Seqs assigned to each event in order. Matches `events.len()`.
    pub seqs: Vec<u64>,
}

pub async fn events_batch(
    State(state): State<AppState>,
    Path(id): Path<String>,
    Json(body): Json<EventsBatchBody>,
) -> Result<Json<EventsBatchResponse>, ApiError> {
    // Worker ID parse is a contract guard — even though the URL id
    // isn't currently used by the handler, an invalid value would
    // signal a misconfigured client. Reject before doing the writes.
    let _worker_id = parse_worker_id(id)?;
    let mut seqs = Vec::with_capacity(body.events.len());
    for entry in body.events {
        let seq = match entry.worker_at {
            Some(at) => state
                .runtime
                .ingest_with_worker_at(entry.kind, at)
                .await
                .map_err(ApiError::storage)?,
            None => state
                .runtime
                .ingest(entry.kind)
                .await
                .map_err(ApiError::storage)?,
        };
        seqs.push(seq);
    }
    Ok(Json(EventsBatchResponse { seqs }))
}

// =============================================================================
// POST /workers/{id}/fence
// =============================================================================

#[derive(Debug, Serialize, Deserialize)]
pub struct FenceBody {
    pub reason: String,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct FenceResponse {
    pub seq: u64,
}

pub async fn fence(
    State(state): State<AppState>,
    Path(id): Path<String>,
    Json(body): Json<FenceBody>,
) -> Result<Json<FenceResponse>, ApiError> {
    let worker_id = parse_worker_id(id)?;
    let seq = state
        .runtime
        .ingest(EventKind::WorkerFenced {
            worker_id,
            reason: body.reason,
        })
        .await
        .map_err(ApiError::storage)?;
    Ok(Json(FenceResponse { seq }))
}
