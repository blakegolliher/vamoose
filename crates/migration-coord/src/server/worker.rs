//! Worker-facing REST endpoints.
//!
//! These sit behind the cluster-secret auth in Phase 2.8. Until then
//! they are reachable directly — the integration tests exercise the
//! handlers in that mode.
//!
//! - **POST /workers/register** — bootstrap. Body: `{job_id, host,
//!   pid, version}`. Coord mints a `WorkerId`, emits `WorkerJoined`
//!   (so the SSE stream observes the join), and returns
//!   `{worker_id}`.
//!
//! - **POST /workers/{id}/heartbeat** — counters + state. Body:
//!   `{state, files_per_sec, bytes_per_sec, errors_per_min,
//!   inflight_ops, queue_depth}`. Coord calls
//!   [`crate::runtime::CoordRuntime::record_heartbeat`] — NO event
//!   ingested (heartbeats deliberately stay out of the event log
//!   per the build prompt). 404 if `worker_id` is unknown.
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
use crate::schema::{EventKind, JobId, WorkerCounters, WorkerId, WorkerState};
use axum::extract::{Path, State};
use axum::Json;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

// =============================================================================
// POST /workers/register
// =============================================================================

#[derive(Debug, Deserialize)]
pub struct RegisterBody {
    pub job_id: String,
    pub host: String,
    pub pid: u32,
    pub version: String,
}

#[derive(Debug, Serialize)]
pub struct RegisterResponse {
    pub worker_id: WorkerId,
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
    let worker_id = WorkerId::new();
    state
        .runtime
        .ingest(EventKind::WorkerJoined {
            worker_id,
            job_id,
            host: body.host,
            pid: body.pid,
            version: body.version,
        })
        .await
        .map_err(ApiError::storage)?;
    Ok(Json(RegisterResponse { worker_id }))
}

// =============================================================================
// POST /workers/{id}/heartbeat
// =============================================================================

#[derive(Debug, Deserialize)]
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

fn parse_worker_id(raw: String) -> Result<WorkerId, ApiError> {
    uuid::Uuid::parse_str(&raw)
        .map(WorkerId)
        .map_err(|e| ApiError::bad_request("invalid_worker_id", e.to_string()))
}

pub async fn heartbeat(
    State(state): State<AppState>,
    Path(id): Path<String>,
    Json(body): Json<HeartbeatBody>,
) -> Result<axum::http::StatusCode, ApiError> {
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
    if updated {
        Ok(axum::http::StatusCode::NO_CONTENT)
    } else {
        Err(ApiError::not_found(
            "worker_not_found",
            format!("no such worker: {worker_id}"),
        ))
    }
}

// =============================================================================
// POST /workers/{id}/events
// =============================================================================

#[derive(Debug, Deserialize)]
pub struct WorkerEventEntry {
    #[serde(flatten)]
    pub kind: EventKind,
    #[serde(default)]
    pub worker_at: Option<DateTime<Utc>>,
}

#[derive(Debug, Deserialize)]
pub struct EventsBatchBody {
    pub events: Vec<WorkerEventEntry>,
}

#[derive(Debug, Serialize)]
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

#[derive(Debug, Deserialize)]
pub struct FenceBody {
    pub reason: String,
}

#[derive(Debug, Serialize)]
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
