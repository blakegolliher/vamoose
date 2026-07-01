//! Read-only REST handlers.
//!
//! All handlers take an [`AppState`](super::AppState) extractor and
//! return either `Json<T>` or an [`super::ApiError`]. Path
//! parameters use axum 0.8 syntax (`{id}`) — see [`super::router`].
//!
//! ## Response shapes
//!
//! - `GET /jobs` → `{ jobs: [Job, ...], next_cursor: "id" | null }`
//! - `GET /jobs/{id}` → `Job`
//! - `GET /jobs/{id}/workers` → `{ workers: [Worker, ...] }`
//! - `GET /jobs/{id}/errors` → `{ buckets: [ErrorBucket, ...] }`
//! - `GET /jobs/{id}/events?since=N&limit=M` →
//!   `{ events: [EventEnvelope, ...], next_since: u64 }`
//!
//! Unknown `{id}` → 404 with `ApiError { code: "job_not_found" }`.

use super::{ApiError, AppState};
use crate::events::read_all_events_since;
use crate::schema::{ErrorBucket, EventEnvelope, Job, JobId, Worker};
use axum::extract::{Path, Query, State};
use axum::Json;
use http::StatusCode;
use serde::{Deserialize, Serialize};

const DEFAULT_PAGE_LIMIT: usize = 50;
const MAX_PAGE_LIMIT: usize = 500;

const DEFAULT_EVENTS_LIMIT: usize = 200;
const MAX_EVENTS_LIMIT: usize = 1000;

// =============================================================================
// GET /jobs
// =============================================================================

#[derive(Debug, Deserialize)]
pub struct ListJobsParams {
    /// `JobId` from a previous response's `next_cursor` (excluded
    /// from this page).
    pub cursor: Option<String>,
    /// Page size. Default 50, capped at 500.
    pub limit: Option<usize>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct ListJobsResponse {
    pub jobs: Vec<Job>,
    pub next_cursor: Option<String>,
}

pub async fn list_jobs(
    State(state): State<AppState>,
    Query(params): Query<ListJobsParams>,
) -> Result<Json<ListJobsResponse>, ApiError> {
    let cursor = match params.cursor {
        Some(s) => Some(
            JobId::new(s).map_err(|e| ApiError::bad_request("invalid_cursor", e.to_string()))?,
        ),
        None => None,
    };
    let limit = params
        .limit
        .unwrap_or(DEFAULT_PAGE_LIMIT)
        .clamp(1, MAX_PAGE_LIMIT);
    let page = state.runtime.jobs_view(cursor.as_ref(), limit).await;
    Ok(Json(ListJobsResponse {
        jobs: page.jobs,
        next_cursor: page.next_cursor.map(|id| id.0),
    }))
}

// =============================================================================
// GET /jobs/{id}
// =============================================================================

pub async fn get_job(
    State(state): State<AppState>,
    Path(id): Path<String>,
) -> Result<Json<Job>, ApiError> {
    let id = JobId::new(id).map_err(|e| ApiError::bad_request("invalid_job_id", e.to_string()))?;
    match state.runtime.job_view(&id).await {
        Some(job) => Ok(Json(job)),
        None => Err(ApiError::not_found(
            "job_not_found",
            format!("no such job: {id}"),
        )),
    }
}

// =============================================================================
// GET /jobs/{id}/workers
// =============================================================================

#[derive(Debug, Serialize, Deserialize)]
pub struct ListWorkersResponse {
    pub workers: Vec<Worker>,
}

pub async fn list_workers(
    State(state): State<AppState>,
    Path(id): Path<String>,
) -> Result<Json<ListWorkersResponse>, ApiError> {
    let id = JobId::new(id).map_err(|e| ApiError::bad_request("invalid_job_id", e.to_string()))?;
    match state.runtime.workers_view_for_job(&id).await {
        Some(workers) => Ok(Json(ListWorkersResponse { workers })),
        None => Err(ApiError::not_found(
            "job_not_found",
            format!("no such job: {id}"),
        )),
    }
}

// =============================================================================
// GET /jobs/{id}/errors
// =============================================================================

#[derive(Debug, Serialize, Deserialize)]
pub struct ListErrorsResponse {
    pub buckets: Vec<ErrorBucket>,
}

pub async fn list_errors(
    State(state): State<AppState>,
    Path(id): Path<String>,
) -> Result<Json<ListErrorsResponse>, ApiError> {
    let id = JobId::new(id).map_err(|e| ApiError::bad_request("invalid_job_id", e.to_string()))?;
    match state.runtime.errors_view_for_job(&id).await {
        Some(buckets) => Ok(Json(ListErrorsResponse { buckets })),
        None => Err(ApiError::not_found(
            "job_not_found",
            format!("no such job: {id}"),
        )),
    }
}

// =============================================================================
// GET /jobs/{id}/events?since=N&limit=M
// =============================================================================

#[derive(Debug, Deserialize)]
pub struct ListEventsParams {
    /// Lower bound (exclusive). Default 0.
    pub since: Option<u64>,
    /// Max events to return. Default 200, capped at 1000.
    pub limit: Option<usize>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct ListEventsResponse {
    pub events: Vec<EventEnvelope>,
    /// Seq of the last event returned (or the original `since` if
    /// the page was empty). Use as `since` on the next request.
    pub next_since: u64,
}

pub async fn list_events(
    State(state): State<AppState>,
    Path(id): Path<String>,
    Query(params): Query<ListEventsParams>,
) -> Result<Json<ListEventsResponse>, ApiError> {
    let id = JobId::new(id).map_err(|e| ApiError::bad_request("invalid_job_id", e.to_string()))?;
    // 404 on unknown job — same shape as the other per-job handlers.
    if state.runtime.job_view(&id).await.is_none() {
        return Err(ApiError::not_found(
            "job_not_found",
            format!("no such job: {id}"),
        ));
    }
    let since = params.since.unwrap_or(0);
    let limit = params
        .limit
        .unwrap_or(DEFAULT_EVENTS_LIMIT)
        .clamp(1, MAX_EVENTS_LIMIT);

    // Per-job catch-up: walk events/{job_id}/<chunks> in seq order.
    let mut all = crate::state::read_job_events(state.runtime.store().as_ref(), &id, since)
        .await
        .map_err(ApiError::storage)?;
    // The helper already returns ascending; truncate to limit.
    all.truncate(limit);
    let next_since = all.last().map(|e| e.seq).unwrap_or(since);
    Ok(Json(ListEventsResponse {
        events: all,
        next_since,
    }))
}

// =============================================================================
// GET /events?since=N&limit=M (cluster-wide variant — used by tests
// and the future /stream catch-up validation)
// =============================================================================

pub async fn list_all_events(
    State(state): State<AppState>,
    Query(params): Query<ListEventsParams>,
) -> Result<Json<ListEventsResponse>, ApiError> {
    let since = params.since.unwrap_or(0);
    let limit = params
        .limit
        .unwrap_or(DEFAULT_EVENTS_LIMIT)
        .clamp(1, MAX_EVENTS_LIMIT);
    let mut all = read_all_events_since(state.runtime.store().as_ref(), since)
        .await
        .map_err(ApiError::storage)?;
    all.truncate(limit);
    let next_since = all.last().map(|e| e.seq).unwrap_or(since);
    Ok(Json(ListEventsResponse {
        events: all,
        next_since,
    }))
}

// =============================================================================
// GET /healthz
// =============================================================================

#[derive(Debug, Serialize, Deserialize)]
pub struct HealthzResponse {
    pub status: String,
    pub last_seq: u64,
    pub subscriber_count: usize,
    pub lease_lost: bool,
}

pub async fn healthz(State(state): State<AppState>) -> (StatusCode, Json<HealthzResponse>) {
    let last_seq = state.runtime.last_seq().await;
    let subscriber_count = state.runtime.subscriber_count();
    let lease_lost = state.runtime.lease_lost().await;
    let (status, body_status) = if lease_lost {
        (StatusCode::SERVICE_UNAVAILABLE, "lease_lost")
    } else {
        (StatusCode::OK, "ok")
    };
    (
        status,
        Json(HealthzResponse {
            status: body_status.to_string(),
            last_seq,
            subscriber_count,
            lease_lost,
        }),
    )
}
