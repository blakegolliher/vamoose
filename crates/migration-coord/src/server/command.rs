//! Command handlers — POST /jobs/{id}/{pause,resume,cancel,drain,
//! retry-failed}. Each:
//!
//! 1. Validates the job exists. 404 if not.
//! 2. Records an audit line at `audit/<YYYY-MM-DD>/<seq>.jsonl`.
//! 3. Ingests the corresponding event so the SSE stream broadcasts
//!    the state change and the reducer updates `phase` /
//!    `phase_history`.
//! 4. Returns `{ command_id }`.
//!
//! Workers don't observe these commands directly in Phase 2. The
//! pause/resume/cancel/drain semantics are realized by the worker
//! polling its per-job state via heartbeat (Phase 3).
//!
//! `retry-failed` is currently a no-op event-wise — there is no
//! `JobRetryFailed` kind in the schema. We record the audit line and
//! return the command_id; Phase 3 wires the actual retry queue. The
//! audit row alone is enough to satisfy the build prompt's "command
//! issued via REST shows up in `audit/`" acceptance gate.
//!
//! ## Token labels
//!
//! Until the auth middleware (Phase 2.8) lands, every command uses
//! the literal label `"anonymous"`. The audit row carries that
//! placeholder so post-merge log inspection identifies which
//! requests landed under un-authenticated mode (a dev-mode
//! disclaimer the operator can grep for).

use super::auth::AdminLabel;
use super::{ApiError, AppState};
use crate::schema::{AuditResult, EventKind, JobId};
use axum::extract::{Extension, Path, State};
use axum::Json;
use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize)]
pub struct CommandAccepted {
    pub command_id: String,
}

#[derive(Debug, Default, Serialize, Deserialize)]
pub struct ReasonBody {
    /// Optional human-readable reason. Defaults to "operator" in
    /// the audit row and the phase_history if not supplied.
    #[serde(default)]
    pub reason: Option<String>,
}

async fn require_job(state: &AppState, id: &JobId) -> Result<(), ApiError> {
    if state.runtime.job_view(id).await.is_none() {
        return Err(ApiError::not_found(
            "job_not_found",
            format!("no such job: {id}"),
        ));
    }
    Ok(())
}

fn parse_id(raw: String) -> Result<JobId, ApiError> {
    JobId::new(raw).map_err(|e| ApiError::bad_request("invalid_job_id", e.to_string()))
}

async fn record_and_ingest(
    state: &AppState,
    label: &AdminLabel,
    action: &str,
    job_id: &JobId,
    reason: String,
    kind: EventKind,
) -> Result<CommandAccepted, ApiError> {
    let target = format!("jobs/{job_id}");
    let args = serde_json::json!({ "reason": reason });
    let command_id = state
        .runtime
        .record_audit(label.as_str(), action, &target, args, AuditResult::Accepted)
        .await
        .map_err(ApiError::storage)?;
    state
        .runtime
        .ingest(kind)
        .await
        .map_err(ApiError::storage)?;
    Ok(CommandAccepted { command_id })
}

fn reason_or_default(body: Option<ReasonBody>, fallback: &str) -> String {
    body.and_then(|b| b.reason)
        .unwrap_or_else(|| fallback.to_string())
}

// =============================================================================
// POST /jobs/{id}/pause
// =============================================================================

pub async fn pause(
    State(state): State<AppState>,
    Extension(label): Extension<AdminLabel>,
    Path(id): Path<String>,
    body: Option<Json<ReasonBody>>,
) -> Result<Json<CommandAccepted>, ApiError> {
    let id = parse_id(id)?;
    require_job(&state, &id).await?;
    let reason = reason_or_default(body.map(|Json(b)| b), "operator");
    let kind = EventKind::JobPaused {
        job_id: id.clone(),
        reason: reason.clone(),
    };
    let accepted = record_and_ingest(&state, &label, "pause", &id, reason, kind).await?;
    Ok(Json(accepted))
}

// =============================================================================
// POST /jobs/{id}/resume
// =============================================================================

pub async fn resume(
    State(state): State<AppState>,
    Extension(label): Extension<AdminLabel>,
    Path(id): Path<String>,
    body: Option<Json<ReasonBody>>,
) -> Result<Json<CommandAccepted>, ApiError> {
    let id = parse_id(id)?;
    require_job(&state, &id).await?;
    let reason = reason_or_default(body.map(|Json(b)| b), "operator");
    let kind = EventKind::JobResumed {
        job_id: id.clone(),
        reason: reason.clone(),
    };
    let accepted = record_and_ingest(&state, &label, "resume", &id, reason, kind).await?;
    Ok(Json(accepted))
}

// =============================================================================
// POST /jobs/{id}/cancel
// =============================================================================

pub async fn cancel(
    State(state): State<AppState>,
    Extension(label): Extension<AdminLabel>,
    Path(id): Path<String>,
    body: Option<Json<ReasonBody>>,
) -> Result<Json<CommandAccepted>, ApiError> {
    let id = parse_id(id)?;
    require_job(&state, &id).await?;
    let reason = reason_or_default(body.map(|Json(b)| b), "operator");
    let kind = EventKind::JobCancelled {
        job_id: id.clone(),
        reason: reason.clone(),
    };
    let accepted = record_and_ingest(&state, &label, "cancel", &id, reason, kind).await?;
    Ok(Json(accepted))
}

// =============================================================================
// POST /jobs/{id}/drain
// =============================================================================
//
// Drain is "stop accepting new work, finish in-flight, then pause".
// At the coord level it's a JobPaused event tagged with reason
// "drain" so the worker side (Phase 3) can tell the two apart by the
// reason field. There's no separate JobDrained variant — distinct
// reason in the audit + phase_history is sufficient.

pub async fn drain(
    State(state): State<AppState>,
    Extension(label): Extension<AdminLabel>,
    Path(id): Path<String>,
    _body: Option<Json<ReasonBody>>,
) -> Result<Json<CommandAccepted>, ApiError> {
    let id = parse_id(id)?;
    require_job(&state, &id).await?;
    let reason = "drain".to_string();
    let kind = EventKind::JobPaused {
        job_id: id.clone(),
        reason: reason.clone(),
    };
    let accepted = record_and_ingest(&state, &label, "drain", &id, reason, kind).await?;
    Ok(Json(accepted))
}

// =============================================================================
// POST /jobs/{id}/retry-failed
// =============================================================================

pub async fn retry_failed(
    State(state): State<AppState>,
    Extension(label): Extension<AdminLabel>,
    Path(id): Path<String>,
    _body: Option<Json<ReasonBody>>,
) -> Result<Json<CommandAccepted>, ApiError> {
    let id = parse_id(id)?;
    require_job(&state, &id).await?;
    // No event for retry-failed in the Phase 2 schema. Record the
    // audit row and return — the Phase 3 worker integration wires
    // the retry queue itself.
    let target = format!("jobs/{id}");
    let command_id = state
        .runtime
        .record_audit(
            label.as_str(),
            "retry-failed",
            &target,
            serde_json::json!({}),
            AuditResult::Accepted,
        )
        .await
        .map_err(ApiError::storage)?;
    Ok(Json(CommandAccepted { command_id }))
}
