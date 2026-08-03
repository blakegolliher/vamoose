//! Command handlers — POST /jobs/{id}/{pause,resume,cancel,drain,
//! retry-failed}. Each:
//!
//! 1. Validates the job exists (404 if not) and that the command is
//!    legal for the job's current phase (409 `invalid_phase` if not,
//!    ledger F25) — rejection happens before any side effect.
//! 2. Ingests the corresponding event so the SSE stream broadcasts
//!    the state change and the reducer updates `phase` /
//!    `phase_history`.
//! 3. Force-flushes the event log (flush-before-ack, ledger F03).
//!    A 200 means the command survives a coord crash — a pause
//!    that only ever lived in the writer buffer would silently
//!    un-pause on failover. The flush is fenced on the lease, so a
//!    deposed coord fails the request instead of acking.
//! 4. Records an audit line at `audit/<YYYY-MM-DD>/<seq>.jsonl`.
//!    Audit comes AFTER the durable event (ledger F22): the
//!    `Accepted` row asserts a command that took effect, so a
//!    failed ingest/flush must never leave one behind.
//! 5. Returns `{ command_id }`.
//!
//! Workers observe pause/resume/cancel state through the control mode returned
//! by heartbeat. Version 1 encodes drain as `JobPaused { reason: "drain" }`, so
//! the worker observes the same pause mode rather than a distinct drain mode.
//!
//! `retry-failed` is currently a no-op event-wise — there is no
//! `JobRetryFailed` kind in the schema. We record the audit line and
//! return the command_id. No retry event or worker queue is implemented yet.
//!
//! ## Token labels
//!
//! Authenticated requests carry the configured admin-token label. Dev-mode
//! requests use the explicit `"dev-mode"` label so the audit trail identifies
//! their unauthenticated origin.

use super::auth::AdminLabel;
use super::{ApiError, AppState};
use crate::schema::{AuditResult, EventKind, JobId};
pub use crate::schema::{CommandAccepted, ReasonBody};
use axum::extract::{Extension, Path, State};
use axum::Json;

async fn require_job(state: &AppState, id: &JobId) -> Result<crate::schema::Job, ApiError> {
    state
        .runtime
        .job_view(id)
        .await
        .ok_or_else(|| ApiError::not_found("job_not_found", format!("no such job: {id}")))
}

/// Phase-legality gate (ledger F25). Rejects a command that makes no
/// sense for the job's current phase with 409 — BEFORE any audit or
/// ingest, so a rejected command leaves no side effects.
fn require_phase(
    allowed: bool,
    command: &str,
    phase: crate::schema::Phase,
) -> Result<(), ApiError> {
    if allowed {
        Ok(())
    } else {
        Err(ApiError::conflict(
            "invalid_phase",
            format!("cannot {command} a job in phase {phase:?}"),
        ))
    }
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
    state
        .runtime
        .ingest(kind)
        .await
        .map_err(ApiError::storage)?;
    // Ack == durable (ledger F03): the event must hit the store
    // before the operator sees 200.
    state.runtime.flush_log().await.map_err(ApiError::storage)?;
    // Audit AFTER the durable effect (ledger F22): the `Accepted`
    // row describes an applied command. If ingest or flush fails
    // above, no audit row is written at all — the trail never
    // asserts a command that didn't take effect.
    let command_id = state
        .runtime
        .record_audit(label.as_str(), action, &target, args, AuditResult::Accepted)
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
    let job = require_job(&state, &id).await?;
    require_phase(job.phase.can_pause(), "pause", job.phase)?;
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
    let job = require_job(&state, &id).await?;
    require_phase(job.phase.can_resume(), "resume", job.phase)?;
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
    let job = require_job(&state, &id).await?;
    // Cancel is legal from any non-terminal phase (including Paused);
    // cancelling an already-terminal job is a conflict.
    require_phase(!job.phase.is_terminal(), "cancel", job.phase)?;
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
// "drain" so the audit and phase history preserve the operator's intent.
// There's no separate JobDrained variant, and heartbeat currently maps the
// paused phase to ControlMode::Pause.

pub async fn drain(
    State(state): State<AppState>,
    Extension(label): Extension<AdminLabel>,
    Path(id): Path<String>,
    _body: Option<Json<ReasonBody>>,
) -> Result<Json<CommandAccepted>, ApiError> {
    let id = parse_id(id)?;
    let job = require_job(&state, &id).await?;
    // Drain rides on JobPaused, so it shares pause's legality.
    require_phase(job.phase.can_pause(), "drain", job.phase)?;
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
    // No event or worker retry queue exists for retry-failed. Record the audit
    // row and return; the accepted response is audit-only in version 1.
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
