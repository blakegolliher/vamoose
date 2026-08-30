//! Worker-facing REST endpoints.
//!
//! These sit behind cluster-secret authentication. In explicit dev mode the
//! auth middleware allows requests and integration tests exercise that path.
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
//!   local `RunControl` on every heartbeat. 404 if
//!   `worker_id` is unknown.
//!
//! - **POST /workers/{id}/events** — batched event submission.
//!   Body: `{events: [{kind, ...payload, worker_at?, client_seq?}]}`.
//!   Trust boundary (ledger F20, D2+D3): the URL id must belong to a
//!   registered worker (404 `worker_not_found`, exactly like
//!   heartbeat), and the ENTIRE batch is validated against the
//!   `validate_worker_event` allow-list + identity binding before
//!   anything is ingested — any offending entry rejects the whole
//!   batch with 403 and nothing applied. Each surviving entry is
//!   then ingested individually (coord assigns seq); the response
//!   reports the assigned seqs.
//!
//!   **Idempotency (ledger F20, D4+D5):** a stamping worker marks
//!   each entry with a per-worker, monotonically increasing
//!   `client_seq`. Entries at or below the caller's high-water mark
//!   (reducer state, so it survives crash + replay) are SKIPPED as
//!   already-applied — no ingest, no seq — which makes a resend
//!   after a lost 200, or a retry after a mid-batch storage
//!   failure, converge to exactly-once effective application.
//!   Forward gaps are legitimate (the worker's buffer drops under
//!   budget pressure); stamps out of order WITHIN one batch mean a
//!   buggy client and reject the whole batch with 400
//!   `client_seq_not_monotonic` before anything is ingested.
//!   Unstamped entries (pre-upgrade workers) keep the documented
//!   at-least-once semantics — applied on every send, never
//!   deduped, and they neither read nor advance the mark.
//!
//!   **Response shape (D4 choice):** `{seqs, deduped}`. `seqs`
//!   holds the coord-assigned seqs of the APPLIED entries in batch
//!   order; `deduped` counts the skipped ones, so
//!   `seqs.len() + deduped == events.len()`. Chosen over per-entry
//!   `Option<u64>` because both compatibility constraints fall out
//!   for free: an old worker never stamps, so nothing is ever
//!   skipped for it and `seqs` keeps its old exact shape (the extra
//!   `deduped` field is ignored by its deserializer); a new worker
//!   treats any 200 as "batch settled" and drops its resend buffer
//!   exactly as today — neither worker generation needs to
//!   correlate seqs to entries.
//!
//! - **POST /workers/{id}/fence** — self-fence. Body: `{reason}`.
//!   Coord emits `WorkerFenced`, which the reducer routes to the
//!   worker's state.
//!
//! ## Ack durability
//!
//! Every event-emitting endpoint force-flushes the event log before
//! responding (flush-before-ack, ledger F03). A 200 with seqs means
//! the events survive a coord crash — the worker drops acked events
//! from its bounded resend buffer, so acking a RAM-only event would
//! be silent data loss. Batches amortize: appends past
//! `max_events_per_chunk` flush at the threshold, and the final
//! flush writes at most one chunk per route. The flush is fenced on
//! the lease ([`crate::runtime::CoordRuntime::flush_log`]); a
//! deposed coord fails the request instead of acking.

use super::{ApiError, AppState};
pub use crate::schema::{
    ControlEnvelope, EventsBatchBody, EventsBatchResponse, FenceBody, FenceResponse, HeartbeatBody,
    HeartbeatResponse, LeaveBody, LeaveResponse, RegisterBody, RegisterResponse, WorkerEventEntry,
};
use crate::schema::{EventKind, JobId, WorkerCounters, WorkerId};
use axum::extract::{Path, State};
use axum::Json;

// =============================================================================
// POST /workers/register
// =============================================================================

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
    // Ack == durable: the returned worker_id is only valid if the
    // WorkerJoined (and any WorkerLeft) events survive a crash.
    state.runtime.flush_log().await.map_err(ApiError::storage)?;
    Ok(Json(RegisterResponse {
        worker_id,
        superseded,
    }))
}

// =============================================================================
// POST /workers/{id}/heartbeat
// =============================================================================

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
            body.latency,
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

/// Trust boundary for the worker events route (ledger F20, D2+D3 —
/// `docs/work-items/COORD_WORKER_EVENT_TRUST.md`).
///
/// **Allow-list (D2):** a worker may submit only worker-nature
/// kinds. Operator/lifecycle kinds (`Job*`, `VerifyStarted`,
/// `VerifyCompleted`) stay on the admin command path, which adds
/// bearer auth and audit rows (`command.rs`); accepting them here
/// would bypass both. `WorkerJoined`/`WorkerLeft` are synthesized by
/// the register path and are not accepted raw. Widening the list for
/// a future worker feature is deliberately a one-line reviewed
/// change. The match enumerates every denied kind (no wildcard) so a
/// new `EventKind` forces an explicit classification here.
///
/// **Identity binding (D3):** every payload `worker_id` field must
/// equal the URL id — a worker reports only about itself. That
/// includes `WorkerFenced`, which is accepted as a *self*-fence
/// report only; fencing another worker via this route is exactly the
/// attack this closes. `ClaimConflictDetected` /
/// `ClaimConflictResolved` carry role fields (`holder`, `contender`,
/// `winner`) that legitimately name other workers — a conflict
/// report names both parties — so they pass on the allow-list alone.
fn validate_worker_event(kind: &EventKind, caller: WorkerId) -> Result<(), ApiError> {
    match kind {
        // Worker-nature kinds carrying attribution: bound to caller.
        EventKind::ProgressDelta { worker_id, .. }
        | EventKind::ErrorEmitted { worker_id, .. }
        | EventKind::WorkerStateChanged { worker_id, .. }
        | EventKind::WorkerFenced { worker_id, .. }
        | EventKind::WorkerRecovered { worker_id } => {
            if *worker_id == caller {
                Ok(())
            } else {
                tracing::warn!(
                    kind = kind.name(),
                    caller = %caller,
                    payload_worker_id = %worker_id,
                    "rejected worker event: payload worker_id differs from caller",
                );
                Err(ApiError::forbidden(
                    "worker_id_mismatch",
                    format!(
                        "event {} carries worker_id {} but the caller is {}",
                        kind.name(),
                        worker_id,
                        caller
                    ),
                ))
            }
        }

        // Worker-nature kinds without caller attribution.
        EventKind::ClaimConflictDetected { .. }
        | EventKind::ClaimConflictResolved { .. }
        | EventKind::VerifyFileMismatch { .. } => Ok(()),

        // Operator/lifecycle kinds and register-path-synthesized
        // kinds: never accepted from workers.
        EventKind::JobCreated { .. }
        | EventKind::JobTotalsSet { .. }
        | EventKind::ProgressSync { .. }
        | EventKind::JobPhaseChanged { .. }
        | EventKind::JobPaused { .. }
        | EventKind::JobResumed { .. }
        | EventKind::JobCancelled { .. }
        | EventKind::JobCompleted { .. }
        | EventKind::JobFailed { .. }
        | EventKind::WorkerJoined { .. }
        | EventKind::WorkerLeft { .. }
        | EventKind::VerifyStarted { .. }
        | EventKind::VerifyCompleted { .. } => {
            tracing::warn!(
                kind = kind.name(),
                caller = %caller,
                "rejected worker event: kind not allowed on the worker events route",
            );
            Err(ApiError::forbidden(
                "event_kind_forbidden",
                format!(
                    "event kind {} is not accepted on the worker events route",
                    kind.name()
                ),
            ))
        }
    }
}

pub async fn events_batch(
    State(state): State<AppState>,
    Path(id): Path<String>,
    Json(body): Json<EventsBatchBody>,
) -> Result<Json<EventsBatchResponse>, ApiError> {
    let worker_id = parse_worker_id(id)?;
    // D3: the caller must be a registered worker. Resolve the id the
    // same way heartbeat resolves a known worker (present in
    // `state.workers` — whatever /workers/register recorded) and
    // reject unknown ids with heartbeat's 404 `worker_not_found`.
    if state.runtime.control_for_worker(worker_id).await.is_none() {
        return Err(ApiError::not_found(
            "worker_not_found",
            format!("no such worker: {worker_id}"),
        ));
    }
    // D2+D3 batch semantics: validate the ENTIRE batch before
    // ingesting anything, so one bad entry cannot smuggle siblings
    // in and an allow-list/binding rejection can never cause partial
    // application.
    for entry in &body.events {
        validate_worker_event(&entry.kind, worker_id)?;
    }
    // D4 in-batch monotonicity, also validated before anything is
    // ingested: stamped entries must be STRICTLY increasing within
    // the batch. Out-of-order (or duplicate) stamps in one batch are
    // a buggy client, not a replay — a replay resends whole batches
    // in original order — so the whole batch is rejected 400-class.
    // (Gaps are legitimate: the worker's buffer drops under budget
    // pressure. Unstamped entries are simply not part of the chain.)
    let mut prev_stamp: Option<u64> = None;
    for entry in &body.events {
        if let Some(cs) = entry.client_seq {
            if prev_stamp.is_some_and(|p| cs <= p) {
                return Err(ApiError::bad_request(
                    "client_seq_not_monotonic",
                    format!(
                        "client_seq {cs} follows {} within one batch; \
                         stamps must be strictly increasing",
                        prev_stamp.unwrap_or(0),
                    ),
                ));
            }
            prev_stamp = Some(cs);
        }
    }
    // D4 dedup: entries stamped at or below the caller's high-water
    // mark are already applied — a resend after a lost 200, or a
    // retry after a mid-batch storage failure re-offering the
    // already-applied prefix. Skip them: no ingest, no seq. The mark
    // lives in reducer state (advanced when the stamped envelope is
    // applied, live or on replay), so reading it once up front is
    // sound — the in-batch check above guarantees the entries we DO
    // apply climb strictly above it. D5 falls out: the only mid-loop
    // failure left after validation is storage, and the worker's
    // retry of the same batch converges to exactly-once.
    let hwm = state.runtime.client_seq_hwm(worker_id).await;
    let mut seqs = Vec::with_capacity(body.events.len());
    let mut deduped = 0u64;
    for entry in body.events {
        if entry.client_seq.is_some_and(|cs| cs <= hwm) {
            deduped += 1;
            continue;
        }
        // The envelope carries the caller id (from_worker, F20
        // residue) so stamped kinds without payload attribution —
        // ClaimConflict*/VerifyFileMismatch — still advance the
        // caller's high-water mark, live and on replay.
        let seq = state
            .runtime
            .ingest_worker_event(entry.kind, entry.worker_at, entry.client_seq, worker_id)
            .await
            .map_err(ApiError::storage)?;
        seqs.push(seq);
    }
    // Ack == durable (ledger F03): the worker treats a 200 as the
    // whole batch settled and drops it from its resend buffer, so
    // the buffered chunks must hit the store before we respond.
    // Fenced on the lease — a deposed coord fails here instead of
    // acking.
    state.runtime.flush_log().await.map_err(ApiError::storage)?;
    Ok(Json(EventsBatchResponse { seqs, deduped }))
}

// =============================================================================
// POST /workers/{id}/fence
// =============================================================================

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
    // Ack == durable: the fence event carries safety semantics — it
    // must not evaporate in a coord crash after the worker saw 200.
    state.runtime.flush_log().await.map_err(ApiError::storage)?;
    Ok(Json(FenceResponse { seq }))
}

// =============================================================================
// POST /workers/{id}/leave
// =============================================================================

/// An orderly exit. The worker has already released or completed its
/// shard; all the coord has to do is stop counting it. Without this
/// a cleanly finished worker sat in `Idle`/`Copying` until the
/// liveness sweep noticed the missing heartbeats.
pub async fn leave(
    State(state): State<AppState>,
    Path(id): Path<String>,
    Json(body): Json<LeaveBody>,
) -> Result<Json<LeaveResponse>, ApiError> {
    let worker_id = parse_worker_id(id)?;
    if !state.runtime.worker_is_registered(&worker_id).await {
        return Err(ApiError::not_found(
            "worker_not_found",
            format!("no such worker: {worker_id}"),
        ));
    }
    let seq = state
        .runtime
        .ingest(EventKind::WorkerLeft {
            worker_id,
            reason: body.reason,
        })
        .await
        .map_err(ApiError::storage)?;
    state.runtime.flush_log().await.map_err(ApiError::storage)?;
    Ok(Json(LeaveResponse { seq }))
}
