//! HTTP server — REST + SSE.
//!
//! This module wraps the [`crate::runtime::CoordRuntime`] in an axum
//! application. Endpoints (defined in nested modules):
//!
//! - **Read** (`server::read`): `GET /jobs`, `GET /jobs/{id}`,
//!   `GET /jobs/{id}/workers`, `GET /jobs/{id}/errors`,
//!   `GET /jobs/{id}/events?since=...`.
//! - **Command** (`server::command`): `POST /jobs/{id}/pause`,
//!   `/resume`, `/cancel`, `/drain`, `/retry-failed`. Each writes an
//!   audit line and emits the corresponding event; workers honor the
//!   command in Phase 3.
//! - **Worker** (`server::worker`): `POST /workers/register`,
//!   `/workers/{id}/heartbeat`, `/workers/{id}/events` (batched),
//!   `/workers/{id}/fence`. Worker integration lands in Phase 3 —
//!   endpoints exist so the contract can be exercised by tests.
//! - **Stream** (`server::stream`): `GET /stream` and
//!   `GET /stream?job_id={id}`. SSE with `Last-Event-ID` resume and
//!   15s keepalive pings.
//!
//! Auth (`server::auth`):
//!
//! - Human endpoints require `Authorization: Bearer <admin-token>`.
//!   Multi-token, all full-authority. Tokens are loaded from a file
//!   `<token>\t<label>\n` at startup; the label appears in the
//!   audit log.
//! - Worker endpoints require `X-Cluster-Secret: <shared-secret>`.
//!   One value per deployment, env-loaded.
//! - `GET /healthz` is exempt from auth.
//!
//! Phase 2 implementation lands in subsequent commits — this module
//! holds the skeleton + documentation so the crate keeps compiling
//! as each piece arrives.

pub mod read;
pub mod stream;

use crate::runtime::CoordRuntime;
use axum::response::{IntoResponse, Response};
use axum::routing::get;
use axum::{Json, Router};
use http::StatusCode;
use serde::Serialize;

/// Application state shared across every handler. Cheap to clone
/// — the runtime itself is `Arc`-backed.
#[derive(Clone)]
pub struct AppState {
    pub runtime: CoordRuntime,
}

impl AppState {
    pub fn new(runtime: CoordRuntime) -> Self {
        Self { runtime }
    }
}

/// Build the read-only axum router. Phase 2.6 (commands), 2.7
/// (worker endpoints), and 2.8 (auth) will wrap this. The stream
/// route is mounted here so SSE is reachable from Phase 2.5 onward;
/// the actual axum SSE adapter ships with the integration tests in
/// Phase 2.11.
pub fn build_router(state: AppState) -> Router {
    Router::new()
        .route("/healthz", get(read::healthz))
        .route("/jobs", get(read::list_jobs))
        .route("/jobs/{id}", get(read::get_job))
        .route("/jobs/{id}/workers", get(read::list_workers))
        .route("/jobs/{id}/errors", get(read::list_errors))
        .route("/jobs/{id}/events", get(read::list_events))
        .route("/events", get(read::list_all_events))
        .with_state(state)
}

// =============================================================================
// Error shape
// =============================================================================

/// Wire shape for an error response. Status code travels in the HTTP
/// envelope; the body carries machine-readable `code` and a
/// human-readable `message`.
#[derive(Debug, Serialize)]
pub struct ApiErrorBody {
    pub code: String,
    pub message: String,
}

#[derive(Debug)]
pub struct ApiError {
    pub status: StatusCode,
    pub body: ApiErrorBody,
}

impl ApiError {
    pub fn new(status: StatusCode, code: impl Into<String>, message: impl Into<String>) -> Self {
        Self {
            status,
            body: ApiErrorBody {
                code: code.into(),
                message: message.into(),
            },
        }
    }

    pub fn bad_request(code: impl Into<String>, message: impl Into<String>) -> Self {
        Self::new(StatusCode::BAD_REQUEST, code, message)
    }

    pub fn not_found(code: impl Into<String>, message: impl Into<String>) -> Self {
        Self::new(StatusCode::NOT_FOUND, code, message)
    }

    pub fn unauthorized(code: impl Into<String>, message: impl Into<String>) -> Self {
        Self::new(StatusCode::UNAUTHORIZED, code, message)
    }

    pub fn internal(message: impl Into<String>) -> Self {
        Self::new(StatusCode::INTERNAL_SERVER_ERROR, "internal", message)
    }

    pub fn storage(err: impl std::fmt::Display) -> Self {
        Self::internal(format!("storage: {err}"))
    }
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        (self.status, Json(self.body)).into_response()
    }
}
