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
//!   audit line; lifecycle commands emit durable events that workers
//!   observe through heartbeat control. `retry-failed` is audit-only.
//! - **Worker** (`server::worker`): `POST /workers/register`,
//!   `/workers/{id}/heartbeat`, `/workers/{id}/events` (batched),
//!   `/workers/{id}/fence`. Current workers use these endpoints when
//!   their `[coord]` section is configured.
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
//! In dev mode both authentication middleware layers allow requests, but the
//! CLI refuses a non-loopback dev-mode bind without an explicit unsafe
//! override.

pub mod auth;
pub mod command;
pub mod listen;
pub mod read;
pub mod stream;
pub mod worker;

use crate::runtime::CoordRuntime;
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use axum::{middleware, Json, Router};
use http::StatusCode;
use serde::Serialize;
use std::sync::Arc;

/// Application state shared across every handler. Cheap to clone
/// — the runtime itself is `Arc`-backed.
#[derive(Clone)]
pub struct AppState {
    pub runtime: CoordRuntime,
    pub auth: Arc<auth::AuthConfig>,
}

impl AppState {
    /// Convenience constructor for tests and dev mode — no
    /// admin tokens, no cluster secret, every request passes
    /// through with the `dev-mode` audit label.
    pub fn new(runtime: CoordRuntime) -> Self {
        Self {
            runtime,
            auth: Arc::new(auth::AuthConfig::default()),
        }
    }

    pub fn with_auth(runtime: CoordRuntime, auth: auth::AuthConfig) -> Self {
        Self {
            runtime,
            auth: Arc::new(auth),
        }
    }
}

/// Build the full axum router. Three route groups:
///
/// - **Public** — only `/healthz`. Reachable without credentials so
///   liveness probes don't need to know the admin token.
/// - **Admin** — read and command endpoints. Behind
///   [`auth::require_admin`].
/// - **Worker** — `/workers/*`. Behind
///   [`auth::require_cluster_secret`].
///
/// In dev mode ([`auth::AuthConfig::is_dev_mode`]) both middleware
/// layers pass every request through and stamp the
/// [`auth::AdminLabel`] with `"dev-mode"` so the audit log records
/// the unauthenticated origin.
pub fn build_router(state: AppState) -> Router {
    let admin = Router::new()
        .route("/jobs", get(read::list_jobs))
        .route("/jobs/{id}", get(read::get_job))
        .route("/jobs/{id}/workers", get(read::list_workers))
        .route("/jobs/{id}/errors", get(read::list_errors))
        .route("/jobs/{id}/events", get(read::list_events))
        .route("/jobs/{id}/pause", post(command::pause))
        .route("/jobs/{id}/resume", post(command::resume))
        .route("/jobs/{id}/cancel", post(command::cancel))
        .route("/jobs/{id}/drain", post(command::drain))
        .route("/jobs/{id}/retry-failed", post(command::retry_failed))
        .route("/events", get(read::list_all_events))
        .route("/prepare", get(read::get_prepare))
        .route_layer(middleware::from_fn_with_state(
            state.clone(),
            auth::require_admin,
        ));

    let workers = Router::new()
        .route("/workers/register", post(worker::register))
        .route("/workers/{id}/heartbeat", post(worker::heartbeat))
        .route("/workers/{id}/events", post(worker::events_batch))
        .route("/workers/{id}/fence", post(worker::fence))
        .route("/workers/{id}/leave", post(worker::leave))
        .route_layer(middleware::from_fn_with_state(
            state.clone(),
            auth::require_cluster_secret,
        ));

    let stream_route = Router::new()
        .route("/stream", get(stream::handler))
        .route_layer(middleware::from_fn_with_state(
            state.clone(),
            auth::require_admin,
        ));

    Router::new()
        .route("/healthz", get(read::healthz))
        .merge(admin)
        .merge(workers)
        .merge(stream_route)
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

    /// 409 — the request is well-formed and the target exists, but
    /// the target's current state does not allow it (e.g. pausing a
    /// cancelled job, ledger F25).
    pub fn conflict(code: impl Into<String>, message: impl Into<String>) -> Self {
        Self::new(StatusCode::CONFLICT, code, message)
    }

    pub fn unauthorized(code: impl Into<String>, message: impl Into<String>) -> Self {
        Self::new(StatusCode::UNAUTHORIZED, code, message)
    }

    /// 403 — the caller is authenticated, but the request exceeds
    /// what that caller is allowed to do (e.g. a worker submitting
    /// operator event kinds or events attributed to another worker,
    /// ledger F20).
    pub fn forbidden(code: impl Into<String>, message: impl Into<String>) -> Self {
        Self::new(StatusCode::FORBIDDEN, code, message)
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
