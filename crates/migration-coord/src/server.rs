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

pub mod stream;
