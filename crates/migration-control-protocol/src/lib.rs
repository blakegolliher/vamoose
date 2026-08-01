//! Shared control-plane contract for vamoose.
//!
//! This crate is specifically the **operator control-plane** protocol. The
//! distinct S3 claim protocol used by workers on the data plane remains owned
//! by `migration-core`; unqualified "claim protocol" references elsewhere in
//! the repository refer to that contract, not this crate.
//!
//! This crate owns the serialized job, worker, event, command, and
//! snapshot schema, the control-plane REST request/response bodies, and the
//! deterministic reducer that folds events into a [`schema::Snapshot`]. It is
//! intentionally independent of coordinator persistence, replay orchestration,
//! leases, runtimes, HTTP servers, and S3. Client crates should depend on this
//! crate for control-plane types instead of depending on `migration-coord`.
//!
//! ## SSE wire contract
//!
//! `GET /stream` accepts the optional [`schema::StreamParams::job_id`] query
//! filter. An event frame uses the [`schema::EventKind::name`] as `event:`, the
//! decimal [`schema::EventEnvelope::seq`] as `id:`, and the complete flattened
//! [`schema::EventEnvelope`] JSON as `data:`. A `Resync` frame has
//! `event: Resync`, no `id:`, and `data: {"skipped":N}`. Idle keepalives are
//! SSE comments (`: ping`).
//!
//! With `Last-Event-ID: N`, the coordinator emits persisted events with
//! `seq > N` before switching to the live tail, without gaps or duplicates.
//! Without that header, a connection starts at the live tail. The coordinator
//! remains responsible for catch-up I/O, subscription handling, and framing;
//! this crate owns the query and payload contract only.
//!
//! ## Preserved compatibility debt
//!
//! [`schema::Snapshot`] still carries the coordinator bookkeeping fields
//! `audit_seq_today`, `audit_seq_date`, and `last_client_seq`. They predate this
//! extraction and remain part of the version-1 serialized snapshot so moving
//! ownership does not change persisted or client-visible data. Their presence
//! here preserves compatibility; separating them requires an explicit,
//! versioned follow-up rather than an incidental layering refactor.

pub mod reducer;
pub mod schema;
