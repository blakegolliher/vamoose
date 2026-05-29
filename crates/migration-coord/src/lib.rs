//! migration-coord
//!
//! Control plane for vamoose deployments. Owns:
//!
//! - The wire schema for jobs, workers, events, and error buckets
//!   (`schema`). Both the HTTP/SSE API and the operator TUI consume
//!   the same types.
//! - The S3 layout for coord state: lease, snapshots, append-only event
//!   logs, audit trail, archived event logs (`layout`).
//! - An abstraction over the storage backend (`store`) so unit tests
//!   can swap in an in-memory fake without an S3 dependency.
//! - The lease protocol (`lease`) — `coord/lease` acquire / refresh /
//!   takeover. Reuses the v2 claim primitives from
//!   `migration_core::s3::S3Client`: `PUT If-None-Match: *` for the
//!   atomic create, `DELETE If-Match` for safe takeover after expiry.
//! - The snapshot writer + reader (`snapshot`) with cadence policy
//!   (1000 events OR 5 minutes, whichever first).
//! - The event-log writer + replay routine (`events`).
//! - An in-memory state struct + reducer (`state`).
//! - Archive-on-completion for finished jobs (`archive`).
//!
//! Phase 1 (this milestone) is **lib-only**: no HTTP, no worker
//! integration. The `vamoose coord` subcommand and the REST/SSE
//! surface land in Phase 2; worker reporting in Phase 3.
//!
//! See `docs/COORD_PLAN.md` for the full phase plan, conflict notes,
//! and acceptance gates.

// migration-core re-exports an aws-sdk-s3 error variant that recent
// clippy flags as large in `Result<T>`. Boxing every Result through the
// coord layer is more churn than the warning is worth; allow it at
// crate scope, matching the parent crate's policy.
#![allow(clippy::result_large_err)]

pub mod errors;
pub mod events;
pub mod layout;
pub mod lease;
pub mod schema;
pub mod snapshot;
pub mod store;

pub use errors::{Error, Result};
