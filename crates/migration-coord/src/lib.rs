//! migration-coord
//!
//! Control plane for vamoose deployments. Owns:
//!
//! - A compatibility re-export of the shared control-plane wire schema
//!   (`schema`), canonically owned by `migration-control-protocol`.
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
//! - The event-log writer (`events`) and persisted replay orchestration
//!   (`state`) over the reducer owned by `migration-control-protocol`.
//! - Archive-on-completion for finished jobs (`archive`).
//! - The REST/SSE server, authentication, and TLS listener (`server`).
//! - The decomposed runtime façade and background lifecycle (`runtime`,
//!   `ticks`).
//!
//! Worker reporting and operator commands are implemented through the shared
//! control protocol. See `docs/CONTROL_PLANE.md` for the current ownership and
//! runtime invariants; `docs/COORD_PLAN.md` is the historical delivery plan.

// migration-core re-exports an aws-sdk-s3 error variant that recent
// clippy flags as large in `Result<T>`. Boxing every Result through the
// coord layer is more churn than the warning is worth; allow it at
// crate scope, matching the parent crate's policy.
#![allow(clippy::result_large_err)]

pub mod archive;
pub mod errors;
pub mod events;
pub mod layout;
pub mod lease;
pub mod reconcile;
pub mod runtime;
pub mod schema;
pub mod server;
pub mod snapshot;
pub mod state;
pub mod store;
pub mod ticks;

pub use errors::{Error, Result};
