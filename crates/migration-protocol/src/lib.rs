//! Shared control-plane contract for vamoose.
//!
//! This crate owns the serialized job, worker, event, command, and
//! snapshot schema plus the deterministic reducer that folds events into a
//! [`schema::Snapshot`]. It is intentionally independent of coordinator
//! persistence, replay orchestration, leases, runtimes, HTTP servers, and S3.
//! Client crates should depend on this crate for protocol types instead of
//! depending on `migration-coord`.

pub mod reducer;
pub mod schema;
