//! Compatibility re-export for the shared control-plane schema.
//!
//! New consumers should import these types from
//! [`migration_control_protocol::schema`]. This module preserves the existing
//! `migration_coord::schema` path during the layering transition.

pub use migration_control_protocol::schema::*;
