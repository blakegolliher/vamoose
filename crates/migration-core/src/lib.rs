//! migration-core
//!
//! Shared building blocks for the migration system. This crate is the
//! single source of truth for:
//!
//! - On-disk and on-S3 record formats (`records`)
//! - The parquet index schema (`schema`)
//! - The S3 layout and key conventions (`layout`)
//! - The claim protocol — conditional PUT semantics, heartbeat,
//!   self-fence (`claim`)
//! - S3 client wrappers used by both worker and aggregator (`s3`)
//! - Parquet shard reader that mmaps a local file and streams rows
//!   in `row_id` order (`shard`)
//!
//! Anything that both the worker and aggregator need to agree on lives
//! here. The mover (`migration-mover`) depends on this crate but adds
//! its own libnfs data-plane implementation.

// `Error::S3(aws_sdk_s3::Error)` carries a ~168-byte variant that
// recent clippy (1.92+) flags as `result_large_err` everywhere `Result<T>`
// appears. Boxing the SDK error across the entire crate is a wider
// refactor than this layer warrants; allow the lint at crate scope.
#![allow(clippy::result_large_err)]

pub mod claim;
#[cfg(test)]
mod contract_drift_tests;
pub mod errors;
pub mod fence;
pub mod latency;
pub mod layout;
pub mod overlap;
pub mod prepare_tools;
pub mod records;
pub mod s3;
pub mod schema;
pub mod shard;
pub mod time;

pub use errors::{Error, Result};
pub use fence::Fence;
