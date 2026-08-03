//! Error types for the migration system.
//!
//! `Error` is the workspace-wide error. Workers and the mover translate
//! these into operational responses (retry, self-fence, fail the file,
//! fail the shard). Callers should match on variants rather than string
//! contents.

use thiserror::Error;

pub type Result<T, E = Error> = std::result::Result<T, E>;

#[derive(Debug, Error)]
pub enum Error {
    /// S3 returned 412 Precondition Failed. For the claim protocol this
    /// is *expected* — it means another worker holds the claim. Callers
    /// should not log this at error level.
    #[error("S3 precondition failed (claim contended)")]
    PreconditionFailed,

    /// The worker has lost its claim and must self-fence. Triggered by
    /// 412 on heartbeat, sustained 5xx past retry budget, or local
    /// clock skew. See DESIGN.md "S3 claim protocol and worker lifecycle".
    #[error("claim invalidated; worker must self-fence: {0}")]
    ClaimInvalidated(String),

    /// Manifest etag changed underneath us — the run was swapped or
    /// re-uploaded. Workers exit cleanly on this.
    #[error("manifest changed: expected etag {expected}, got {actual}")]
    ManifestChanged { expected: String, actual: String },

    /// Parquet KV footer reports a `format_version` we don't speak.
    /// Per SCHEMA_CONTRACT.md, the worker refuses such shards rather
    /// than guessing at compatibility.
    #[error("schema format_version mismatch: expected {expected}, got {actual} (see SCHEMA_CONTRACT.md)")]
    SchemaVersionMismatch { expected: u32, actual: u32 },

    /// A single parquet row violated a contract invariant (e.g.
    /// `file_type=0` / Unknown, which is forbidden in parquet per
    /// SCHEMA_CONTRACT.md). Surface enough context that the operator
    /// can pinpoint the row without re-reading the shard.
    #[error("corrupt row {row_id}: {reason}")]
    CorruptRow { row_id: u64, reason: String },

    /// Parquet shard could not be read or decoded. The worker classifies this
    /// separately from a worker-local storage or transport failure.
    #[error("shard {shard} corrupt: {source}")]
    ShardCorrupt {
        shard: String,
        #[source]
        source: anyhow::Error,
    },

    /// A required column is missing from the parquet schema.
    #[error("required column missing from shard: {0}")]
    MissingColumn(&'static str),

    /// Source and destination endpoints overlap such that any write to
    /// the destination could land on the source. Surfaced by the
    /// startup guard before any data-plane operation; refusing to
    /// start is the only safe response. See BUGFIX_PLAN.md "Fix 3".
    #[error(
        "source and destination overlap; refusing to start\n\n\
         {detail}"
    )]
    SourceDestOverlap { detail: String },

    #[error("S3: {0}")]
    S3(#[from] aws_sdk_s3::Error),

    #[error("parquet: {0}")]
    Parquet(#[from] parquet::errors::ParquetError),

    #[error("arrow: {0}")]
    Arrow(#[from] arrow::error::ArrowError),

    #[error("io: {0}")]
    Io(#[from] std::io::Error),

    #[error("serde_json: {0}")]
    Json(#[from] serde_json::Error),

    #[error("{0}")]
    Other(#[from] anyhow::Error),
}

impl Error {
    /// True if this error means "another worker beat us to a claim" —
    /// not a real error, just a signal to try a different shard.
    pub fn is_contention(&self) -> bool {
        matches!(self, Error::PreconditionFailed)
    }

    /// True if this error requires the worker to self-fence and stop
    /// writing to dest.
    pub fn requires_fence(&self) -> bool {
        matches!(self, Error::ClaimInvalidated(_))
    }
}
