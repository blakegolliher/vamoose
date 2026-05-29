//! Coord error type.
//!
//! Wraps `migration_core::Error` for storage failures (so the v2
//! `PreconditionFailed` signal propagates cleanly into the lease
//! logic), and adds coord-specific variants for lease, snapshot, and
//! event-log invariants.

use thiserror::Error;

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Debug, Error)]
pub enum Error {
    /// The lease is held by another coord and has not expired yet.
    /// Carries the holder identity and the expiry the loser saw so
    /// the operator can tell whether it's a real conflict or a
    /// stale-clock race.
    #[error("lease held by {holder} until {expires_at}")]
    LeaseHeld {
        holder: String,
        expires_at: chrono::DateTime<chrono::Utc>,
    },

    /// `coord/lease` exists but cannot be parsed.
    #[error("lease object is malformed: {0}")]
    LeaseMalformed(String),

    /// We held the lease but lost it during a refresh (someone took
    /// over after our previous refresh expired). Caller must stop
    /// writing.
    #[error("lease was taken over (no longer held by us)")]
    LeaseLost,

    /// The snapshot file is present but cannot be parsed at the
    /// current schema_version.
    #[error("snapshot is malformed or schema_version newer than supported: {0}")]
    SnapshotMalformed(String),

    /// The event log has a gap that replay cannot bridge — the
    /// snapshot's `last_seq` points past the first available event,
    /// or two chunks overlap.
    #[error("event log gap: expected seq {expected}, found {found}")]
    EventLogGap { expected: u64, found: u64 },

    /// An event-log chunk is present but a line cannot be parsed at
    /// the current schema_version, or carries a schema_version newer
    /// than supported.
    #[error("event chunk {key} malformed at line {line}: {detail}")]
    ChunkMalformed {
        key: String,
        line: usize,
        detail: String,
    },

    /// Underlying storage call failed.
    #[error("storage: {0}")]
    Storage(#[from] migration_core::Error),

    /// Serde failed to (de)serialize.
    #[error("serde: {0}")]
    Serde(#[from] serde_json::Error),

    #[error("{0}")]
    Other(#[from] anyhow::Error),
}
