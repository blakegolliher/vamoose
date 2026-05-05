//! On-S3 JSON record types.
//!
//! These are the serialization formats for `manifest.json`, claim
//! objects, progress files, batch audit records, and failure records.
//! The schema is stable across worker and aggregator; bump
//! `RUN_FORMAT_VERSION` and add migration logic if you change shapes.

use crate::time::UtcTime;
use serde::{Deserialize, Serialize};

/// On-disk format version. Bump on breaking changes.
pub const RUN_FORMAT_VERSION: u32 = 1;

// =============================================================================
// Manifest
// =============================================================================

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Manifest {
    pub format_version: u32,
    pub run_id: String,
    pub created_utc: UtcTime,
    pub shards: Vec<ShardEntry>,
    pub total_rows: u64,
    pub source: Endpoint,
    pub dest: Endpoint,
    pub options: MigrationOptions,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ShardEntry {
    /// S3 key relative to the run root, e.g. `index/part-0042.parquet`.
    pub key: String,
    pub rows: u64,
    pub bytes: u64,
    /// ETag returned by S3 at upload time. Workers verify on download.
    pub etag: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Endpoint {
    pub kind: EndpointKind,
    /// libnfs URL, e.g. `nfs://host/export`.
    pub url: String,
    /// Logical path prefix inside the export.
    #[serde(default = "default_root")]
    pub root: String,
}

fn default_root() -> String {
    "/".to_string()
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum EndpointKind {
    Nfs,
    /// Posix kernel mount. Escape hatch only — not the default path.
    Posix,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MigrationOptions {
    pub preserve_owner: bool,
    pub preserve_mode: bool,
    pub preserve_times: bool,
    /// Honored when the walker emits `xattr_blob`. No-op until then.
    pub preserve_xattr: bool,
    pub server_side_copy: ServerSideCopy,
}

impl Default for MigrationOptions {
    fn default() -> Self {
        Self {
            preserve_owner: true,
            preserve_mode: true,
            preserve_times: true,
            preserve_xattr: true,
            server_side_copy: ServerSideCopy::Auto,
        }
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ServerSideCopy {
    Auto,
    Force,
    Off,
}

// =============================================================================
// Claim
// =============================================================================

/// The contents of `shards/<shard>.parquet.claim`.
///
/// Authoritative ownership comes from the S3 object's etag — this body
/// is informational. `epoch` increments on each heartbeat; if a reader
/// sees an unchanged epoch over multiple heartbeat intervals, the owner
/// is assumed dead.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ClaimRecord {
    pub host: String,
    pub claimed_utc: UtcTime,
    pub epoch: u64,
    pub state: ClaimState,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ClaimState {
    /// Worker is currently processing this shard.
    Active,
    /// All rows in the shard processed successfully (or failures
    /// recorded). Final, immutable state.
    Completed,
    /// Worker hit an unrecoverable error on this shard (e.g. corrupt
    /// parquet). Another worker should *not* reclaim — the shard needs
    /// human attention.
    Failed,
}

// =============================================================================
// Progress
// =============================================================================

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProgressRecord {
    pub host: String,
    pub started_utc: UtcTime,
    pub heartbeat_utc: UtcTime,
    pub current_shard: Option<String>,
    pub shard_rows_total: u64,
    pub shard_rows_done: u64,
    pub shard_bytes_done: u64,
    pub files_ok: u64,
    pub files_failed: u64,
    pub throughput_mb_s_1m: f64,
    /// "active", "degraded", "draining", "exiting".
    pub status: String,
}

// =============================================================================
// Batch audit
// =============================================================================

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BatchRecord {
    pub host: String,
    pub shard: String,
    pub batch_seq: u64,
    pub row_first: u64,
    pub row_last: u64,
    pub files: u64,
    pub bytes: u64,
    pub started_utc: UtcTime,
    pub finished_utc: UtcTime,
    pub strategy_counts: StrategyCounts,
    pub failures: u64,
}

/// How many files in this batch went down each mover strategy. Useful
/// for figuring out where wall-clock went.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct StrategyCounts {
    pub server_side_copy: u64,
    pub libnfs_io_uring: u64,
    pub kernel_cfr: u64,
    pub symlink: u64,
    pub hardlink: u64,
    pub empty: u64,
}

// =============================================================================
// Failure
// =============================================================================

/// A single per-file failure. POSIX paths can contain non-UTF-8 bytes,
/// so the path is base64-encoded raw bytes.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FailureRecord {
    pub row_id: u64,
    pub shard: String,
    /// Base64-encoded raw bytes of the source path.
    pub path_b64: String,
    /// errno name (e.g. "ENOSPC", "EACCES") or other short tag.
    pub error: String,
    pub phase: FailurePhase,
    pub ts: UtcTime,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum FailurePhase {
    Open,
    Read,
    Write,
    Setattr,
    Setxattr,
    Rename,
    Symlink,
    Hardlink,
    ServerSideCopy,
}

// =============================================================================
// Downgrade
// =============================================================================
//
// A downgrade is a successful copy that had to drop a metadata
// attribute the user asked for — usually because the source row's
// column was null. The file *is* on dest; the user just doesn't have
// 100% fidelity. Distinct from failures so the failure-rate metric
// stays clean.

/// A single downgrade event. Format defined in SCHEMA_CONTRACT.md
/// "Null attribute semantics".
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DowngradeRecord {
    pub row_id: u64,
    pub shard: String,
    /// Base64-encoded raw bytes of the source path.
    pub path_b64: String,
    pub downgrade: DowngradeKind,
    pub ts: UtcTime,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum DowngradeKind {
    /// Source `mtime_sec` was null; mover skipped utimes for mtime.
    NullMtime,
    /// Source `atime_sec` was null and atime preservation was
    /// requested.
    NullAtime,
    /// Source `uid` or `gid` was null; mover skipped chown.
    NullOwner,
    /// Hardlink grouping fell back to inode-only because `fsid` was
    /// null. Per shard, only the first occurrence emits a record.
    FsidUngrouped,
    /// Symlink mode bits could not be preserved because the destination
    /// uses NFSv3 (`nfs_chmod` follows symlinks; there is no
    /// lchmod-equivalent). Distinct from `NullOwner`/`NullMtime` —
    /// here the source attribute is present and non-null, but the
    /// protocol cannot apply it to a symlink. See SCHEMA_CONTRACT.md
    /// "Symlink mode preservation".
    #[serde(rename = "SYMLINK_MODE_NFSV3")]
    SymlinkModeNfsV3,
    /// Source EOF arrived before `row.size` bytes had been read.
    /// `size` is advisory by default (SCHEMA_CONTRACT.md "Size
    /// semantics") so the file is still committed; the record exists
    /// so the operator can see that actual bytes copied differ from
    /// the indexed size. Distinct from a hard `SIZE_CHANGED` failure
    /// (which only fires when `[copy].require_unchanged_size = true`).
    EarlyEof,
    /// Symlink mtime/atime could not be preserved because the
    /// destination uses NFSv3 (`nfs_utimes` follows symlinks and
    /// would modify the target instead; there is no
    /// lutimes-equivalent). Distinct from `NullMtime` — here the
    /// source attribute is present and non-null, but the protocol
    /// cannot apply it to a symlink. Mirrors `SymlinkModeNfsV3`
    /// for the time attributes.
    #[serde(rename = "SYMLINK_TIME_NFSV3")]
    SymlinkTimeNfsV3,
}
