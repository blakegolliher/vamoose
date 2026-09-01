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

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
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
    /// Compatibility-reserved policy field. The current NFSv3 mover does not
    /// select a server-side COPY strategy.
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
/// Authoritative ownership comes from the S3 object's etag — this body is
/// informational. `epoch` identifies the acquisition generation and advances
/// on reclaim; a holder never rewrites the claim during heartbeat.
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
    /// Rows R8 caught at commit time — fence tripped between dispatch
    /// and the row's rename/link/symlink op. NOT a failure: the next
    /// reclaimer will copy the row. `#[serde(default)]` so older
    /// progress objects from pre-R8 builds still parse.
    #[serde(default)]
    pub files_fenced: u64,
    pub throughput_mb_s_1m: f64,
    /// "active", "degraded", "draining", "exiting".
    pub status: String,
    /// Per-RPC latency over the last heartbeat window (source NFS,
    /// destination NFS, S3) with the derived busy shares — the
    /// "who is slow" answer, kept in the bucket so a run can be
    /// read back after the fact. `None` on older writers.
    #[serde(default)]
    pub latency: Option<crate::latency::Summary>,
    /// Etag of the claim object this worker currently holds, if any.
    /// `None` means the worker is between shards (idle/scanning) or
    /// has self-fenced. `Some(etag)` is the proof-of-ownership that
    /// peers cross-check against the claim body's etag for the
    /// progress-file liveness fast-reclaim path. See
    /// `docs/work-items/PROGRESS_LIVENESS_CROSS_CHECK.md`.
    /// `#[serde(default)]` so pre-cross-check progress objects parse
    /// as `None`, which the reclaimer treats as "not held → eligible".
    #[serde(default)]
    pub held_etag: Option<String>,
    /// Heartbeat interval (seconds) the writing worker is configured
    /// with. Peers compute a freshness threshold of `2 ×
    /// heartbeat_sec` from this value so the cross-check is
    /// calibrated against the *writer*, not assumed from the reader's
    /// config. `0` (the default for pre-cross-check progress
    /// objects) means "missing → defer to lease".
    #[serde(default)]
    pub heartbeat_sec: u64,
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
    /// Compatibility-reserved wire counter; no server-side COPY path exists.
    pub server_side_copy: u64,
    /// Historical wire name for regular-file libnfs copies.
    pub libnfs_io_uring: u64,
    /// Compatibility-reserved wire counter; no kernel-CFR path exists.
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
    /// Compatibility-reserved serialized phase; no current mover path emits it.
    ServerSideCopy,
    /// R8: the fence tripped between the shard processor's last
    /// between-row check and the mover's commit-point op (rename / link
    /// / symlink). The row was *not* committed and is *not* a per-file
    /// failure — the shard goes back to claimable when our claim
    /// terminates, and the next reclaimer will copy this row. The shard
    /// processor recognizes this phase, skips the failures sink, and
    /// emits a WARN with the row_id for operator visibility. See
    /// `docs/CLAIM_PROTOCOL.md` "Race catalog" row R8.
    Fenced,
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
    /// The source's `(size, mtime, ctime)` changed between the
    /// pre- and post-copy stat brackets (`pipelined_copy`'s torn-read
    /// detection): the file was modified while being copied, so the
    /// destination holds some interleaving of the pre- and
    /// post-versions. The file is still committed — at-least-once
    /// semantics, the source remains intact — and this record is the
    /// operator-visible trace of the tear. `pre`/`post` are the
    /// bracket triples `(size, mtime_sec, ctime_sec)`. Async-path
    /// only for now; the sync mover has no stat bracket. A future
    /// multi-pass driver (MULTI_PASS_MOVER.md) would re-copy torn
    /// rows; until then this record is all the remediation there is.
    TornCopy {
        pre: (u64, i64, i64),
        post: (u64, i64, i64),
    },
}

#[cfg(test)]
mod tests {
    use super::*;

    // R8: pin the wire form for FailurePhase::Fenced so a future
    // rename_all change doesn't silently shift the on-disk JSON
    // shape. The aggregator parses these strings; downgrade-records
    // and failure-records share the same enum across the schema.
    #[test]
    fn failure_phase_fenced_serializes_to_snake_case() {
        let json = serde_json::to_string(&FailurePhase::Fenced).unwrap();
        assert_eq!(json, "\"fenced\"");
    }

    #[test]
    fn failure_phase_fenced_roundtrips_through_json() {
        let json = serde_json::to_string(&FailurePhase::Fenced).unwrap();
        let decoded: FailurePhase = serde_json::from_str(&json).unwrap();
        assert_eq!(decoded, FailurePhase::Fenced);
    }

    // Pre-cross-check progress objects (written by workers before
    // held_etag/heartbeat_sec were added) must still parse. Both
    // new fields are `#[serde(default)]`; absence means `None` and
    // `0`, which the reclaimer's predicate degrades safely on.
    #[test]
    fn progress_record_parses_old_schema_without_cross_check_fields() {
        let json = r#"{
            "host": "host-A",
            "started_utc": "2025-01-01T00:00:00Z",
            "heartbeat_utc": "2025-01-01T00:00:30Z",
            "current_shard": null,
            "shard_rows_total": 0,
            "shard_rows_done": 0,
            "shard_bytes_done": 0,
            "files_ok": 0,
            "files_failed": 0,
            "throughput_mb_s_1m": 0.0,
            "status": "active"
        }"#;
        let p: ProgressRecord = serde_json::from_str(json).unwrap();
        assert_eq!(p.host, "host-A");
        assert_eq!(p.held_etag, None);
        assert_eq!(p.heartbeat_sec, 0);
    }

    #[test]
    fn progress_record_roundtrips_with_cross_check_fields() {
        let p = ProgressRecord {
            host: "host-B".into(),
            started_utc: UtcTime::now(),
            heartbeat_utc: UtcTime::now(),
            current_shard: Some("part-0001.parquet".into()),
            shard_rows_total: 100,
            shard_rows_done: 50,
            shard_bytes_done: 1024,
            files_ok: 50,
            files_failed: 0,
            files_fenced: 0,
            throughput_mb_s_1m: 12.5,
            status: "active".into(),
            held_etag: Some("etag-abc".into()),
            heartbeat_sec: 30,
            latency: None,
        };
        let json = serde_json::to_string(&p).unwrap();
        let decoded: ProgressRecord = serde_json::from_str(&json).unwrap();
        assert_eq!(decoded.held_etag.as_deref(), Some("etag-abc"));
        assert_eq!(decoded.heartbeat_sec, 30);
    }
}
