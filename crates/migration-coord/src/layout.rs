//! S3 key layout for coord state.
//!
//! All coord-owned S3 keys are constructed through this module so the
//! REST handlers, snapshot writer, event-log writer, and archive logic
//! cannot drift on key format. Mirrors the role
//! `migration_core::layout` plays for worker-owned keys.
//!
//! Layout (relative to the bucket root — note that this bucket may
//! also hold worker data at the same root; the prefixes here are all
//! disjoint from those in `migration_core::layout`):
//!
//! ```text
//! coord/lease                          # lease object (atomic create/takeover)
//! state/snapshot.json                  # current full state
//! state/snapshot-<ts>.json             # rolling history (last N)
//! events/_cluster/<seq:020>.jsonl      # worker join/leave, coord lifecycle
//! events/<job_id>/<seq:020>.jsonl      # per-job event chunks
//! jobs/<job_id>/config.json            # immutable original config
//! jobs/<job_id>/files/failed.jsonl     # per-file failure list (for retries)
//! audit/<YYYY-MM-DD>/<seq>.jsonl       # admin command audit trail
//! archivelogs/<job_id>/<seq:020>.jsonl # event chunks rolled here on completion
//! ```
//!
//! Chunk size: every event-log file holds at most 1000 events. Rolled
//! at the same cadence as the snapshot (1000 events OR 5 minutes).
//!
//! Retention: `archivelogs/` has no coord-side compaction or
//! retention policy in v1 — chunks accumulate until the operator
//! garbage-collects them (e.g. an S3 bucket lifecycle rule on the
//! `archivelogs/` prefix). Nothing in the coord hot path reads them;
//! they exist for audit and a future `restore-from-archive` workflow.
//!
//! Sequence width: 20 digits, zero-padded. Lexical sort = numeric sort
//! up to `2^64 - 1` (`18_446_744_073_709_551_615`), and S3
//! `ListObjectsV2` returns keys in lexical order — replay walks the
//! chunks in seq order without an explicit sort.

pub const LEASE_KEY: &str = "coord/lease";

pub const COORD_PREFIX: &str = "coord/";
pub const STATE_PREFIX: &str = "state/";
pub const EVENTS_PREFIX: &str = "events/";
pub const CLUSTER_EVENTS_PREFIX: &str = "events/_cluster/";
pub const JOBS_PREFIX: &str = "jobs/";
pub const AUDIT_PREFIX: &str = "audit/";
pub const ARCHIVE_PREFIX: &str = "archivelogs/";

pub const SNAPSHOT_KEY: &str = "state/snapshot.json";
pub const EVENT_CHUNK_EXT: &str = ".jsonl";

/// Width of the zero-padded `seq` portion of an event-chunk key. Chosen
/// so lexical sort matches numeric sort for any `u64`.
pub const SEQ_WIDTH: usize = 20;

/// Historical snapshot key for a given timestamp. `ts` should be an
/// `RFC 3339` UTC timestamp; the writer uses `Utc::now()` formatted
/// without colons (`20260528T143200Z` style) so the key is filesystem-
/// friendly when an operator downloads the bucket.
pub fn snapshot_history_key(ts: &str) -> String {
    format!("state/snapshot-{ts}.json")
}

/// Cluster-wide event-log chunk key.
///
/// `start_seq` is the lowest sequence number contained in the chunk.
pub fn cluster_events_chunk_key(start_seq: u64) -> String {
    format!("{CLUSTER_EVENTS_PREFIX}{start_seq:020}{EVENT_CHUNK_EXT}")
}

/// Per-job event-log chunk key. `job_id` is treated as opaque — the
/// caller is responsible for ensuring it does not contain `/`.
pub fn job_events_chunk_key(job_id: &str, start_seq: u64) -> String {
    format!("{EVENTS_PREFIX}{job_id}/{start_seq:020}{EVENT_CHUNK_EXT}")
}

/// Per-job event-log prefix — used to list chunks during replay and
/// during archive-on-completion.
pub fn job_events_prefix(job_id: &str) -> String {
    format!("{EVENTS_PREFIX}{job_id}/")
}

pub fn job_config_key(job_id: &str) -> String {
    format!("{JOBS_PREFIX}{job_id}/config.json")
}

pub fn job_failed_files_key(job_id: &str) -> String {
    format!("{JOBS_PREFIX}{job_id}/files/failed.jsonl")
}

/// Audit-chunk key. `date` should be `YYYY-MM-DD` UTC. Sequence is the
/// audit-line sequence within that day. The counter is persisted via
/// snapshots only, so a crash can rewind it; the audit writer handles
/// that by allocating keys with `put_if_absent` and advancing past any
/// collision — existing rows are never overwritten (ledger F22).
pub fn audit_chunk_key(date: &str, seq: u64) -> String {
    format!("{AUDIT_PREFIX}{date}/{seq:020}{EVENT_CHUNK_EXT}")
}

/// Archived per-job event-log chunk key. Mirrors
/// `job_events_chunk_key` but under `archivelogs/`.
pub fn archive_chunk_key(job_id: &str, start_seq: u64) -> String {
    format!("{ARCHIVE_PREFIX}{job_id}/{start_seq:020}{EVENT_CHUNK_EXT}")
}

/// All coord-owned top-level prefixes. Used by the disjointness test
/// against `migration_core::layout` and by `doctor` to enumerate keys
/// the coord owns.
pub const COORD_OWNED_PREFIXES: &[&str] = &[
    COORD_PREFIX,
    STATE_PREFIX,
    EVENTS_PREFIX,
    JOBS_PREFIX,
    AUDIT_PREFIX,
    ARCHIVE_PREFIX,
];

#[cfg(test)]
mod tests {
    use super::*;

    /// Worker (`migration-core::layout`) prefixes the coord must not
    /// collide with. Hard-coded rather than imported so a future change
    /// in either side raises the diff in *both* files — the test does
    /// not silently rubber-stamp a layout drift.
    const WORKER_PREFIXES: &[&str] = &[
        "index/",
        "shards/",
        "progress/",
        "batches/",
        "failures/",
        "downgrades/",
    ];
    const WORKER_ROOT_KEYS: &[&str] = &["manifest.json"];

    #[test]
    fn coord_prefixes_disjoint_from_worker() {
        for c in COORD_OWNED_PREFIXES {
            for w in WORKER_PREFIXES {
                assert!(
                    !c.starts_with(w) && !w.starts_with(c),
                    "coord prefix {c:?} overlaps worker prefix {w:?}",
                );
            }
            for w in WORKER_ROOT_KEYS {
                assert!(
                    !w.starts_with(*c),
                    "worker root key {w:?} would land under coord prefix {c:?}",
                );
            }
        }
    }

    #[test]
    fn key_constants_match_documented_layout() {
        assert_eq!(LEASE_KEY, "coord/lease");
        assert_eq!(SNAPSHOT_KEY, "state/snapshot.json");
        assert_eq!(
            cluster_events_chunk_key(0),
            "events/_cluster/00000000000000000000.jsonl"
        );
        assert_eq!(
            cluster_events_chunk_key(u64::MAX),
            "events/_cluster/18446744073709551615.jsonl"
        );
        assert_eq!(
            job_events_chunk_key("bobby", 17),
            "events/bobby/00000000000000000017.jsonl"
        );
        assert_eq!(job_events_prefix("bobby"), "events/bobby/");
        assert_eq!(job_config_key("bobby"), "jobs/bobby/config.json");
        assert_eq!(
            job_failed_files_key("bobby"),
            "jobs/bobby/files/failed.jsonl"
        );
        assert_eq!(
            audit_chunk_key("2026-05-29", 3),
            "audit/2026-05-29/00000000000000000003.jsonl"
        );
        assert_eq!(
            archive_chunk_key("bobby", 17),
            "archivelogs/bobby/00000000000000000017.jsonl"
        );
    }

    /// Lexical sort = numeric sort for any pair of seqs at the chosen
    /// width. Documented in the module header; this test pins it.
    #[test]
    fn seq_width_preserves_numeric_order() {
        let lo = cluster_events_chunk_key(7);
        let mid = cluster_events_chunk_key(1_000_000);
        let hi = cluster_events_chunk_key(u64::MAX);
        assert!(lo < mid);
        assert!(mid < hi);
    }
}
