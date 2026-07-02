//! Downgrade sink — collects [`DowngradeRecord`]s emitted by the
//! mover when it has to drop a metadata attribute the user asked for
//! (null mtime, null owner, fsid fallback). The orchestrator owns the
//! sink, updates the current shard name when it claims one, and
//! flushes drained JSONL to S3 at shard completion.
//!
//! Lives in the mover crate because the mover is the producer; the
//! orchestrator wires it into the rest of the system. Records use
//! types from `migration_core::records` so workers, aggregator, and
//! retry tooling all agree on the shape.
//!
//! Concurrency: cheap clone (Arc inside). Records are appended under a
//! `std::sync::Mutex` — contention is low because in M2 the mover is
//! single-row, and even at M3 concurrency the per-row append is a
//! single push.

use base64::Engine;
use migration_core::records::{DowngradeKind, DowngradeRecord};
use migration_core::time::UtcTime;
use std::sync::{Arc, Mutex};

#[derive(Default)]
struct Inner {
    /// Last value supplied to `set_current_shard`. Stamped on every
    /// record so callers don't have to thread the shard name through
    /// the per-row dispatch path.
    shard: String,
    records: Vec<DowngradeRecord>,
}

/// Cheap-clone collector of downgrade records.
#[derive(Clone, Default)]
pub struct DowngradeSink {
    inner: Arc<Mutex<Inner>>,
}

impl DowngradeSink {
    pub fn new() -> Self {
        Self::default()
    }

    /// Update the shard name stamped onto subsequent records. Caller
    /// (the orchestrator) sets this when a shard is claimed and
    /// resets it (to "") when releasing.
    pub fn set_current_shard(&self, shard: impl Into<String>) {
        if let Ok(mut g) = self.inner.lock() {
            g.shard = shard.into();
        }
    }

    /// Record one downgrade for the row at `(row_id, path)`.
    pub fn record(&self, row_id: u64, path: &[u8], kind: DowngradeKind) {
        let mut g = match self.inner.lock() {
            Ok(g) => g,
            Err(_) => return,
        };
        let path_b64 = base64::engine::general_purpose::STANDARD.encode(path);
        let shard = g.shard.clone();
        g.records.push(DowngradeRecord {
            row_id,
            shard,
            path_b64,
            downgrade: kind,
            ts: UtcTime::now(),
        });
    }

    /// Number of buffered records (observability + tests).
    pub fn len(&self) -> usize {
        self.inner.lock().map(|g| g.records.len()).unwrap_or(0)
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Drain all buffered records into a JSONL byte buffer suitable
    /// for an S3 PUT. The buffer is empty if nothing was recorded.
    pub fn drain_jsonl(&self) -> Vec<u8> {
        let mut g = match self.inner.lock() {
            Ok(g) => g,
            Err(_) => return Vec::new(),
        };
        let mut out = Vec::with_capacity(g.records.len() * 256);
        for record in g.records.drain(..) {
            match serde_json::to_vec(&record) {
                Ok(line) => {
                    out.extend_from_slice(&line);
                    out.push(b'\n');
                }
                Err(e) => {
                    tracing::warn!(error = ?e, "downgrade serialize failed");
                }
            }
        }
        out
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn record_and_drain_roundtrip() {
        let sink = DowngradeSink::new();
        sink.set_current_shard("part-0042.parquet");
        sink.record(1, b"/foo/bar", DowngradeKind::NullMtime);
        sink.record(2, b"/baz", DowngradeKind::NullOwner);
        assert_eq!(sink.len(), 2);

        let body = sink.drain_jsonl();
        assert_eq!(sink.len(), 0, "drain empties the buffer");
        let lines: Vec<_> = body
            .split(|&b| b == b'\n')
            .filter(|l| !l.is_empty())
            .collect();
        assert_eq!(lines.len(), 2);

        let r0: DowngradeRecord = serde_json::from_slice(lines[0]).unwrap();
        assert_eq!(r0.row_id, 1);
        assert_eq!(r0.shard, "part-0042.parquet");
        assert_eq!(r0.downgrade, DowngradeKind::NullMtime);
        // path_b64 must round-trip the raw bytes — base64 of "/foo/bar"
        let decoded = base64::engine::general_purpose::STANDARD
            .decode(&r0.path_b64)
            .unwrap();
        assert_eq!(decoded, b"/foo/bar");
    }

    #[test]
    fn empty_sink_drains_to_empty() {
        let sink = DowngradeSink::new();
        assert!(sink.is_empty());
        assert!(sink.drain_jsonl().is_empty());
    }

    #[test]
    fn screaming_snake_case_serialization() {
        let sink = DowngradeSink::new();
        sink.set_current_shard("s");
        sink.record(0, b"/p", DowngradeKind::NullMtime);
        sink.record(0, b"/p", DowngradeKind::FsidUngrouped);
        let body = sink.drain_jsonl();
        let s = std::str::from_utf8(&body).unwrap();
        assert!(s.contains("\"NULL_MTIME\""), "body: {s}");
        assert!(s.contains("\"FSID_UNGROUPED\""), "body: {s}");
    }

    /// `EarlyEof` must serialize as `EARLY_EOF` (default
    /// SCREAMING_SNAKE_CASE rename — no explicit `serde(rename)`
    /// override). This is the operator-visible tag for the
    /// premature-EOF surface added by PR2_HARDENING; see
    /// `mover::stream_copy` and `docs/CORRECTNESS_RULES.md`
    /// "Cross-check C library FFI".
    #[test]
    fn early_eof_serializes_with_canonical_tag() {
        let sink = DowngradeSink::new();
        sink.set_current_shard("part-0001.parquet");
        sink.record(42, b"/some/file.bin", DowngradeKind::EarlyEof);

        let body = sink.drain_jsonl();
        let s = std::str::from_utf8(&body).unwrap();
        assert!(
            s.contains("\"EARLY_EOF\""),
            "expected EARLY_EOF tag, got body: {s}"
        );

        let line = body.split(|&b| b == b'\n').next().unwrap();
        let r: DowngradeRecord = serde_json::from_slice(line).unwrap();
        assert_eq!(r.downgrade, DowngradeKind::EarlyEof);
        assert_eq!(r.row_id, 42);
        assert_eq!(r.shard, "part-0001.parquet");
    }

    /// `SymlinkTimeNfsV3` must serialize as the literal string
    /// `SYMLINK_TIME_NFSV3` — same canonicalization as
    /// `SymlinkModeNfsV3`. Operator-visible tag emitted by
    /// `do_symlink` when the source row has a real mtime that
    /// NFSv3 can't preserve on a link (no lutimes-equivalent).
    #[test]
    fn symlink_time_nfsv3_serializes_with_canonical_tag() {
        let sink = DowngradeSink::new();
        sink.set_current_shard("part-0001.parquet");
        sink.record(99, b"/some/link", DowngradeKind::SymlinkTimeNfsV3);

        let body = sink.drain_jsonl();
        let s = std::str::from_utf8(&body).unwrap();
        assert!(s.contains("\"SYMLINK_TIME_NFSV3\""), "body: {s}");
        assert!(!s.contains("NFS_V3"), "must not split NFS_V3: {s}");

        let line = body.split(|&b| b == b'\n').next().unwrap();
        let r: DowngradeRecord = serde_json::from_slice(line).unwrap();
        assert_eq!(r.downgrade, DowngradeKind::SymlinkTimeNfsV3);
        assert_eq!(r.row_id, 99);
    }

    /// `TornCopy` must round-trip through the sink's JSONL with its
    /// pre/post `(size, mtime, ctime)` payload intact, tagged
    /// `TORN_COPY` (default SCREAMING_SNAKE_CASE rename). Emitted by
    /// `file_mover::copy_regular` when `pipelined_copy` observed the
    /// source change between the pre/post stat brackets — the file
    /// still committed (at-least-once; source intact), so the record
    /// is the only operator-visible trace of the tear. See
    /// docs/work-items/MOVER_TORN_COPY_SURFACE.md (F05).
    #[test]
    fn downgrade_sink_roundtrips_torn_record() {
        let sink = DowngradeSink::new();
        sink.set_current_shard("part-0007.parquet");
        sink.record(
            11,
            b"/hot/file.bin",
            DowngradeKind::TornCopy {
                pre: (1024, 100, 100),
                post: (2048, 200, 300),
            },
        );

        let body = sink.drain_jsonl();
        let s = std::str::from_utf8(&body).unwrap();
        assert!(s.contains("\"TORN_COPY\""), "body: {s}");

        let line = body.split(|&b| b == b'\n').next().unwrap();
        let r: DowngradeRecord = serde_json::from_slice(line).unwrap();
        assert_eq!(r.row_id, 11);
        assert_eq!(r.shard, "part-0007.parquet");
        assert_eq!(
            r.downgrade,
            DowngradeKind::TornCopy {
                pre: (1024, 100, 100),
                post: (2048, 200, 300),
            },
        );
        let decoded = base64::engine::general_purpose::STANDARD
            .decode(&r.path_b64)
            .unwrap();
        assert_eq!(decoded, b"/hot/file.bin");
    }

    /// `SymlinkModeNfsV3` must serialize as the literal string
    /// `SYMLINK_MODE_NFSV3` — the operator-facing tag specified in
    /// SCHEMA_CONTRACT.md and BUGFIX_PLAN.md "Fix 5". The default
    /// SCREAMING_SNAKE_CASE rename would produce `SYMLINK_MODE_NFS_V3`,
    /// hence the explicit `serde(rename = ...)` on the variant.
    #[test]
    fn symlink_mode_nfsv3_serializes_with_canonical_tag() {
        let sink = DowngradeSink::new();
        sink.set_current_shard("s");
        sink.record(7, b"/link", DowngradeKind::SymlinkModeNfsV3);

        let body = sink.drain_jsonl();
        let s = std::str::from_utf8(&body).unwrap();
        assert!(
            s.contains("\"SYMLINK_MODE_NFSV3\""),
            "expected canonical tag, got body: {s}",
        );
        assert!(!s.contains("NFS_V3"), "must not split NFS_V3: {s}");

        // Round-trip: serialize and parse back.
        let line = body.split(|&b| b == b'\n').next().unwrap();
        let r: DowngradeRecord = serde_json::from_slice(line).unwrap();
        assert_eq!(r.downgrade, DowngradeKind::SymlinkModeNfsV3);
    }
}
