//! Failure sink — collects [`FailureRecord`]s for files that didn't
//! make it from source to dest. Sibling of [`crate::DowngradeSink`]
//! and built on the same shape: cheap-clone, per-shard stamp, drain
//! to JSONL for an S3 PUT.
//!
//! The processor records on Err from the mover; the orchestrator
//! drains and PUTs to `failures/host-<id>/<shard-stem>-e<epoch>.jsonl`
//! (one object per shard flush — see `layout::failures_flush_key`)
//! after each shard.
//! Intentionally not behind a trait — there's only one production
//! impl, and tests inspect the buffered records directly.

use base64::Engine;
use migration_core::records::{FailurePhase, FailureRecord};
use migration_core::time::UtcTime;
use std::sync::{Arc, Mutex};

#[derive(Default)]
struct Inner {
    /// Last value supplied to `set_current_shard`. Stamped on every
    /// record so the mover doesn't have to thread shard names down
    /// the per-row dispatch path.
    shard: String,
    records: Vec<FailureRecord>,
}

#[derive(Clone, Default)]
pub struct FailureSink {
    inner: Arc<Mutex<Inner>>,
}

impl FailureSink {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn set_current_shard(&self, shard: impl Into<String>) {
        if let Ok(mut g) = self.inner.lock() {
            g.shard = shard.into();
        }
    }

    pub fn record(&self, row_id: u64, path: &[u8], phase: FailurePhase, error: impl Into<String>) {
        let mut g = match self.inner.lock() {
            Ok(g) => g,
            Err(_) => return,
        };
        let path_b64 = base64::engine::general_purpose::STANDARD.encode(path);
        let shard = g.shard.clone();
        g.records.push(FailureRecord {
            row_id,
            shard,
            path_b64,
            error: error.into(),
            phase,
            ts: UtcTime::now(),
        });
    }

    pub fn len(&self) -> usize {
        self.inner.lock().map(|g| g.records.len()).unwrap_or(0)
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

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
                    tracing::warn!(error = ?e, "failure serialize failed");
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
        let sink = FailureSink::new();
        sink.set_current_shard("part-0042.parquet");
        sink.record(7, b"/etc/passwd", FailurePhase::Open, "EACCES");
        sink.record(8, b"/var/x", FailurePhase::Write, "ENOSPC");
        assert_eq!(sink.len(), 2);

        let body = sink.drain_jsonl();
        assert_eq!(sink.len(), 0);
        let lines: Vec<_> = body
            .split(|&b| b == b'\n')
            .filter(|l| !l.is_empty())
            .collect();
        assert_eq!(lines.len(), 2);

        let r0: FailureRecord = serde_json::from_slice(lines[0]).unwrap();
        assert_eq!(r0.row_id, 7);
        assert_eq!(r0.shard, "part-0042.parquet");
        assert_eq!(r0.error, "EACCES");
        assert_eq!(r0.phase, FailurePhase::Open);

        let decoded = base64::engine::general_purpose::STANDARD
            .decode(&r0.path_b64)
            .unwrap();
        assert_eq!(decoded, b"/etc/passwd");
    }

    #[test]
    fn empty_sink_drains_to_empty() {
        let sink = FailureSink::new();
        assert!(sink.is_empty());
        assert!(sink.drain_jsonl().is_empty());
    }
}
