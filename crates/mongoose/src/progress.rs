//! Local copy progress (`progress.json`) and the per-shard JSONL
//! result files (`failures/`, `downgrades/`).

use crate::util::{read_json_opt, utc_now, write_json_atomic};
use crate::workdir::WorkDir;
use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};
use std::path::{Path, PathBuf};

/// Operator-visible copy state, rewritten atomically after every
/// shard. Doubles as the shard-granularity resume checkpoint: a
/// re-run skips every shard listed in `completed_shards`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CopyProgress {
    pub run_id: String,
    pub started_utc: String,
    pub updated_utc: String,
    pub shards_total: u64,
    /// Manifest `path`s of shards fully processed (all rows dispatched
    /// and recorded; failures drained to JSONL).
    pub completed_shards: Vec<String>,
    pub files_ok: u64,
    pub files_failed: u64,
    /// Committed copies whose source changed mid-copy (also counted in
    /// `files_ok`; each has a TornCopy downgrade record).
    pub files_torn: u64,
    pub bytes_moved: u64,
    pub throughput_mb_s_1m: f64,
    /// True once every shard completed and the root mtime restore ran.
    pub done: bool,
}

impl CopyProgress {
    pub fn fresh(run_id: &str, shards_total: u64) -> Self {
        Self {
            run_id: run_id.to_string(),
            started_utc: utc_now(),
            updated_utc: utc_now(),
            shards_total,
            completed_shards: Vec::new(),
            files_ok: 0,
            files_failed: 0,
            files_torn: 0,
            bytes_moved: 0,
            throughput_mb_s_1m: 0.0,
            done: false,
        }
    }

    /// Resume an existing progress file when it belongs to this run;
    /// start fresh otherwise (a different run id in the same work dir
    /// is refused upstream by the run-spec check).
    pub fn load_or_fresh(workdir: &WorkDir, run_id: &str, shards_total: u64) -> Result<Self> {
        match read_json_opt::<CopyProgress>(&workdir.progress_json())? {
            Some(p) if p.run_id == run_id => Ok(Self {
                shards_total,
                updated_utc: utc_now(),
                ..p
            }),
            _ => Ok(Self::fresh(run_id, shards_total)),
        }
    }

    pub fn is_completed(&self, shard_path: &str) -> bool {
        self.completed_shards.iter().any(|s| s == shard_path)
    }

    pub fn write(&mut self, workdir: &WorkDir) -> Result<()> {
        self.updated_utc = utc_now();
        write_json_atomic(&workdir.progress_json(), self)
    }
}

/// Write one shard's drained sink body to `<dir>/<stem>.jsonl`.
/// An empty body removes any stale file from a previous attempt of
/// the same shard, so results always reflect the last completed pass.
/// Returns the path written, or `None` when there was nothing.
pub fn write_shard_jsonl(dir: &Path, stem: &str, body: &[u8]) -> Result<Option<PathBuf>> {
    let path = dir.join(format!("{stem}.jsonl"));
    if body.is_empty() {
        match std::fs::remove_file(&path) {
            Ok(()) => {}
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
            Err(e) => return Err(e).with_context(|| format!("removing stale {}", path.display())),
        }
        return Ok(None);
    }
    std::fs::create_dir_all(dir).with_context(|| format!("creating {}", dir.display()))?;
    std::fs::write(&path, body).with_context(|| format!("writing {}", path.display()))?;
    Ok(Some(path))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn progress_resumes_only_its_own_run() {
        let dir = tempfile::tempdir().unwrap();
        let wd = WorkDir::new(dir.path());
        let mut p = CopyProgress::fresh("run-a", 3);
        p.completed_shards
            .push("canonical/part-0000.parquet".into());
        p.files_ok = 10;
        p.write(&wd).unwrap();

        let resumed = CopyProgress::load_or_fresh(&wd, "run-a", 3).unwrap();
        assert!(resumed.is_completed("canonical/part-0000.parquet"));
        assert!(!resumed.is_completed("canonical/part-0001.parquet"));
        assert_eq!(resumed.files_ok, 10);

        let other = CopyProgress::load_or_fresh(&wd, "run-b", 3).unwrap();
        assert!(
            other.completed_shards.is_empty(),
            "different run starts fresh"
        );
    }

    #[test]
    fn shard_jsonl_written_only_when_nonempty_and_stale_files_removed() {
        let dir = tempfile::tempdir().unwrap();
        let sink_dir = dir.path().join("failures");

        // Nothing to write, nothing created.
        assert!(write_shard_jsonl(&sink_dir, "part-0000", b"")
            .unwrap()
            .is_none());
        assert!(!sink_dir.exists());

        let body = b"{\"row_id\":1}\n";
        let path = write_shard_jsonl(&sink_dir, "part-0000", body)
            .unwrap()
            .expect("written");
        assert_eq!(std::fs::read(&path).unwrap(), body);

        // A clean re-run of the shard removes the stale record file.
        assert!(write_shard_jsonl(&sink_dir, "part-0000", b"")
            .unwrap()
            .is_none());
        assert!(!path.exists());
    }
}
