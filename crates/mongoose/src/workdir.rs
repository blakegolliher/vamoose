//! The local work directory: layout, run identity, and checkpoints.

use crate::util::{read_json_opt, write_json_atomic};
use anyhow::Result;
use migration_core::records::Endpoint;
use serde::{Deserialize, Serialize};
use std::path::{Path, PathBuf};

/// Typed view over `<work-dir>`; all paths below the run live here.
#[derive(Debug, Clone)]
pub struct WorkDir {
    root: PathBuf,
}

impl WorkDir {
    pub fn new(root: impl Into<PathBuf>) -> Self {
        Self { root: root.into() }
    }

    pub fn root(&self) -> &Path {
        &self.root
    }

    pub fn run_json(&self) -> PathBuf {
        self.root.join("run.json")
    }

    pub fn scan_root(&self) -> PathBuf {
        self.root.join("scan")
    }

    pub fn scan_json(&self) -> PathBuf {
        self.root.join("scan.json")
    }

    pub fn canonical_dir(&self) -> PathBuf {
        self.root.join("canonical")
    }

    pub fn rewrite_json(&self) -> PathBuf {
        self.root.join("rewrite.json")
    }

    pub fn manifest_json(&self) -> PathBuf {
        self.root.join("manifest.json")
    }

    pub fn progress_json(&self) -> PathBuf {
        self.root.join("progress.json")
    }

    pub fn failures_dir(&self) -> PathBuf {
        self.root.join("failures")
    }

    pub fn downgrades_dir(&self) -> PathBuf {
        self.root.join("downgrades")
    }

    /// `passes/pass-NNNN` — one resync pass's own work dir (same
    /// layout as the root: scan/, canonical/, manifest.json, …).
    pub fn pass_dir(&self, pass: u32) -> PathBuf {
        self.root.join("passes").join(format!("pass-{pass:04}"))
    }

    /// Which pass's canonical index is the resync baseline; written
    /// atomically at pass completion (the pass commit point).
    pub fn baseline_json(&self) -> PathBuf {
        self.root.join("baseline.json")
    }

    /// The NEW+DIRTY shard subset a resync pass copies.
    pub fn delta_manifest_json(&self) -> PathBuf {
        self.root.join(crate::sync::DELTA_MANIFEST)
    }

    /// Classifier outputs: keep lists, deleted.jsonl, checkpoint.
    pub fn classify_dir(&self) -> PathBuf {
        self.root.join("classify")
    }

    /// Delta shards emitted for this pass.
    pub fn delta_dir(&self) -> PathBuf {
        self.root.join("delta")
    }

    /// Resolve a manifest shard path (work-dir-relative) to an
    /// absolute path.
    pub fn shard_path(&self, relative: &str) -> PathBuf {
        self.root.join(relative)
    }

    /// `scan/attempt-0001`, `attempt-0002`, …: an interrupted scan's
    /// directory is never reused, so partial output cannot pass for a
    /// complete one.
    pub fn next_attempt_dir(&self) -> Result<PathBuf> {
        let scan_root = self.scan_root();
        for n in 1..10_000u32 {
            let candidate = scan_root.join(format!("attempt-{n:04}"));
            if !candidate.exists() {
                return Ok(candidate);
            }
        }
        anyhow::bail!("too many scan attempts under {}", scan_root.display())
    }
}

/// Identity of one run, written first and compared on every resume so
/// a flag edit mid-run cannot silently mix two migrations.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct RunSpec {
    pub run_id: String,
    pub created_utc: String,
    pub source: Endpoint,
    pub dest: Endpoint,
}

/// Write the run spec on first use; on resume, refuse a spec that
/// names a different source or destination.
pub fn ensure_run_spec(path: &Path, fresh: RunSpec) -> Result<RunSpec> {
    match read_json_opt::<RunSpec>(path)? {
        Some(existing) => {
            let same = existing.source.url == fresh.source.url
                && existing.source.root == fresh.source.root
                && existing.dest.url == fresh.dest.url
                && existing.dest.root == fresh.dest.root;
            if !same {
                anyhow::bail!(
                    "this work dir belongs to run {} ({}{} -> {}{}); the flags now say \
                     {}{} -> {}{}. Restore the flags or use a fresh --work-dir.",
                    existing.run_id,
                    existing.source.url,
                    existing.source.root,
                    existing.dest.url,
                    existing.dest.root,
                    fresh.source.url,
                    fresh.source.root,
                    fresh.dest.url,
                    fresh.dest.root,
                );
            }
            Ok(existing)
        }
        None => {
            write_json_atomic(path, &fresh)?;
            Ok(fresh)
        }
    }
}

/// Roots are absolute paths inside the export: `/`, `/a/b`. Accept
/// `a/b` and trailing slashes from operators.
pub fn normalize_root(raw: &str) -> String {
    let trimmed = raw.trim().trim_matches('/');
    if trimmed.is_empty() {
        "/".to_string()
    } else {
        format!("/{trimmed}")
    }
}

pub fn default_run_id() -> String {
    format!("run-{}", chrono::Utc::now().format("%Y%m%dT%H%M%SZ"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use migration_core::records::EndpointKind;

    fn endpoint(url: &str, root: &str) -> Endpoint {
        Endpoint {
            kind: EndpointKind::Nfs,
            url: url.into(),
            root: root.into(),
        }
    }

    fn spec() -> RunSpec {
        RunSpec {
            run_id: "run-1".into(),
            created_utc: "2026-08-31T00:00:00Z".into(),
            source: endpoint("nfs://s/e", "/"),
            dest: endpoint("nfs://d/e", "/"),
        }
    }

    #[test]
    fn layout_paths_hang_off_the_root() {
        let wd = WorkDir::new("/var/lib/mongoose/run-001");
        assert_eq!(
            wd.run_json(),
            PathBuf::from("/var/lib/mongoose/run-001/run.json")
        );
        assert_eq!(
            wd.canonical_dir(),
            PathBuf::from("/var/lib/mongoose/run-001/canonical")
        );
        assert_eq!(
            wd.shard_path("canonical/part-0000.parquet"),
            PathBuf::from("/var/lib/mongoose/run-001/canonical/part-0000.parquet")
        );
        assert_eq!(
            wd.failures_dir(),
            PathBuf::from("/var/lib/mongoose/run-001/failures")
        );
    }

    #[test]
    fn attempt_dirs_never_reuse() {
        let dir = tempfile::tempdir().unwrap();
        let wd = WorkDir::new(dir.path());
        assert_eq!(
            wd.next_attempt_dir().unwrap(),
            wd.scan_root().join("attempt-0001")
        );
        std::fs::create_dir_all(wd.scan_root().join("attempt-0001")).unwrap();
        assert_eq!(
            wd.next_attempt_dir().unwrap(),
            wd.scan_root().join("attempt-0002")
        );
    }

    #[test]
    fn run_spec_is_sticky_per_work_dir() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("run.json");
        let first = ensure_run_spec(&path, spec()).unwrap();
        assert_eq!(first, spec());

        // A later timestamp is not identity: the original is kept.
        let mut later = spec();
        later.created_utc = "2099-01-01T00:00:00Z".into();
        assert_eq!(ensure_run_spec(&path, later).unwrap(), spec());

        // A different destination is refused.
        let mut moved = spec();
        moved.dest.root = "/elsewhere".into();
        let err = ensure_run_spec(&path, moved).unwrap_err();
        assert!(format!("{err:#}").contains("--work-dir"));
    }

    #[test]
    fn roots_normalize_to_absolute_without_trailing_slash() {
        assert_eq!(normalize_root(""), "/");
        assert_eq!(normalize_root("/"), "/");
        assert_eq!(normalize_root("data"), "/data");
        assert_eq!(normalize_root("/data/"), "/data");
        assert_eq!(normalize_root(" /a/b/ "), "/a/b");
    }
}
