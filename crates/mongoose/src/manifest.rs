//! The local manifest — mongoose's run plan.
//!
//! Deliberately its own format rather than a reuse of
//! `migration_core::records::Manifest`: that shape carries S3
//! assumptions (per-shard ETags, S3 keys) that have no local meaning.
//! Endpoints and copy options are reused from `migration_core` so the
//! mover sees the exact types it already understands.
//!
//! Shard `path`s are **work-dir-relative local paths**
//! (`canonical/part-0000.parquet`), never S3 keys.

use crate::util::{read_json_opt, sha256_file, utc_now, write_json_atomic};
use crate::workdir::WorkDir;
use anyhow::{Context, Result};
use migration_core::records::{Endpoint, MigrationOptions};
use serde::{Deserialize, Serialize};
use std::path::Path;

/// Bump on breaking changes to the local manifest shape.
pub const MANIFEST_FORMAT_VERSION: u32 = 1;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LocalManifest {
    pub format_version: u32,
    pub run_id: String,
    pub created_utc: String,
    pub source: Endpoint,
    pub dest: Endpoint,
    pub options: MigrationOptions,
    /// Sorted by `path`; `copy` processes them in this order.
    pub shards: Vec<LocalShard>,
    pub total_rows: u64,
    pub total_bytes: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct LocalShard {
    /// Work-dir-relative path, e.g. `canonical/part-0000.parquet`.
    pub path: String,
    pub rows: u64,
    pub bytes: u64,
    pub sha256: String,
}

impl LocalShard {
    /// `part-0000.parquet` — the name stamped on failure/downgrade
    /// records and used for the per-shard JSONL files.
    pub fn file_name(&self) -> &str {
        self.path.rsplit('/').next().unwrap_or(&self.path)
    }

    /// `part-0000` — stem for `failures/<stem>.jsonl`.
    pub fn stem(&self) -> &str {
        let name = self.file_name();
        name.strip_suffix(".parquet").unwrap_or(name)
    }
}

/// The subset of `mig-walker-rewrite`'s `--report` JSON mongoose
/// consumes (schema version 1; same contract `vamoose prepare` uses).
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct RewriteReport {
    pub schema_version: u32,
    pub output_dir: String,
    pub complete: bool,
    pub shards: Vec<RewriteShard>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct RewriteShard {
    pub output_name: String,
    pub output_bytes: u64,
    pub output_sha256: String,
    pub rows: u64,
}

/// Build `manifest.json` from a completed rewrite report: every shard
/// is re-verified on disk (size and SHA256) so the manifest never
/// trusts a checkpoint over the bytes. Shards are sorted by path.
pub fn build(
    workdir: &WorkDir,
    run_id: &str,
    source: Endpoint,
    dest: Endpoint,
    options: MigrationOptions,
) -> Result<LocalManifest> {
    let report_path = workdir.rewrite_json();
    let report: RewriteReport = read_json_opt(&report_path)?
        .ok_or_else(|| anyhow::anyhow!("rewrite report {} is missing", report_path.display()))?;
    if report.schema_version != 1 || !report.complete {
        anyhow::bail!(
            "rewrite report {} is not a completed schema-version-1 checkpoint",
            report_path.display()
        );
    }
    if report.shards.is_empty() {
        anyhow::bail!(
            "rewrite report {} contains no shards",
            report_path.display()
        );
    }

    let canonical = workdir.canonical_dir();
    let mut shards = Vec::with_capacity(report.shards.len());
    for shard in &report.shards {
        let path = canonical.join(&shard.output_name);
        shards.push(verify_shard_on_disk(&path, shard)?);
    }
    shards.sort_by(|a, b| a.path.cmp(&b.path));

    let manifest = LocalManifest {
        format_version: MANIFEST_FORMAT_VERSION,
        run_id: run_id.to_string(),
        created_utc: utc_now(),
        source,
        dest,
        options,
        total_rows: shards.iter().map(|s| s.rows).sum(),
        total_bytes: shards.iter().map(|s| s.bytes).sum(),
        shards,
    };
    write_json_atomic(&workdir.manifest_json(), &manifest)?;
    Ok(manifest)
}

fn verify_shard_on_disk(path: &Path, shard: &RewriteShard) -> Result<LocalShard> {
    let size = std::fs::metadata(path)
        .map(|m| m.len())
        .with_context(|| format!("canonical shard {} is missing", path.display()))?;
    if size != shard.output_bytes {
        anyhow::bail!(
            "canonical shard {} is {size} bytes; rewrite checkpoint says {}",
            path.display(),
            shard.output_bytes
        );
    }
    let digest = sha256_file(path)?;
    if digest != shard.output_sha256 {
        anyhow::bail!(
            "canonical shard {} SHA256 does not match the rewrite checkpoint",
            path.display()
        );
    }
    Ok(LocalShard {
        path: format!("canonical/{}", shard.output_name),
        rows: shard.rows,
        bytes: size,
        sha256: digest,
    })
}

/// Load a previously built manifest; `Ok(None)` when prepare has not
/// finished.
pub fn load(workdir: &WorkDir) -> Result<Option<LocalManifest>> {
    load_file(&workdir.manifest_json())
}

/// [`load`] for an arbitrary manifest file (the resync delta manifest
/// shares the format under a different name).
pub fn load_file(path: &Path) -> Result<Option<LocalManifest>> {
    let Some(m) = read_json_opt::<LocalManifest>(path)? else {
        return Ok(None);
    };
    if m.format_version != MANIFEST_FORMAT_VERSION {
        anyhow::bail!(
            "manifest format_version {} does not match this mongoose ({})",
            m.format_version,
            MANIFEST_FORMAT_VERSION,
        );
    }
    Ok(Some(m))
}

#[cfg(test)]
mod tests {
    use super::*;
    use migration_core::records::EndpointKind;

    fn endpoint(url: &str) -> Endpoint {
        Endpoint {
            kind: EndpointKind::Nfs,
            url: url.into(),
            root: "/".into(),
        }
    }

    /// A work dir with a completed rewrite report and matching shard
    /// files on disk. Shards are reported out of order on purpose.
    fn fixture(dir: &Path) -> WorkDir {
        let wd = WorkDir::new(dir);
        std::fs::create_dir_all(wd.canonical_dir()).unwrap();
        let mut shards = Vec::new();
        for (name, body, rows) in [
            ("part-0001.parquet", b"beta".as_slice(), 7u64),
            ("part-0000.parquet", b"alpha".as_slice(), 5u64),
        ] {
            let path = wd.canonical_dir().join(name);
            std::fs::write(&path, body).unwrap();
            shards.push(RewriteShard {
                output_name: name.into(),
                output_bytes: body.len() as u64,
                output_sha256: sha256_file(&path).unwrap(),
                rows,
            });
        }
        let report = RewriteReport {
            schema_version: 1,
            output_dir: wd.canonical_dir().to_string_lossy().into_owned(),
            complete: true,
            shards,
        };
        write_json_atomic(&wd.rewrite_json(), &report).unwrap();
        wd
    }

    #[test]
    fn build_verifies_sorts_and_round_trips() {
        let dir = tempfile::tempdir().unwrap();
        let wd = fixture(dir.path());
        let m = build(
            &wd,
            "run-t",
            endpoint("nfs://s/e"),
            endpoint("nfs://d/e"),
            MigrationOptions::default(),
        )
        .unwrap();

        // Shard ordering: sorted by path even though the report listed
        // part-0001 first.
        let paths: Vec<&str> = m.shards.iter().map(|s| s.path.as_str()).collect();
        assert_eq!(
            paths,
            vec!["canonical/part-0000.parquet", "canonical/part-0001.parquet"]
        );
        assert_eq!(m.total_rows, 12);
        assert_eq!(m.total_bytes, 9);
        assert_eq!(m.shards[0].stem(), "part-0000");
        assert_eq!(m.shards[0].file_name(), "part-0000.parquet");

        let back = load(&wd).unwrap().expect("manifest.json written");
        assert_eq!(back.run_id, "run-t");
        assert_eq!(back.shards, m.shards);
    }

    #[test]
    fn build_refuses_missing_or_tampered_shards() {
        let dir = tempfile::tempdir().unwrap();
        let wd = fixture(dir.path());

        // Tamper with one shard: same length, different bytes.
        std::fs::write(wd.canonical_dir().join("part-0000.parquet"), b"aLpha").unwrap();
        let err = build(
            &wd,
            "r",
            endpoint("nfs://s/e"),
            endpoint("nfs://d/e"),
            MigrationOptions::default(),
        )
        .unwrap_err();
        assert!(format!("{err:#}").contains("SHA256"), "{err:#}");

        // Remove it entirely.
        std::fs::remove_file(wd.canonical_dir().join("part-0000.parquet")).unwrap();
        let err = build(
            &wd,
            "r",
            endpoint("nfs://s/e"),
            endpoint("nfs://d/e"),
            MigrationOptions::default(),
        )
        .unwrap_err();
        assert!(format!("{err:#}").contains("missing"), "{err:#}");
    }

    #[test]
    fn build_refuses_an_incomplete_report() {
        let dir = tempfile::tempdir().unwrap();
        let wd = fixture(dir.path());
        let mut report: RewriteReport = read_json_opt(&wd.rewrite_json()).unwrap().unwrap();
        report.complete = false;
        write_json_atomic(&wd.rewrite_json(), &report).unwrap();
        let err = build(
            &wd,
            "r",
            endpoint("nfs://s/e"),
            endpoint("nfs://d/e"),
            MigrationOptions::default(),
        )
        .unwrap_err();
        assert!(format!("{err:#}").contains("not a completed"), "{err:#}");
    }

    #[test]
    fn load_absent_manifest_is_none() {
        let dir = tempfile::tempdir().unwrap();
        assert!(load(&WorkDir::new(dir.path())).unwrap().is_none());
    }
}
