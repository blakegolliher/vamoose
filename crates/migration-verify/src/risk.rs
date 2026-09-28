//! Canonical migration risk-evidence artifact.
//!
//! Risk evidence is the set of paths whose migration history (failure,
//! downgrade, or retried shard) makes them mandatory content targets. The
//! evidence is staged into one immutable `risk-evidence.jsonl` beside the
//! verification database. Its SHA-256 is part of the request fingerprint, so
//! a resumed verification cannot silently see different history.
//!
//! Canonical form: one JSON object per line with exactly the fields
//! `path_b64`, `reason`, `source`, `source_etag`; lines sorted by
//! `(path bytes, reason, source, source_etag)` and exact duplicates removed.
//! Sorting and deduplication run through a transient SQLite spool so a
//! reclaimed shard with millions of paths never has to fit in memory.

use crate::artifact::{create_temp, digest_file, publish_existing_temp};
use crate::model::{ArtifactDigest, RiskReason};
use anyhow::{Context, Result};
use base64::Engine;
use rusqlite::{params, Connection};
use serde::{Deserialize, Serialize};
use std::io::{BufRead, BufReader, BufWriter, Write};
use std::path::{Path, PathBuf};

pub const RISK_EVIDENCE_FILENAME: &str = "risk-evidence.jsonl";
const SPOOL_FILENAME: &str = "risk-spool.sqlite";

/// Canonical location of the risk-evidence artifact for a verification.
pub fn risk_evidence_path(work_dir: &Path) -> PathBuf {
    work_dir.join(RISK_EVIDENCE_FILENAME)
}

/// One risk-evidence line. `path` is the verifier's raw relative path (no
/// leading slash), exactly the identity used by SQLite keys and mismatch
/// records.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct RiskEvidenceRecord {
    pub path: Vec<u8>,
    pub reason: RiskReason,
    pub source: String,
    pub source_etag: String,
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Line {
    path_b64: String,
    reason: RiskReason,
    source: String,
    source_etag: String,
}

impl RiskEvidenceRecord {
    /// Builds a record from a migration-history path: the parquet `path`
    /// column or a failure/downgrade `path_b64`, which is relative to the
    /// endpoint root with one leading slash. The verifier keys paths without
    /// that slash.
    pub fn from_history_path(
        index_path: &[u8],
        reason: RiskReason,
        source: &str,
        source_etag: &str,
    ) -> Result<Self> {
        if !reason.is_history() {
            anyhow::bail!(
                "risk reason {} is derived by the verifier, not recorded as history",
                reason.as_str()
            );
        }
        let path = index_path.strip_prefix(b"/").ok_or_else(|| {
            anyhow::anyhow!(
                "history path {:?} does not start with '/'",
                String::from_utf8_lossy(index_path)
            )
        })?;
        Ok(Self {
            path: path.to_vec(),
            reason,
            source: source.to_string(),
            source_etag: source_etag.to_string(),
        })
    }

    /// Builds a record from a base64 history path as written by the worker
    /// sinks.
    pub fn from_history_path_b64(
        path_b64: &str,
        reason: RiskReason,
        source: &str,
        source_etag: &str,
    ) -> Result<Self> {
        let raw = base64::engine::general_purpose::STANDARD
            .decode(path_b64)
            .with_context(|| format!("invalid base64 history path {path_b64:?}"))?;
        Self::from_history_path(&raw, reason, source, source_etag)
    }

    /// Parses one canonical line strictly: exactly the four fields, valid
    /// base64, and a history reason.
    pub fn parse_line(line: &[u8]) -> Result<Self> {
        let parsed: Line = serde_json::from_slice(line).context("malformed risk-evidence line")?;
        if !parsed.reason.is_history() {
            anyhow::bail!(
                "risk-evidence reason {} is not a migration-history reason",
                parsed.reason.as_str()
            );
        }
        let path = base64::engine::general_purpose::STANDARD
            .decode(&parsed.path_b64)
            .context("invalid path_b64 in risk-evidence line")?;
        Ok(Self {
            path,
            reason: parsed.reason,
            source: parsed.source,
            source_etag: parsed.source_etag,
        })
    }

    /// Canonical JSON line including the trailing newline.
    pub fn canonical_line(&self) -> Vec<u8> {
        let mut line = serde_json::to_vec(&Line {
            path_b64: base64::engine::general_purpose::STANDARD.encode(&self.path),
            reason: self.reason,
            source: self.source.clone(),
            source_etag: self.source_etag.clone(),
        })
        .expect("risk-evidence line serializes");
        line.push(b'\n');
        line
    }
}

/// Collects risk-evidence records into a transient SQLite spool and publishes
/// them as the canonical immutable artifact. The spool is scratch, never a
/// checkpoint: it is deleted before staging starts and after publication.
pub struct RiskEvidenceStager {
    conn: Connection,
    spool_path: PathBuf,
    artifact_path: PathBuf,
    records: u64,
}

impl RiskEvidenceStager {
    pub fn new(work_dir: &Path) -> Result<Self> {
        std::fs::create_dir_all(work_dir)
            .with_context(|| format!("creating {}", work_dir.display()))?;
        let spool_path = work_dir.join(SPOOL_FILENAME);
        remove_spool(&spool_path)?;
        let conn = Connection::open(&spool_path)
            .with_context(|| format!("opening risk spool {}", spool_path.display()))?;
        conn.execute_batch(
            "PRAGMA journal_mode=OFF;
             PRAGMA synchronous=OFF;
             PRAGMA temp_store=FILE;
             CREATE TABLE evidence (
               path BLOB NOT NULL,
               reason TEXT NOT NULL,
               source TEXT NOT NULL,
               source_etag TEXT NOT NULL,
               PRIMARY KEY(path, reason, source, source_etag)
             ) WITHOUT ROWID;
             BEGIN;",
        )?;
        Ok(Self {
            conn,
            spool_path,
            artifact_path: risk_evidence_path(work_dir),
            records: 0,
        })
    }

    pub fn add(&mut self, record: &RiskEvidenceRecord) -> Result<()> {
        if !record.reason.is_history() {
            anyhow::bail!(
                "risk reason {} cannot be staged as history",
                record.reason.as_str()
            );
        }
        self.conn.execute(
            "INSERT OR IGNORE INTO evidence(path,reason,source,source_etag) VALUES(?1,?2,?3,?4)",
            params![
                &record.path,
                record.reason.as_str(),
                &record.source,
                &record.source_etag
            ],
        )?;
        self.records += 1;
        Ok(())
    }

    /// Validates and imports an operator-supplied artifact in canonical line
    /// format. Every non-empty line must parse; a truncated or malformed line
    /// fails the whole import. Returns the number of lines imported.
    pub fn add_local_artifact(&mut self, path: &Path) -> Result<u64> {
        let mut imported = 0;
        for_each_line(path, |number, line| {
            let record = RiskEvidenceRecord::parse_line(line)
                .with_context(|| format!("{} line {number}", path.display()))?;
            self.add(&record)?;
            imported += 1;
            Ok(())
        })?;
        Ok(imported)
    }

    /// Number of `add` calls so far (before deduplication).
    pub fn records_added(&self) -> u64 {
        self.records
    }

    /// Writes the sorted, deduplicated artifact beside the verification
    /// database, fsyncs it, publishes it without replacement, and removes the
    /// spool.
    pub fn publish(self) -> Result<ArtifactDigest> {
        self.conn.execute_batch("COMMIT;")?;
        let (temporary, file) = create_temp(&self.artifact_path)?;
        {
            let mut writer = BufWriter::new(&file);
            let mut stmt = self.conn.prepare(
                "SELECT path,reason,source,source_etag FROM evidence
                  ORDER BY path,reason,source,source_etag",
            )?;
            let mut rows = stmt.query([])?;
            while let Some(row) = rows.next()? {
                let reason: String = row.get(1)?;
                let record = RiskEvidenceRecord {
                    path: row.get(0)?,
                    reason: RiskReason::parse(&reason)
                        .ok_or_else(|| anyhow::anyhow!("unknown spooled reason {reason:?}"))?,
                    source: row.get(2)?,
                    source_etag: row.get(3)?,
                };
                writer.write_all(&record.canonical_line())?;
            }
            writer.flush()?;
        }
        file.sync_all()?;
        let digest = digest_file(&temporary)?;
        publish_existing_temp(&temporary, &self.artifact_path, &digest.sha256)?;
        drop(self.conn);
        remove_spool(&self.spool_path)?;
        Ok(ArtifactDigest {
            path: self.artifact_path.display().to_string(),
            sha256: digest.sha256,
            bytes: digest.bytes,
        })
    }
}

/// Publishes the canonical artifact for an operator assertion that the run
/// has no risk history: zero lines.
pub fn stage_asserted_empty(work_dir: &Path) -> Result<ArtifactDigest> {
    RiskEvidenceStager::new(work_dir)?.publish()
}

/// Validates and copies an operator-supplied canonical artifact into the
/// verification directory.
pub fn stage_local_artifact(work_dir: &Path, artifact: &Path) -> Result<ArtifactDigest> {
    let mut stager = RiskEvidenceStager::new(work_dir)?;
    stager.add_local_artifact(artifact)?;
    stager.publish()
}

/// Confirms the artifact on disk still matches its recorded digest.
pub fn validate_artifact(digest: &ArtifactDigest) -> Result<()> {
    let path = Path::new(&digest.path);
    let actual = digest_file(path)
        .with_context(|| format!("validating risk-evidence artifact {}", path.display()))?;
    if actual.sha256 != digest.sha256 || actual.bytes != digest.bytes {
        anyhow::bail!(
            "risk-evidence artifact {} failed digest validation",
            path.display()
        );
    }
    Ok(())
}

/// Streams every record of a canonical artifact, failing closed on any
/// malformed line.
pub(crate) fn for_each_record(
    path: &Path,
    mut visit: impl FnMut(RiskEvidenceRecord) -> Result<()>,
) -> Result<()> {
    for_each_line(path, |number, line| {
        let record = RiskEvidenceRecord::parse_line(line)
            .with_context(|| format!("{} line {number}", path.display()))?;
        visit(record)
    })
}

fn for_each_line(path: &Path, mut visit: impl FnMut(u64, &[u8]) -> Result<()>) -> Result<()> {
    let file = std::fs::File::open(path)
        .with_context(|| format!("opening risk evidence {}", path.display()))?;
    let mut reader = BufReader::new(file);
    let mut line = Vec::new();
    let mut number = 0;
    loop {
        line.clear();
        let read = reader.read_until(b'\n', &mut line)?;
        if read == 0 {
            break;
        }
        number += 1;
        let complete = line.last() == Some(&b'\n');
        if complete {
            line.pop();
        }
        if line.is_empty() {
            continue;
        }
        if !complete {
            anyhow::bail!(
                "{} line {number} is truncated (missing newline)",
                path.display()
            );
        }
        if line.last() == Some(&b'\r') {
            anyhow::bail!(
                "{} line {number} has a carriage return; canonical lines end in a bare newline",
                path.display()
            );
        }
        visit(number, &line)?;
    }
    Ok(())
}

fn remove_spool(path: &Path) -> Result<()> {
    for candidate in [
        path.to_path_buf(),
        path.with_extension("sqlite-journal"),
        path.with_extension("sqlite-wal"),
        path.with_extension("sqlite-shm"),
    ] {
        match std::fs::remove_file(&candidate) {
            Ok(()) => {}
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => {
                return Err(error).with_context(|| format!("removing {}", candidate.display()))
            }
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn record(path: &[u8], reason: RiskReason, source: &str) -> RiskEvidenceRecord {
        RiskEvidenceRecord {
            path: path.to_vec(),
            reason,
            source: source.to_string(),
            source_etag: "etag".to_string(),
        }
    }

    #[test]
    fn canonical_lines_round_trip_raw_paths() {
        let original = record(
            b"dir/\xff\x00name",
            RiskReason::MigrationFailure,
            "failures/x",
        );
        let line = original.canonical_line();
        assert!(line.ends_with(b"\n"));
        assert!(line.starts_with(br#"{"path_b64":""#));
        let parsed = RiskEvidenceRecord::parse_line(&line[..line.len() - 1]).unwrap();
        assert_eq!(parsed, original);
    }

    #[test]
    fn history_paths_strip_exactly_one_leading_slash() {
        let record =
            RiskEvidenceRecord::from_history_path(b"/a/b", RiskReason::RetriedShard, "s", "e")
                .unwrap();
        assert_eq!(record.path, b"a/b");
        assert!(
            RiskEvidenceRecord::from_history_path(b"a/b", RiskReason::RetriedShard, "s", "e")
                .is_err()
        );
        assert!(RiskEvidenceRecord::from_history_path(
            b"/a",
            RiskReason::MetadataMismatch,
            "s",
            "e"
        )
        .is_err());
        let b64 = base64::engine::general_purpose::STANDARD.encode(b"/x");
        assert_eq!(
            RiskEvidenceRecord::from_history_path_b64(
                &b64,
                RiskReason::MigrationDowngrade,
                "s",
                "e"
            )
            .unwrap()
            .path,
            b"x"
        );
        assert!(RiskEvidenceRecord::from_history_path_b64(
            "!!",
            RiskReason::MigrationDowngrade,
            "s",
            "e"
        )
        .is_err());
    }

    #[test]
    fn parse_rejects_malformed_lines_and_non_history_reasons() {
        assert!(RiskEvidenceRecord::parse_line(b"{").is_err());
        assert!(RiskEvidenceRecord::parse_line(
            br#"{"path_b64":"!!","reason":"migration_failure","source":"s","source_etag":"e"}"#
        )
        .is_err());
        assert!(RiskEvidenceRecord::parse_line(
            br#"{"path_b64":"YQ==","reason":"seeded","source":"s","source_etag":"e"}"#
        )
        .is_err());
        assert!(RiskEvidenceRecord::parse_line(
            br#"{"path_b64":"YQ==","reason":"migration_failure","source":"s","source_etag":"e","extra":1}"#
        )
        .is_err());
        assert!(RiskEvidenceRecord::parse_line(
            br#"{"path_b64":"YQ==","reason":"migration_failure","source":"s"}"#
        )
        .is_err());
        let ok = RiskEvidenceRecord::parse_line(
            br#"{"path_b64":"YQ==","reason":"migration_failure","source":"s","source_etag":"e"}"#,
        )
        .unwrap();
        assert_eq!(ok.path, b"a");
    }

    #[test]
    fn stager_sorts_deduplicates_and_publishes_immutably() {
        let dir = tempfile::tempdir().unwrap();
        let mut stager = RiskEvidenceStager::new(dir.path()).unwrap();
        stager
            .add(&record(b"b", RiskReason::RetriedShard, "index/2"))
            .unwrap();
        stager
            .add(&record(b"a", RiskReason::MigrationFailure, "failures/1"))
            .unwrap();
        stager
            .add(&record(
                b"a",
                RiskReason::MigrationDowngrade,
                "downgrades/1",
            ))
            .unwrap();
        stager
            .add(&record(b"a", RiskReason::MigrationFailure, "failures/1"))
            .unwrap();
        assert_eq!(stager.records_added(), 4);
        let digest = stager.publish().unwrap();
        assert_eq!(Path::new(&digest.path), risk_evidence_path(dir.path()));
        assert!(!dir.path().join(SPOOL_FILENAME).exists());

        let published = std::fs::read(&digest.path).unwrap();
        let mut expected = Vec::new();
        expected
            .extend(record(b"a", RiskReason::MigrationDowngrade, "downgrades/1").canonical_line());
        expected.extend(record(b"a", RiskReason::MigrationFailure, "failures/1").canonical_line());
        expected.extend(record(b"b", RiskReason::RetriedShard, "index/2").canonical_line());
        assert_eq!(published, expected);
        assert_eq!(digest.bytes, expected.len() as u64);
        validate_artifact(&digest).unwrap();

        // Re-staging identical evidence is idempotent; different evidence is
        // refused rather than replacing the published artifact.
        let mut again = RiskEvidenceStager::new(dir.path()).unwrap();
        again
            .add(&record(b"b", RiskReason::RetriedShard, "index/2"))
            .unwrap();
        again
            .add(&record(b"a", RiskReason::MigrationFailure, "failures/1"))
            .unwrap();
        again
            .add(&record(
                b"a",
                RiskReason::MigrationDowngrade,
                "downgrades/1",
            ))
            .unwrap();
        assert_eq!(again.publish().unwrap(), digest);
        let mut different = RiskEvidenceStager::new(dir.path()).unwrap();
        different
            .add(&record(b"z", RiskReason::RetriedShard, "index/9"))
            .unwrap();
        assert!(different.publish().is_err());

        // Tampering is detected on validation.
        let mut tampered = published.clone();
        tampered.push(b'\n');
        std::fs::write(&digest.path, &tampered).unwrap();
        assert!(validate_artifact(&digest).is_err());
    }

    #[test]
    fn asserted_empty_publishes_a_zero_line_artifact() {
        let dir = tempfile::tempdir().unwrap();
        let digest = stage_asserted_empty(dir.path()).unwrap();
        assert_eq!(digest.bytes, 0);
        assert_eq!(
            digest.sha256,
            "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
        );
        let mut count = 0;
        for_each_record(Path::new(&digest.path), |_| {
            count += 1;
            Ok(())
        })
        .unwrap();
        assert_eq!(count, 0);
    }

    #[test]
    fn local_artifacts_are_validated_and_canonicalized() {
        let dir = tempfile::tempdir().unwrap();
        let operator = dir.path().join("operator.jsonl");
        let mut body = Vec::new();
        body.extend(record(b"z", RiskReason::RetriedShard, "index/2").canonical_line());
        body.extend(b"\n");
        body.extend(record(b"a", RiskReason::MigrationFailure, "failures/1").canonical_line());
        body.extend(record(b"a", RiskReason::MigrationFailure, "failures/1").canonical_line());
        std::fs::write(&operator, &body).unwrap();
        let work = dir.path().join("work");
        let digest = stage_local_artifact(&work, &operator).unwrap();
        let mut expected = Vec::new();
        expected.extend(record(b"a", RiskReason::MigrationFailure, "failures/1").canonical_line());
        expected.extend(record(b"z", RiskReason::RetriedShard, "index/2").canonical_line());
        assert_eq!(std::fs::read(&digest.path).unwrap(), expected);

        // Truncated final line (no newline) fails closed.
        let truncated = dir.path().join("truncated.jsonl");
        let mut partial = record(b"a", RiskReason::MigrationFailure, "failures/1").canonical_line();
        partial.truncate(partial.len() - 10);
        std::fs::write(&truncated, &partial).unwrap();
        assert!(stage_local_artifact(&dir.path().join("work2"), &truncated).is_err());

        // Invalid base64 fails closed.
        let bad = dir.path().join("bad.jsonl");
        std::fs::write(
            &bad,
            br#"{"path_b64":"%%%","reason":"migration_failure","source":"s","source_etag":"e"}
"#,
        )
        .unwrap();
        assert!(stage_local_artifact(&dir.path().join("work3"), &bad).is_err());

        // Windows line endings are not silently accepted.
        let crlf = dir.path().join("crlf.jsonl");
        let mut line = record(b"a", RiskReason::MigrationFailure, "failures/1").canonical_line();
        line.insert(line.len() - 1, b'\r');
        std::fs::write(&crlf, &line).unwrap();
        assert!(stage_local_artifact(&dir.path().join("work4"), &crlf).is_err());
    }
}
