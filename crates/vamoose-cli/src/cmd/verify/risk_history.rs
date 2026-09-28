//! Migration risk-history discovery for a configured S3 run.
//!
//! Before the blocking verifier starts, `vamoose verify --mode sample` turns
//! the run's failure, downgrade, and retry history into the canonical
//! risk-evidence artifact that `migration-verify` imports. Everything here
//! fails closed: an unfinished migration, an unreadable or malformed sink
//! object, or an index shard whose ETag drifted from the manifest is an
//! operational failure, never a silently smaller sample.

#![allow(dead_code)] // Wired into `vamoose verify --mode sample` by the CLI slice.

use anyhow::{Context, Result};
use migration_core::claim::ClaimStore;
use migration_core::layout;
use migration_core::records::{
    ClaimRecord, ClaimState, DowngradeRecord, FailureRecord, Manifest, ShardEntry,
};
use migration_core::shard::ShardReader;
use migration_verify::{ArtifactDigest, RiskEvidenceRecord, RiskEvidenceStager, RiskReason};
use std::path::{Path, PathBuf};

/// A manifest shard whose terminal claim shows a reclaim (`epoch > 1`).
struct RetriedShard<'a> {
    filename: String,
    entry: &'a ShardEntry,
}

/// Discovers the run's risk history and publishes the canonical artifact
/// beside the verification database in `work_dir`.
///
/// 1. Every manifest shard must have a terminal `Completed` claim.
/// 2. Every object under `failures/` and `downgrades/` is read in lexical
///    key order; every non-empty line must parse as its record type.
/// 3. Every shard with `epoch > 1` has its immutable index downloaded,
///    ETag-verified against the manifest, and streamed for regular-file
///    paths.
///
/// Sorting and deduplication happen in the stager's SQLite spool, so a
/// reclaimed shard with millions of paths never has to fit in memory.
pub(crate) async fn stage_configured_run(
    store: &dyn ClaimStore,
    manifest: &Manifest,
    work_dir: &Path,
) -> Result<ArtifactDigest> {
    let retried = require_terminal_claims(store, manifest).await?;
    let mut stager = RiskEvidenceStager::new(work_dir)?;
    for (prefix, reason) in [
        (layout::FAILURES_PREFIX, RiskReason::MigrationFailure),
        (layout::DOWNGRADES_PREFIX, RiskReason::MigrationDowngrade),
    ] {
        ingest_sink_prefix(store, prefix, reason, &mut stager).await?;
    }
    let scratch_dir = work_dir.join("risk-scratch");
    for shard in &retried {
        stager = ingest_retried_shard(store, shard, &scratch_dir, stager).await?;
    }
    if scratch_dir.exists() {
        std::fs::remove_dir_all(&scratch_dir)
            .with_context(|| format!("removing {}", scratch_dir.display()))?;
    }
    stager.publish()
}

async fn require_terminal_claims<'a>(
    store: &dyn ClaimStore,
    manifest: &'a Manifest,
) -> Result<Vec<RetriedShard<'a>>> {
    let mut retried = Vec::new();
    for entry in &manifest.shards {
        let filename = entry
            .key
            .strip_prefix(layout::INDEX_PREFIX)
            .filter(|name| !name.is_empty() && !name.contains('/'))
            .ok_or_else(|| {
                anyhow::anyhow!(
                    "manifest shard key {:?} is not an object under {}",
                    entry.key,
                    layout::INDEX_PREFIX
                )
            })?;
        let claim_key = layout::claim_key(filename);
        let Some((body, _)) = store
            .get(&claim_key)
            .await
            .with_context(|| format!("reading {claim_key}"))?
        else {
            anyhow::bail!(
                "shard {filename} has no claim; the migration has not completed, so sample \
                 verification cannot start"
            );
        };
        let record: ClaimRecord =
            serde_json::from_slice(&body).with_context(|| format!("parsing {claim_key}"))?;
        match record.state {
            ClaimState::Completed => {}
            ClaimState::Active => anyhow::bail!(
                "shard {filename} is still active (held by {}, epoch {}); sample verification \
                 must not race an unfinished migration",
                record.host,
                record.epoch
            ),
            ClaimState::Failed => anyhow::bail!(
                "shard {filename} terminated as failed (host {}, epoch {}); repair the migration \
                 before verifying content",
                record.host,
                record.epoch
            ),
        }
        if record.epoch > 1 {
            retried.push(RetriedShard {
                filename: filename.to_string(),
                entry,
            });
        }
    }
    Ok(retried)
}

async fn ingest_sink_prefix(
    store: &dyn ClaimStore,
    prefix: &str,
    reason: RiskReason,
    stager: &mut RiskEvidenceStager,
) -> Result<()> {
    let mut entries = store
        .list(prefix)
        .await
        .with_context(|| format!("listing {prefix}"))?;
    entries.sort_by(|a, b| a.key.cmp(&b.key));
    for entry in entries {
        let Some((body, etag)) = store
            .get(&entry.key)
            .await
            .with_context(|| format!("reading {}", entry.key))?
        else {
            anyhow::bail!("{} disappeared between LIST and GET", entry.key);
        };
        if !entry.etag.is_empty() && !etag.is_empty() && entry.etag != etag {
            anyhow::bail!(
                "{} changed between LIST and GET (etag {} != {})",
                entry.key,
                entry.etag,
                etag
            );
        }
        ingest_sink_object(&entry.key, &etag, &body, reason, stager)?;
    }
    Ok(())
}

/// Parses one sink object. Every non-empty line must be a complete record
/// of the expected type with a decodable path.
fn ingest_sink_object(
    key: &str,
    etag: &str,
    body: &[u8],
    reason: RiskReason,
    stager: &mut RiskEvidenceStager,
) -> Result<u64> {
    let mut added = 0;
    for (index, line) in body.split(|byte| *byte == b'\n').enumerate() {
        if line.is_empty() {
            continue;
        }
        let number = index + 1;
        let path_b64 = match reason {
            RiskReason::MigrationFailure => {
                serde_json::from_slice::<FailureRecord>(line)
                    .with_context(|| format!("{key} line {number} is not a failure record"))?
                    .path_b64
            }
            RiskReason::MigrationDowngrade => {
                serde_json::from_slice::<DowngradeRecord>(line)
                    .with_context(|| format!("{key} line {number} is not a downgrade record"))?
                    .path_b64
            }
            other => anyhow::bail!("{} is not a sink reason", other.as_str()),
        };
        let record = RiskEvidenceRecord::from_history_path_b64(&path_b64, reason, key, etag)
            .with_context(|| format!("{key} line {number}"))?;
        stager.add(&record)?;
        added += 1;
    }
    Ok(added)
}

/// Downloads a reclaimed shard's immutable index, verifies its ETag against
/// the manifest, and adds every regular-file path as `retried_shard`. The
/// stager moves through a blocking task for the parquet decode and is
/// returned to the caller.
async fn ingest_retried_shard(
    store: &dyn ClaimStore,
    shard: &RetriedShard<'_>,
    scratch_dir: &Path,
    mut stager: RiskEvidenceStager,
) -> Result<RiskEvidenceStager> {
    let key = shard.entry.key.clone();
    let Some((body, etag)) = store
        .get(&key)
        .await
        .with_context(|| format!("reading retried index shard {key}"))?
    else {
        anyhow::bail!("retried index shard {key} is missing from the run");
    };
    verify_shard_etag(shard, &etag)?;
    std::fs::create_dir_all(scratch_dir)
        .with_context(|| format!("creating {}", scratch_dir.display()))?;
    let scratch = scratch_dir.join(&shard.filename);
    std::fs::write(&scratch, &body).with_context(|| format!("writing {}", scratch.display()))?;
    let (stager, result) = tokio::task::spawn_blocking(move || {
        let result = read_regular_paths(&scratch, &key, &etag, &mut stager);
        remove_scratch(&scratch);
        (stager, result)
    })
    .await
    .context("retried shard decode task panicked")?;
    result?;
    Ok(stager)
}

fn read_regular_paths(
    scratch: &Path,
    key: &str,
    etag: &str,
    stager: &mut RiskEvidenceStager,
) -> Result<u64> {
    let reader =
        ShardReader::open(scratch).with_context(|| format!("opening retried index shard {key}"))?;
    let mut added = 0;
    for row in reader
        .into_rows()
        .with_context(|| format!("reading retried index shard {key}"))?
    {
        let row = row.with_context(|| format!("decoding retried index shard {key}"))?;
        if !row.file_type.has_data() {
            continue;
        }
        let record =
            RiskEvidenceRecord::from_history_path(&row.path, RiskReason::RetriedShard, key, etag)
                .with_context(|| format!("{key} row {}", row.row_id))?;
        stager.add(&record)?;
        added += 1;
    }
    Ok(added)
}

fn remove_scratch(scratch: &PathBuf) {
    if let Err(error) = std::fs::remove_file(scratch) {
        if error.kind() != std::io::ErrorKind::NotFound {
            tracing::warn!(error = ?error, scratch = %scratch.display(), "scratch cleanup failed");
        }
    }
}

/// Mirrors the worker's shard integrity rule: an empty ETag on either side
/// means the check cannot run and is an error, never a silent pass.
fn verify_shard_etag(shard: &RetriedShard<'_>, actual: &str) -> Result<()> {
    if shard.entry.etag.is_empty() {
        anyhow::bail!(
            "shard {}: manifest etag is missing (empty); cannot verify the retried index shard",
            shard.filename
        );
    }
    if actual.is_empty() {
        anyhow::bail!(
            "shard {}: download returned an empty etag; cannot verify the retried index shard \
             against the manifest",
            shard.filename
        );
    }
    if shard.entry.etag != actual {
        anyhow::bail!(
            "shard {}: index etag {actual} does not match the manifest etag {}; the run was \
             swapped or re-indexed since the manifest was published",
            shard.filename,
            shard.entry.etag
        );
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{
        BinaryBuilder, Int32Builder, Int64Builder, UInt32Builder, UInt64Builder, UInt8Builder,
    };
    use arrow::record_batch::RecordBatch;
    use base64::Engine;
    use migration_core::claim::test_util::FakeStore;
    use migration_core::records::{
        DowngradeKind, Endpoint, EndpointKind, FailurePhase, MigrationOptions,
    };
    use migration_core::schema::{self, FileTypeTag};
    use migration_core::time::UtcTime;
    use parquet::arrow::ArrowWriter;
    use parquet::file::properties::WriterProperties;
    use parquet::format::KeyValue;
    use std::sync::Arc;

    fn b64(path: &[u8]) -> String {
        base64::engine::general_purpose::STANDARD.encode(path)
    }

    fn manifest(shards: Vec<ShardEntry>) -> Manifest {
        Manifest {
            format_version: migration_core::records::RUN_FORMAT_VERSION,
            run_id: "run-1".into(),
            created_utc: UtcTime::now(),
            shards,
            total_rows: 0,
            source: Endpoint {
                kind: EndpointKind::Nfs,
                url: "nfs://source/export".into(),
                root: "/".into(),
            },
            dest: Endpoint {
                kind: EndpointKind::Nfs,
                url: "nfs://destination/export".into(),
                root: "/".into(),
            },
            exclusions: vec![],
            options: MigrationOptions::default(),
        }
    }

    fn shard_entry(filename: &str, etag: &str) -> ShardEntry {
        ShardEntry {
            key: layout::index_key(filename),
            rows: 0,
            bytes: 0,
            etag: etag.to_string(),
        }
    }

    fn claim(host: &str, epoch: u64, state: ClaimState) -> Vec<u8> {
        serde_json::to_vec(&ClaimRecord {
            host: host.into(),
            claimed_utc: UtcTime::now(),
            epoch,
            state,
        })
        .unwrap()
    }

    async fn put(store: &FakeStore, key: &str, body: Vec<u8>) -> String {
        store.put_if_absent(key, body).await.unwrap()
    }

    fn failure_line(path: &[u8]) -> Vec<u8> {
        let mut line = serde_json::to_vec(&FailureRecord {
            row_id: 1,
            shard: "part-0000.parquet".into(),
            path_b64: b64(path),
            error: "EIO".into(),
            phase: FailurePhase::Read,
            ts: UtcTime::now(),
        })
        .unwrap();
        line.push(b'\n');
        line
    }

    fn downgrade_line(path: &[u8], downgrade: DowngradeKind) -> Vec<u8> {
        let mut line = serde_json::to_vec(&DowngradeRecord {
            row_id: 2,
            shard: "part-0001.parquet".into(),
            path_b64: b64(path),
            downgrade,
            ts: UtcTime::now(),
        })
        .unwrap();
        line.push(b'\n');
        line
    }

    fn expected_line(path: &[u8], reason: RiskReason, source: &str, etag: &str) -> Vec<u8> {
        RiskEvidenceRecord {
            path: path.to_vec(),
            reason,
            source: source.into(),
            source_etag: etag.into(),
        }
        .canonical_line()
    }

    /// Minimal canonical parquet shard with the given `(path, file_type)`
    /// rows and a valid KV footer.
    fn shard_bytes(rows: &[(&[u8], FileTypeTag)]) -> Vec<u8> {
        let mut row_id = UInt64Builder::new();
        let mut path = BinaryBuilder::new();
        let mut size = UInt64Builder::new();
        let mut mtime_sec = Int64Builder::new();
        let mut mtime_nsec = Int32Builder::new();
        let mut atime_sec = Int64Builder::new();
        let mut atime_nsec = Int32Builder::new();
        let mut mode = UInt32Builder::new();
        let mut uid = UInt32Builder::new();
        let mut gid = UInt32Builder::new();
        let mut nlink = UInt32Builder::new();
        let mut inode = UInt64Builder::new();
        let mut fsid = UInt64Builder::new();
        let mut xattr = BinaryBuilder::new();
        let mut symlink = BinaryBuilder::new();
        let mut file_type = UInt8Builder::new();
        for (index, (row_path, kind)) in rows.iter().enumerate() {
            row_id.append_value(schema::make_row_id(0, index as u64));
            path.append_value(row_path);
            size.append_value(10);
            mtime_sec.append_null();
            mtime_nsec.append_null();
            atime_sec.append_null();
            atime_nsec.append_null();
            mode.append_value(0o100644);
            uid.append_null();
            gid.append_null();
            nlink.append_null();
            inode.append_null();
            fsid.append_null();
            xattr.append_null();
            symlink.append_null();
            file_type.append_value(*kind as u8);
        }
        let batch = RecordBatch::try_new(
            schema::canonical_schema(),
            vec![
                Arc::new(row_id.finish()),
                Arc::new(path.finish()),
                Arc::new(size.finish()),
                Arc::new(mtime_sec.finish()),
                Arc::new(mtime_nsec.finish()),
                Arc::new(atime_sec.finish()),
                Arc::new(atime_nsec.finish()),
                Arc::new(mode.finish()),
                Arc::new(uid.finish()),
                Arc::new(gid.finish()),
                Arc::new(nlink.finish()),
                Arc::new(inode.finish()),
                Arc::new(fsid.finish()),
                Arc::new(xattr.finish()),
                Arc::new(symlink.finish()),
                Arc::new(file_type.finish()),
            ],
        )
        .unwrap();
        let props = WriterProperties::builder()
            .set_key_value_metadata(Some(vec![
                KeyValue {
                    key: schema::KV_FORMAT_VERSION.into(),
                    value: Some(schema::FORMAT_VERSION.to_string()),
                },
                KeyValue {
                    key: schema::KV_CONTRACT_VERSION.into(),
                    value: Some(schema::CONTRACT_VERSION.to_string()),
                },
                KeyValue {
                    key: schema::KV_SHARD_INDEX.into(),
                    value: Some("0".into()),
                },
            ]))
            .build();
        let mut out = Vec::new();
        let mut writer = ArrowWriter::try_new(&mut out, batch.schema(), Some(props)).unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();
        out
    }

    #[tokio::test]
    async fn staging_requires_a_completed_claim_for_every_shard() {
        let store = FakeStore::new();
        let dir = tempfile::tempdir().unwrap();
        let manifest = manifest(vec![
            shard_entry("part-0000.parquet", "e0"),
            shard_entry("part-0001.parquet", "e1"),
        ]);

        let error = stage_configured_run(&store, &manifest, dir.path())
            .await
            .unwrap_err()
            .to_string();
        assert!(error.contains("part-0000.parquet has no claim"), "{error}");

        put(
            &store,
            &layout::claim_key("part-0000.parquet"),
            claim("host-a", 1, ClaimState::Completed),
        )
        .await;
        put(
            &store,
            &layout::claim_key("part-0001.parquet"),
            claim("host-b", 1, ClaimState::Active),
        )
        .await;
        let error = stage_configured_run(&store, &manifest, dir.path())
            .await
            .unwrap_err()
            .to_string();
        assert!(
            error.contains("part-0001.parquet is still active"),
            "{error}"
        );

        let failed = FakeStore::new();
        put(
            &failed,
            &layout::claim_key("part-0000.parquet"),
            claim("host-a", 1, ClaimState::Completed),
        )
        .await;
        put(
            &failed,
            &layout::claim_key("part-0001.parquet"),
            claim("host-b", 3, ClaimState::Failed),
        )
        .await;
        let error = stage_configured_run(&failed, &manifest, dir.path())
            .await
            .unwrap_err()
            .to_string();
        assert!(error.contains("terminated as failed"), "{error}");

        // Nothing was published on any failed attempt.
        assert!(!migration_verify::risk_evidence_path(dir.path()).exists());

        let complete = FakeStore::new();
        for shard in ["part-0000.parquet", "part-0001.parquet"] {
            put(
                &complete,
                &layout::claim_key(shard),
                claim("host-a", 1, ClaimState::Completed),
            )
            .await;
        }
        let digest = stage_configured_run(&complete, &manifest, dir.path())
            .await
            .unwrap();
        assert_eq!(digest.bytes, 0);
        assert!(!dir.path().join("risk-scratch").exists());
    }

    #[tokio::test]
    async fn sinks_are_ingested_in_key_order_deduplicated_and_etag_stamped() {
        let store = FakeStore::new();
        let dir = tempfile::tempdir().unwrap();
        put(
            &store,
            &layout::claim_key("part-0000.parquet"),
            claim("host-a", 1, ClaimState::Completed),
        )
        .await;
        let mut failures = failure_line(b"/data/z");
        failures.extend(failure_line(b"/data/a"));
        failures.extend(failure_line(b"/data/a"));
        failures.extend(b"\n");
        let failures_etag = put(
            &store,
            &layout::failures_flush_key("host-a", "part-0000.parquet", 1),
            failures,
        )
        .await;
        let mut downgrades = downgrade_line(b"/data/a", DowngradeKind::EarlyEof);
        downgrades.extend(downgrade_line(
            b"/bad-\xff",
            DowngradeKind::TornCopy {
                pre: (1, 2, 3),
                post: (4, 5, 6),
            },
        ));
        let downgrades_etag = put(
            &store,
            &layout::downgrades_flush_key("host-b", "part-0000.parquet", 1),
            downgrades,
        )
        .await;
        let manifest = manifest(vec![shard_entry("part-0000.parquet", "unused")]);

        let digest = stage_configured_run(&store, &manifest, dir.path())
            .await
            .unwrap();
        let published = std::fs::read(&digest.path).unwrap();
        let failures_key = layout::failures_flush_key("host-a", "part-0000.parquet", 1);
        let downgrades_key = layout::downgrades_flush_key("host-b", "part-0000.parquet", 1);
        let mut expected = Vec::new();
        expected.extend(expected_line(
            b"bad-\xff",
            RiskReason::MigrationDowngrade,
            &downgrades_key,
            &downgrades_etag,
        ));
        expected.extend(expected_line(
            b"data/a",
            RiskReason::MigrationDowngrade,
            &downgrades_key,
            &downgrades_etag,
        ));
        expected.extend(expected_line(
            b"data/a",
            RiskReason::MigrationFailure,
            &failures_key,
            &failures_etag,
        ));
        expected.extend(expected_line(
            b"data/z",
            RiskReason::MigrationFailure,
            &failures_key,
            &failures_etag,
        ));
        assert_eq!(published, expected);
        assert_eq!(digest.bytes, expected.len() as u64);
        migration_verify::validate_risk_artifact(&digest).unwrap();

        // Re-staging the same history is idempotent.
        assert_eq!(
            stage_configured_run(&store, &manifest, dir.path())
                .await
                .unwrap(),
            digest
        );
    }

    #[tokio::test]
    async fn malformed_sink_rows_fail_closed() {
        let manifest = manifest(vec![shard_entry("part-0000.parquet", "unused")]);
        for (label, body) in [
            ("garbage", b"{not json}\n".to_vec()),
            (
                "bad base64",
                br#"{"row_id":1,"shard":"s","path_b64":"%%%","error":"EIO","phase":"read","ts":"2026-01-01T00:00:00Z"}
"#
                .to_vec(),
            ),
            (
                "missing path",
                br#"{"row_id":1,"shard":"s","error":"EIO","phase":"read","ts":"2026-01-01T00:00:00Z"}
"#
                .to_vec(),
            ),
            ("path without leading slash", failure_line(b"relative")),
        ] {
            let store = FakeStore::new();
            let dir = tempfile::tempdir().unwrap();
            put(
                &store,
                &layout::claim_key("part-0000.parquet"),
                claim("host-a", 1, ClaimState::Completed),
            )
            .await;
            put(
                &store,
                &layout::failures_flush_key("host-a", "part-0000.parquet", 1),
                body,
            )
            .await;
            assert!(
                stage_configured_run(&store, &manifest, dir.path())
                    .await
                    .is_err(),
                "{label} must fail closed"
            );
            assert!(!migration_verify::risk_evidence_path(dir.path()).exists());
        }

        // A downgrade object that only parses as a failure record is refused
        // too: the record type is fixed by the prefix.
        let store = FakeStore::new();
        let dir = tempfile::tempdir().unwrap();
        put(
            &store,
            &layout::claim_key("part-0000.parquet"),
            claim("host-a", 1, ClaimState::Completed),
        )
        .await;
        put(
            &store,
            &layout::downgrades_flush_key("host-a", "part-0000.parquet", 1),
            failure_line(b"/x"),
        )
        .await;
        assert!(stage_configured_run(&store, &manifest, dir.path())
            .await
            .is_err());
    }

    #[tokio::test]
    async fn retried_shards_add_every_regular_file_and_etag_drift_fails() {
        let store = FakeStore::new();
        let dir = tempfile::tempdir().unwrap();
        put(
            &store,
            &layout::claim_key("part-0000.parquet"),
            claim("host-a", 1, ClaimState::Completed),
        )
        .await;
        put(
            &store,
            &layout::claim_key("part-0001.parquet"),
            claim("host-b", 2, ClaimState::Completed),
        )
        .await;
        let index_etag = put(
            &store,
            &layout::index_key("part-0001.parquet"),
            shard_bytes(&[
                (b"/dir", FileTypeTag::Dir),
                (b"/dir/file-b", FileTypeTag::Regular),
                (b"/dir/link", FileTypeTag::Symlink),
                (b"/dir/file-a", FileTypeTag::Regular),
                (b"/dir/\xff", FileTypeTag::Regular),
                (b"/dir/fifo", FileTypeTag::Fifo),
            ]),
        )
        .await;
        // Shard 0 (epoch 1) is never downloaded, so it needs no index.
        let retried = manifest(vec![
            shard_entry("part-0000.parquet", "irrelevant"),
            shard_entry("part-0001.parquet", &index_etag),
        ]);

        let digest = stage_configured_run(&store, &retried, dir.path())
            .await
            .unwrap();
        let key = layout::index_key("part-0001.parquet");
        let mut expected = Vec::new();
        // Raw bytes order: the high-bit name sorts after the ASCII names.
        for path in [&b"dir/file-a"[..], b"dir/file-b", b"dir/\xff"] {
            expected.extend(expected_line(
                path,
                RiskReason::RetriedShard,
                &key,
                &index_etag,
            ));
        }
        assert_eq!(std::fs::read(&digest.path).unwrap(), expected);
        assert!(!dir.path().join("risk-scratch").exists());

        // ETag drift: the manifest names a different index.
        let drifted = manifest(vec![
            shard_entry("part-0000.parquet", "irrelevant"),
            shard_entry("part-0001.parquet", "etag-from-another-run"),
        ]);
        let other = tempfile::tempdir().unwrap();
        let error = stage_configured_run(&store, &drifted, other.path())
            .await
            .unwrap_err()
            .to_string();
        assert!(
            error.contains("does not match the manifest etag"),
            "{error}"
        );

        // An empty manifest etag cannot be verified and is refused.
        let empty = manifest_with_empty_etag();
        let error = stage_configured_run(&store, &empty, other.path())
            .await
            .unwrap_err()
            .to_string();
        assert!(error.contains("manifest etag is missing"), "{error}");

        // A missing index for a retried shard is an error, not an empty set.
        let missing = FakeStore::new();
        put(
            &missing,
            &layout::claim_key("part-0000.parquet"),
            claim("host-a", 1, ClaimState::Completed),
        )
        .await;
        put(
            &missing,
            &layout::claim_key("part-0001.parquet"),
            claim("host-b", 2, ClaimState::Completed),
        )
        .await;
        let error = stage_configured_run(&missing, &retried, other.path())
            .await
            .unwrap_err()
            .to_string();
        assert!(error.contains("missing from the run"), "{error}");
    }

    fn manifest_with_empty_etag() -> Manifest {
        manifest(vec![
            shard_entry("part-0000.parquet", "irrelevant"),
            shard_entry("part-0001.parquet", ""),
        ])
    }
}
