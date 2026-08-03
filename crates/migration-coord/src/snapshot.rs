//! Snapshot writer + loader.
//!
//! The lease holder writes `state/snapshot.json` from its in-memory
//! state on cadence (the policy lives in the runtime ticks). Each
//! write also drops a timestamped copy under
//! `state/snapshot-<ts>.json`; the loader prunes that history to the
//! newest N copies.
//!
//! Atomicity: a vamoose bucket has versioning **off** (enforced at
//! startup via `migration_core::s3::S3Client::get_bucket_versioning`),
//! and coord is single-writer (lease guarantees that). Both
//! conditions together mean an unconditional PUT is atomic from any
//! reader's viewpoint — last-write-wins, no torn body, no orphan
//! versions. We do not use the temp-key + delete-then-create dance
//! here; that pattern exists for *contended* writers, which the
//! snapshot is not.
//!
//! Schema-version mismatch handling is the responsibility of
//! `crate::errors` — `SnapshotMalformed` covers both "newer than I
//! understand" and "doesn't deserialize at all".

use crate::errors::{Error, Result};
use crate::layout::{snapshot_history_key, SNAPSHOT_KEY};
use crate::schema::{Snapshot, SCHEMA_VERSION};
use crate::store::CoordStore;
use chrono::{DateTime, Utc};

/// Format used for the history-key timestamp. Sortable, filesystem-
/// friendly, no colons (so operators downloading the bucket onto
/// Windows-side tooling don't trip over reserved chars).
const HISTORY_TS_FMT: &str = "%Y%m%dT%H%M%S%.3fZ";

fn history_ts(now: DateTime<Utc>) -> String {
    now.format(HISTORY_TS_FMT).to_string()
}

/// Persist `snapshot` to S3 and add a timestamped history copy. The
/// caller stamps the snapshot's `written_at` before calling.
///
/// Pruning: after writing the new history entry, list all
/// `state/snapshot-*.json` and delete all but the newest
/// `history_keep`. The current `state/snapshot.json` is **not**
/// counted as history — it is always preserved.
pub async fn write(
    store: &dyn CoordStore,
    snapshot: &Snapshot,
    history_keep: usize,
    now: DateTime<Utc>,
) -> Result<()> {
    let body = serde_json::to_vec(snapshot)?;
    store.put(SNAPSHOT_KEY, body.clone()).await?;
    let hist_key = snapshot_history_key(&history_ts(now));
    store.put(&hist_key, body).await?;

    if history_keep == 0 {
        // Caller explicitly disabled history (e.g. tests). Sweep
        // every history file we just wrote and any prior ones.
        prune_history(store, 0).await?;
    } else {
        prune_history(store, history_keep).await?;
    }
    Ok(())
}

/// Load the current snapshot. Returns `None` on a fresh bucket (no
/// prior coord has ever run).
///
/// Refuses to load a snapshot whose `schema_version` exceeds the
/// crate's `SCHEMA_VERSION` — a newer coord must have written it,
/// and downgrading is not supported.
pub async fn load(store: &dyn CoordStore) -> Result<Option<Snapshot>> {
    let (body, _etag) = match store.get(SNAPSHOT_KEY).await? {
        Some(o) => o,
        None => return Ok(None),
    };
    let snap: Snapshot = serde_json::from_slice(&body)
        .map_err(|e| Error::SnapshotMalformed(format!("parse: {e}")))?;
    if snap.schema_version > SCHEMA_VERSION {
        return Err(Error::SnapshotMalformed(format!(
            "schema_version {} > supported {}",
            snap.schema_version, SCHEMA_VERSION,
        )));
    }
    Ok(Some(snap))
}

/// List the history keys (newest first). Used by replay diagnostics
/// and by tests.
pub async fn list_history(store: &dyn CoordStore) -> Result<Vec<String>> {
    let entries = store.list("state/snapshot-").await?;
    let mut keys: Vec<String> = entries.into_iter().map(|e| e.key).collect();
    // S3 lexical order is ascending; we want newest first so the
    // pruner can drop tail entries trivially.
    keys.sort_by(|a, b| b.cmp(a));
    Ok(keys)
}

async fn prune_history(store: &dyn CoordStore, keep: usize) -> Result<()> {
    let keys = list_history(store).await?;
    for k in keys.into_iter().skip(keep) {
        store.delete(&k).await?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::schema::{Job, JobConfig, JobId, Phase};
    use crate::store::MemStore;
    use chrono::{Duration, TimeZone};
    use std::collections::BTreeMap;

    fn at(secs: i64) -> DateTime<Utc> {
        Utc.with_ymd_and_hms(2026, 5, 29, 14, 32, 0).unwrap() + Duration::seconds(secs)
    }

    fn sample_snapshot(written_at: DateTime<Utc>, last_seq: u64) -> Snapshot {
        let mut snap = Snapshot::empty(written_at);
        snap.last_seq = last_seq;
        snap.jobs.insert(
            JobId::new("bobby").unwrap(),
            Job {
                id: JobId::new("bobby").unwrap(),
                name: "bobby-migration".into(),
                source: "nfs://src".into(),
                dest: "nfs://dst".into(),
                owner: "blake".into(),
                created_at: written_at,
                config_hash: crate::schema::ConfigHash("deadbeef".into()),
                config: JobConfig {
                    source: "nfs://src".into(),
                    dest: "nfs://dst".into(),
                    claim_version: 2,
                    conflict_policy: crate::schema::ConflictPolicy::Fail,
                    exclusions: vec![],
                    parallelism: Default::default(),
                    rate_caps: Default::default(),
                    acl_handling: crate::schema::AclHandling::PosixOnly,
                    verify_mode: crate::schema::VerifyMode::Stat,
                },
                phase: Phase::Scanning,
                phase_history: vec![],
                progress: Default::default(),
                throughput: Default::default(),
                eta: Default::default(),
                health: Default::default(),
                assigned_workers: vec![],
            },
        );
        snap
    }

    #[tokio::test]
    async fn write_then_load_roundtrips() {
        let s = MemStore::new();
        let snap = sample_snapshot(at(0), 42);
        write(&s, &snap, 5, at(0)).await.unwrap();

        let loaded = load(&s).await.unwrap().unwrap();
        assert_eq!(loaded, snap);
    }

    #[tokio::test]
    async fn load_returns_none_on_empty_store() {
        let s = MemStore::new();
        assert!(load(&s).await.unwrap().is_none());
    }

    #[tokio::test]
    async fn each_write_drops_a_history_copy() {
        let s = MemStore::new();
        for i in 0..3 {
            let snap = sample_snapshot(at(i * 1000), i as u64);
            write(&s, &snap, 10, at(i * 1000)).await.unwrap();
        }
        let history = list_history(&s).await.unwrap();
        assert_eq!(history.len(), 3);
        // Newest first.
        assert!(history[0] > history[1]);
        assert!(history[1] > history[2]);
    }

    #[tokio::test]
    async fn history_pruned_to_keep_count() {
        let s = MemStore::new();
        for i in 0..5 {
            let snap = sample_snapshot(at(i * 1000), i as u64);
            write(&s, &snap, 2, at(i * 1000)).await.unwrap();
        }
        let history = list_history(&s).await.unwrap();
        assert_eq!(history.len(), 2, "expected newest two history copies kept");
        // And the live snapshot is the latest write.
        let live = load(&s).await.unwrap().unwrap();
        assert_eq!(live.last_seq, 4);
    }

    #[tokio::test]
    async fn keep_zero_drops_all_history() {
        let s = MemStore::new();
        let snap = sample_snapshot(at(0), 1);
        write(&s, &snap, 0, at(0)).await.unwrap();
        assert!(list_history(&s).await.unwrap().is_empty());
        // Live snapshot is preserved.
        assert!(load(&s).await.unwrap().is_some());
    }

    #[tokio::test]
    async fn refuses_snapshot_with_newer_schema_version() {
        let s = MemStore::new();
        let mut bytes: BTreeMap<String, serde_json::Value> = Default::default();
        bytes.insert(
            "schema_version".into(),
            serde_json::json!(SCHEMA_VERSION + 1),
        );
        bytes.insert("written_at".into(), serde_json::json!(at(0).to_rfc3339()));
        bytes.insert("last_seq".into(), serde_json::json!(0));
        bytes.insert("jobs".into(), serde_json::json!({}));
        bytes.insert("workers".into(), serde_json::json!({}));
        bytes.insert("error_buckets".into(), serde_json::json!({}));
        bytes.insert("audit_seq_today".into(), serde_json::json!(0));
        bytes.insert("audit_seq_date".into(), serde_json::json!(""));
        let body = serde_json::to_vec(&bytes).unwrap();
        s.put(SNAPSHOT_KEY, body).await.unwrap();

        let err = load(&s).await.unwrap_err();
        assert!(
            matches!(err, Error::SnapshotMalformed(_)),
            "expected SnapshotMalformed, got {err:?}",
        );
    }

    #[tokio::test]
    async fn malformed_snapshot_propagates() {
        let s = MemStore::new();
        s.put(SNAPSHOT_KEY, b"not json".to_vec()).await.unwrap();
        let err = load(&s).await.unwrap_err();
        assert!(matches!(err, Error::SnapshotMalformed(_)));
    }
}
