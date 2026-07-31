//! Archive-on-completion.
//!
//! When a job reaches a terminal phase (`Completed`, `Failed`,
//! `Cancelled`), the coord rolls its `events/{job_id}/` chunks under
//! `archivelogs/{job_id}/` and deletes the originals. The job's
//! `jobs/{job_id}/config.json` is left in place — operators may want
//! to inspect or replay the immutable config long after the events
//! are no longer hot.
//!
//! ## Why archive
//!
//! The replay routine walks every chunk under `events/` to rebuild
//! state at restart. Terminal jobs no longer contribute to live
//! state but their event chunks would still cost the next coord
//! cold-start a list + read. Moving them to `archivelogs/` keeps
//! the hot path proportional to live work.
//!
//! Archive is a sibling of replay — it never runs during normal
//! event ingest, only on the terminal-phase trigger. Replay does
//! not read `archivelogs/`; recovering archived state would be a
//! separate operator workflow (a future `vamoose coord
//! restore-from-archive` subcommand, if it ever ships).
//!
//! ## Idempotency
//!
//! Re-archiving an already-archived job is a no-op (the events/
//! prefix is empty so the loop does nothing). A partial archive
//! that crashed midway is safe to resume — already-moved chunks no
//! longer appear under `events/`, and any that were copied to
//! `archivelogs/` but not yet deleted from `events/` are overwritten
//! on retry with the same content.
//!
//! ## Atomicity caveat
//!
//! The archive runs as `get → put → delete` per chunk, not
//! transactionally. A crash mid-archive leaves a state where some
//! chunks are in both locations. Subsequent replay still sees the
//! events/ side and folds them; the next archive pass will finish
//! the move. The duplicate in `archivelogs/` is harmless until
//! operator garbage-collects (out of scope for v1).

use crate::errors::Result;
use crate::layout::{job_events_prefix, ARCHIVE_PREFIX};
use crate::schema::JobId;
use crate::store::CoordStore;

/// Outcome of an archive pass — useful for diagnostics, audit, and
/// integration tests.
#[derive(Debug, Default, PartialEq, Eq)]
pub struct ArchiveOutcome {
    pub chunks_moved: usize,
    pub bytes_moved: u64,
}

/// Roll `events/{job_id}/...` chunks to `archivelogs/{job_id}/...`
/// and delete the originals. The job's `jobs/{job_id}/config.json`
/// is **not** touched.
///
/// Returns the count and total byte size of the chunks moved.
pub async fn archive_job(store: &dyn CoordStore, job_id: &JobId) -> Result<ArchiveOutcome> {
    let src_prefix = job_events_prefix(job_id.as_str());
    let dst_prefix = format!("{ARCHIVE_PREFIX}{}/", job_id.as_str());
    let entries = store.list(&src_prefix).await?;

    let mut out = ArchiveOutcome::default();
    for entry in entries {
        // Compute the destination key by swapping the events/ prefix
        // for archivelogs/. The chunk's basename (zero-padded
        // seq.jsonl) is preserved verbatim so archivelogs/ keys sort
        // and merge identically to events/.
        let basename = entry.key.strip_prefix(&src_prefix).ok_or_else(|| {
            crate::Error::Other(anyhow::anyhow!(
                "list returned a key not under prefix {src_prefix}: {key}",
                key = entry.key,
            ))
        })?;
        let dst_key = format!("{dst_prefix}{basename}");

        let (body, _etag) = match store.get(&entry.key).await? {
            Some(o) => o,
            None => continue, // Race: another archiver already moved it.
        };
        let size = body.len() as u64;
        store.put(&dst_key, body).await?;
        store.delete(&entry.key).await?;

        out.chunks_moved += 1;
        out.bytes_moved += size;
    }
    Ok(out)
}

/// List archived chunk keys for a job (newest first by seq).
/// Reserved for the future `vamoose coord restore-from-archive`
/// workflow; included here so the prefix decision is co-located with
/// the writer.
pub async fn list_archived_chunks(store: &dyn CoordStore, job_id: &JobId) -> Result<Vec<String>> {
    let prefix = format!("{ARCHIVE_PREFIX}{}/", job_id.as_str());
    let entries = store.list(&prefix).await?;
    Ok(entries.into_iter().map(|e| e.key).collect())
}

/// Predicate the runtime uses: was this prefix already drained from
/// the hot path? Cheap shortcut that avoids the full archive scan
/// when called repeatedly.
pub async fn is_job_archived(store: &dyn CoordStore, job_id: &JobId) -> Result<bool> {
    let prefix = job_events_prefix(job_id.as_str());
    Ok(store.list(&prefix).await?.is_empty())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::events::{EventLogConfig, EventLogWriter};
    use crate::layout::{
        archive_chunk_key, cluster_events_chunk_key, job_config_key, job_events_chunk_key,
    };
    use crate::schema::{EventEnvelope, EventKind, JobId, SCHEMA_VERSION};
    use crate::store::MemStore;
    use chrono::{Duration, TimeZone, Utc};

    fn jid(s: &str) -> JobId {
        JobId::new(s).unwrap()
    }

    fn env(seq: u64, job: &str) -> EventEnvelope {
        EventEnvelope {
            seq,
            at: Utc.with_ymd_and_hms(2026, 5, 29, 14, 32, 0).unwrap(),
            schema_version: SCHEMA_VERSION,
            worker_at: None,
            client_seq: None,
            from_worker: None,
            kind: EventKind::VerifyStarted {
                job_id: JobId::new(job).unwrap(),
            },
        }
    }

    async fn seed_log_for(s: &MemStore, job: &str, n: u64) {
        let mut w = EventLogWriter::new(EventLogConfig {
            max_events_per_chunk: 2,
            max_chunk_age: Duration::seconds(60),
        });
        for seq in 1..=n {
            w.append(s, env(seq, job)).await.unwrap();
        }
        w.flush_all(s).await.unwrap();
    }

    #[tokio::test]
    async fn archive_moves_every_chunk_and_clears_events_prefix() {
        let s = MemStore::new();
        seed_log_for(&s, "bobby", 5).await;
        // Sanity: 3 chunks under events/bobby/ (size=2 chunks, 5 events
        // = 2 full + 1 trailing of 1).
        assert_eq!(s.list("events/bobby/").await.unwrap().len(), 3);

        let out = archive_job(&s, &jid("bobby")).await.unwrap();
        assert_eq!(out.chunks_moved, 3);
        assert!(out.bytes_moved > 0);

        assert!(s.list("events/bobby/").await.unwrap().is_empty());
        let archived = list_archived_chunks(&s, &jid("bobby")).await.unwrap();
        assert_eq!(archived.len(), 3);
        // Archived keys preserve the seq-padded basename.
        for k in &archived {
            assert!(k.starts_with("archivelogs/bobby/"));
            assert!(k.ends_with(".jsonl"));
        }
    }

    #[tokio::test]
    async fn archive_leaves_job_config_in_place() {
        let s = MemStore::new();
        seed_log_for(&s, "bobby", 3).await;
        s.put(&job_config_key("bobby"), br#"{"x":1}"#.to_vec())
            .await
            .unwrap();

        archive_job(&s, &jid("bobby")).await.unwrap();
        let cfg = s.get(&job_config_key("bobby")).await.unwrap();
        assert!(cfg.is_some(), "config.json must survive archive");
    }

    #[tokio::test]
    async fn archive_leaves_other_jobs_alone() {
        let s = MemStore::new();
        seed_log_for(&s, "bobby", 3).await;
        seed_log_for(&s, "mary", 3).await;

        archive_job(&s, &jid("bobby")).await.unwrap();
        assert!(s.list("events/bobby/").await.unwrap().is_empty());
        assert!(!s.list("events/mary/").await.unwrap().is_empty());
        assert!(list_archived_chunks(&s, &jid("mary"))
            .await
            .unwrap()
            .is_empty());
    }

    #[tokio::test]
    async fn archive_leaves_cluster_log_alone() {
        let s = MemStore::new();
        seed_log_for(&s, "bobby", 3).await;
        // Manually drop a cluster chunk.
        s.put(&cluster_events_chunk_key(99), b"{}\n".to_vec())
            .await
            .unwrap();
        archive_job(&s, &jid("bobby")).await.unwrap();
        assert_eq!(s.list("events/_cluster/").await.unwrap().len(), 1);
    }

    #[tokio::test]
    async fn archive_of_empty_prefix_is_noop() {
        let s = MemStore::new();
        let out = archive_job(&s, &jid("ghost")).await.unwrap();
        assert_eq!(out, ArchiveOutcome::default());
    }

    #[tokio::test]
    async fn is_job_archived_reports_state_correctly() {
        let s = MemStore::new();
        seed_log_for(&s, "bobby", 3).await;
        assert!(!is_job_archived(&s, &jid("bobby")).await.unwrap());
        archive_job(&s, &jid("bobby")).await.unwrap();
        assert!(is_job_archived(&s, &jid("bobby")).await.unwrap());
    }

    #[tokio::test]
    async fn archive_is_idempotent() {
        let s = MemStore::new();
        seed_log_for(&s, "bobby", 3).await;
        let first = archive_job(&s, &jid("bobby")).await.unwrap();
        let second = archive_job(&s, &jid("bobby")).await.unwrap();
        assert_eq!(first.chunks_moved, 2);
        assert_eq!(second.chunks_moved, 0);
        // Re-archiving did not delete archived data.
        let archived = list_archived_chunks(&s, &jid("bobby")).await.unwrap();
        assert_eq!(archived.len(), 2);
    }

    #[tokio::test]
    async fn archived_basename_round_trips_through_layout_helpers() {
        let s = MemStore::new();
        // Seed one chunk with a known start_seq.
        let key = job_events_chunk_key("bobby", 17);
        s.put(&key, b"{\"seq\":17,\"at\":\"2026-05-29T00:00:00Z\",\"kind\":\"VerifyStarted\",\"job_id\":\"bobby\"}\n".to_vec())
            .await
            .unwrap();

        archive_job(&s, &jid("bobby")).await.unwrap();
        let archived = list_archived_chunks(&s, &jid("bobby")).await.unwrap();
        assert_eq!(archived.len(), 1);
        // The archive layout helper produces the exact key we expect.
        assert_eq!(archived[0], archive_chunk_key("bobby", 17));
    }
}
