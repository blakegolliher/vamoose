//! Event log writer + chunk reader.
//!
//! Two consumers:
//!
//! - The coord runtime calls [`EventLogWriter::append`] for every
//!   event the system emits. The writer buffers events in memory per
//!   chunk (one per active job + one cluster-wide) and flushes to S3
//!   when either threshold is hit:
//!     - `max_events_per_chunk` (default 1000)
//!     - `max_chunk_age` since the chunk's first event (default 5 min)
//!   The age threshold is evaluated on demand via
//!   [`EventLogWriter::flush_aged`]; the coord runtime ticks this
//!   alongside the snapshot tick.
//!
//! - Replay calls [`read_chunk`] / [`list_chunks`] to walk the log in
//!   `seq` order and rebuild state on top of the latest snapshot.
//!
//! ## Routing
//!
//! [`crate::schema::EventKind::job_id`] picks the chunk:
//! - `Some(job_id)` → `events/{job_id}/{start_seq:020}.jsonl`
//! - `None`         → `events/_cluster/{start_seq:020}.jsonl`
//!
//! Worker lifecycle events without a job_id (`WorkerLeft`,
//! `WorkerStateChanged`, `WorkerFenced`, `WorkerRecovered`) land in
//! the cluster log.
//!
//! ## Seq assignment
//!
//! Seq is **not** owned by the writer. The caller assigns seq from
//! its own monotonic counter (single-writer guaranteed by the lease)
//! and seals the envelope before calling `append`. This keeps the
//! writer stateless w.r.t. the counter — replay rebuilds the counter
//! by reading the highest seq seen across the snapshot and the log.
//!
//! ## Chunk file format
//!
//! Newline-delimited JSON. One [`crate::schema::EventEnvelope`] per
//! line, no trailing comma, no enclosing array. Matches the build
//! prompt's `events/<job_id>/<seq:020>.jsonl` layout. Empty chunks
//! are never flushed.

use crate::errors::{Error, Result};
use crate::layout::{cluster_events_chunk_key, job_events_chunk_key};
use crate::schema::{EventEnvelope, JobId, SCHEMA_VERSION};
use crate::store::CoordStore;
use chrono::{DateTime, Duration, Utc};
use std::collections::BTreeMap;

/// Default — matches the build prompt's snapshot cadence so chunks
/// and snapshots roll together. The numbers can be tuned per
/// deployment via [`EventLogConfig`].
const DEFAULT_MAX_EVENTS_PER_CHUNK: usize = 1000;
const DEFAULT_MAX_CHUNK_AGE_SECS: i64 = 5 * 60;

#[derive(Debug, Clone, Copy)]
pub struct EventLogConfig {
    pub max_events_per_chunk: usize,
    pub max_chunk_age: Duration,
}

impl Default for EventLogConfig {
    fn default() -> Self {
        Self {
            max_events_per_chunk: DEFAULT_MAX_EVENTS_PER_CHUNK,
            max_chunk_age: Duration::seconds(DEFAULT_MAX_CHUNK_AGE_SECS),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
enum ChunkRoute {
    Cluster,
    Job(JobId),
}

impl ChunkRoute {
    fn from_envelope(env: &EventEnvelope) -> Self {
        match env.kind.job_id() {
            Some(j) => ChunkRoute::Job(j.clone()),
            None => ChunkRoute::Cluster,
        }
    }

    fn chunk_key(&self, start_seq: u64) -> String {
        match self {
            ChunkRoute::Cluster => cluster_events_chunk_key(start_seq),
            ChunkRoute::Job(j) => job_events_chunk_key(j.as_str(), start_seq),
        }
    }
}

#[derive(Debug)]
struct OpenChunk {
    start_seq: u64,
    started_at: DateTime<Utc>,
    buf: Vec<EventEnvelope>,
}

impl OpenChunk {
    fn new(start_seq: u64, started_at: DateTime<Utc>) -> Self {
        Self {
            start_seq,
            started_at,
            buf: Vec::with_capacity(64),
        }
    }

    fn serialize(&self) -> Result<Vec<u8>> {
        let mut out = Vec::with_capacity(self.buf.len() * 256);
        for env in &self.buf {
            serde_json::to_writer(&mut out, env)?;
            out.push(b'\n');
        }
        Ok(out)
    }
}

/// Append-only event log writer. Holds one in-memory open chunk per
/// route until a threshold flush.
///
/// The writer owns no clock — the caller passes the envelope's `at`
/// for routing and chunk-age decisions. This makes the writer's tests
/// deterministic; in production the coord runtime stamps `at =
/// Utc::now()` at ingest time.
#[derive(Debug)]
pub struct EventLogWriter {
    cfg: EventLogConfig,
    chunks: BTreeMap<ChunkRoute, OpenChunk>,
}

impl EventLogWriter {
    pub fn new(cfg: EventLogConfig) -> Self {
        Self {
            cfg,
            chunks: BTreeMap::new(),
        }
    }

    /// Append an envelope to its route's open chunk. If the chunk is
    /// now at or above `max_events_per_chunk`, flushes it.
    ///
    /// Returns the key the chunk would be (or was just) written to —
    /// useful for tests and for the runtime's logging.
    pub async fn append(&mut self, store: &dyn CoordStore, env: EventEnvelope) -> Result<String> {
        let route = ChunkRoute::from_envelope(&env);
        let chunk = self
            .chunks
            .entry(route.clone())
            .or_insert_with(|| OpenChunk::new(env.seq, env.at));
        chunk.buf.push(env);
        let key = route.chunk_key(chunk.start_seq);
        if chunk.buf.len() >= self.cfg.max_events_per_chunk {
            self.flush_route(store, &route).await?;
        }
        Ok(key)
    }

    /// Flush every open chunk older than `max_chunk_age` relative to
    /// `now`. Empty chunks are never flushed.
    pub async fn flush_aged(&mut self, store: &dyn CoordStore, now: DateTime<Utc>) -> Result<()> {
        let mut to_flush = Vec::new();
        for (route, chunk) in &self.chunks {
            if !chunk.buf.is_empty() && now - chunk.started_at >= self.cfg.max_chunk_age {
                to_flush.push(route.clone());
            }
        }
        for route in to_flush {
            self.flush_route(store, &route).await?;
        }
        Ok(())
    }

    /// Force-flush every open chunk. Called on graceful shutdown.
    pub async fn flush_all(&mut self, store: &dyn CoordStore) -> Result<()> {
        let routes: Vec<_> = self.chunks.keys().cloned().collect();
        for route in routes {
            self.flush_route(store, &route).await?;
        }
        Ok(())
    }

    /// Test-only — number of open chunks (used to assert flush
    /// behavior).
    #[cfg(test)]
    fn open_chunks(&self) -> usize {
        self.chunks.values().filter(|c| !c.buf.is_empty()).count()
    }

    async fn flush_route(&mut self, store: &dyn CoordStore, route: &ChunkRoute) -> Result<()> {
        let chunk = match self.chunks.get_mut(route) {
            Some(c) if !c.buf.is_empty() => c,
            _ => return Ok(()),
        };
        let key = route.chunk_key(chunk.start_seq);
        let body = chunk.serialize()?;
        store.put(&key, body).await?;
        // Drop the buffered events and free the slot — the next event
        // for this route opens a fresh chunk at its own start_seq.
        self.chunks.remove(route);
        Ok(())
    }
}

// =============================================================================
// Replay-side: read chunks back
// =============================================================================

/// All chunk keys for a single route, sorted ascending by start_seq.
/// `route_prefix` is one of `events/_cluster/` or
/// `events/<job_id>/`.
pub async fn list_chunks(store: &dyn CoordStore, route_prefix: &str) -> Result<Vec<String>> {
    let entries = store.list(route_prefix).await?;
    // S3 lexical order = numeric seq order (chunk-key seq is zero-
    // padded to SEQ_WIDTH). Already ascending; no resort needed.
    Ok(entries.into_iter().map(|e| e.key).collect())
}

/// Read every envelope from a chunk file. Caller is responsible for
/// merging across chunks in seq order.
pub async fn read_chunk(store: &dyn CoordStore, key: &str) -> Result<Vec<EventEnvelope>> {
    let (body, _etag) = match store.get(key).await? {
        Some(o) => o,
        None => return Ok(Vec::new()),
    };
    let mut envelopes = Vec::new();
    for (i, line) in body.split(|b| *b == b'\n').enumerate() {
        if line.is_empty() {
            continue;
        }
        let env: EventEnvelope =
            serde_json::from_slice(line).map_err(|e| Error::ChunkMalformed {
                key: key.to_string(),
                line: i,
                detail: e.to_string(),
            })?;
        if env.schema_version > SCHEMA_VERSION {
            return Err(Error::ChunkMalformed {
                key: key.to_string(),
                line: i,
                detail: format!(
                    "schema_version {} > supported {}",
                    env.schema_version, SCHEMA_VERSION
                ),
            });
        }
        envelopes.push(env);
    }
    Ok(envelopes)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::schema::{EventKind, JobId, WorkerId};
    use crate::store::MemStore;
    use chrono::TimeZone;

    fn base() -> DateTime<Utc> {
        Utc.with_ymd_and_hms(2026, 5, 29, 14, 32, 0).unwrap()
    }

    fn jid(s: &str) -> JobId {
        JobId::new(s).unwrap()
    }

    fn env_for_job(seq: u64, at: DateTime<Utc>, job: &str) -> EventEnvelope {
        EventEnvelope {
            seq,
            at,
            schema_version: SCHEMA_VERSION,
            worker_at: None,
            kind: EventKind::VerifyStarted { job_id: jid(job) },
        }
    }

    fn env_cluster(seq: u64, at: DateTime<Utc>) -> EventEnvelope {
        EventEnvelope {
            seq,
            at,
            schema_version: SCHEMA_VERSION,
            worker_at: None,
            kind: EventKind::WorkerLeft {
                worker_id: WorkerId::new(),
                reason: "drain".into(),
            },
        }
    }

    fn small_cfg() -> EventLogConfig {
        EventLogConfig {
            max_events_per_chunk: 3,
            max_chunk_age: Duration::seconds(60),
        }
    }

    #[tokio::test]
    async fn append_below_threshold_does_not_flush() {
        let s = MemStore::new();
        let mut w = EventLogWriter::new(small_cfg());
        w.append(&s, env_for_job(1, base(), "bobby")).await.unwrap();
        w.append(&s, env_for_job(2, base(), "bobby")).await.unwrap();
        assert_eq!(w.open_chunks(), 1);
        // Nothing written to S3 yet.
        assert!(s.list("events/").await.unwrap().is_empty());
    }

    #[tokio::test]
    async fn append_at_threshold_flushes() {
        let s = MemStore::new();
        let mut w = EventLogWriter::new(small_cfg());
        for seq in 1..=3 {
            w.append(&s, env_for_job(seq, base(), "bobby"))
                .await
                .unwrap();
        }
        assert_eq!(w.open_chunks(), 0);
        let chunks = s.list("events/bobby/").await.unwrap();
        assert_eq!(chunks.len(), 1);
        assert_eq!(chunks[0].key, "events/bobby/00000000000000000001.jsonl");
    }

    #[tokio::test]
    async fn job_and_cluster_routes_are_independent() {
        let s = MemStore::new();
        let mut w = EventLogWriter::new(small_cfg());
        w.append(&s, env_for_job(1, base(), "bobby")).await.unwrap();
        w.append(&s, env_cluster(2, base())).await.unwrap();
        w.append(&s, env_for_job(3, base(), "mary")).await.unwrap();
        assert_eq!(w.open_chunks(), 3);
    }

    #[tokio::test]
    async fn flush_aged_only_flushes_old_chunks() {
        let s = MemStore::new();
        let mut w = EventLogWriter::new(small_cfg());
        w.append(&s, env_for_job(1, base(), "bobby")).await.unwrap();
        // 30s later, open a new route.
        w.append(&s, env_for_job(2, base() + Duration::seconds(30), "mary"))
            .await
            .unwrap();
        // Tick at base + 90s: bobby's chunk is 90s old, mary's is 60s.
        // max_chunk_age = 60, so both qualify.
        w.flush_aged(&s, base() + Duration::seconds(90))
            .await
            .unwrap();
        assert_eq!(w.open_chunks(), 0);
    }

    #[tokio::test]
    async fn flush_aged_leaves_recent_chunks_alone() {
        let s = MemStore::new();
        let mut w = EventLogWriter::new(small_cfg());
        w.append(&s, env_for_job(1, base(), "bobby")).await.unwrap();
        w.flush_aged(&s, base() + Duration::seconds(30))
            .await
            .unwrap();
        assert_eq!(w.open_chunks(), 1);
        assert!(s.list("events/").await.unwrap().is_empty());
    }

    #[tokio::test]
    async fn flush_all_drains_every_chunk() {
        let s = MemStore::new();
        let mut w = EventLogWriter::new(small_cfg());
        w.append(&s, env_for_job(1, base(), "bobby")).await.unwrap();
        w.append(&s, env_cluster(2, base())).await.unwrap();
        w.flush_all(&s).await.unwrap();
        assert_eq!(w.open_chunks(), 0);

        let cluster = s.list("events/_cluster/").await.unwrap();
        let bobby = s.list("events/bobby/").await.unwrap();
        assert_eq!(cluster.len(), 1);
        assert_eq!(bobby.len(), 1);
    }

    #[tokio::test]
    async fn empty_writer_flush_is_noop() {
        let s = MemStore::new();
        let mut w = EventLogWriter::new(small_cfg());
        w.flush_aged(&s, base() + Duration::days(1)).await.unwrap();
        w.flush_all(&s).await.unwrap();
        assert!(s.list("events/").await.unwrap().is_empty());
    }

    #[tokio::test]
    async fn round_trip_through_chunk_and_back() {
        let s = MemStore::new();
        let mut w = EventLogWriter::new(small_cfg());
        let envs: Vec<_> = (1..=3)
            .map(|seq| env_for_job(seq, base(), "bobby"))
            .collect();
        for e in &envs {
            w.append(&s, e.clone()).await.unwrap();
        }
        // append at threshold flushes.
        let chunks = list_chunks(&s, "events/bobby/").await.unwrap();
        assert_eq!(chunks.len(), 1);
        let read_back = read_chunk(&s, &chunks[0]).await.unwrap();
        assert_eq!(read_back, envs);
    }

    #[tokio::test]
    async fn list_chunks_returns_ascending_seq_order() {
        let s = MemStore::new();
        // Write chunks out of order deliberately to confirm the
        // zero-padded keys still sort correctly.
        for start_seq in &[1001u64, 1u64, 2001u64] {
            let key = job_events_chunk_key("bobby", *start_seq);
            s.put(&key, b"{}\n".to_vec()).await.unwrap();
        }
        let chunks = list_chunks(&s, "events/bobby/").await.unwrap();
        assert_eq!(chunks.len(), 3);
        assert_eq!(chunks[0], "events/bobby/00000000000000000001.jsonl");
        assert_eq!(chunks[1], "events/bobby/00000000000000001001.jsonl");
        assert_eq!(chunks[2], "events/bobby/00000000000000002001.jsonl");
    }

    #[tokio::test]
    async fn read_chunk_skips_trailing_blank_line() {
        let s = MemStore::new();
        let mut bytes = Vec::new();
        let env = env_cluster(7, base());
        serde_json::to_writer(&mut bytes, &env).unwrap();
        bytes.push(b'\n');
        bytes.push(b'\n'); // trailing empty line
        let key = cluster_events_chunk_key(7);
        s.put(&key, bytes).await.unwrap();

        let envs = read_chunk(&s, &key).await.unwrap();
        assert_eq!(envs.len(), 1);
        assert_eq!(envs[0].seq, 7);
    }

    #[tokio::test]
    async fn read_chunk_rejects_future_schema_version() {
        let s = MemStore::new();
        let bad = format!(
            r#"{{"seq":1,"at":"{}","schema_version":{},"kind":"VerifyStarted","job_id":"bobby"}}{}"#,
            base().to_rfc3339(),
            SCHEMA_VERSION + 1,
            "\n",
        );
        let key = job_events_chunk_key("bobby", 1);
        s.put(&key, bad.into_bytes()).await.unwrap();

        let err = read_chunk(&s, &key).await.unwrap_err();
        assert!(
            matches!(err, Error::ChunkMalformed { .. }),
            "expected ChunkMalformed, got {err:?}",
        );
    }

    #[tokio::test]
    async fn read_chunk_rejects_garbage_line() {
        let s = MemStore::new();
        let key = cluster_events_chunk_key(1);
        s.put(&key, b"not json\n".to_vec()).await.unwrap();
        let err = read_chunk(&s, &key).await.unwrap_err();
        match err {
            Error::ChunkMalformed { key: k, line, .. } => {
                assert_eq!(k, key);
                assert_eq!(line, 0);
            }
            other => panic!("expected ChunkMalformed, got {other:?}"),
        }
    }
}
