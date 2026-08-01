//! Event log writer + chunk reader.
//!
//! Two consumers:
//!
//! - The coord runtime calls `EventLogWriter::buffer` for every
//!   event the system emits (under its state lock — pure
//!   bookkeeping), then drives the actual S3 PUTs through
//!   `pending_flushes`/`complete_flush` *outside* that lock (F45b).
//!   The writer buffers events in memory per chunk (one per active
//!   job + one cluster-wide) and a chunk becomes due when either
//!   threshold is hit:
//!     - `max_events_per_chunk` (default 1000)
//!     - `max_chunk_age` since the chunk's first event (default 5 min)
//!
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
pub(crate) enum ChunkRoute {
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

/// What the runtime buffered on an [`EventLogWriter::buffer`] call.
/// `route` + `threshold_reached` let the caller decide to flush the
/// affected route *after* dropping whatever lock guards the writer —
/// the F45b contract that no store PUT runs under the runtime's
/// state mutex.
#[derive(Debug)]
pub(crate) struct AppendOutcome {
    /// Key the open chunk will flush to (start-seq keyed).
    pub(crate) key: String,
    /// True once the route's open chunk holds
    /// `max_events_per_chunk` events — the caller should flush it.
    pub(crate) threshold_reached: bool,
    pub(crate) route: ChunkRoute,
}

/// Which open chunks a [`EventLogWriter::pending_flushes`] call
/// snapshots for flushing.
#[derive(Debug)]
pub(crate) enum FlushScope {
    /// Every non-empty open chunk.
    All,
    /// Only chunks older than `max_chunk_age` relative to the
    /// carried `now`.
    Aged(DateTime<Utc>),
    /// Only the named route (the threshold-flush path).
    Route(ChunkRoute),
}

/// A serialized chunk snapshot ready to PUT. Produced by
/// [`EventLogWriter::pending_flushes`] under the state lock, PUT by
/// the caller *outside* it, then acknowledged with
/// [`EventLogWriter::complete_flush`]. Until `complete_flush` runs
/// the buffered envelopes stay in the writer — a failed PUT loses
/// nothing and the next flush retries the same seqs (possibly with
/// newer appends folded in; the key is start-seq-stable so the
/// retried object is a superset at the same key).
#[derive(Debug)]
pub(crate) struct PendingFlush {
    pub(crate) route: ChunkRoute,
    pub(crate) key: String,
    pub(crate) body: Vec<u8>,
    /// Highest seq serialized into `body`. `complete_flush` drops
    /// exactly the prefix `<= end_seq`, so envelopes appended while
    /// the PUT was in flight survive and re-key a fresh chunk at
    /// their own start seq — ranges never overlap.
    pub(crate) end_seq: u64,
}

/// Append-only event log writer. Holds one in-memory open chunk per
/// route until a threshold flush.
///
/// The writer owns no clock — the caller passes the envelope's `at`
/// for routing and chunk-age decisions. This makes the writer's tests
/// deterministic; in production the coord runtime stamps `at =
/// Utc::now()` at ingest time.
///
/// Two flush surfaces:
///
/// - `append`/`flush_all`/`flush_aged` — self-contained convenience
///   API (PUT inline) for replay tooling and tests.
/// - `buffer` + `pending_flushes` + `complete_flush` — the split
///   API the coord runtime uses so the PUT can run outside its
///   state mutex (F45b). The convenience methods are implemented on
///   top of the split ones, so there is a single serialization and
///   removal path.
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

    /// Buffer an envelope into its route's open chunk. Pure
    /// bookkeeping — never touches the store, so it is safe to call
    /// under the runtime's state mutex. The caller inspects
    /// [`AppendOutcome::threshold_reached`] and flushes the route
    /// once it has released that lock.
    pub(crate) fn buffer(&mut self, env: EventEnvelope) -> AppendOutcome {
        let route = ChunkRoute::from_envelope(&env);
        let chunk = self
            .chunks
            .entry(route.clone())
            .or_insert_with(|| OpenChunk::new(env.seq, env.at));
        chunk.buf.push(env);
        AppendOutcome {
            key: route.chunk_key(chunk.start_seq),
            threshold_reached: chunk.buf.len() >= self.cfg.max_events_per_chunk,
            route,
        }
    }

    /// Append an envelope to its route's open chunk. If the chunk is
    /// now at or above `max_events_per_chunk`, flushes it.
    ///
    /// Returns the key the chunk would be (or was just) written to —
    /// useful for tests and for the runtime's logging.
    pub async fn append(&mut self, store: &dyn CoordStore, env: EventEnvelope) -> Result<String> {
        let out = self.buffer(env);
        if out.threshold_reached {
            self.flush_scope(store, &FlushScope::Route(out.route))
                .await?;
        }
        Ok(out.key)
    }

    /// Snapshot every open chunk selected by `scope` as a
    /// [`PendingFlush`] (serialized body + key + covered seq range).
    /// Read-only: the buffered envelopes stay in the writer until
    /// the caller confirms the PUT with [`Self::complete_flush`].
    pub(crate) fn pending_flushes(&self, scope: &FlushScope) -> Result<Vec<PendingFlush>> {
        let mut out = Vec::new();
        for (route, chunk) in &self.chunks {
            if chunk.buf.is_empty() {
                continue;
            }
            let selected = match scope {
                FlushScope::All => true,
                FlushScope::Aged(now) => *now - chunk.started_at >= self.cfg.max_chunk_age,
                FlushScope::Route(r) => route == r,
            };
            if !selected {
                continue;
            }
            out.push(PendingFlush {
                route: route.clone(),
                key: route.chunk_key(chunk.start_seq),
                body: chunk.serialize()?,
                end_seq: chunk.buf.last().expect("non-empty checked above").seq,
            });
        }
        Ok(out)
    }

    /// Record that a [`PendingFlush`] PUT succeeded: drop the
    /// route's buffered prefix `<= end_seq`. Envelopes appended
    /// while the PUT was in flight remain and re-key a fresh chunk
    /// at their own start seq, so consecutive chunk keys cover
    /// non-overlapping ascending ranges.
    pub(crate) fn complete_flush(&mut self, route: &ChunkRoute, end_seq: u64) {
        let Some(chunk) = self.chunks.get_mut(route) else {
            return;
        };
        chunk.buf.retain(|e| e.seq > end_seq);
        if chunk.buf.is_empty() {
            self.chunks.remove(route);
        } else {
            let (start_seq, started_at) = (chunk.buf[0].seq, chunk.buf[0].at);
            chunk.start_seq = start_seq;
            chunk.started_at = started_at;
        }
    }

    /// Convenience: snapshot + PUT + complete for `scope`, inline.
    /// Used by the self-contained methods below; the runtime uses
    /// the split API instead so its PUTs run outside the state lock.
    async fn flush_scope(&mut self, store: &dyn CoordStore, scope: &FlushScope) -> Result<()> {
        for p in self.pending_flushes(scope)? {
            store.put(&p.key, p.body).await?;
            self.complete_flush(&p.route, p.end_seq);
        }
        Ok(())
    }

    /// Flush every open chunk older than `max_chunk_age` relative to
    /// `now`. Empty chunks are never flushed.
    pub async fn flush_aged(&mut self, store: &dyn CoordStore, now: DateTime<Utc>) -> Result<()> {
        self.flush_scope(store, &FlushScope::Aged(now)).await
    }

    /// Force-flush every open chunk. Called on graceful shutdown.
    pub async fn flush_all(&mut self, store: &dyn CoordStore) -> Result<()> {
        self.flush_scope(store, &FlushScope::All).await
    }

    /// Number of events currently buffered across all open chunks.
    /// The runtime exposes this so the CLI can report exactly how
    /// many events a lease-lost shutdown dropped.
    pub fn buffered_events(&self) -> usize {
        self.chunks.values().map(|c| c.buf.len()).sum()
    }

    /// Snapshot every buffered (not-yet-flushed) envelope with
    /// `seq > since`, across all routes, ascending by seq. The SSE
    /// catch-up path reads this so a client resuming while events
    /// sit in the writer buffer does not miss the tail between the
    /// last flushed chunk and the live broadcast.
    pub fn unflushed_since(&self, since: u64) -> Vec<EventEnvelope> {
        let mut out: Vec<EventEnvelope> = self
            .chunks
            .values()
            .flat_map(|c| c.buf.iter())
            .filter(|e| e.seq > since)
            .cloned()
            .collect();
        out.sort_by_key(|e| e.seq);
        out
    }

    /// True if the writer currently buffers unflushed events for
    /// `job_id`'s route. The archive tick uses this to defer a
    /// terminal job whose tail is not yet on disk — archiving it now
    /// would leave the eventual flush stranded under `events/`.
    pub fn has_buffered_for_job(&self, job_id: &JobId) -> bool {
        self.chunks
            .get(&ChunkRoute::Job(job_id.clone()))
            .is_some_and(|c| !c.buf.is_empty())
    }

    /// Test-only — number of open chunks (used to assert flush
    /// behavior).
    #[cfg(test)]
    fn open_chunks(&self) -> usize {
        self.chunks.values().filter(|c| !c.buf.is_empty()).count()
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

/// Parse the zero-padded start seq embedded in a chunk key
/// (`.../<start_seq:020>.jsonl`). Returns `None` for keys that do not
/// match the layout — callers treat those conservatively (read them).
pub(crate) fn chunk_start_seq(key: &str) -> Option<u64> {
    let basename = key.rsplit('/').next()?;
    let stem = basename.strip_suffix(crate::layout::EVENT_CHUNK_EXT)?;
    stem.parse().ok()
}

/// Given ascending chunk keys for **one** route, drop the leading
/// chunks that cannot contain any event with `seq > since`: a chunk is
/// skippable when the *next* chunk's start seq is `<= since` (every
/// event in it is then strictly below `since`). The boundary chunk —
/// the one `since` falls inside — is always read: one chunk of slack
/// instead of clever boundary math. A key whose seq cannot be parsed
/// stops the skipping so malformed keys are still read (and rejected
/// loudly by `read_chunk`'s schema checks) rather than silently
/// dropped.
pub(crate) fn skip_chunks_below(keys: &[String], since: u64) -> &[String] {
    let mut start = 0;
    while start + 1 < keys.len() {
        match chunk_start_seq(&keys[start + 1]) {
            Some(next_start) if next_start <= since => start += 1,
            _ => break,
        }
    }
    &keys[start..]
}

/// Read every envelope across **every** route under `events/`
/// with `seq > since`, sorted ascending by seq. Used by the SSE
/// resume path: when a client connects with `Last-Event-ID = N`,
/// the handler emits these in order before switching to the live
/// broadcast.
///
/// Seq-aware: chunk keys embed their zero-padded start seq, so per
/// route we skip the leading chunks that cannot contain `seq > since`
/// (see `skip_chunks_below`) instead of GETting the entire history.
///
/// Memory: O(events-since-checkpoint). The SSE catch-up window is
/// expected to be small (a reconnect after a brief blip); a client
/// that lags by hours past the snapshot cadence gets a synthetic
/// `Resync` from the live side instead and re-fetches `/jobs`.
pub async fn read_all_events_since(
    store: &dyn CoordStore,
    since: u64,
) -> Result<Vec<EventEnvelope>> {
    let entries = store.list(crate::layout::EVENTS_PREFIX).await?;
    // Group chunk keys by route (`events/_cluster/`, `events/<job>/`)
    // so the skip logic sees each route's ascending seq sequence. The
    // LIST is lexically ordered, so within a route keys are already
    // ascending by start seq.
    let mut by_route: BTreeMap<String, Vec<String>> = BTreeMap::new();
    for entry in entries {
        if !entry.key.ends_with(crate::layout::EVENT_CHUNK_EXT) {
            continue;
        }
        let Some(slash) = entry.key.rfind('/') else {
            continue;
        };
        let route = entry.key[..=slash].to_string();
        by_route.entry(route).or_default().push(entry.key);
    }

    let mut envelopes = Vec::new();
    for keys in by_route.values() {
        for key in skip_chunks_below(keys, since) {
            for env in read_chunk(store, key).await? {
                if env.seq > since {
                    envelopes.push(env);
                }
            }
        }
    }
    envelopes.sort_by_key(|e| e.seq);
    Ok(envelopes)
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
            client_seq: None,
            from_worker: None,
            kind: EventKind::VerifyStarted { job_id: jid(job) },
        }
    }

    fn env_cluster(seq: u64, at: DateTime<Utc>) -> EventEnvelope {
        EventEnvelope {
            seq,
            at,
            schema_version: SCHEMA_VERSION,
            worker_at: None,
            client_seq: None,
            from_worker: None,
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
    async fn unflushed_since_returns_buffered_tail_across_routes() {
        let s = MemStore::new();
        let mut w = EventLogWriter::new(small_cfg());
        // Interleave two routes so the tail spans chunks; keep each
        // below the flush threshold (3).
        w.append(&s, env_for_job(1, base(), "bobby")).await.unwrap();
        w.append(&s, env_cluster(2, base())).await.unwrap();
        w.append(&s, env_for_job(3, base(), "bobby")).await.unwrap();

        let all = w.unflushed_since(0);
        assert_eq!(all.iter().map(|e| e.seq).collect::<Vec<_>>(), [1, 2, 3]);

        let tail = w.unflushed_since(2);
        assert_eq!(tail.iter().map(|e| e.seq).collect::<Vec<_>>(), [3]);

        // Flushed events leave the tail.
        w.flush_all(&s).await.unwrap();
        assert!(w.unflushed_since(0).is_empty());
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

    #[test]
    fn chunk_start_seq_parses_layout_keys() {
        assert_eq!(
            chunk_start_seq("events/bobby/00000000000000000017.jsonl"),
            Some(17),
        );
        assert_eq!(
            chunk_start_seq("events/_cluster/18446744073709551615.jsonl"),
            Some(u64::MAX),
        );
        assert_eq!(chunk_start_seq("events/bobby/not-a-seq.jsonl"), None);
        assert_eq!(chunk_start_seq("events/bobby/17.txt"), None);
    }

    #[test]
    fn skip_chunks_below_keeps_one_chunk_of_slack() {
        let keys: Vec<String> = [1u64, 1000, 2000]
            .iter()
            .map(|s| job_events_chunk_key("bobby", *s))
            .collect();
        // since inside the middle chunk's range: drop only the first.
        assert_eq!(skip_chunks_below(&keys, 1500), &keys[1..]);
        // since exactly at a chunk start: that chunk may still hold
        // events > since — keep it, drop everything before.
        assert_eq!(skip_chunks_below(&keys, 2000), &keys[2..]);
        // since below everything: keep all. since = 0 (full read).
        assert_eq!(skip_chunks_below(&keys, 500), &keys[..]);
        assert_eq!(skip_chunks_below(&keys, 0), &keys[..]);
        // since past the end: only the final chunk is read (slack).
        assert_eq!(skip_chunks_below(&keys, 99_999), &keys[2..]);
        // Empty route.
        assert_eq!(skip_chunks_below(&[], 10), &[] as &[String]);
    }

    #[test]
    fn skip_chunks_below_stops_at_unparseable_key() {
        let keys = vec![
            job_events_chunk_key("bobby", 1),
            "events/bobby/garbage.jsonl".to_string(),
            job_events_chunk_key("bobby", 2000),
        ];
        // The unparseable successor halts skipping — chunk 0 is kept
        // so nothing is silently dropped.
        assert_eq!(skip_chunks_below(&keys, 1500), &keys[..]);
    }

    #[tokio::test]
    async fn read_all_events_since_skips_per_route_independently() {
        let s = MemStore::new();
        // bobby: chunks starting at 1 and 100; cluster: one chunk at
        // 50. Serialize one envelope per chunk directly.
        async fn put_env(s: &MemStore, key: &str, env: &EventEnvelope) {
            let mut body = Vec::new();
            serde_json::to_writer(&mut body, env).unwrap();
            body.push(b'\n');
            s.put(key, body).await.unwrap();
        }
        put_env(
            &s,
            &job_events_chunk_key("bobby", 1),
            &env_for_job(1, base(), "bobby"),
        )
        .await;
        put_env(
            &s,
            &job_events_chunk_key("bobby", 100),
            &env_for_job(100, base(), "bobby"),
        )
        .await;
        put_env(&s, &cluster_events_chunk_key(50), &env_cluster(50, base())).await;

        // since = 40: bobby's 1-chunk stays (its successor starts at
        // 100 > 40 — slack), the cluster chunk is its route's only
        // chunk. Everything with seq > 40 comes back merged ascending.
        let events = read_all_events_since(&s, 40).await.unwrap();
        let seqs: Vec<u64> = events.iter().map(|e| e.seq).collect();
        assert_eq!(seqs, vec![50, 100]);

        // since = 100: bobby's 1-chunk is now skippable (next start
        // 100 <= 100); no event anywhere exceeds 100.
        let events = read_all_events_since(&s, 100).await.unwrap();
        assert!(events.is_empty());
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
