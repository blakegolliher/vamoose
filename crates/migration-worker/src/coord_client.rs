//! Worker → coord HTTP client.
//!
//! Wraps `reqwest` for the four worker-facing REST endpoints. Their
//! request and response contracts are owned by
//! [`migration_control_protocol::schema`]:
//!
//! - `POST /workers/register`
//! - `POST /workers/{id}/heartbeat`
//! - `POST /workers/{id}/events`
//! - `POST /workers/{id}/fence`
//!
//! Plus the two primitives that make reconnect-and-resume sane in the
//! presence of a flaky coord:
//!
//! - [`EventBuffer`] — bounded byte-budget ring. Events are pushed
//!   here from the shard processor (in-process). The driver pulls
//!   batches and POSTs them. On overflow the OLDEST events are
//!   dropped and a counter is incremented; the worker logs the drop
//!   but does not block ingest. Losing tail is better than wedging
//!   the copy loop. Every push is stamped with a per-worker,
//!   monotonically increasing `client_seq` (ledger F20, D4) that
//!   rides the entry through drain, resend, and the coord's durable
//!   log — the coord skips stamps at or below its per-worker
//!   high-water mark, so a resend after a lost 200 (or a retry after
//!   a coord-side storage failure) applies exactly once. Dropped
//!   entries take their stamps with them: forward gaps are fine,
//!   only order matters, and a stamp is never reused.
//!
//! - [`Backoff`] — exponential reconnect schedule, 1s → 30s. Reset
//!   to 1s on the first successful HTTP exchange after a series of
//!   failures.
//!
//! `coord_driver` ties these pieces together: event channel to buffer and HTTP
//! batches, heartbeat ticks, reconnect/backoff, and run-control updates.
//!
//! ## Batch response contract (F20 D4)
//!
//! `EventsBatchResponse` is `{seqs, deduped}`: `seqs` covers the
//! entries the coord APPLIED (in batch order) and `deduped` counts
//! the ones it skipped as already-applied. Any 200 means the whole
//! batch is settled — the driver drops it from the resend buffer
//! exactly as it always has, without correlating seqs to entries; a
//! pre-D4 worker that only reads `seqs` stays correct because its
//! unstamped entries are never deduped.

use migration_control_protocol::schema::{EventEnvelope, JobId, WorkerId};
use migration_control_protocol::schema::{
    EventsBatchBody, EventsBatchResponse, FenceBody, FenceResponse, HeartbeatBody,
    HeartbeatResponse, LeaveBody, LeaveResponse, RegisterBody, RegisterResponse, WorkerEventEntry,
};
use reqwest::header::{HeaderMap, HeaderValue};
use reqwest::Client;
use std::collections::VecDeque;
use std::time::Duration;
use thiserror::Error;

// =============================================================================
// Errors
// =============================================================================

#[derive(Debug, Error)]
pub enum CoordError {
    /// Network / IO failure. The driver should backoff and retry.
    #[error("transport: {0}")]
    Transport(#[from] reqwest::Error),

    /// HTTP status indicating the coord refused the request. 4xx is
    /// usually a config bug (bad cluster secret, unknown job_id) and
    /// retrying does not help. 5xx is transient.
    #[error("http {status}: {body}")]
    Http {
        status: reqwest::StatusCode,
        body: String,
    },

    /// Local serde failure. Never expected in practice — the worker
    /// owns the types it sends. Surfaced to make the error type
    /// total rather than panicking.
    #[error("serde: {0}")]
    Serde(#[from] serde_json::Error),
}

impl CoordError {
    /// Whether the driver should keep retrying. 4xx errors stop the
    /// loop and surface to the operator (they mean the operator
    /// fixed the wrong knob); everything else backs off and retries.
    pub fn is_retryable(&self) -> bool {
        match self {
            Self::Transport(_) => true,
            Self::Http { status, .. } => status.is_server_error(),
            Self::Serde(_) => false,
        }
    }

    /// `404` from the coord: the configured job is not (yet) in its
    /// registry. Registration treats this as "wait", not "fail", so
    /// workers may start before the coord has seeded the job.
    pub fn is_job_not_found(&self) -> bool {
        matches!(self, Self::Http { status, .. } if *status == reqwest::StatusCode::NOT_FOUND)
    }
}

pub type Result<T> = std::result::Result<T, CoordError>;

// =============================================================================
// EventBuffer
// =============================================================================

/// Bounded ring buffer for outbound events. The byte budget is
/// computed by serialized JSON length per entry — close enough to
/// wire size for backpressure; not exact, but the drop policy only
/// needs "approximately within budget".
///
/// Single-producer / single-consumer in the worker (shard_processor
/// pushes, driver drains), so internal Mutex is the caller's
/// responsibility — the buffer itself is owned by the driver task.
#[derive(Debug)]
pub struct EventBuffer {
    queue: VecDeque<Buffered>,
    bytes: usize,
    budget: usize,
    drops: u64,
    /// Next `client_seq` stamp (ledger F20, D4). Starts at 1;
    /// consumed by every push — including pushes that are dropped on
    /// the spot — so a stamp is never reused and drops surface as
    /// forward gaps, which the coord tolerates by design.
    next_client_seq: u64,
}

#[derive(Debug, Clone)]
struct Buffered {
    envelope: EventEnvelope,
    /// Pre-computed serialized size. Cheap to maintain on push and
    /// avoids re-serializing on every drop check.
    size: usize,
}

impl EventBuffer {
    /// Build a buffer with the given byte budget. A budget of 0
    /// means "drop everything immediately" (useful for unit tests
    /// of the drop counter).
    pub fn new(budget_bytes: usize) -> Self {
        Self {
            queue: VecDeque::new(),
            bytes: 0,
            budget: budget_bytes,
            drops: 0,
            next_client_seq: 1,
        }
    }

    pub fn len(&self) -> usize {
        self.queue.len()
    }

    pub fn is_empty(&self) -> bool {
        self.queue.is_empty()
    }

    pub fn bytes(&self) -> usize {
        self.bytes
    }

    pub fn drops(&self) -> u64 {
        self.drops
    }

    /// Push one event. If the buffer would exceed its byte budget,
    /// drop oldest entries until it fits. Returns the number of
    /// events dropped to make room (also folded into `drops()`).
    ///
    /// An event larger than the entire budget is still dropped
    /// immediately (with the same accounting) — the buffer never
    /// stores a single oversize entry.
    ///
    /// Every push consumes a `client_seq` stamp (F20 D4), written
    /// onto the envelope so it survives drain + requeue and rides
    /// the wire to the coord. Stamping happens here — not at send
    /// time — precisely so a resend carries the ORIGINAL stamps and
    /// the coord can recognize the replay.
    pub fn push(&mut self, mut envelope: EventEnvelope) -> usize {
        envelope.client_seq = Some(self.next_client_seq);
        self.next_client_seq += 1;
        let size = match serde_json::to_vec(&envelope) {
            Ok(b) => b.len(),
            Err(_) => {
                // Serialization shouldn't fail for our own types; if
                // it does, the safest thing is to drop the entry on
                // the floor and bump the counter.
                self.drops = self.drops.saturating_add(1);
                return 0;
            }
        };
        let mut dropped = 0;
        // First evict to make room for the incoming entry.
        while self.bytes + size > self.budget && !self.queue.is_empty() {
            let head = self.queue.pop_front().unwrap();
            self.bytes -= head.size;
            self.drops = self.drops.saturating_add(1);
            dropped += 1;
        }
        // If a single entry exceeds the entire budget, drop it.
        if size > self.budget {
            self.drops = self.drops.saturating_add(1);
            return dropped + 1;
        }
        self.queue.push_back(Buffered { envelope, size });
        self.bytes += size;
        dropped
    }

    /// Take up to `max` entries from the head. Used by the driver
    /// to assemble an HTTP batch.
    pub fn drain_batch(&mut self, max: usize) -> Vec<EventEnvelope> {
        let n = max.min(self.queue.len());
        let mut out = Vec::with_capacity(n);
        for _ in 0..n {
            let b = self.queue.pop_front().unwrap();
            self.bytes -= b.size;
            out.push(b.envelope);
        }
        out
    }

    /// Push a batch back to the FRONT — the driver calls this when
    /// an HTTP send fails so the events keep their order for the
    /// next retry. If pushing back would exceed budget, the tail
    /// (newer entries already in the buffer) is dropped first; we
    /// favor not losing in-flight events over not losing freshly
    /// generated ones, because the in-flight ones are older.
    pub fn requeue_front(&mut self, mut batch: Vec<EventEnvelope>) {
        // Walk in reverse so push_front lands them in original order.
        while let Some(e) = batch.pop() {
            let size = serde_json::to_vec(&e).map(|b| b.len()).unwrap_or(0);
            while self.bytes + size > self.budget && !self.queue.is_empty() {
                // Evict from the BACK on requeue — keep the older
                // (in-flight) events; the newer ones lose out.
                let tail = self.queue.pop_back().unwrap();
                self.bytes -= tail.size;
                self.drops = self.drops.saturating_add(1);
            }
            if size > self.budget {
                self.drops = self.drops.saturating_add(1);
                continue;
            }
            self.queue.push_front(Buffered { envelope: e, size });
            self.bytes += size;
        }
    }
}

// =============================================================================
// Backoff
// =============================================================================

/// Exponential reconnect schedule. Starts at `initial`, doubles up
/// to `max`. Reset to `initial` on success.
///
/// Defaults are 1s initial, 30s max — long enough that a flapping
/// coord doesn't drown the worker's logs, short enough that a one-
/// minute coord restart is invisible to the shard loop.
#[derive(Debug, Clone)]
pub struct Backoff {
    initial: Duration,
    max: Duration,
    current: Duration,
}

impl Backoff {
    pub fn new(initial: Duration, max: Duration) -> Self {
        assert!(initial > Duration::ZERO);
        assert!(max >= initial);
        Self {
            initial,
            max,
            current: initial,
        }
    }

    pub fn default_schedule() -> Self {
        Self::new(Duration::from_secs(1), Duration::from_secs(30))
    }

    /// Current delay. Call this BEFORE sleeping.
    pub fn current(&self) -> Duration {
        self.current
    }

    /// Advance to the next delay (double, capped at max). Returns
    /// the delay that was just advanced TO — convenient for `sleep(
    /// backoff.step())`.
    pub fn step(&mut self) -> Duration {
        let doubled = self.current.saturating_mul(2);
        self.current = doubled.min(self.max);
        self.current
    }

    /// Jump straight to the maximum delay. Used while waiting on a
    /// condition that is expected to take a while (the coord has not
    /// seeded the job yet) so the wait does not spam the log.
    pub fn saturate(&mut self) {
        self.current = self.max;
    }

    /// Reset to the initial delay. Call after a successful HTTP
    /// exchange.
    pub fn reset(&mut self) {
        self.current = self.initial;
    }
}

// =============================================================================
// CoordClient
// =============================================================================

/// Worker-side HTTP client for the coord. Stateless except for the
/// cached headers — every call is a single HTTP round trip.
///
/// The cluster secret (if set) is sent as `X-Cluster-Secret` on
/// every request; admin Bearer auth is not used from the worker
/// side. TLS verification, timeouts, and the connection pool are
/// configured at `Client` construction time and inherited.
#[derive(Debug, Clone)]
pub struct CoordClient {
    http: Client,
    base_url: String,
}

impl CoordClient {
    /// Build a client with the given base URL (e.g.
    /// `https://coord.example:8443`). Optional cluster secret is
    /// pinned into the default header map so every request carries
    /// it automatically.
    ///
    /// `verify_tls = false` accepts any TLS cert — the same lab
    /// escape hatch the worker's S3 client supports.
    pub fn new(
        base_url: impl Into<String>,
        cluster_secret: Option<&str>,
        verify_tls: bool,
        request_timeout: Duration,
    ) -> Result<Self> {
        let mut headers = HeaderMap::new();
        if let Some(s) = cluster_secret {
            let mut v = HeaderValue::from_str(s).map_err(|_| CoordError::Http {
                status: reqwest::StatusCode::BAD_REQUEST,
                body: "invalid X-Cluster-Secret value (non-ASCII)".into(),
            })?;
            v.set_sensitive(true);
            headers.insert("x-cluster-secret", v);
        }
        let http = Client::builder()
            .danger_accept_invalid_certs(!verify_tls)
            .default_headers(headers)
            .timeout(request_timeout)
            .build()?;
        Ok(Self {
            http,
            base_url: base_url.into().trim_end_matches('/').to_string(),
        })
    }

    fn url(&self, path: &str) -> String {
        format!("{}{}", self.base_url, path)
    }

    /// `POST /workers/register`. Returns the coord-minted `WorkerId`
    /// and the list of prior workers it just superseded (for
    /// diagnostics; the worker normally ignores it).
    pub async fn register(
        &self,
        job_id: &JobId,
        host: String,
        pid: u32,
        start_time: chrono::DateTime<chrono::Utc>,
        version: String,
    ) -> Result<RegisterResponse> {
        let body = RegisterBody {
            job_id: job_id.0.clone(),
            host,
            pid,
            start_time,
            version,
        };
        let resp = self
            .http
            .post(self.url("/workers/register"))
            .json(&body)
            .send()
            .await?;
        check_status(resp)
            .await?
            .json::<RegisterResponse>()
            .await
            .map_err(Into::into)
    }

    /// `POST /workers/{id}/heartbeat`. Returns the control envelope —
    /// the caller flips its local RunControl on every successful
    /// heartbeat.
    pub async fn heartbeat(
        &self,
        worker_id: WorkerId,
        body: HeartbeatBody,
    ) -> Result<HeartbeatResponse> {
        let resp = self
            .http
            .post(self.url(&format!("/workers/{worker_id}/heartbeat")))
            .json(&body)
            .send()
            .await?;
        check_status(resp)
            .await?
            .json::<HeartbeatResponse>()
            .await
            .map_err(Into::into)
    }

    /// `POST /workers/{id}/events`. Sends a batch in a single round
    /// trip. The returned `seqs` vec covers the entries the coord
    /// APPLIED, in batch order; entries the coord skipped as
    /// already-applied (`client_seq` dedup on a resend) are counted
    /// in the response's `deduped` field and get no seq. Any `Ok`
    /// means the whole batch is settled — the caller drops it from
    /// the resend buffer without correlating seqs to entries.
    ///
    /// `worker_at` is set to the envelope's `at` on every entry —
    /// preserved for diagnostics only; the coord never compares it
    /// across nodes. `client_seq` carries the stamp the
    /// [`EventBuffer`] wrote at push time (F20 D4).
    pub async fn events_batch(
        &self,
        worker_id: WorkerId,
        events: Vec<EventEnvelope>,
    ) -> Result<Vec<u64>> {
        let body = EventsBatchBody {
            events: events
                .into_iter()
                .map(|e| WorkerEventEntry {
                    worker_at: Some(e.at),
                    client_seq: e.client_seq,
                    kind: e.kind,
                })
                .collect(),
        };
        let resp = self
            .http
            .post(self.url(&format!("/workers/{worker_id}/events")))
            .json(&body)
            .send()
            .await?;
        let resp = check_status(resp).await?;
        let parsed: EventsBatchResponse = resp.json().await?;
        Ok(parsed.seqs)
    }

    /// `POST /workers/{id}/fence`. Fire-and-forget on the worker
    /// side — failures here are logged but do not block exit (the
    /// fence is already in effect locally regardless of whether the
    /// coord ever learns about it).
    pub async fn fence(&self, worker_id: WorkerId, reason: String) -> Result<FenceResponse> {
        let body = FenceBody { reason };
        let resp = self
            .http
            .post(self.url(&format!("/workers/{worker_id}/fence")))
            .json(&body)
            .send()
            .await?;
        check_status(resp)
            .await?
            .json::<FenceResponse>()
            .await
            .map_err(Into::into)
    }
}

impl CoordClient {
    /// `POST /workers/{id}/leave`. Best-effort announcement of an
    /// orderly exit so the coord marks us Disconnected now rather
    /// than after its liveness timeout.
    pub async fn leave(&self, worker_id: WorkerId, reason: String) -> Result<LeaveResponse> {
        let body = LeaveBody { reason };
        let resp = self
            .http
            .post(self.url(&format!("/workers/{worker_id}/leave")))
            .json(&body)
            .send()
            .await?;
        check_status(resp)
            .await?
            .json::<LeaveResponse>()
            .await
            .map_err(Into::into)
    }
}

// Re-export the control mode so the orchestrator side can match on
// `mode` without depending on the coordinator implementation.
pub use migration_control_protocol::schema::ControlMode;

async fn check_status(resp: reqwest::Response) -> Result<reqwest::Response> {
    let status = resp.status();
    if status.is_success() {
        Ok(resp)
    } else {
        let body = resp.text().await.unwrap_or_default();
        Err(CoordError::Http { status, body })
    }
}

// =============================================================================
// Tests — primitives only. The CoordClient methods are exercised in
// tests/coord_client_integration.rs against an in-process coord.
// =============================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::{TimeZone, Utc};
    use migration_control_protocol::schema::{ConfigHash, EventKind, JobId, SCHEMA_VERSION};

    fn at(secs: i64) -> chrono::DateTime<chrono::Utc> {
        Utc.timestamp_opt(secs, 0).unwrap()
    }

    fn job_created(seq: u64) -> EventEnvelope {
        EventEnvelope {
            seq,
            at: at(seq as i64),
            schema_version: SCHEMA_VERSION,
            worker_at: None,
            client_seq: None,
            from_worker: None,
            kind: EventKind::JobCreated {
                job_id: JobId::new("bobby").unwrap(),
                name: "bobby-mig".into(),
                source: "nfs://src".into(),
                dest: "nfs://dst".into(),
                owner: "test".into(),
                config_hash: ConfigHash("ab".into()),
                total_files: 0,
                total_bytes: 0,
            },
        }
    }

    #[test]
    fn buffer_push_drain_round_trips_within_budget() {
        let mut buf = EventBuffer::new(64 * 1024);
        assert!(buf.is_empty());
        for s in 1..=10 {
            let dropped = buf.push(job_created(s));
            assert_eq!(dropped, 0, "no drops within budget");
        }
        assert_eq!(buf.len(), 10);
        let batch = buf.drain_batch(4);
        assert_eq!(batch.len(), 4);
        assert_eq!(batch[0].seq, 1);
        assert_eq!(buf.len(), 6);
        assert!(buf.bytes() > 0);
    }

    #[test]
    fn buffer_drops_oldest_when_over_budget() {
        // Pick a budget that fits ~3 events.
        let sample = serde_json::to_vec(&job_created(0)).unwrap().len();
        let budget = sample * 3 + sample / 2;
        let mut buf = EventBuffer::new(budget);

        for s in 1..=5 {
            buf.push(job_created(s));
        }
        // Two of the oldest should have been dropped to make room.
        assert!(buf.drops() >= 2, "drops={}", buf.drops());
        let batch = buf.drain_batch(usize::MAX);
        let seqs: Vec<u64> = batch.iter().map(|e| e.seq).collect();
        assert!(seqs.contains(&5), "newest entry must survive");
        assert!(!seqs.contains(&1), "oldest entry must have been dropped");
    }

    #[test]
    fn buffer_zero_budget_drops_everything() {
        let mut buf = EventBuffer::new(0);
        let dropped = buf.push(job_created(1));
        assert_eq!(dropped, 1);
        assert_eq!(buf.len(), 0);
        assert_eq!(buf.drops(), 1);
    }

    #[test]
    fn buffer_requeue_preserves_order_when_room() {
        let mut buf = EventBuffer::new(64 * 1024);
        let to_requeue = vec![job_created(1), job_created(2), job_created(3)];
        buf.push(job_created(10));
        buf.requeue_front(to_requeue);
        let batch = buf.drain_batch(usize::MAX);
        let seqs: Vec<u64> = batch.iter().map(|e| e.seq).collect();
        assert_eq!(seqs, vec![1, 2, 3, 10]);
    }

    #[test]
    fn buffer_requeue_evicts_tail_under_pressure() {
        // Budget for ~2 events.
        let sample = serde_json::to_vec(&job_created(0)).unwrap().len();
        let budget = sample * 2 + sample / 2;
        let mut buf = EventBuffer::new(budget);
        buf.push(job_created(10));
        buf.push(job_created(11));
        // Now requeue an older batch — the newer tail (11) should
        // be evicted to keep the older ones.
        buf.requeue_front(vec![job_created(1), job_created(2)]);
        let batch = buf.drain_batch(usize::MAX);
        let seqs: Vec<u64> = batch.iter().map(|e| e.seq).collect();
        assert_eq!(seqs[0], 1, "older requeued entry must lead");
        assert_eq!(seqs[1], 2);
        assert!(buf.drops() > 0, "tail eviction must bump drops");
    }

    /// F20 D4 acceptance (work item COORD_EVENT_IDEMPOTENCY.md): the
    /// buffer stamps every push with a per-worker, monotonically
    /// increasing `client_seq` starting at 1. Stamps survive
    /// drain + requeue (the resend path re-sends the SAME stamps, so
    /// the coord can dedup), drops under budget pressure leave
    /// forward gaps, and the counter never reuses a stamp.
    #[test]
    fn event_buffer_stamps_monotonic_client_seq() {
        let mut buf = EventBuffer::new(64 * 1024);
        for s in 1..=3 {
            buf.push(job_created(s));
        }
        let batch = buf.drain_batch(usize::MAX);
        let stamps: Vec<u64> = batch
            .iter()
            .map(|e| e.client_seq.expect("push must stamp client_seq"))
            .collect();
        assert_eq!(stamps, vec![1, 2, 3], "stamps start at 1 and increase");

        // Resend path: a failed POST requeues the batch; the retry
        // must carry the ORIGINAL stamps (that is what lets the coord
        // dedup a replay), not fresh ones.
        buf.requeue_front(batch);
        let batch = buf.drain_batch(usize::MAX);
        let stamps: Vec<u64> = batch.iter().map(|e| e.client_seq.unwrap()).collect();
        assert_eq!(stamps, vec![1, 2, 3], "requeue + drain preserves stamps");

        // New pushes continue the sequence — the counter lives on the
        // buffer, not on the batch.
        buf.push(job_created(4));
        let batch = buf.drain_batch(usize::MAX);
        assert_eq!(
            batch[0].client_seq,
            Some(4),
            "counter continues after drain"
        );

        // Drops under budget pressure: evicted entries take their
        // stamps with them (forward gap), and no stamp is ever
        // reused. Budget for ~2 events; five pushes evict the oldest.
        let sample = serde_json::to_vec(&job_created(0)).unwrap().len() + 32;
        let mut small = EventBuffer::new(sample * 2 + sample / 2);
        for s in 1..=5 {
            small.push(job_created(s));
        }
        assert!(small.drops() > 0, "budget pressure must have dropped");
        let batch = small.drain_batch(usize::MAX);
        let stamps: Vec<u64> = batch.iter().map(|e| e.client_seq.unwrap()).collect();
        for w in stamps.windows(2) {
            assert!(w[0] < w[1], "stamps stay strictly increasing: {stamps:?}");
        }
        assert_eq!(
            *stamps.last().unwrap(),
            5,
            "the newest push holds the newest stamp: {stamps:?}",
        );
        assert!(
            !stamps.contains(&1),
            "the evicted oldest entry's stamp is gone for good (forward gap): {stamps:?}",
        );
        // The counter never rewinds to fill the gap.
        small.push(job_created(6));
        let batch = small.drain_batch(usize::MAX);
        assert_eq!(
            batch.last().unwrap().client_seq,
            Some(6),
            "dropped stamps are never reused",
        );
    }

    #[test]
    fn backoff_doubles_then_caps() {
        let mut b = Backoff::new(Duration::from_millis(100), Duration::from_millis(800));
        assert_eq!(b.current(), Duration::from_millis(100));
        assert_eq!(b.step(), Duration::from_millis(200));
        assert_eq!(b.step(), Duration::from_millis(400));
        assert_eq!(b.step(), Duration::from_millis(800));
        // Capped.
        assert_eq!(b.step(), Duration::from_millis(800));
        assert_eq!(b.step(), Duration::from_millis(800));
    }

    #[test]
    fn backoff_reset_returns_to_initial() {
        let mut b = Backoff::new(Duration::from_millis(50), Duration::from_secs(1));
        b.step();
        b.step();
        b.reset();
        assert_eq!(b.current(), Duration::from_millis(50));
    }

    #[test]
    fn coord_error_retryability_classifies_correctly() {
        let server_err = CoordError::Http {
            status: reqwest::StatusCode::INTERNAL_SERVER_ERROR,
            body: "boom".into(),
        };
        assert!(server_err.is_retryable());

        let client_err = CoordError::Http {
            status: reqwest::StatusCode::UNAUTHORIZED,
            body: "no".into(),
        };
        assert!(!client_err.is_retryable());
    }
}
