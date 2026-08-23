//! SSE stream — catch-up + live tail + keepalive.
//!
//! Produces a `Stream<Item = StreamFrame>` that the axum handler
//! formats as `text/event-stream`. The stream interleaves:
//!
//! 1. **Catch-up** — if the client provided `Last-Event-ID`, merge
//!    matching envelopes from the writer's unflushed in-memory tail
//!    and the on-disk log (`events/...`) and yield them in seq
//!    order before switching to live. Reading the flushed chunks
//!    alone is not enough: events buffered in the
//!    [`crate::events::EventLogWriter`] have already been acked and
//!    broadcast, so a client resuming before the next flush would
//!    silently skip them.
//! 2. **Live tail** — receive from the runtime's broadcast bus.
//!    Each envelope is yielded as [`StreamFrame::Event`].
//! 3. **Resync on lag** — if the subscriber falls past the bus
//!    capacity, [`tokio::sync::broadcast::Receiver::recv`] returns
//!    [`tokio::sync::broadcast::error::RecvError::Lagged`]; we yield
//!    [`StreamFrame::Resync`] so the client refetches `/jobs`.
//! 4. **Keepalive** — every 15s the stream yields
//!    [`StreamFrame::Keepalive`], which renders as an SSE comment
//!    line (`: ping\n\n`). This keeps stateful proxies from
//!    closing an idle connection.
//!
//! ## Catch-up + live race
//!
//! The handler subscribes to the bus **before** kicking off the
//! catch-up read, so events that arrive during catch-up are
//! buffered in the receiver. Catch-up then snapshots the writer's
//! unflushed tail *before* listing flushed chunks: at
//! tail-snapshot time every already-ingested event is either still
//! in the buffer (captured by the tail) or already flushed
//! (captured by the later chunk read), and everything ingested
//! after the subscribe arrives on the receiver. Reading in the
//! opposite order would race a concurrent flush — an event could
//! leave the buffer after the chunk read and before the tail read,
//! and appear in neither. The merged catch-up set is emitted in
//! seq order, and the live-tail loop filters by seq strictly
//! greater than the highest emitted so far, so overlap between the
//! tail, the chunks, and the receiver dedups to exactly-once
//! within one connection. Gaps can still reach the client via the
//! bounded bus (`Lagged` → `Resync`, below); the catch-up seam
//! itself does not introduce them.
//!
//! ## Filtering
//!
//! `JobFilter::All` includes every envelope (cluster + per-job).
//! `JobFilter::Job(id)` only emits envelopes whose
//! [`EventKind::job_id`](crate::schema::EventKind::job_id) equals `id`. Worker lifecycle events
//! without a job_id (`WorkerLeft` etc.) never appear in a per-job
//! stream — they ride on the cluster-wide stream only.

use crate::errors::Result;
use crate::events::read_all_events_since;
use crate::runtime::CoordRuntime;
use crate::schema::{EventEnvelope, JobId};
use futures::Stream;
use std::time::Duration;
use tokio::sync::broadcast::error::RecvError;

/// Items the SSE wire layer renders. See module doc for semantics.
///
/// `Event` dwarfs the other variants (the envelope is ~200 bytes),
/// but it is also what virtually every yielded frame is — boxing it
/// to shrink the enum would buy nothing on `Resync`/`Keepalive`
/// (rare) while adding an allocation per event on the hot streaming
/// path.
#[allow(clippy::large_enum_variant)]
#[derive(Debug, Clone, PartialEq)]
pub enum StreamFrame {
    Event(EventEnvelope),
    /// Subscriber lagged past the bus capacity. The client should
    /// refetch `/jobs` and ignore any per-seq tracking before this
    /// frame. `skipped` is the number of events dropped — telemetry
    /// only, no functional dependency.
    Resync {
        skipped: u64,
    },
    /// Periodic keepalive comment so proxies don't close idle
    /// connections.
    Keepalive,
}

/// Per-job vs cluster-wide filter for the stream.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum JobFilter {
    All,
    Job(JobId),
}

impl JobFilter {
    fn matches(&self, env: &EventEnvelope) -> bool {
        match self {
            JobFilter::All => true,
            JobFilter::Job(id) => env.kind.job_id().map(|j| j == id).unwrap_or(false),
        }
    }
}

/// Tuning knobs for the stream. Defaults match the build prompt's
/// "keepalive every 15s" spec.
#[derive(Debug, Clone, Copy)]
pub struct StreamConfig {
    pub keepalive_interval: Duration,
}

impl Default for StreamConfig {
    fn default() -> Self {
        Self {
            keepalive_interval: Duration::from_secs(15),
        }
    }
}

/// Build the SSE stream. `last_event_id` is the seq the client last
/// successfully processed (from the `Last-Event-ID` header). `None`
/// or `Some(0)` means "start from now" — no catch-up.
///
/// The bus subscription happens synchronously *before* this
/// function returns. Without that guarantee, the broadcast channel
/// would have no receiver during the brief window between handler
/// construction and the first stream poll, and any ingest landing
/// in that window would be lost on the live tail. The stream body
/// only captures the receiver, a cloned store, and an
/// [`crate::runtime::EventTailReader`] — the `CoordRuntime` (and with it the
/// broadcast sender) is *not* held by the body, so a
/// `drop(runtime)` outside the stream actually closes the channel
/// and the loop observes `RecvError::Closed`.
pub fn sse_stream(
    rt: CoordRuntime,
    last_event_id: Option<u64>,
    filter: JobFilter,
    cfg: StreamConfig,
) -> impl Stream<Item = Result<StreamFrame>> + 'static {
    let mut rx = rt.subscribe();
    let store = rt.store().clone();
    let tail = rt.tail_reader();
    // Drop the runtime handle now — only `rx`, `store`, and `tail`
    // cross into the stream body. The tail reader deliberately does
    // not hold the broadcast sender, so the drop semantics above
    // still hold. See doc comment for why.
    drop(rt);

    async_stream::try_stream! {
        // `Last-Event-ID` header semantics:
        //   None       -> no catch-up; client wants live tail only.
        //   Some(0)    -> catch up from the very first event (the
        //                 client has never seen anything).
        //   Some(N>0)  -> catch up everything with seq > N.
        let mut highest: u64 = last_event_id.unwrap_or(0);

        // Step 1: catch-up = unflushed writer tail + flushed chunks.
        //
        // Order matters: snapshot the tail BEFORE listing chunks. At
        // tail-snapshot time an already-ingested event is either
        // still buffered (captured here) or already flushed (visible
        // to the chunk read below); anything ingested after the
        // subscribe above is buffered in `rx`. The opposite order
        // would race a concurrent flush and miss events that move
        // from buffer to chunk between the two reads. A flush in the
        // window between the two reads can land the same envelope in
        // both — the ascending emit with the `seq > highest` guard
        // dedups it.
        if let Some(since) = last_event_id {
            let tail_events = tail.unflushed_since(since).await;
            let mut catch_up = read_all_events_since(store.as_ref(), since).await?;
            catch_up.extend(tail_events);
            catch_up.sort_by_key(|e| e.seq);
            for env in catch_up {
                if env.seq > highest && filter.matches(&env) {
                    highest = env.seq;
                    yield StreamFrame::Event(env);
                }
            }
        }

        // Step 2: live tail + keepalive.
        let mut keepalive = tokio::time::interval(cfg.keepalive_interval);
        // The first .tick() on a fresh Interval returns immediately;
        // consume it so clients don't see a keepalive before any
        // real event has had a chance to arrive.
        keepalive.tick().await;

        loop {
            tokio::select! {
                msg = rx.recv() => {
                    match msg {
                        Ok(env) => {
                            if env.seq > highest && filter.matches(&env) {
                                highest = env.seq;
                                yield StreamFrame::Event(env);
                            }
                        }
                        Err(RecvError::Lagged(n)) => {
                            // After Resync the client refetches the
                            // snapshot; reset `highest` so subsequent
                            // events flow through with whatever seqs
                            // the client now considers fresh.
                            highest = 0;
                            yield StreamFrame::Resync { skipped: n };
                        }
                        Err(RecvError::Closed) => return,
                    }
                }
                _ = keepalive.tick() => {
                    yield StreamFrame::Keepalive;
                }
            }
        }
    }
}

// =============================================================================
// axum wire layer
// =============================================================================

use super::{ApiError, AppState};
pub use crate::schema::StreamParams;
use axum::extract::{Query, State};
use axum::response::sse::{Event as SseEvent, KeepAlive, Sse};
use axum::response::IntoResponse;
use futures::TryStreamExt;
use http::HeaderMap;

/// GET /stream and GET /stream?job_id={id}
///
/// Headers honored:
/// - `Last-Event-ID: <seq>` → catch-up from log (seq > the header)
///   before switching to live.
pub async fn handler(
    State(state): State<AppState>,
    Query(params): Query<StreamParams>,
    headers: HeaderMap,
) -> std::result::Result<impl IntoResponse, ApiError> {
    let filter = match params.job_id {
        Some(s) => {
            let id = crate::schema::JobId::new(s)
                .map_err(|e| ApiError::bad_request("invalid_job_id", e.to_string()))?;
            // 404 on unknown — clients ask for a job they can browse.
            if state.runtime.job_view(&id).await.is_none() {
                return Err(ApiError::not_found(
                    "job_not_found",
                    format!("no such job: {id}"),
                ));
            }
            JobFilter::Job(id)
        }
        None => JobFilter::All,
    };

    let last_event_id = headers
        .get("last-event-id")
        .and_then(|v| v.to_str().ok())
        .and_then(|s| s.parse::<u64>().ok());

    let stream = sse_stream(
        state.runtime.clone(),
        last_event_id,
        filter,
        StreamConfig::default(),
    )
    .map_ok(|frame| match frame {
        StreamFrame::Event(env) => {
            let kind_name = env.kind.name();
            let data = serde_json::to_string(&env).unwrap_or_else(|_| "{}".to_string());
            SseEvent::default()
                .event(kind_name)
                .id(env.seq.to_string())
                .data(data)
        }
        StreamFrame::Resync { skipped } => SseEvent::default()
            .event("Resync")
            .data(format!("{{\"skipped\":{skipped}}}")),
        StreamFrame::Keepalive => SseEvent::default().comment("ping"),
    })
    .map_err(|e| std::io::Error::other(e.to_string()));

    Ok(Sse::new(stream).keep_alive(KeepAlive::default()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::lease::{Identity, LeaseConfig};
    use crate::runtime::test_clock::FixedClock;
    use crate::runtime::{CoordRuntime, RuntimeConfig};
    use crate::schema::{EventKind, JobId, WorkerId};
    use crate::store::{CoordStore, MemStore};
    use chrono::{Duration, TimeZone, Utc};
    use futures::StreamExt;
    use std::sync::Arc;

    fn jid(s: &str) -> JobId {
        JobId::new(s).unwrap()
    }

    fn me(holder: &str) -> Identity {
        Identity {
            holder_id: holder.into(),
            host: "h".into(),
            pid: 1,
        }
    }

    fn at(secs: i64) -> chrono::DateTime<Utc> {
        Utc.with_ymd_and_hms(2026, 5, 29, 14, 32, 0).unwrap() + Duration::seconds(secs)
    }

    fn cfg_for_tests() -> RuntimeConfig {
        RuntimeConfig {
            lease: LeaseConfig {
                ttl: Duration::seconds(30),
                grace: Duration::seconds(5),
            },
            events: crate::events::EventLogConfig {
                max_events_per_chunk: 10,
                max_chunk_age: Duration::seconds(60),
            },
            bus_capacity: 4,
            lease_retry_interval: std::time::Duration::from_millis(10),
            lease_retry_max_attempts: Some(3),
        }
    }

    async fn fresh_runtime() -> (CoordRuntime, Arc<FixedClock>, Arc<MemStore>) {
        let mem = Arc::new(MemStore::new());
        let store: Arc<dyn CoordStore> = mem.clone();
        let clock = FixedClock::new(at(0));
        let rt = CoordRuntime::start(store, clock.clone(), me("A"), cfg_for_tests())
            .await
            .unwrap();
        (rt, clock, mem)
    }

    fn job_created(job: &str) -> EventKind {
        EventKind::JobCreated {
            job_id: jid(job),
            name: format!("{job}-mig"),
            source: "s".into(),
            dest: "d".into(),
            owner: "test".into(),
            config_hash: crate::schema::ConfigHash("ab".into()),
            total_files: 0,
            total_bytes: 0,
        }
    }

    fn progress_delta(job: &str, files: u64) -> EventKind {
        EventKind::ProgressDelta {
            job_id: jid(job),
            worker_id: WorkerId::new(),
            files_delta: files,
            bytes_delta: 0,
            errors_delta: 0,
        }
    }

    fn worker_left() -> EventKind {
        EventKind::WorkerLeft {
            worker_id: WorkerId::new(),
            reason: "drain".into(),
        }
    }

    #[tokio::test]
    async fn live_tail_emits_post_subscribe_events() {
        let (rt, _clock, _store) = fresh_runtime().await;
        let stream = sse_stream(
            rt.clone(),
            None,
            JobFilter::All,
            StreamConfig {
                keepalive_interval: std::time::Duration::from_secs(60),
            },
        );
        tokio::pin!(stream);

        // Yield once so the subscribe inside the stream runs before
        // we ingest.
        tokio::task::yield_now().await;
        // Drive the stream once to advance into the live-tail loop.
        // We do this by spawning a tiny task that polls the stream
        // for one event after ingest.
        let seq1 = rt.ingest(job_created("bobby")).await.unwrap();
        let seq2 = rt.ingest(job_created("mary")).await.unwrap();

        let f1 = stream.next().await.unwrap().unwrap();
        let f2 = stream.next().await.unwrap().unwrap();
        match (&f1, &f2) {
            (StreamFrame::Event(e1), StreamFrame::Event(e2)) => {
                assert_eq!(e1.seq, seq1);
                assert_eq!(e2.seq, seq2);
            }
            other => panic!("expected two Event frames, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn catch_up_emits_log_events_before_live() {
        let (rt, _clock, _store) = fresh_runtime().await;
        // Ingest three events and flush them to the log so the
        // catch-up path can read them.
        let _ = rt.ingest(job_created("bobby")).await.unwrap();
        let _ = rt.ingest(progress_delta("bobby", 10)).await.unwrap();
        let _ = rt.ingest(progress_delta("bobby", 5)).await.unwrap();
        rt.flush_log().await.unwrap();

        // Client connects with Last-Event-ID = 1 → expect events
        // 2 and 3 from catch-up, then nothing live.
        let stream = sse_stream(
            rt.clone(),
            Some(1),
            JobFilter::All,
            StreamConfig {
                keepalive_interval: std::time::Duration::from_secs(60),
            },
        );
        tokio::pin!(stream);

        let f2 = stream.next().await.unwrap().unwrap();
        let f3 = stream.next().await.unwrap().unwrap();
        match (&f2, &f3) {
            (StreamFrame::Event(e2), StreamFrame::Event(e3)) => {
                assert_eq!((e2.seq, e3.seq), (2, 3));
            }
            other => panic!("expected catch-up of seq 2 and 3, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn catch_up_then_live_with_no_gaps_no_dupes() {
        let (rt, _clock, _store) = fresh_runtime().await;
        let _ = rt.ingest(job_created("bobby")).await.unwrap();
        let _ = rt.ingest(progress_delta("bobby", 10)).await.unwrap();
        rt.flush_log().await.unwrap();

        let stream = sse_stream(
            rt.clone(),
            Some(0),
            JobFilter::All,
            StreamConfig {
                keepalive_interval: std::time::Duration::from_secs(60),
            },
        );
        tokio::pin!(stream);
        // Wait for the subscribe to happen.
        tokio::task::yield_now().await;
        // Live ingest while catch-up is "in flight" (real-world race).
        let s3 = rt.ingest(progress_delta("bobby", 5)).await.unwrap();
        let s4 = rt.ingest(progress_delta("bobby", 1)).await.unwrap();

        // Last-Event-ID = 0 → catch-up emits 1 and 2 from log, then
        // live emits 3 and 4. No dupes; no missing 3 between catch-up
        // and live (live receiver had buffered them).
        let mut seqs = Vec::new();
        for _ in 0..4 {
            match stream.next().await.unwrap().unwrap() {
                StreamFrame::Event(env) => seqs.push(env.seq),
                other => panic!("unexpected frame: {other:?}"),
            }
        }
        assert_eq!(seqs, vec![1, 2, s3, s4]);
        assert_eq!(seqs, vec![1, 2, 3, 4]);
    }

    // =========================================================
    // Catch-up gap (ledger F18) — catch-up must serve the
    // writer's unflushed in-memory tail, not just flushed
    // chunks. Note these tests deliberately do NOT call
    // rt.flush_log(); the flushed-log variants above
    // (catch_up_emits_log_events_before_live etc.) stay as-is
    // so flushed + unflushed catch-up are both pinned.
    // =========================================================

    async fn next_event_seq(stream: &mut (impl Stream<Item = Result<StreamFrame>> + Unpin)) -> u64 {
        let frame = tokio::time::timeout(std::time::Duration::from_secs(2), stream.next())
            .await
            .expect("stream must yield within 2s — catch-up is dropping events")
            .expect("stream ended unexpectedly")
            .unwrap();
        match frame {
            StreamFrame::Event(env) => env.seq,
            other => panic!("expected an Event frame, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn sse_catchup_includes_unflushed_tail() {
        let (rt, _clock, _store) = fresh_runtime().await;
        // Ingest WITHOUT flushing — all three events live only in
        // the writer's open chunk (max_events_per_chunk = 10).
        let s1 = rt.ingest(job_created("bobby")).await.unwrap();
        let s2 = rt.ingest(progress_delta("bobby", 10)).await.unwrap();
        let s3 = rt.ingest(progress_delta("bobby", 5)).await.unwrap();

        // Client connects with Last-Event-ID = 0 while everything
        // sits unflushed. It must still receive 1..=3, in order,
        // with no gap and no duplicate.
        let stream = sse_stream(
            rt.clone(),
            Some(0),
            JobFilter::All,
            StreamConfig {
                keepalive_interval: std::time::Duration::from_secs(60),
            },
        );
        tokio::pin!(stream);

        let got = [
            next_event_seq(&mut stream).await,
            next_event_seq(&mut stream).await,
            next_event_seq(&mut stream).await,
        ];
        assert_eq!(got, [s1, s2, s3]);

        // No duplicates: the very next frame is the next live event,
        // not a replay of the tail.
        let s4 = rt.ingest(progress_delta("bobby", 1)).await.unwrap();
        assert_eq!(next_event_seq(&mut stream).await, s4);
    }

    #[tokio::test]
    async fn sse_reconnect_mid_buffer_no_gap_no_dup() {
        let (rt, _clock, _store) = fresh_runtime().await;
        // 10 events → exactly one threshold flush
        // (max_events_per_chunk = 10 in cfg_for_tests)…
        rt.ingest(job_created("bobby")).await.unwrap();
        for _ in 0..9 {
            rt.ingest(progress_delta("bobby", 1)).await.unwrap();
        }
        // …then 5 more that stay in the writer buffer.
        for _ in 0..5 {
            rt.ingest(progress_delta("bobby", 1)).await.unwrap();
        }
        assert_eq!(
            rt.buffered_event_count().await,
            5,
            "test setup: tail unflushed"
        );

        // Reconnect mid-buffer: Last-Event-ID = 12 straddles the
        // flushed chunk (1..=10) and the unflushed tail (11..=15).
        let stream = sse_stream(
            rt.clone(),
            Some(12),
            JobFilter::All,
            StreamConfig {
                keepalive_interval: std::time::Duration::from_secs(60),
            },
        );
        tokio::pin!(stream);

        let got = [
            next_event_seq(&mut stream).await,
            next_event_seq(&mut stream).await,
            next_event_seq(&mut stream).await,
        ];
        assert_eq!(got, [13, 14, 15]);

        // Exactly 13..=15 — nothing further is pending.
        let extra =
            tokio::time::timeout(std::time::Duration::from_millis(200), stream.next()).await;
        assert!(extra.is_err(), "no frame may follow seq 15, got {extra:?}",);
    }

    #[tokio::test]
    async fn job_filter_drops_other_jobs_and_cluster_events() {
        let (rt, _clock, _store) = fresh_runtime().await;
        let stream = sse_stream(
            rt.clone(),
            None,
            JobFilter::Job(jid("bobby")),
            StreamConfig {
                keepalive_interval: std::time::Duration::from_secs(60),
            },
        );
        tokio::pin!(stream);
        tokio::task::yield_now().await;

        let _bobby1 = rt.ingest(job_created("bobby")).await.unwrap();
        let _mary = rt.ingest(job_created("mary")).await.unwrap();
        let _worker = rt.ingest(worker_left()).await.unwrap();
        let _bobby2 = rt.ingest(progress_delta("bobby", 10)).await.unwrap();

        // Only the two bobby events should come through.
        let f1 = stream.next().await.unwrap().unwrap();
        let f2 = stream.next().await.unwrap().unwrap();
        match (&f1, &f2) {
            (StreamFrame::Event(e1), StreamFrame::Event(e2)) => {
                assert_eq!(e1.kind.job_id().unwrap(), &jid("bobby"));
                assert_eq!(e2.kind.job_id().unwrap(), &jid("bobby"));
                assert!(matches!(e2.kind, EventKind::ProgressDelta { .. }));
            }
            other => panic!("expected two filtered events, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn lagged_subscriber_yields_resync() {
        let (rt, _clock, _store) = fresh_runtime().await;
        let stream = sse_stream(
            rt.clone(),
            None,
            JobFilter::All,
            StreamConfig {
                keepalive_interval: std::time::Duration::from_secs(60),
            },
        );
        tokio::pin!(stream);
        // Subscribe.
        tokio::task::yield_now().await;

        // Bus capacity is 4 (cfg_for_tests). Burst past it without
        // letting the stream poll — the subscriber's slot fills and
        // overflows.
        for _ in 0..10 {
            rt.ingest(job_created("bobby")).await.unwrap();
        }

        // Drain a few frames. The first non-Event frame should be a
        // Resync. After Resync, live frames resume.
        let mut saw_resync = false;
        let mut saw_event_after_resync = false;
        for _ in 0..12 {
            let f = stream.next().await.unwrap().unwrap();
            match f {
                StreamFrame::Resync { skipped } => {
                    assert!(skipped > 0, "Resync.skipped should be positive");
                    saw_resync = true;
                }
                StreamFrame::Event(_) if saw_resync => {
                    saw_event_after_resync = true;
                    break;
                }
                StreamFrame::Event(_) => {} // pre-overflow events
                StreamFrame::Keepalive => {}
            }
        }
        assert!(saw_resync, "expected a Resync frame after overflow");
        assert!(
            saw_event_after_resync,
            "expected event flow to resume after Resync",
        );
    }

    #[tokio::test]
    async fn keepalive_fires_on_idle() {
        let (rt, _clock, _store) = fresh_runtime().await;
        let stream = sse_stream(
            rt.clone(),
            None,
            JobFilter::All,
            StreamConfig {
                keepalive_interval: std::time::Duration::from_millis(50),
            },
        );
        tokio::pin!(stream);
        // No ingest; the first frame should be a Keepalive.
        let f = stream.next().await.unwrap().unwrap();
        assert!(matches!(f, StreamFrame::Keepalive));
    }

    #[tokio::test]
    async fn runtime_drop_closes_stream() {
        let (rt, _clock, _store) = fresh_runtime().await;
        let stream = sse_stream(
            rt.clone(),
            None,
            JobFilter::All,
            StreamConfig {
                keepalive_interval: std::time::Duration::from_secs(60),
            },
        );
        tokio::pin!(stream);
        tokio::task::yield_now().await;
        drop(rt);
        // The next yield is `None` — the broadcast channel closed.
        assert!(stream.next().await.is_none());
    }
}
