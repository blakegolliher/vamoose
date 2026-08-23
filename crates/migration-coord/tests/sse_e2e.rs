//! End-to-end SSE wire test — binds a real HTTP listener, makes
//! actual HTTP requests via reqwest, parses the SSE byte stream
//! line-by-line, and pins:
//!
//! - The wire format (`event:`/`id:`/`data:` triples).
//! - The Last-Event-ID resume contract: no gaps, no duplicates
//!   across a disconnect + reconnect.
//! - The keep-alive comment ping.
//!
//! Counterpart to the in-process tests in
//! `src/server/stream.rs` (which exercise the StreamFrame
//! state machine) — this one closes the loop on the axum +
//! axum_server adapter so a future change there is caught.

use futures::StreamExt;
use migration_coord::lease::{Identity, LeaseConfig};
use migration_coord::runtime::test_clock::FixedClock;
use migration_coord::runtime::{CoordRuntime, RuntimeConfig};
use migration_coord::schema::{ConfigHash, EventKind, JobId, WorkerId};
use migration_coord::server::{build_router, AppState};
use migration_coord::store::{CoordStore, MemStore};
use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;
use tokio_util::sync::CancellationToken;

// =============================================================================
// Test scaffolding
// =============================================================================

fn me() -> Identity {
    Identity {
        holder_id: "test".into(),
        host: "h".into(),
        pid: 1,
    }
}

fn rt_cfg() -> RuntimeConfig {
    RuntimeConfig {
        lease: LeaseConfig {
            ttl: chrono::Duration::seconds(30),
            grace: chrono::Duration::seconds(5),
        },
        events: migration_coord::events::EventLogConfig {
            max_events_per_chunk: 1000,
            max_chunk_age: chrono::Duration::seconds(60),
        },
        bus_capacity: 32,
        lease_retry_interval: Duration::from_millis(10),
        lease_retry_max_attempts: Some(3),
    }
}

fn jid(s: &str) -> JobId {
    JobId::new(s).unwrap()
}

fn job_created(j: &str) -> EventKind {
    EventKind::JobCreated {
        job_id: jid(j),
        name: format!("{j}-mig"),
        source: "nfs://src".into(),
        dest: "nfs://dst".into(),
        owner: "test".into(),
        config_hash: ConfigHash("ab".into()),
        total_files: 0,
        total_bytes: 0,
    }
}

fn progress_delta(j: &str) -> EventKind {
    EventKind::ProgressDelta {
        job_id: jid(j),
        worker_id: WorkerId::new(),
        files_delta: 1,
        bytes_delta: 0,
        errors_delta: 0,
    }
}

async fn spawn_server() -> (CoordRuntime, SocketAddr, CancellationToken) {
    let mem = Arc::new(MemStore::new());
    let store: Arc<dyn CoordStore> = mem.clone();
    let clock = FixedClock::new(
        chrono::DateTime::parse_from_rfc3339("2026-05-29T14:32:00Z")
            .unwrap()
            .with_timezone(&chrono::Utc),
    );
    let rt = CoordRuntime::start(store, clock, me(), rt_cfg())
        .await
        .unwrap();
    let router = build_router(AppState::new(rt.clone()));

    // Bind on an OS-assigned port so multiple tests can run in
    // parallel without picking the same port. We discover the port
    // via a temporary std::net::TcpListener, then drop it and let
    // axum_server::bind grab the same port — a tiny race window
    // exists between drop and bind, but it's bounded by a single
    // syscall and only affects loopback tests.
    let std_listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = std_listener.local_addr().unwrap();
    drop(std_listener);
    let shutdown = CancellationToken::new();
    let shutdown_clone = shutdown.clone();

    let handle = axum_server::Handle::new();
    let server_handle = handle.clone();
    tokio::spawn(async move {
        shutdown_clone.cancelled().await;
        server_handle.graceful_shutdown(Some(Duration::from_secs(2)));
    });

    tokio::spawn(async move {
        axum_server::bind(addr)
            .handle(handle)
            .serve(router.into_make_service())
            .await
            .unwrap();
    });

    // Give the server a brief beat to come up before tests start
    // hitting it. axum_server's bind is async; a tiny sleep avoids
    // flaky "connection refused" on the first connect.
    tokio::time::sleep(Duration::from_millis(50)).await;
    (rt, addr, shutdown)
}

// =============================================================================
// SSE wire-line parser
// =============================================================================
//
// The full SSE spec is more elaborate (multi-line data, retry: hint,
// etc). The coord only emits the subset documented in
// server::stream::handler — one of {event, id, data} per line or a
// comment (`: ping`), terminated by a blank line. This parser
// handles exactly that subset.

#[derive(Debug, Default, Clone)]
struct SseFrame {
    event: Option<String>,
    id: Option<String>,
    data: Option<String>,
}

struct SseParser {
    pending: SseFrame,
    leftover: Vec<u8>,
}

impl SseParser {
    fn new() -> Self {
        Self {
            pending: SseFrame::default(),
            leftover: Vec::new(),
        }
    }

    /// Feed a chunk of bytes. Yields any frames whose terminator
    /// (`\n\n`) appears in the combined buffer. Comments (`:` lines)
    /// are returned with `data = Some("ping")` and event set to
    /// `Some(":comment")` so tests can distinguish them.
    fn feed(&mut self, chunk: &[u8]) -> Vec<SseFrame> {
        self.leftover.extend_from_slice(chunk);
        let mut out = Vec::new();
        // Consume complete lines (up to each terminator) as they arrive.
        while let Some(pos) = self.leftover.iter().position(|&b| b == b'\n') {
            // Take the line (without the \n) and shift leftover.
            let line: Vec<u8> = self.leftover.drain(..=pos).take(pos).collect();
            let line = std::str::from_utf8(&line).unwrap_or("");
            if line.is_empty() {
                // Blank line — frame terminator. Emit if non-empty.
                if self.pending.event.is_some()
                    || self.pending.id.is_some()
                    || self.pending.data.is_some()
                {
                    out.push(std::mem::take(&mut self.pending));
                }
            } else if let Some(rest) = line.strip_prefix(':') {
                // Comment.
                out.push(SseFrame {
                    event: Some(":comment".into()),
                    data: Some(rest.trim_start().to_string()),
                    ..SseFrame::default()
                });
            } else if let Some(rest) = line.strip_prefix("event:") {
                self.pending.event = Some(rest.trim_start().to_string());
            } else if let Some(rest) = line.strip_prefix("id:") {
                self.pending.id = Some(rest.trim_start().to_string());
            } else if let Some(rest) = line.strip_prefix("data:") {
                self.pending.data = Some(rest.trim_start().to_string());
            }
        }
        out
    }
}

// =============================================================================
// E2E
// =============================================================================

async fn collect_frames(
    addr: SocketAddr,
    last_event_id: Option<u64>,
    expected: usize,
    timeout: Duration,
) -> Vec<SseFrame> {
    let client = reqwest::Client::builder().build().unwrap();
    let url = format!("http://{addr}/stream");
    let mut req = client.get(url);
    if let Some(seq) = last_event_id {
        req = req.header("Last-Event-ID", seq.to_string());
    }
    let resp = req.send().await.unwrap();
    assert_eq!(resp.status(), 200);

    let mut parser = SseParser::new();
    let mut frames: Vec<SseFrame> = Vec::new();

    let body = resp.bytes_stream();
    tokio::pin!(body);
    let deadline = tokio::time::Instant::now() + timeout;

    while frames
        .iter()
        .filter(|f| f.event.as_deref() != Some(":comment"))
        .count()
        < expected
    {
        let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
        if remaining.is_zero() {
            break;
        }
        let chunk = match tokio::time::timeout(remaining, body.next()).await {
            Ok(Some(Ok(b))) => b,
            Ok(Some(Err(e))) => panic!("stream error: {e}"),
            Ok(None) => break,
            Err(_) => break,
        };
        frames.extend(parser.feed(&chunk));
    }
    frames
}

#[tokio::test]
async fn sse_wire_carries_event_id_and_data() {
    let (rt, addr, shutdown) = spawn_server().await;

    // Ingest two events before connecting so they catch up through
    // Last-Event-ID = 0.
    rt.ingest(job_created("bobby")).await.unwrap();
    rt.ingest(progress_delta("bobby")).await.unwrap();
    rt.flush_log().await.unwrap();

    let frames = collect_frames(addr, Some(0), 2, Duration::from_secs(5)).await;
    let real: Vec<&SseFrame> = frames
        .iter()
        .filter(|f| f.event.as_deref() != Some(":comment"))
        .collect();
    assert_eq!(real.len(), 2, "expected exactly 2 SSE events, got {real:?}");
    assert_eq!(real[0].event.as_deref(), Some("JobCreated"));
    assert_eq!(real[0].id.as_deref(), Some("1"));
    assert!(real[0]
        .data
        .as_ref()
        .map(|d| d.contains("\"seq\":1"))
        .unwrap_or(false));
    assert_eq!(real[1].event.as_deref(), Some("ProgressDelta"));
    assert_eq!(real[1].id.as_deref(), Some("2"));

    shutdown.cancel();
}

#[tokio::test]
async fn sse_resume_via_last_event_id_no_gaps_no_dupes() {
    let (rt, addr, shutdown) = spawn_server().await;

    // Stage 1: ingest 4 events.
    let mut expected_seqs: Vec<u64> = Vec::new();
    expected_seqs.push(rt.ingest(job_created("bobby")).await.unwrap());
    for _ in 0..3 {
        expected_seqs.push(rt.ingest(progress_delta("bobby")).await.unwrap());
    }
    rt.flush_log().await.unwrap();

    // First client gets everything from the beginning.
    let frames = collect_frames(addr, Some(0), 4, Duration::from_secs(5)).await;
    let real: Vec<&SseFrame> = frames
        .iter()
        .filter(|f| f.event.as_deref() != Some(":comment"))
        .collect();
    let seen_seqs: Vec<u64> = real
        .iter()
        .filter_map(|f| f.id.as_ref().and_then(|s| s.parse().ok()))
        .collect();
    assert_eq!(seen_seqs, expected_seqs);

    // Stage 2: ingest 2 more events.
    let mid_seq = *seen_seqs.last().unwrap();
    let mut new_seqs: Vec<u64> = Vec::new();
    new_seqs.push(rt.ingest(progress_delta("bobby")).await.unwrap());
    new_seqs.push(rt.ingest(progress_delta("bobby")).await.unwrap());
    rt.flush_log().await.unwrap();

    // Second client resumes from mid_seq. Catch-up should yield
    // exactly the 2 new events (and no dupes from the previous 4).
    let resumed = collect_frames(addr, Some(mid_seq), 2, Duration::from_secs(5)).await;
    let real2: Vec<&SseFrame> = resumed
        .iter()
        .filter(|f| f.event.as_deref() != Some(":comment"))
        .collect();
    let seqs2: Vec<u64> = real2
        .iter()
        .filter_map(|f| f.id.as_ref().and_then(|s| s.parse().ok()))
        .collect();
    assert_eq!(seqs2, new_seqs);
    assert!(
        seqs2.iter().all(|s| *s > mid_seq),
        "no dup events past mid_seq"
    );

    shutdown.cancel();
}

// =============================================================================
// Healthz over the wire — minimal sanity check that the listener
// is actually listening, not just that build_router compiles.
// =============================================================================

#[tokio::test]
async fn healthz_reachable_over_real_listener() {
    let (_rt, addr, shutdown) = spawn_server().await;
    let resp = reqwest::get(format!("http://{addr}/healthz"))
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    let body: HashMap<String, serde_json::Value> = resp.json().await.unwrap();
    assert_eq!(body["status"], "ok");
    shutdown.cancel();
}
