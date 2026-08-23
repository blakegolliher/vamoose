//! End-to-end test for `migration-tui` against an in-process coord.
//!
//! Verifies that the REST + SSE client + state reducer converge on
//! the same view of the world the coord has — bootstrap via REST,
//! advance via SSE, and assert the AppState's snapshot matches the
//! coord's after each step.

use chrono::{TimeZone, Utc};
use futures::StreamExt;
use migration_control_protocol::schema::{ConfigHash, EventKind, JobId, WorkerId};
use migration_coord::lease::{Identity, LeaseConfig};
use migration_coord::runtime::test_clock::FixedClock;
use migration_coord::runtime::{CoordRuntime, RuntimeConfig};
use migration_coord::server::auth::AuthConfig;
use migration_coord::server::{build_router, AppState as ServerAppState};
use migration_coord::store::{CoordStore, MemStore};
use migration_tui::client::{Client, SseFrame};
use migration_tui::state::{AppState, ConnectionStatus};
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;
use tokio_util::sync::CancellationToken;

// =============================================================================
// Scaffolding — mirrors the other in-process coord harnesses.
// =============================================================================

fn me() -> Identity {
    Identity {
        holder_id: "tui-test".into(),
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

async fn spawn_coord() -> (CoordRuntime, SocketAddr, CancellationToken) {
    spawn_coord_cfg(rt_cfg()).await
}

async fn spawn_coord_cfg(cfg: RuntimeConfig) -> (CoordRuntime, SocketAddr, CancellationToken) {
    let mem = Arc::new(MemStore::new());
    let store: Arc<dyn CoordStore> = mem.clone();
    let clock = FixedClock::new(
        chrono::DateTime::parse_from_rfc3339("2026-05-29T14:32:00Z")
            .unwrap()
            .with_timezone(&chrono::Utc),
    );
    let rt = CoordRuntime::start(store, clock, me(), cfg).await.unwrap();
    let router = build_router(ServerAppState::with_auth(rt.clone(), AuthConfig::default()));

    let std_listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = std_listener.local_addr().unwrap();
    drop(std_listener);
    let cancel = CancellationToken::new();
    let sc = cancel.clone();
    let handle = axum_server::Handle::new();
    let sh = handle.clone();
    tokio::spawn(async move {
        sc.cancelled().await;
        sh.graceful_shutdown(Some(Duration::from_secs(2)));
    });
    tokio::spawn(async move {
        axum_server::bind(addr)
            .handle(handle)
            .serve(router.into_make_service())
            .await
            .unwrap();
    });
    tokio::time::sleep(Duration::from_millis(50)).await;
    (rt, addr, cancel)
}

fn client_for(addr: SocketAddr) -> Client {
    Client::new(
        format!("http://{addr}"),
        None, // no admin token (coord runs in dev mode)
        true, // verify_tls irrelevant for http
        Duration::from_secs(5),
    )
    .expect("Client::new")
}

// =============================================================================
// Tests
// =============================================================================

#[tokio::test]
async fn healthz_round_trip() {
    let (_rt, addr, shutdown) = spawn_coord().await;
    let client = client_for(addr);
    let h = client.healthz().await.expect("healthz");
    assert_eq!(h.status, "ok");
    assert_eq!(h.last_seq, 0);
    assert!(!h.lease_lost);
    shutdown.cancel();
}

#[tokio::test]
async fn get_jobs_returns_ingested_state() {
    let (rt, addr, shutdown) = spawn_coord().await;
    rt.ingest(job_created("alpha")).await.unwrap();
    rt.ingest(job_created("bravo")).await.unwrap();

    let client = client_for(addr);
    let resp = client.get_jobs(None, None).await.expect("get_jobs");
    let names: Vec<String> = resp.jobs.iter().map(|j| j.name.clone()).collect();
    assert!(names.contains(&"alpha-mig".to_string()));
    assert!(names.contains(&"bravo-mig".to_string()));
    assert_eq!(resp.jobs.len(), 2);
    shutdown.cancel();
}

#[tokio::test]
async fn sse_catches_up_then_streams_live() {
    let (rt, addr, shutdown) = spawn_coord().await;
    // Stage 1: ingest 2 events BEFORE the client connects.
    rt.ingest(job_created("alpha")).await.unwrap();
    rt.ingest(EventKind::ProgressDelta {
        job_id: jid("alpha"),
        worker_id: WorkerId::new(),
        files_delta: 5,
        bytes_delta: 1024,
        errors_delta: 0,
    })
    .await
    .unwrap();
    rt.flush_log().await.unwrap();

    // Stage 2: client connects with Last-Event-ID: 0 to catch up.
    let client = client_for(addr);
    let stream = client.stream(Some(0), None).await.expect("stream");
    tokio::pin!(stream);

    let mut app = AppState::empty(Utc.timestamp_opt(0, 0).unwrap());

    // Drain the first 2 catch-up frames.
    let mut received = 0;
    while received < 2 {
        let frame = tokio::time::timeout(Duration::from_secs(5), stream.next())
            .await
            .expect("timeout")
            .expect("stream not closed")
            .expect("ok");
        match frame {
            SseFrame::Event { envelope, .. } => {
                assert!(app.apply_envelope(&envelope));
                received += 1;
            }
            SseFrame::Keepalive | SseFrame::Resync | SseFrame::UnknownEvent { .. } => {}
        }
    }
    assert_eq!(app.last_seq(), 2);
    let alpha = app.job(&jid("alpha")).expect("alpha present");
    assert_eq!(alpha.progress.files_done, 5);
    assert_eq!(alpha.progress.bytes_done, 1024);

    // Stage 3: ingest more live events, verify they stream through.
    rt.ingest(EventKind::ProgressDelta {
        job_id: jid("alpha"),
        worker_id: WorkerId::new(),
        files_delta: 3,
        bytes_delta: 2048,
        errors_delta: 1,
    })
    .await
    .unwrap();

    let frame = tokio::time::timeout(Duration::from_secs(5), stream.next())
        .await
        .expect("live event arrives")
        .expect("stream open")
        .expect("ok");
    let SseFrame::Event { envelope, .. } = frame else {
        panic!("expected Event, got {frame:?}");
    };
    assert!(app.apply_envelope(&envelope));
    assert_eq!(app.last_seq(), 3);
    let alpha = app.job(&jid("alpha")).unwrap();
    assert_eq!(alpha.progress.files_done, 8);
    assert_eq!(alpha.progress.bytes_done, 1024 + 2048);
    assert_eq!(alpha.progress.errors_total, 1);

    shutdown.cancel();
}

#[tokio::test]
async fn appstate_after_replay_matches_coord_view() {
    // Full bootstrap dance: ingest a varied event sequence, replay
    // through SSE with Last-Event-ID: 0, and assert every job /
    // worker the coord sees is present in the TUI state with the
    // same fields.
    let (rt, addr, shutdown) = spawn_coord().await;
    rt.ingest(job_created("alpha")).await.unwrap();
    let w_alpha = WorkerId::new();
    rt.ingest(EventKind::WorkerJoined {
        worker_id: w_alpha,
        job_id: jid("alpha"),
        host: "host-a".into(),
        pid: 100,
        start_time: Utc.timestamp_opt(1_700_000_000, 0).unwrap(),
        version: "0.6.0".into(),
    })
    .await
    .unwrap();
    rt.ingest(EventKind::ProgressDelta {
        job_id: jid("alpha"),
        worker_id: w_alpha,
        files_delta: 10,
        bytes_delta: 4096,
        errors_delta: 0,
    })
    .await
    .unwrap();
    rt.ingest(job_created("bravo")).await.unwrap();
    rt.ingest(EventKind::JobPaused {
        job_id: jid("alpha"),
        reason: "operator".into(),
    })
    .await
    .unwrap();
    rt.flush_log().await.unwrap();

    let client = client_for(addr);
    let stream = client.stream(Some(0), None).await.expect("stream");
    tokio::pin!(stream);
    let mut app = AppState::empty(Utc.timestamp_opt(0, 0).unwrap());
    app.mark_connected(Utc.timestamp_opt(0, 0).unwrap());

    // Drain 5 events (the 5 we ingested).
    let mut received = 0;
    while received < 5 {
        let frame = tokio::time::timeout(Duration::from_secs(5), stream.next())
            .await
            .expect("timeout")
            .expect("not closed")
            .expect("ok");
        if let SseFrame::Event { envelope, .. } = frame {
            assert!(app.apply_envelope(&envelope));
            received += 1;
        }
    }

    // Compare TUI state vs coord state — same shape for the fields
    // the TUI cares about.
    let coord_snap = rt.state().await;
    assert_eq!(app.snapshot.last_seq, coord_snap.last_seq);
    assert_eq!(app.snapshot.jobs.len(), coord_snap.jobs.len());
    for (id, coord_job) in &coord_snap.jobs {
        let tui_job = app.job(id).expect("TUI has job");
        assert_eq!(tui_job.phase, coord_job.phase, "phase for {id}");
        assert_eq!(tui_job.progress, coord_job.progress, "progress for {id}");
        assert_eq!(
            tui_job.assigned_workers, coord_job.assigned_workers,
            "assigned_workers for {id}"
        );
    }
    for (id, coord_worker) in &coord_snap.workers {
        let tui_worker = app.worker(id).expect("TUI has worker");
        assert_eq!(tui_worker.host, coord_worker.host);
        assert_eq!(tui_worker.pid, coord_worker.pid);
        assert_eq!(tui_worker.job_id, coord_worker.job_id);
        assert_eq!(tui_worker.start_time, coord_worker.start_time);
    }

    shutdown.cancel();
}

#[tokio::test]
async fn appstate_drops_duplicate_envelopes_observed_twice() {
    // If the SSE catch-up overlaps with a stale frame from a prior
    // connection (or if we set Last-Event-ID too low on reconnect),
    // apply_envelope must dedup by seq.
    let (rt, addr, shutdown) = spawn_coord().await;
    rt.ingest(job_created("alpha")).await.unwrap();
    rt.ingest(EventKind::ProgressDelta {
        job_id: jid("alpha"),
        worker_id: WorkerId::new(),
        files_delta: 5,
        bytes_delta: 100,
        errors_delta: 0,
    })
    .await
    .unwrap();
    rt.flush_log().await.unwrap();

    let client = client_for(addr);
    let stream = client.stream(Some(0), None).await.expect("stream");
    tokio::pin!(stream);

    let mut app = AppState::empty(Utc.timestamp_opt(0, 0).unwrap());
    // Drain 2 events on the first pass.
    let mut received = 0;
    while received < 2 {
        let frame = tokio::time::timeout(Duration::from_secs(5), stream.next())
            .await
            .expect("timeout")
            .expect("not closed")
            .expect("ok");
        if let SseFrame::Event { envelope, .. } = frame {
            assert!(app.apply_envelope(&envelope));
            received += 1;
        }
    }
    let before = app.job(&jid("alpha")).unwrap().progress.files_done;

    // Now SIMULATE a reconnect with Last-Event-ID: 0 — should
    // replay both events; apply_envelope drops them as stale.
    let stream2 = client.stream(Some(0), None).await.expect("stream2");
    tokio::pin!(stream2);
    let mut dups = 0;
    let mut frames_seen = 0;
    while frames_seen < 2 {
        let frame = tokio::time::timeout(Duration::from_secs(5), stream2.next())
            .await
            .expect("timeout")
            .expect("not closed")
            .expect("ok");
        if let SseFrame::Event { envelope, .. } = frame {
            if !app.apply_envelope(&envelope) {
                dups += 1;
            }
            frames_seen += 1;
        }
    }
    assert_eq!(dups, 2, "both replayed events must be deduped");
    // Files count unchanged.
    let after = app.job(&jid("alpha")).unwrap().progress.files_done;
    assert_eq!(before, after);

    shutdown.cancel();
}

#[tokio::test]
async fn workers_view_round_trips_through_rest() {
    let (rt, addr, shutdown) = spawn_coord().await;
    rt.ingest(job_created("alpha")).await.unwrap();
    let w = WorkerId::new();
    rt.ingest(EventKind::WorkerJoined {
        worker_id: w,
        job_id: jid("alpha"),
        host: "host-a".into(),
        pid: 42,
        start_time: Utc.timestamp_opt(1_700_000_000, 0).unwrap(),
        version: "0.6.0".into(),
    })
    .await
    .unwrap();

    let client = client_for(addr);
    let resp = client.get_workers("alpha").await.expect("get_workers");
    assert_eq!(resp.workers.len(), 1);
    assert_eq!(resp.workers[0].id, w);
    assert_eq!(resp.workers[0].host, "host-a");

    shutdown.cancel();
}

#[tokio::test]
async fn unknown_job_returns_404() {
    let (_rt, addr, shutdown) = spawn_coord().await;
    let client = client_for(addr);
    let err = client.get_job("ghost").await.expect_err("must 404");
    let migration_tui::client::ClientError::Http { status, .. } = err else {
        panic!("expected Http error");
    };
    assert_eq!(status, reqwest::StatusCode::NOT_FOUND);
    shutdown.cancel();
}

#[tokio::test]
async fn marker_mark_connected_then_event_keeps_connected() {
    // Sanity: connection status is independent of the reducer. A
    // mark_connected followed by event application leaves the
    // state Connected.
    let (rt, addr, shutdown) = spawn_coord().await;
    rt.ingest(job_created("alpha")).await.unwrap();
    rt.flush_log().await.unwrap();
    let client = client_for(addr);
    let stream = client.stream(Some(0), None).await.expect("stream");
    tokio::pin!(stream);

    let mut app = AppState::empty(Utc.timestamp_opt(0, 0).unwrap());
    app.mark_connected(Utc.timestamp_opt(100, 0).unwrap());
    assert!(matches!(app.connection, ConnectionStatus::Connected { .. }));

    // Drain one event.
    while let Some(frame) = tokio::time::timeout(Duration::from_secs(5), stream.next())
        .await
        .expect("timeout")
    {
        let frame = frame.expect("ok");
        if let SseFrame::Event { envelope, .. } = frame {
            app.apply_envelope(&envelope);
            break;
        }
    }
    assert!(matches!(app.connection, ConnectionStatus::Connected { .. }));

    shutdown.cancel();
}

#[tokio::test]
async fn command_methods_round_trip_through_coord() {
    let (rt, addr, shutdown) = spawn_coord().await;
    rt.ingest(job_created("alpha")).await.unwrap();
    let client = client_for(addr);

    // pause → coord acks with a command_id and emits JobPaused.
    let r = client.pause("alpha", Some("smoke")).await.expect("pause");
    assert!(!r.command_id.is_empty());
    let job = rt.job_view(&jid("alpha")).await.unwrap();
    assert_eq!(job.phase, migration_control_protocol::schema::Phase::Paused);

    // resume.
    let r = client.resume("alpha", None).await.expect("resume");
    assert!(!r.command_id.is_empty());

    // drain (currently routes through JobPaused per Phase 2 caveat).
    let r = client
        .drain("alpha", Some("end of shift"))
        .await
        .expect("drain");
    assert!(!r.command_id.is_empty());

    // resume again so we can cancel cleanly.
    client.resume("alpha", None).await.expect("resume");

    // cancel → terminal.
    let r = client
        .cancel("alpha", Some("operator"))
        .await
        .expect("cancel");
    assert!(!r.command_id.is_empty());
    let job = rt.job_view(&jid("alpha")).await.unwrap();
    assert_eq!(
        job.phase,
        migration_control_protocol::schema::Phase::Cancelled
    );

    // retry-failed is audit-only today; should still 200.
    let r = client
        .retry_failed("alpha", None)
        .await
        .expect("retry-failed");
    assert!(!r.command_id.is_empty());

    shutdown.cancel();
}

#[tokio::test]
async fn unknown_job_command_returns_404() {
    let (_rt, addr, shutdown) = spawn_coord().await;
    let client = client_for(addr);
    let err = client.pause("ghost", None).await.expect_err("must 404");
    let migration_tui::client::ClientError::Http { status, .. } = err else {
        panic!("expected Http error");
    };
    assert_eq!(status, reqwest::StatusCode::NOT_FOUND);
    shutdown.cancel();
}

// =============================================================================
// F26 — REST bootstrap + real Resync recovery
// =============================================================================

fn progress(job: &str, worker: WorkerId, files: u64, bytes: u64) -> EventKind {
    EventKind::ProgressDelta {
        job_id: jid(job),
        worker_id: worker,
        files_delta: files,
        bytes_delta: bytes,
        errors_delta: 0,
    }
}

/// Fast reconnects so the driver-level tests don't sit in backoff.
fn fast_opts() -> migration_tui::app::RunOpts {
    migration_tui::app::RunOpts {
        render_tick: Duration::from_millis(50),
        reconnect_initial: Duration::from_millis(50),
        reconnect_max: Duration::from_millis(500),
    }
}

/// Mimic the event loop: receive one driver input (bounded by
/// `timeout`) and fold it into `state` through the real reducer.
/// Returns the input's Debug form so callers can classify what
/// arrived (the reducer consumes the input itself).
async fn pump_one(
    rx: &mut tokio::sync::mpsc::Receiver<migration_tui::app::Input>,
    state: &Arc<tokio::sync::Mutex<AppState>>,
    timeout: Duration,
) -> Option<String> {
    let input = tokio::time::timeout(timeout, rx.recv())
        .await
        .ok()
        .flatten()?;
    let tag = format!("{input:?}");
    let mut s = state.lock().await;
    migration_tui::app::handle_input(&mut s, input, Utc::now());
    Some(tag)
}

#[tokio::test(flavor = "multi_thread")]
async fn bootstrap_fetches_jobs_then_streams_from_last_seq() {
    // F26 + F23: a terminal job whose event chunks were archived
    // (moved to archivelogs/ and DELETED from events/) exists in the
    // coord's /jobs view but can never appear in a seq-0 SSE replay.
    // The TUI must bootstrap over REST and open the stream from the
    // healthz cursor, not 0.
    let (rt, addr, shutdown) = spawn_coord().await;
    let w = WorkerId::new();
    rt.ingest(job_created("alpha")).await.unwrap();
    rt.ingest(progress("alpha", w, 7, 2048)).await.unwrap();
    rt.ingest(EventKind::JobCompleted {
        job_id: jid("alpha"),
    })
    .await
    .unwrap();
    rt.flush_log().await.unwrap();
    rt.write_snapshot(3).await.unwrap();
    let archived = rt.archive_terminal_jobs().await.unwrap();
    assert!(
        archived.iter().any(|(id, _)| *id == jid("alpha")),
        "harness: alpha's chunks must actually archive (the F23 path)"
    );
    // Live job ingested after the archive pass.
    rt.ingest(job_created("bravo")).await.unwrap();
    rt.ingest(progress("bravo", w, 1, 100)).await.unwrap();
    rt.flush_log().await.unwrap();
    let coord_seq = rt.last_seq().await;

    let client = client_for(addr);
    let now = Utc.timestamp_opt(0, 0).unwrap();
    let state = Arc::new(tokio::sync::Mutex::new(AppState::empty(now)));
    let (tx, mut rx) = tokio::sync::mpsc::channel(64);
    let cancel = CancellationToken::new();
    let driver = tokio::spawn(migration_tui::app::sse_driver(
        client,
        Arc::clone(&state),
        tx,
        cancel.clone(),
        fast_opts(),
    ));

    // Pump the reducer until both jobs are present and the link is
    // up, recording every replayed Event frame's debug tag.
    let mut replayed: Vec<String> = Vec::new();
    let deadline = tokio::time::Instant::now() + Duration::from_secs(60);
    loop {
        assert!(
            tokio::time::Instant::now() < deadline,
            "timed out waiting for bootstrap; replayed so far: {replayed:?}"
        );
        if let Some(tag) = pump_one(&mut rx, &state, Duration::from_secs(5)).await {
            if tag.starts_with("SseFrame(Event") {
                replayed.push(tag);
            }
        }
        let s = state.lock().await;
        if s.job(&jid("alpha")).is_some()
            && s.job(&jid("bravo")).is_some()
            && matches!(s.connection, ConnectionStatus::Connected { .. })
        {
            break;
        }
    }

    {
        let s = state.lock().await;
        // The archived job's state came from REST — SSE can't replay
        // it, its chunks are gone from events/.
        let alpha = s.job(&jid("alpha")).unwrap();
        assert_eq!(
            alpha.phase,
            migration_control_protocol::schema::Phase::Completed
        );
        assert_eq!(alpha.progress.files_done, 7);
        assert_eq!(alpha.progress.bytes_done, 2048);
        // Cursor pinned by proxy (per the work item): a stream opened
        // at healthz.last_seq replays none of the live job's early
        // events; seq-0 would have replayed bravo's two.
        assert!(
            replayed.is_empty(),
            "stream must resume from the bootstrap cursor, not 0; replayed: {replayed:?}"
        );
        assert_eq!(s.last_seq(), coord_seq);
    }

    // The live tail still flows after bootstrap (cursor not too high
    // either): one more event must arrive over SSE. A fresh worker id:
    // the F24 stream caps coalesce ProgressDelta to 1 Hz per
    // (job, worker), so a same-key delta inside the cap window is
    // (correctly) dropped from the bus and would never arrive live.
    let w_live = WorkerId::new();
    rt.ingest(progress("bravo", w_live, 2, 50)).await.unwrap();
    let deadline = tokio::time::Instant::now() + Duration::from_secs(60);
    loop {
        assert!(
            tokio::time::Instant::now() < deadline,
            "live event after bootstrap never arrived"
        );
        pump_one(&mut rx, &state, Duration::from_secs(5)).await;
        let s = state.lock().await;
        if s.job(&jid("bravo")).map(|j| j.progress.files_done) == Some(3) {
            break;
        }
    }

    cancel.cancel();
    drop(rx);
    let _ = driver.await;
    shutdown.cancel();
}

#[tokio::test(flavor = "multi_thread")]
async fn resync_refetches_snapshot_and_resumes() {
    // F26: when the coord's per-subscriber bus overflows it emits
    // Resync and drops the tail on the floor (the skipped
    // ProgressDelta/ErrorEmitted are gone from the stream forever).
    // The driver must drop the stream, re-fetch the REST snapshot,
    // apply it via replace_snapshot, and resume from the new cursor
    // — leaving the client's counters EXACTLY equal to the coord's.
    let mut cfg = rt_cfg();
    cfg.bus_capacity = 4; // same tiny bus the coord's stream unit tests use
    let (rt, addr, shutdown) = spawn_coord_cfg(cfg).await;
    let w = WorkerId::new();
    rt.ingest(job_created("alpha")).await.unwrap();
    rt.ingest(progress("alpha", w, 5, 1024)).await.unwrap();
    rt.flush_log().await.unwrap();

    let client = client_for(addr);
    let now = Utc.timestamp_opt(0, 0).unwrap();
    let state = Arc::new(tokio::sync::Mutex::new(AppState::empty(now)));
    // Tiny input channel: once we stop pumping, the driver blocks on
    // send, stops reading its socket, TCP buffers fill, and the
    // coord-side bus (capacity 4) overflows.
    let (tx, mut rx) = tokio::sync::mpsc::channel(4);
    let cancel = CancellationToken::new();
    let driver = tokio::spawn(migration_tui::app::sse_driver(
        client,
        Arc::clone(&state),
        tx,
        cancel.clone(),
        fast_opts(),
    ));

    // Phase 1: pump until the initial view is synced.
    let deadline = tokio::time::Instant::now() + Duration::from_secs(60);
    loop {
        assert!(
            tokio::time::Instant::now() < deadline,
            "timed out waiting for initial sync"
        );
        pump_one(&mut rx, &state, Duration::from_secs(5)).await;
        let s = state.lock().await;
        if s.job(&jid("alpha")).map(|j| j.progress.files_done) == Some(5)
            && matches!(s.connection, ConnectionStatus::Connected { .. })
        {
            break;
        }
    }

    // Phase 2: stall the consumer and flood the bus with events big
    // enough to overrun the socket buffering between coord and
    // client. These are the events the old code lost forever.
    //
    // Two floods. The ErrorEmitted burst exercises error-bucket
    // divergence, but the F24 bus caps admit only ~10 of it per
    // class per second, which left the forced overflow dependent on
    // socket-buffer timing (passed solo in 0.1s, failed under
    // full-workspace load — see LESSONS.md "Outbound caps starve
    // overflow-forcing harnesses"). The VerifyFileMismatch burst is
    // the deterministic overflow: uncapped by StreamCaps and a
    // reducer no-op, so 80 big frames must queue behind the blocked
    // SSE writer and lag the 4-slot bus regardless of scheduling.
    let big = "x".repeat(128 * 1024);
    for i in 0..80u32 {
        rt.ingest(EventKind::ErrorEmitted {
            job_id: jid("alpha"),
            worker_id: w,
            class: migration_control_protocol::schema::ErrorClass::Timeout,
            path: format!("/p/{i}"),
            retryable: true,
            message: big.clone(),
        })
        .await
        .unwrap();
    }
    for i in 0..80u32 {
        rt.ingest(EventKind::VerifyFileMismatch {
            job_id: jid("alpha"),
            path: format!("/v/{i}"),
            expected: big.clone(),
            got: big.clone(),
        })
        .await
        .unwrap();
    }
    for _ in 0..5 {
        rt.ingest(progress("alpha", w, 10, 4096)).await.unwrap();
    }

    // Phase 3: resume pumping. Require that a Resync actually fired
    // (otherwise the harness proved nothing) and that the client
    // converges on the coord's exact derived state.
    let mut saw_resync = false;
    let mut converged = false;
    let mut last_tags: Vec<String> = Vec::new();
    let deadline = tokio::time::Instant::now() + Duration::from_secs(90);
    while tokio::time::Instant::now() < deadline {
        if let Some(tag) = pump_one(&mut rx, &state, Duration::from_secs(2)).await {
            if tag.contains("Resync") {
                saw_resync = true;
            }
            // Keep a short tail of reducer inputs for the failure
            // message — enough to see a wedge (Fatal? Disconnected
            // loop? nothing at all?) straight from a CI log.
            last_tags.push(tag.chars().take(90).collect());
            if last_tags.len() > 12 {
                last_tags.remove(0);
            }
        }
        let s = state.lock().await;
        let coord = rt.state().await;
        if s.snapshot.jobs == coord.jobs
            && s.snapshot.workers == coord.workers
            && s.snapshot.error_buckets == coord.error_buckets
            && s.last_seq() >= coord.last_seq
        {
            converged = true;
            break;
        }
    }
    assert!(
        saw_resync,
        "harness failed to force a bus overflow — Resync never reached the reducer"
    );
    if !converged {
        let s = state.lock().await;
        let coord = rt.state().await;
        panic!(
            "client never converged with the coord after Resync — the \
             overflow-dropped counters were lost (the F26 desync).\n\
             connection={:?} driver_finished={} client_seq={} coord_seq={}\n\
             jobs_eq={} workers_eq={} buckets_eq={}\n\
             recent reducer inputs: {:#?}",
            s.connection,
            driver.is_finished(),
            s.last_seq(),
            coord.last_seq,
            s.snapshot.jobs == coord.jobs,
            s.snapshot.workers == coord.workers,
            s.snapshot.error_buckets == coord.error_buckets,
            last_tags,
        );
    }

    cancel.cancel();
    drop(rx);
    let _ = driver.await;
    shutdown.cancel();
}

// =============================================================================
// F38 — unknown EventKind tolerance
// =============================================================================

/// Render one envelope the way the coord's SSE layer does:
/// `event:` = the kind tag, `id:` = seq, `data:` = the envelope JSON.
fn sse_frame_for(env: &migration_control_protocol::schema::EventEnvelope) -> String {
    format!(
        "event:{}\nid:{}\ndata:{}\n\n",
        env.kind.name(),
        env.seq,
        serde_json::to_string(env).expect("envelope serializes")
    )
}

#[tokio::test]
async fn unknown_kind_frame_interleaved_keeps_client_in_sync() {
    // F38 end-to-end: real coord events plus a synthetic frame from a
    // "future coord" (an EventKind this build has no variant for),
    // driven through the same parser + reducer path the live TUI
    // uses. The live coord can't emit an unknown kind — its schema is
    // closed — so the raw SSE body is assembled from the coord's own
    // envelopes with the future frame appended, per the work item.
    use migration_tui::app::{handle_input, Input};

    let (rt, addr, shutdown) = spawn_coord().await;
    rt.ingest(job_created("alpha")).await.unwrap();
    rt.ingest(EventKind::ProgressDelta {
        job_id: jid("alpha"),
        worker_id: WorkerId::new(),
        files_delta: 5,
        bytes_delta: 1024,
        errors_delta: 0,
    })
    .await
    .unwrap();
    rt.flush_log().await.unwrap();

    let client = client_for(addr);
    let evs = client.get_events("alpha", 0).await.expect("events").events;
    assert_eq!(evs.len(), 2, "harness expects the two ingested events");

    let mut body = String::new();
    for env in &evs {
        body.push_str(&sse_frame_for(env));
    }
    // The future coord's next event — seq 3, a kind we don't know.
    body.push_str("event:ShardRebalanced\nid:3\ndata:{\"kind\":\"ShardRebalanced\",\"seq\":3}\n\n");
    // A keepalive after it proves the stream keeps flowing.
    body.push_str(": ping\n\n");

    let byte_stream = futures::stream::iter(vec![std::result::Result::<
        bytes::Bytes,
        reqwest::Error,
    >::Ok(bytes::Bytes::from(body))]);
    let stream = migration_tui::client::parse_sse_stream(byte_stream);
    tokio::pin!(stream);

    let now = Utc.timestamp_opt(50, 0).unwrap();
    let mut app = AppState::empty(now);
    app.mark_connected(now);
    let mut frames = 0usize;
    while let Some(frame) = stream.next().await {
        let frame = frame.expect(
            "no frame may surface as Err — an Err disconnects the driver \
             and replays the same frame forever (the F38 reconnect storm)",
        );
        handle_input(&mut app, Input::SseFrame(frame), now);
        frames += 1;
    }
    assert_eq!(frames, 4, "2 events + unknown + keepalive all yielded");

    // Client ends in sync with the coord ...
    let coord_snap = rt.state().await;
    assert_eq!(app.snapshot.jobs.len(), coord_snap.jobs.len());
    let alpha = app.job(&jid("alpha")).expect("alpha present");
    let coord_alpha = coord_snap.jobs.get(&jid("alpha")).unwrap();
    assert_eq!(alpha.progress, coord_alpha.progress);
    // ... with the cursor past the unknown frame, the counter bumped,
    // and the connection never torn down.
    assert_eq!(app.last_seq(), 3, "resume cursor is past the unknown frame");
    assert_eq!(app.unknown_events, 1);
    assert!(matches!(app.connection, ConnectionStatus::Connected { .. }));

    shutdown.cancel();
}

#[tokio::test]
async fn sse_stream_survives_past_rest_request_timeout() {
    // Regression for the smoke-test report "connected goes to
    // unconnected frequently". The fix split the reqwest client in
    // two: REST keeps a per-request timeout, but the SSE stream
    // uses a separate client with NO request timeout. Without
    // the fix, this test would fail after `request_timeout` —
    // the stream would error out and the second frame would never
    // arrive.
    let (rt, addr, shutdown) = spawn_coord().await;
    rt.ingest(job_created("alpha")).await.unwrap();
    rt.flush_log().await.unwrap();

    // Deliberately short REST timeout — well under the gap
    // between the two events we feed in.
    let client = Client::new(
        format!("http://{addr}"),
        None,
        true,
        Duration::from_millis(500),
    )
    .expect("Client::new");

    let stream = client.stream(Some(0), None).await.expect("stream");
    tokio::pin!(stream);

    // Drain the first catch-up event.
    let first = tokio::time::timeout(Duration::from_secs(5), stream.next())
        .await
        .expect("first frame")
        .expect("not closed")
        .expect("ok");
    assert!(matches!(first, SseFrame::Event { .. }));

    // Idle for longer than the REST timeout. If the stream client
    // were sharing the REST client's timeout, the connection
    // would be killed during this sleep and the next stream.next()
    // would yield an error instead of a fresh event.
    tokio::time::sleep(Duration::from_millis(750)).await;

    // Now ingest another event and verify the same stream still
    // delivers it without disconnecting.
    rt.ingest(EventKind::ProgressDelta {
        job_id: jid("alpha"),
        worker_id: WorkerId::new(),
        files_delta: 1,
        bytes_delta: 100,
        errors_delta: 0,
    })
    .await
    .unwrap();

    let next = tokio::time::timeout(Duration::from_secs(5), stream.next())
        .await
        .expect("second frame within 5s of ingest")
        .expect("stream still open")
        .expect("frame ok");
    assert!(
        matches!(next, SseFrame::Event { .. } | SseFrame::Keepalive),
        "expected Event or Keepalive, got {next:?}"
    );

    shutdown.cancel();
}
