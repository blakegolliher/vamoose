//! End-to-end test for `migration-tui` against an in-process coord.
//!
//! Verifies that the REST + SSE client + state reducer converge on
//! the same view of the world the coord has — bootstrap via REST,
//! advance via SSE, and assert the AppState's snapshot matches the
//! coord's after each step.

use chrono::{TimeZone, Utc};
use futures::StreamExt;
use migration_coord::lease::{Identity, LeaseConfig};
use migration_coord::runtime::test_clock::FixedClock;
use migration_coord::runtime::{CoordRuntime, RuntimeConfig};
use migration_coord::schema::{ConfigHash, EventKind, JobId, WorkerId};
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
    }
}

async fn spawn_coord() -> (CoordRuntime, SocketAddr, CancellationToken) {
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
            SseFrame::Keepalive | SseFrame::Resync => {}
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
        match frame {
            SseFrame::Event { envelope, .. } => {
                assert!(app.apply_envelope(&envelope));
                received += 1;
            }
            _ => {}
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
    assert_eq!(job.phase, migration_coord::schema::Phase::Paused);

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
    assert_eq!(job.phase, migration_coord::schema::Phase::Cancelled);

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
