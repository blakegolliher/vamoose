//! Integration tests for the worker-facing REST endpoints
//! (server::worker). Same in-process harness as the read + command
//! endpoint tests.

use http::{Request, StatusCode};
use http_body_util::BodyExt;
use migration_coord::lease::{Identity, LeaseConfig};
use migration_coord::runtime::test_clock::FixedClock;
use migration_coord::runtime::{CoordRuntime, RuntimeConfig};
use migration_coord::schema::{ConfigHash, EventKind, JobId, WorkerId, WorkerState};
use migration_coord::server::{build_router, AppState};
use migration_coord::store::{CoordStore, MemStore};
use serde_json::Value;
use std::sync::Arc;
use tower::ServiceExt;
use uuid::Uuid;

fn me(holder: &str) -> Identity {
    Identity {
        holder_id: holder.into(),
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
        bus_capacity: 16,
        lease_retry_interval: std::time::Duration::from_millis(10),
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

async fn fresh_app() -> (axum::Router, CoordRuntime, Arc<MemStore>) {
    let mem = Arc::new(MemStore::new());
    let store: Arc<dyn CoordStore> = mem.clone();
    let clock = FixedClock::new(
        chrono::DateTime::parse_from_rfc3339("2026-05-29T14:32:00Z")
            .unwrap()
            .with_timezone(&chrono::Utc),
    );
    let rt = CoordRuntime::start(store, clock, me("A"), rt_cfg())
        .await
        .unwrap();
    let router = build_router(AppState::new(rt.clone()));
    (router, rt, mem)
}

async fn post_json(router: axum::Router, uri: &str, body: Value) -> (StatusCode, Value) {
    let body_bytes = serde_json::to_vec(&body).unwrap();
    let req = Request::builder()
        .method("POST")
        .uri(uri)
        .header("content-type", "application/json")
        .body(axum::body::Body::from(body_bytes))
        .unwrap();
    let resp = router.oneshot(req).await.unwrap();
    let status = resp.status();
    let body_bytes = resp.into_body().collect().await.unwrap().to_bytes();
    let json: Value = if body_bytes.is_empty() {
        Value::Null
    } else {
        serde_json::from_slice(&body_bytes).unwrap()
    };
    (status, json)
}

// =============================================================================
// register
// =============================================================================

#[tokio::test]
async fn register_returns_worker_id_and_emits_join_event() {
    let (app, rt, _store) = fresh_app().await;
    rt.ingest(job_created("bobby")).await.unwrap();

    let (status, body) = post_json(
        app,
        "/workers/register",
        serde_json::json!({
            "job_id": "bobby",
            "host": "host-a",
            "pid": 4242,
            "start_time": "2026-05-29T14:31:55Z",
            "version": "0.6.0",
        }),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let worker_id_str = body["worker_id"].as_str().unwrap();
    let worker_id = WorkerId(Uuid::parse_str(worker_id_str).unwrap());

    // Reducer applied WorkerJoined → worker on state, assigned to job.
    let snap = rt.state().await;
    assert!(snap.workers.contains_key(&worker_id));
    let job = snap.jobs[&jid("bobby")].clone();
    assert!(job.assigned_workers.contains(&worker_id));
}

#[tokio::test]
async fn register_404s_on_unknown_job() {
    let (app, _rt, _store) = fresh_app().await;
    let (status, body) = post_json(
        app,
        "/workers/register",
        serde_json::json!({
            "job_id": "ghost",
            "host": "h",
            "pid": 1,
            "start_time": "2026-05-29T14:31:55Z",
            "version": "0.6",
        }),
    )
    .await;
    assert_eq!(status, StatusCode::NOT_FOUND);
    assert_eq!(body["code"], "job_not_found");
}

#[tokio::test]
async fn register_400s_on_bad_job_id() {
    let (app, _rt, _store) = fresh_app().await;
    let (status, body) = post_json(
        app,
        "/workers/register",
        serde_json::json!({
            "job_id": "",
            "host": "h",
            "pid": 1,
            "start_time": "2026-05-29T14:31:55Z",
            "version": "0.6",
        }),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert_eq!(body["code"], "invalid_job_id");
}

// =============================================================================
// register — dedup
// =============================================================================

#[tokio::test]
async fn reregister_same_host_supersedes_prior_worker() {
    let (app, rt, _store) = fresh_app().await;
    rt.ingest(job_created("bobby")).await.unwrap();

    // First register.
    let (s1, b1) = post_json(
        app.clone(),
        "/workers/register",
        serde_json::json!({
            "job_id": "bobby",
            "host": "host-a",
            "pid": 100,
            "start_time": "2026-05-29T14:00:00Z",
            "version": "0.6.0",
        }),
    )
    .await;
    assert_eq!(s1, StatusCode::OK);
    let first = WorkerId(Uuid::parse_str(b1["worker_id"].as_str().unwrap()).unwrap());
    // No prior workers to supersede on the first registration.
    assert!(b1
        .get("superseded")
        .map(|v| v.as_array().map(|a| a.is_empty()).unwrap_or(true))
        .unwrap_or(true));

    // Re-register from same host with a different pid + start_time.
    // Simulates a worker process restart.
    let (s2, b2) = post_json(
        app,
        "/workers/register",
        serde_json::json!({
            "job_id": "bobby",
            "host": "host-a",
            "pid": 200,
            "start_time": "2026-05-29T14:05:00Z",
            "version": "0.6.0",
        }),
    )
    .await;
    assert_eq!(s2, StatusCode::OK);
    let second = WorkerId(Uuid::parse_str(b2["worker_id"].as_str().unwrap()).unwrap());
    assert_ne!(first, second, "re-register must mint a new WorkerId");

    // `superseded` reports the first worker.
    let superseded: Vec<String> = b2["superseded"]
        .as_array()
        .unwrap()
        .iter()
        .map(|v| v.as_str().unwrap().to_string())
        .collect();
    assert_eq!(superseded, vec![first.to_string()]);

    // Prior worker flipped to Disconnected; new worker is Idle.
    let snap = rt.state().await;
    assert_eq!(
        snap.workers[&first].state,
        WorkerState::Disconnected,
        "prior worker must be Disconnected after re-register"
    );
    assert_eq!(
        snap.workers[&first].last_error.as_deref(),
        Some("reregister")
    );
    assert_eq!(snap.workers[&second].state, WorkerState::Idle);
}

#[tokio::test]
async fn reregister_identical_tuple_is_a_no_op_dedup() {
    // Same (host, pid, start_time) means "the same worker process
    // re-asserting itself" — coord should NOT mark anything stale.
    // It still mints a fresh WorkerId (that's the contract for any
    // register call), but no WorkerLeft is emitted.
    let (app, rt, _store) = fresh_app().await;
    rt.ingest(job_created("bobby")).await.unwrap();

    let payload = serde_json::json!({
        "job_id": "bobby",
        "host": "host-a",
        "pid": 100,
        "start_time": "2026-05-29T14:00:00Z",
        "version": "0.6.0",
    });
    let (_, b1) = post_json(app.clone(), "/workers/register", payload.clone()).await;
    let first = WorkerId(Uuid::parse_str(b1["worker_id"].as_str().unwrap()).unwrap());

    let (s2, b2) = post_json(app, "/workers/register", payload).await;
    assert_eq!(s2, StatusCode::OK);

    let superseded = b2["superseded"].as_array().map(|a| a.len()).unwrap_or(0);
    assert_eq!(superseded, 0, "identical tuple must not supersede");

    // First worker stays Idle.
    let snap = rt.state().await;
    assert_eq!(snap.workers[&first].state, WorkerState::Idle);
}

// =============================================================================
// heartbeat
// =============================================================================

async fn register_one(app: &axum::Router, job: &str) -> WorkerId {
    let (status, body) = post_json(
        app.clone(),
        "/workers/register",
        serde_json::json!({
            "job_id": job,
            "host": "h",
            "pid": 1,
            "start_time": "2026-05-29T14:31:55Z",
            "version": "0.6",
        }),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    WorkerId(Uuid::parse_str(body["worker_id"].as_str().unwrap()).unwrap())
}

#[tokio::test]
async fn heartbeat_updates_state_and_returns_control_envelope() {
    let (app, rt, _store) = fresh_app().await;
    rt.ingest(job_created("bobby")).await.unwrap();
    let wid = register_one(&app, "bobby").await;
    let last_seq_before = rt.last_seq().await;

    let (status, body) = post_json(
        app,
        &format!("/workers/{wid}/heartbeat"),
        serde_json::json!({
            "state": "Copying",
            "files_per_sec": 12.5,
            "bytes_per_sec": 1_000_000,
            "errors_per_min": 0.5,
            "inflight_ops": 7,
            "queue_depth": 100,
        }),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    // Response shape: control envelope + last_seq + server_time.
    assert_eq!(body["control"]["mode"], "run");
    assert_eq!(body["last_seq"].as_u64().unwrap(), last_seq_before);
    assert!(body["server_time"].is_string());

    // No event added — heartbeat is intentionally not in the event log.
    assert_eq!(rt.last_seq().await, last_seq_before);

    // State actually updated.
    let snap = rt.state().await;
    let w = &snap.workers[&wid];
    assert_eq!(w.state, WorkerState::Copying);
    assert_eq!(w.inflight_ops, 7);
    assert_eq!(w.queue_depth, 100);
    assert!((w.counters.files_per_sec - 12.5).abs() < 1e-6);
}

#[tokio::test]
async fn heartbeat_control_mode_tracks_job_phase() {
    let (app, rt, _store) = fresh_app().await;
    rt.ingest(job_created("bobby")).await.unwrap();
    let wid = register_one(&app, "bobby").await;

    // Baseline: brand-new job is Planned → ControlMode::Run.
    let (status, body) = post_json(
        app.clone(),
        &format!("/workers/{wid}/heartbeat"),
        serde_json::json!({ "state": "Idle" }),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["control"]["mode"], "run");

    // Pause the job. Next heartbeat must report "pause".
    rt.ingest(EventKind::JobPaused {
        job_id: jid("bobby"),
        reason: "operator".into(),
    })
    .await
    .unwrap();
    let (status, body) = post_json(
        app.clone(),
        &format!("/workers/{wid}/heartbeat"),
        serde_json::json!({ "state": "Idle" }),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["control"]["mode"], "pause");

    // Resume puts it back to "run".
    rt.ingest(EventKind::JobResumed {
        job_id: jid("bobby"),
        reason: "operator".into(),
    })
    .await
    .unwrap();
    let (_, body) = post_json(
        app.clone(),
        &format!("/workers/{wid}/heartbeat"),
        serde_json::json!({ "state": "Idle" }),
    )
    .await;
    assert_eq!(body["control"]["mode"], "run");

    // Cancel is terminal → "cancel".
    rt.ingest(EventKind::JobCancelled {
        job_id: jid("bobby"),
        reason: "operator".into(),
    })
    .await
    .unwrap();
    let (_, body) = post_json(
        app,
        &format!("/workers/{wid}/heartbeat"),
        serde_json::json!({ "state": "Idle" }),
    )
    .await;
    assert_eq!(body["control"]["mode"], "cancel");
}

#[tokio::test]
async fn heartbeat_404s_on_unknown_worker() {
    let (app, _rt, _store) = fresh_app().await;
    let bogus = WorkerId::new();
    let (status, body) = post_json(
        app,
        &format!("/workers/{bogus}/heartbeat"),
        serde_json::json!({ "state": "Idle" }),
    )
    .await;
    assert_eq!(status, StatusCode::NOT_FOUND);
    assert_eq!(body["code"], "worker_not_found");
}

#[tokio::test]
async fn heartbeat_400s_on_bad_worker_id() {
    let (app, _rt, _store) = fresh_app().await;
    let (status, body) = post_json(
        app,
        "/workers/not-a-uuid/heartbeat",
        serde_json::json!({ "state": "Idle" }),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert_eq!(body["code"], "invalid_worker_id");
}

// =============================================================================
// events batch
// =============================================================================

#[tokio::test]
async fn events_batch_ingests_each_event_in_order() {
    let (app, rt, _store) = fresh_app().await;
    rt.ingest(job_created("bobby")).await.unwrap();
    let wid = register_one(&app, "bobby").await;
    let last_seq_before = rt.last_seq().await;

    let body = serde_json::json!({
        "events": [
            {
                "kind": "ProgressDelta",
                "job_id": "bobby",
                "worker_id": wid,
                "files_delta": 5,
                "bytes_delta": 1024,
                "errors_delta": 0,
            },
            {
                "kind": "ProgressDelta",
                "job_id": "bobby",
                "worker_id": wid,
                "files_delta": 3,
                "bytes_delta": 512,
                "errors_delta": 1,
                "worker_at": "2026-05-29T14:31:59Z",
            }
        ]
    });
    let (status, resp) = post_json(app, &format!("/workers/{wid}/events"), body).await;
    assert_eq!(status, StatusCode::OK);
    let seqs: Vec<u64> = resp["seqs"]
        .as_array()
        .unwrap()
        .iter()
        .map(|s| s.as_u64().unwrap())
        .collect();
    assert_eq!(seqs.len(), 2);
    assert_eq!(seqs[0], last_seq_before + 1);
    assert_eq!(seqs[1], last_seq_before + 2);

    // Counters folded into the job's progress.
    let job = rt.job_view(&jid("bobby")).await.unwrap();
    assert_eq!(job.progress.files_done, 8);
    assert_eq!(job.progress.bytes_done, 1536);
}

#[tokio::test]
async fn events_batch_400s_on_bad_worker_id() {
    let (app, _rt, _store) = fresh_app().await;
    let (status, body) = post_json(
        app,
        "/workers/not-a-uuid/events",
        serde_json::json!({ "events": [] }),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert_eq!(body["code"], "invalid_worker_id");
}

// =============================================================================
// fence
// =============================================================================

#[tokio::test]
async fn fence_marks_worker_and_emits_event() {
    let (app, rt, _store) = fresh_app().await;
    rt.ingest(job_created("bobby")).await.unwrap();
    let wid = register_one(&app, "bobby").await;

    let (status, body) = post_json(
        app,
        &format!("/workers/{wid}/fence"),
        serde_json::json!({ "reason": "self-fence R7" }),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert!(body["seq"].as_u64().unwrap() > 0);

    let snap = rt.state().await;
    let w = &snap.workers[&wid];
    assert_eq!(w.state, WorkerState::Fenced);
    assert_eq!(w.fence_reason.as_deref(), Some("self-fence R7"));
}
