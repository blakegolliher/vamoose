//! Integration tests for the worker-facing REST endpoints
//! (server::worker). Same in-process harness as the read + command
//! endpoint tests.

use http::{Request, StatusCode};
use http_body_util::BodyExt;
use migration_coord::lease::{Identity, LeaseConfig};
use migration_coord::runtime::test_clock::FixedClock;
use migration_coord::runtime::{CoordRuntime, RuntimeConfig};
use migration_coord::schema::{
    ConfigHash, ErrorClass, EventKind, JobId, Phase, ShardId, WorkerId, WorkerState,
};
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

async fn fresh_app_with_clock() -> (axum::Router, CoordRuntime, Arc<MemStore>, Arc<FixedClock>) {
    let mem = Arc::new(MemStore::new());
    let store: Arc<dyn CoordStore> = mem.clone();
    let clock = FixedClock::new(
        chrono::DateTime::parse_from_rfc3339("2026-05-29T14:32:00Z")
            .unwrap()
            .with_timezone(&chrono::Utc),
    );
    let rt = CoordRuntime::start(store, clock.clone(), me("A"), rt_cfg())
        .await
        .unwrap();
    let router = build_router(AppState::new(rt.clone()));
    (router, rt, mem, clock)
}

async fn fresh_app() -> (axum::Router, CoordRuntime, Arc<MemStore>) {
    let (router, rt, mem, _clock) = fresh_app_with_clock().await;
    (router, rt, mem)
}

/// Simulate a coord crash (no graceful shutdown, no flush) followed
/// by a takeover: advance the clock past the lease ttl + grace and
/// start a fresh runtime against the same store.
async fn crash_restart(mem: &Arc<MemStore>, clock: &Arc<FixedClock>) -> CoordRuntime {
    clock.advance(chrono::Duration::seconds(60));
    let store: Arc<dyn CoordStore> = mem.clone();
    CoordRuntime::start(store, clock.clone(), me("B"), rt_cfg())
        .await
        .unwrap()
}

fn delta_event(wid: &WorkerId, files: u64) -> Value {
    serde_json::json!({
        "kind": "ProgressDelta",
        "job_id": "bobby",
        "worker_id": wid,
        "files_delta": files,
        "bytes_delta": 0,
        "errors_delta": 0,
    })
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
// events batch — trust boundary (F20 D2+D3,
// docs/work-items/COORD_WORKER_EVENT_TRUST.md).
//
// The worker route accepts only worker-nature kinds, bound to the
// caller's URL id. Operator/lifecycle kinds stay on the admin
// command path (bearer auth + audit rows); `WorkerJoined`/
// `WorkerLeft` are synthesized by register/heartbeat and are not
// accepted raw. Unknown callers are rejected with the same status
// heartbeat uses for an unknown worker. The whole batch is
// validated before anything is ingested — one bad entry rejects
// the batch wholesale.
// =============================================================================

async fn register_with_host(app: &axum::Router, job: &str, host: &str) -> WorkerId {
    let (status, body) = post_json(
        app.clone(),
        "/workers/register",
        serde_json::json!({
            "job_id": job,
            "host": host,
            "pid": 1,
            "start_time": "2026-05-29T14:31:55Z",
            "version": "0.6",
        }),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    WorkerId(Uuid::parse_str(body["worker_id"].as_str().unwrap()).unwrap())
}

/// Wire form of one batch entry: `EventKind` is `#[serde(tag =
/// "kind")]`, so its serialization is exactly the flattened entry
/// shape the route expects (no `worker_at`).
fn entry(kind: &EventKind) -> Value {
    serde_json::to_value(kind).unwrap()
}

#[tokio::test]
async fn worker_route_rejects_operator_kinds() {
    let (app, rt, _store) = fresh_app().await;
    rt.ingest(job_created("bobby")).await.unwrap();
    let wid = register_one(&app, "bobby").await;
    let seq_before = rt.last_seq().await;

    // The nine operator/lifecycle kinds from the work item, plus the
    // two register/heartbeat-synthesized kinds that must not be
    // accepted raw either.
    let forbidden = vec![
        job_created("bobby"),
        EventKind::JobPaused {
            job_id: jid("bobby"),
            reason: "rogue".into(),
        },
        EventKind::JobResumed {
            job_id: jid("bobby"),
            reason: "rogue".into(),
        },
        EventKind::JobCancelled {
            job_id: jid("bobby"),
            reason: "rogue".into(),
        },
        EventKind::JobCompleted {
            job_id: jid("bobby"),
        },
        EventKind::JobFailed {
            job_id: jid("bobby"),
            reason: "rogue".into(),
        },
        EventKind::JobPhaseChanged {
            job_id: jid("bobby"),
            from: Phase::Planned,
            to: Phase::Cutover,
            reason: "rogue".into(),
        },
        EventKind::VerifyStarted {
            job_id: jid("bobby"),
        },
        EventKind::VerifyCompleted {
            job_id: jid("bobby"),
            mismatches: 0,
        },
        EventKind::WorkerJoined {
            worker_id: WorkerId::new(),
            job_id: jid("bobby"),
            host: "h".into(),
            pid: 9,
            start_time: "2026-05-29T14:31:55Z".parse().unwrap(),
            version: "0.6".into(),
        },
        EventKind::WorkerLeft {
            worker_id: wid,
            reason: "rogue".into(),
        },
    ];
    for kind in forbidden {
        let name = kind.name();
        let (status, body) = post_json(
            app.clone(),
            &format!("/workers/{wid}/events"),
            serde_json::json!({ "events": [entry(&kind)] }),
        )
        .await;
        assert_eq!(
            status,
            StatusCode::FORBIDDEN,
            "{name} must be 403 on the worker events route",
        );
        assert!(
            body["message"].as_str().unwrap_or("").contains(name),
            "error must name the rejected kind {name}, got: {body}",
        );
        assert_eq!(
            rt.last_seq().await,
            seq_before,
            "{name}: nothing may enter the event log",
        );
    }
    let job = rt.job_view(&jid("bobby")).await.unwrap();
    assert_eq!(
        job.phase,
        Phase::Planned,
        "job lifecycle must be untouched by rejected operator kinds",
    );
}

#[tokio::test]
async fn worker_route_rejects_foreign_worker_id() {
    let (app, rt, _store) = fresh_app().await;
    rt.ingest(job_created("bobby")).await.unwrap();
    let a = register_with_host(&app, "bobby", "host-a").await;
    let b = register_with_host(&app, "bobby", "host-b").await;
    let seq_before = rt.last_seq().await;

    // Worker A reports progress attributed to worker B.
    let (status, body) = post_json(
        app,
        &format!("/workers/{a}/events"),
        serde_json::json!({ "events": [delta_event(&b, 5)] }),
    )
    .await;
    assert_eq!(
        status,
        StatusCode::FORBIDDEN,
        "a worker must not submit events attributed to another worker",
    );
    let msg = body["message"].as_str().unwrap_or("");
    assert!(
        msg.contains(&a.to_string()) && msg.contains(&b.to_string()),
        "error must name both the caller and the payload id, got: {body}",
    );
    assert_eq!(rt.last_seq().await, seq_before, "state must be unchanged");
    let job = rt.job_view(&jid("bobby")).await.unwrap();
    assert_eq!(job.progress.files_done, 0, "foreign delta must not apply");
}

#[tokio::test]
async fn worker_route_rejects_unregistered_caller() {
    let (app, rt, _store) = fresh_app().await;
    rt.ingest(job_created("bobby")).await.unwrap();
    let ghost = WorkerId::new();
    let seq_before = rt.last_seq().await;

    // Never registered — even a well-formed self-attributed
    // ProgressDelta is rejected. Same status heartbeat uses for an
    // unknown worker (404 worker_not_found).
    let (status, body) = post_json(
        app,
        &format!("/workers/{ghost}/events"),
        serde_json::json!({ "events": [delta_event(&ghost, 5)] }),
    )
    .await;
    assert_eq!(
        status,
        StatusCode::NOT_FOUND,
        "an unregistered caller must be rejected like heartbeat rejects unknown workers",
    );
    assert_eq!(body["code"], "worker_not_found");
    assert_eq!(rt.last_seq().await, seq_before, "state must be unchanged");
    let job = rt.job_view(&jid("bobby")).await.unwrap();
    assert_eq!(job.progress.files_done, 0);
}

#[tokio::test]
async fn worker_fenced_self_only() {
    let (app, rt, _store) = fresh_app().await;
    rt.ingest(job_created("bobby")).await.unwrap();
    let a = register_with_host(&app, "bobby", "host-a").await;
    let b = register_with_host(&app, "bobby", "host-b").await;
    let seq_before = rt.last_seq().await;

    // A tries to fence B → 403, B untouched.
    let (status, _body) = post_json(
        app.clone(),
        &format!("/workers/{a}/events"),
        serde_json::json!({ "events": [entry(&EventKind::WorkerFenced {
            worker_id: b,
            reason: "rogue".into(),
        })] }),
    )
    .await;
    assert_eq!(
        status,
        StatusCode::FORBIDDEN,
        "one worker must not fence another via the events route",
    );
    let snap = rt.state().await;
    assert_ne!(
        snap.workers[&b].state,
        WorkerState::Fenced,
        "the foreign fence must not be applied",
    );
    assert_eq!(rt.last_seq().await, seq_before);

    // A reports its own fence → applied.
    let (status, resp) = post_json(
        app,
        &format!("/workers/{a}/events"),
        serde_json::json!({ "events": [entry(&EventKind::WorkerFenced {
            worker_id: a,
            reason: "self-fence R7".into(),
        })] }),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "self-fence report must flow");
    assert_eq!(resp["seqs"].as_array().unwrap().len(), 1);
    let snap = rt.state().await;
    assert_eq!(snap.workers[&a].state, WorkerState::Fenced);
    assert_eq!(
        snap.workers[&a].fence_reason.as_deref(),
        Some("self-fence R7")
    );
}

#[tokio::test]
async fn one_bad_entry_rejects_whole_batch() {
    let (app, rt, _store) = fresh_app().await;
    rt.ingest(job_created("bobby")).await.unwrap();
    let wid = register_one(&app, "bobby").await;
    let seq_before = rt.last_seq().await;

    // A perfectly valid self-attributed ProgressDelta riding next to
    // a smuggled JobCancelled: the whole batch dies, NEITHER applies.
    let (status, _body) = post_json(
        app,
        &format!("/workers/{wid}/events"),
        serde_json::json!({ "events": [
            delta_event(&wid, 5),
            entry(&EventKind::JobCancelled {
                job_id: jid("bobby"),
                reason: "smuggled".into(),
            }),
        ] }),
    )
    .await;
    assert_eq!(status, StatusCode::FORBIDDEN);
    assert_eq!(
        rt.last_seq().await,
        seq_before,
        "rejected batches must never partially apply",
    );
    let job = rt.job_view(&jid("bobby")).await.unwrap();
    assert_eq!(
        job.progress.files_done, 0,
        "the valid sibling must not be smuggled in",
    );
    assert_eq!(job.phase, Phase::Planned, "the job must not be cancelled");
}

#[tokio::test]
async fn allowed_kinds_still_flow() {
    let (app, rt, _store) = fresh_app().await;
    rt.ingest(job_created("bobby")).await.unwrap();
    let wid = register_one(&app, "bobby").await;
    // Unregistered peer id — legal in claim-conflict ROLE fields
    // (holder/contender/winner name both parties of a conflict by
    // design; they are not caller attribution).
    let peer = WorkerId::new();
    let seq_before = rt.last_seq().await;

    let events = vec![
        entry(&EventKind::ProgressDelta {
            job_id: jid("bobby"),
            worker_id: wid,
            files_delta: 7,
            bytes_delta: 2048,
            errors_delta: 0,
        }),
        entry(&EventKind::ErrorEmitted {
            job_id: jid("bobby"),
            worker_id: wid,
            class: ErrorClass::Timeout,
            path: "/a/b".into(),
            retryable: true,
            message: "timed out".into(),
        }),
        entry(&EventKind::WorkerStateChanged {
            worker_id: wid,
            from: WorkerState::Idle,
            to: WorkerState::Copying,
        }),
        entry(&EventKind::ClaimConflictDetected {
            job_id: jid("bobby"),
            shard_id: ShardId("s1".into()),
            holder: peer,
            contender: wid,
        }),
        entry(&EventKind::ClaimConflictResolved {
            job_id: jid("bobby"),
            shard_id: ShardId("s1".into()),
            winner: peer,
        }),
        entry(&EventKind::VerifyFileMismatch {
            job_id: jid("bobby"),
            path: "/a/c".into(),
            expected: "aa".into(),
            got: "bb".into(),
        }),
        entry(&EventKind::WorkerFenced {
            worker_id: wid,
            reason: "self-fence R7".into(),
        }),
        entry(&EventKind::WorkerRecovered { worker_id: wid }),
    ];
    let n = events.len() as u64;
    let (status, resp) = post_json(
        app,
        &format!("/workers/{wid}/events"),
        serde_json::json!({ "events": events }),
    )
    .await;
    assert_eq!(
        status,
        StatusCode::OK,
        "allow-listed kinds must flow: {resp}"
    );
    let seqs: Vec<u64> = resp["seqs"]
        .as_array()
        .unwrap()
        .iter()
        .map(|s| s.as_u64().unwrap())
        .collect();
    assert_eq!(seqs.len() as u64, n);
    for (i, s) in seqs.iter().enumerate() {
        assert_eq!(*s, seq_before + 1 + i as u64, "seqs assigned in order");
    }

    // The events reached state exactly as before.
    let job = rt.job_view(&jid("bobby")).await.unwrap();
    assert_eq!(job.progress.files_done, 7);
    assert_eq!(job.progress.bytes_done, 2048);
    let snap = rt.state().await;
    assert_eq!(snap.error_buckets[&jid("bobby")][0].count, 1);
    // Fence then recover: reducer saw both, worker ends Idle with the
    // fence reason cleared.
    assert_eq!(snap.workers[&wid].state, WorkerState::Idle);
    assert_eq!(snap.workers[&wid].fence_reason, None);
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

// =============================================================================
// Ack durability (ledger F03) — a 200 with seqs means the events
// survive a coord crash. The worker drops acked events from its
// bounded resend buffer, so an ack for a RAM-only event is data loss.
// =============================================================================

#[tokio::test]
async fn worker_events_ack_implies_durable() {
    let (app, rt, mem, clock) = fresh_app_with_clock().await;
    rt.ingest(job_created("bobby")).await.unwrap();
    // Registered caller — the F20 trust boundary rejects events for
    // unknown worker ids before they reach the log.
    let wid = register_one(&app, "bobby").await;

    let body = serde_json::json!({
        "events": [delta_event(&wid, 5), delta_event(&wid, 3)],
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

    // The acked events must be readable from store chunks, not just
    // the in-memory writer buffer.
    let durable = migration_coord::events::read_all_events_since(mem.as_ref(), 0)
        .await
        .unwrap();
    let durable_seqs: Vec<u64> = durable.iter().map(|e| e.seq).collect();
    for s in &seqs {
        assert!(
            durable_seqs.contains(s),
            "acked seq {s} is not in any flushed chunk (durable seqs: {durable_seqs:?})",
        );
    }

    // Crash-restart: replay from the same store must contain every
    // acked seq (state reflects the deltas, last_seq covers them).
    let rt2 = crash_restart(&mem, &clock).await;
    let last = rt2.last_seq().await;
    for s in &seqs {
        assert!(
            *s <= last,
            "acked seq {s} lost across crash-restart (replayed last_seq = {last})",
        );
    }
    let job = rt2
        .job_view(&jid("bobby"))
        .await
        .expect("job must survive the crash — its events were acked");
    assert_eq!(job.progress.files_done, 8);
}

#[tokio::test]
async fn seq_never_regresses_across_restart() {
    let (app, rt, mem, clock) = fresh_app_with_clock().await;
    rt.ingest(job_created("bobby")).await.unwrap();
    let wid = register_one(&app, "bobby").await;

    let body = serde_json::json!({
        "events": [delta_event(&wid, 1), delta_event(&wid, 2), delta_event(&wid, 3)],
    });
    let (status, resp) = post_json(app, &format!("/workers/{wid}/events"), body).await;
    assert_eq!(status, StatusCode::OK);
    let max_acked = resp["seqs"]
        .as_array()
        .unwrap()
        .iter()
        .map(|s| s.as_u64().unwrap())
        .max()
        .unwrap();

    // Crash + takeover. The first seq the new coord assigns must be
    // strictly greater than every acked seq — otherwise the TUI's
    // `seq <= last_seen_seq` drop rule silently discards new events.
    let rt2 = crash_restart(&mem, &clock).await;
    let first_new = rt2.ingest(job_created("mary")).await.unwrap();
    assert!(
        first_new > max_acked,
        "seq regressed across restart: new coord assigned {first_new}, \
         but {max_acked} was already acked",
    );
}

/// Lease-fence composition (ledger F02 x F03): once the lease is
/// observed lost, the events endpoint must fail the request — no
/// seqs handed out, no store writes. A deposed coord that acks is a
/// deposed coord that loses data.
#[tokio::test]
async fn fenced_coord_never_acks() {
    let (app, rt, mem, _clock) = fresh_app_with_clock().await;
    rt.ingest(job_created("bobby")).await.unwrap();
    // Register while the lease is still held (register itself
    // flushes), so the events POST below passes the F20 registration
    // check and fails on the lease fence, which is what this test
    // pins.
    let wid = register_one(&app, "bobby").await;
    rt.flush_log().await.unwrap();
    let pre_fence_seq = rt.last_seq().await;

    rt.mark_lease_lost().await;
    let writes_before = mem.write_count();

    let body = serde_json::json!({ "events": [delta_event(&wid, 5)] });
    let (status, _resp) = post_json(app, &format!("/workers/{wid}/events"), body).await;
    assert_eq!(
        status,
        StatusCode::INTERNAL_SERVER_ERROR,
        "a fenced coord must not ack worker events",
    );
    assert_eq!(
        mem.write_count(),
        writes_before,
        "a fenced coord must not write to the store on the events path",
    );
    // Nothing new became durable either.
    let durable = migration_coord::events::read_all_events_since(mem.as_ref(), pre_fence_seq)
        .await
        .unwrap();
    assert!(
        durable.is_empty(),
        "no event past the pre-fence flush may be durable: {durable:?}",
    );
}

// =============================================================================
// Throughput sanity (F03 test 6) — whatever durability mechanism
// lands must amortize: a batch of 1000 events acks in ~one flush
// write, not 1000 individual PUTs.
// =============================================================================

#[tokio::test]
async fn events_batch_1000_amortizes_flush_writes() {
    let (app, rt, mem, _clock) = fresh_app_with_clock().await;
    rt.ingest(job_created("bobby")).await.unwrap();
    let wid = register_one(&app, "bobby").await;
    // Clear the setup events (register flushes its own WorkerJoined)
    // so the write count below isolates the batch itself.
    rt.flush_log().await.unwrap();
    let seq_before = rt.last_seq().await;

    let events: Vec<Value> = (0..1000).map(|_| delta_event(&wid, 1)).collect();
    let writes_before = mem.write_count();
    let (status, resp) = post_json(
        app,
        &format!("/workers/{wid}/events"),
        serde_json::json!({ "events": events }),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(resp["seqs"].as_array().unwrap().len(), 1000);

    let batch_writes = mem.write_count() - writes_before;
    assert!(
        batch_writes <= 2,
        "a 1000-event batch must amortize to at most 2 chunk writes, saw {batch_writes}",
    );

    // And every acked event is durable.
    let durable = migration_coord::events::read_all_events_since(mem.as_ref(), seq_before)
        .await
        .unwrap();
    assert_eq!(durable.len(), 1000);
}

/// Manual throughput bench — run with `cargo test -- --ignored`.
/// MemStore-backed, so this measures the coord-side per-event cost
/// (lock + reduce + serialize + amortized flush), not S3 latency.
#[tokio::test]
#[ignore = "throughput bench; run manually"]
async fn bench_events_batch_throughput() {
    let (app, rt, _mem, _clock) = fresh_app_with_clock().await;
    rt.ingest(job_created("bobby")).await.unwrap();
    let wid = register_one(&app, "bobby").await;

    const BATCHES: usize = 10;
    const PER_BATCH: usize = 1000;
    let start = std::time::Instant::now();
    for _ in 0..BATCHES {
        let events: Vec<Value> = (0..PER_BATCH).map(|_| delta_event(&wid, 1)).collect();
        let (status, _) = post_json(
            app.clone(),
            &format!("/workers/{wid}/events"),
            serde_json::json!({ "events": events }),
        )
        .await;
        assert_eq!(status, StatusCode::OK);
    }
    let elapsed = start.elapsed();
    let total = (BATCHES * PER_BATCH) as f64;
    let rate = total / elapsed.as_secs_f64();
    eprintln!("events_batch throughput: {rate:.0} events/s ({total} events in {elapsed:?})");
    assert!(
        rate > 10_000.0,
        "flush-before-ack must sustain the 10k events/s target; measured {rate:.0}/s",
    );
}
