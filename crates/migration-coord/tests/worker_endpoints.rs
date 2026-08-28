//! Integration tests for the worker-facing REST endpoints
//! (server::worker). Same in-process harness as the read + command
//! endpoint tests.

use async_trait::async_trait;
use http::{Request, StatusCode};
use http_body_util::BodyExt;
use migration_coord::lease::{Identity, LeaseConfig};
use migration_coord::runtime::test_clock::FixedClock;
use migration_coord::runtime::{CoordRuntime, RuntimeConfig};
use migration_coord::schema::{
    ConfigHash, ErrorClass, EventKind, JobId, Phase, ShardId, WorkerId, WorkerState,
};
use migration_coord::server::{build_router, AppState};
use migration_coord::store::{CoordStore, ListEntry, MemStore, PutOutcome};
use migration_core::claim::DeleteOutcome;
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
        total_files: 0,
        total_bytes: 0,
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

// =============================================================================
// events batch — idempotency (F20 D4+D5,
// docs/work-items/COORD_EVENT_IDEMPOTENCY.md).
//
// Workers stamp each batch entry with a per-worker, monotonically
// increasing `client_seq`. The coord keeps a per-worker high-water
// mark in reducer state (so snapshots and replay carry it) and skips
// entries at or below it — a resend after a lost 200, or a retry of
// a batch that died mid-flush, converges to exactly-once effective
// application. Unstamped entries keep today's documented
// at-least-once semantics (pre-upgrade workers, no flag day).
// Forward gaps in client_seq are legitimate (the worker's buffer
// drops under budget pressure); only order matters — stamps out of
// order WITHIN one batch mean a buggy client and reject the batch.
// =============================================================================

/// Stamp a wire-form batch entry with a `client_seq`.
fn with_cs(mut entry: Value, cs: u64) -> Value {
    entry["client_seq"] = serde_json::json!(cs);
    entry
}

fn stamped_delta(wid: &WorkerId, files: u64, cs: u64) -> Value {
    with_cs(delta_event(wid, files), cs)
}

/// Durable (flushed-to-store) envelopes with `seq > since`, in seq
/// order — the ground truth the convergence assertions compare.
async fn durable_since(mem: &MemStore, since: u64) -> Vec<migration_coord::schema::EventEnvelope> {
    migration_coord::events::read_all_events_since(mem, since)
        .await
        .unwrap()
}

/// Acceptance test 1 (work item): a stamped batch applied once, then
/// the IDENTICAL batch again (the 200 was lost on the wire and the
/// worker's resend buffer re-sent everything). The second response
/// must succeed — the worker needs its 200 to drop the buffer — but
/// nothing may apply twice: job counters, error buckets, coord
/// last_seq, and the durable log contents must be identical to the
/// single send.
#[tokio::test]
async fn resend_after_lost_response_is_idempotent() {
    let (app, rt, mem) = fresh_app().await;
    rt.ingest(job_created("bobby")).await.unwrap();
    let wid = register_one(&app, "bobby").await;
    rt.flush_log().await.unwrap();
    let seq_before = rt.last_seq().await;

    let batch = serde_json::json!({ "events": [
        stamped_delta(&wid, 5, 1),
        stamped_delta(&wid, 3, 2),
        with_cs(
            entry(&EventKind::ErrorEmitted {
                job_id: jid("bobby"),
                worker_id: wid,
                class: ErrorClass::Timeout,
                path: "/a/b".into(),
                retryable: true,
                message: "timed out".into(),
            }),
            3,
        ),
    ] });

    let (status, resp) = post_json(
        app.clone(),
        &format!("/workers/{wid}/events"),
        batch.clone(),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "first send must apply: {resp}");
    assert_eq!(resp["seqs"].as_array().unwrap().len(), 3);
    let job = rt.job_view(&jid("bobby")).await.unwrap();
    assert_eq!(job.progress.files_done, 8);
    let durable_once = durable_since(&mem, seq_before).await;
    assert_eq!(durable_once.len(), 3, "flush-before-ack: batch durable");

    // The 200 is lost; the worker re-sends the identical batch.
    let (status, resp) = post_json(app, &format!("/workers/{wid}/events"), batch).await;
    assert_eq!(
        status,
        StatusCode::OK,
        "a resend of an already-applied batch must still succeed \
         (the worker needs the 200 to drop its resend buffer): {resp}",
    );
    assert_eq!(
        resp["seqs"].as_array().unwrap().len(),
        0,
        "no new seqs may be assigned for already-applied entries: {resp}",
    );
    assert_eq!(
        resp["deduped"].as_u64().unwrap_or(0),
        3,
        "the response must report the skipped entries: {resp}",
    );
    assert_eq!(
        rt.last_seq().await,
        seq_before + 3,
        "the resend must not grow the event log",
    );
    let job = rt.job_view(&jid("bobby")).await.unwrap();
    assert_eq!(
        job.progress.files_done, 8,
        "job counters must be identical to a single send",
    );
    let snap = rt.state().await;
    assert_eq!(
        snap.error_buckets[&jid("bobby")][0].count,
        1,
        "error buckets must be identical to a single send",
    );
    let durable_twice = durable_since(&mem, seq_before).await;
    assert_eq!(
        durable_twice, durable_once,
        "durable log contents must be identical to a single send",
    );
}

/// Acceptance test 2 (work item): the high-water mark must survive
/// crash + replay — it rides the durable event stream, not process
/// RAM. Ingest stamped events, crash-restart the coord (the
/// harness's replay pattern), then resend the old batch to the NEW
/// runtime: still deduped.
#[tokio::test]
async fn replay_reconstructs_hwm() {
    let (app, rt, mem, clock) = fresh_app_with_clock().await;
    rt.ingest(job_created("bobby")).await.unwrap();
    let wid = register_one(&app, "bobby").await;
    rt.flush_log().await.unwrap();
    let seq_before = rt.last_seq().await;

    let batch = serde_json::json!({ "events": [
        stamped_delta(&wid, 5, 1),
        stamped_delta(&wid, 3, 2),
    ] });
    let (status, _) = post_json(app, &format!("/workers/{wid}/events"), batch.clone()).await;
    assert_eq!(status, StatusCode::OK);
    let durable_once = durable_since(&mem, seq_before).await;

    // Coord crashes; a fresh runtime replays snapshot + log.
    let rt2 = crash_restart(&mem, &clock).await;
    let app2 = build_router(AppState::new(rt2.clone()));
    let seq_after_replay = rt2.last_seq().await;

    // The worker never saw the (lost) 200 and re-sends the old batch.
    let (status, resp) = post_json(app2, &format!("/workers/{wid}/events"), batch).await;
    assert_eq!(status, StatusCode::OK, "resend must succeed: {resp}");
    assert_eq!(
        resp["seqs"].as_array().unwrap().len(),
        0,
        "replay must reconstruct the HWM — old entries stay deduped: {resp}",
    );
    assert_eq!(
        rt2.last_seq().await,
        seq_after_replay,
        "the deduped resend must not grow the event log after replay",
    );
    let job = rt2.job_view(&jid("bobby")).await.unwrap();
    assert_eq!(
        job.progress.files_done, 8,
        "counters must be identical to the single pre-crash send",
    );
    let durable_twice = durable_since(&mem, seq_before).await;
    assert_eq!(
        durable_twice, durable_once,
        "durable log must be identical to the single pre-crash send",
    );
}

/// Store double for the storage-failure test: delegates to a
/// MemStore, but while armed, fails every `put` under `events/` —
/// the chunk-flush seam the ingest loop `?`-propagates from. Same
/// pattern as `FailArchivePuts` in archive_wiring.rs.
#[derive(Debug)]
struct FailEventPuts {
    inner: Arc<MemStore>,
    armed: std::sync::atomic::AtomicBool,
}

impl FailEventPuts {
    fn new(inner: Arc<MemStore>) -> Self {
        Self {
            inner,
            armed: std::sync::atomic::AtomicBool::new(false),
        }
    }

    fn arm(&self) {
        self.armed.store(true, std::sync::atomic::Ordering::SeqCst);
    }

    fn disarm(&self) {
        self.armed.store(false, std::sync::atomic::Ordering::SeqCst);
    }
}

#[async_trait]
impl CoordStore for FailEventPuts {
    async fn get(&self, key: &str) -> migration_coord::Result<Option<(Vec<u8>, String)>> {
        self.inner.get(key).await
    }
    async fn head(&self, key: &str) -> migration_coord::Result<Option<String>> {
        self.inner.head(key).await
    }
    async fn put(&self, key: &str, body: Vec<u8>) -> migration_coord::Result<String> {
        if self.armed.load(std::sync::atomic::Ordering::SeqCst) && key.starts_with("events/") {
            return Err(anyhow::anyhow!("injected storage failure writing {key}").into());
        }
        self.inner.put(key, body).await
    }
    async fn put_if_absent(&self, key: &str, body: Vec<u8>) -> migration_coord::Result<PutOutcome> {
        self.inner.put_if_absent(key, body).await
    }
    async fn delete(&self, key: &str) -> migration_coord::Result<()> {
        self.inner.delete(key).await
    }
    async fn delete_if_match(
        &self,
        key: &str,
        etag: &str,
    ) -> migration_coord::Result<DeleteOutcome> {
        self.inner.delete_if_match(key, etag).await
    }
    async fn list(&self, prefix: &str) -> migration_coord::Result<Vec<ListEntry>> {
        self.inner.list(prefix).await
    }
}

/// Acceptance test 3 (work item, the headline): a storage failure
/// mid-batch (the ingest loop `?`-propagates from a chunk flush)
/// leaves a prefix applied with no record; the worker's retry of the
/// SAME batch must converge to exactly-once effective application —
/// state, log contents, and counters identical to a single clean
/// send. `max_events_per_chunk = 2` forces chunk flushes mid-loop so
/// the injected `put` failure lands between entries.
#[tokio::test]
async fn storage_failure_then_retry_converges() {
    let mem = Arc::new(MemStore::new());
    let flaky = Arc::new(FailEventPuts::new(mem.clone()));
    let store: Arc<dyn CoordStore> = flaky.clone();
    let clock = FixedClock::new(
        chrono::DateTime::parse_from_rfc3339("2026-05-29T14:32:00Z")
            .unwrap()
            .with_timezone(&chrono::Utc),
    );
    let mut cfg = rt_cfg();
    cfg.events.max_events_per_chunk = 2; // flush (and fail) mid-batch
    let rt = CoordRuntime::start(store, clock, me("A"), cfg)
        .await
        .unwrap();
    let app = build_router(AppState::new(rt.clone()));

    rt.ingest(job_created("bobby")).await.unwrap();
    let wid = register_one(&app, "bobby").await;
    rt.flush_log().await.unwrap();
    let seq_before = rt.last_seq().await;

    // Distinct powers of two so any double-apply shows up in the sum.
    let batch = serde_json::json!({ "events": [
        stamped_delta(&wid, 1, 1),
        stamped_delta(&wid, 2, 2),
        stamped_delta(&wid, 4, 3),
        stamped_delta(&wid, 8, 4),
        stamped_delta(&wid, 16, 5),
    ] });

    flaky.arm();
    let (status, _) = post_json(
        app.clone(),
        &format!("/workers/{wid}/events"),
        batch.clone(),
    )
    .await;
    assert_eq!(
        status,
        StatusCode::INTERNAL_SERVER_ERROR,
        "the injected chunk-flush failure must fail the batch",
    );

    // Storage recovers; the worker retries the whole batch.
    flaky.disarm();
    let (status, resp) = post_json(app, &format!("/workers/{wid}/events"), batch).await;
    assert_eq!(status, StatusCode::OK, "retry must succeed: {resp}");

    // Exactly-once convergence: identical to a single clean send.
    let job = rt.job_view(&jid("bobby")).await.unwrap();
    assert_eq!(
        job.progress.files_done, 31,
        "every entry must apply exactly once across failure + retry",
    );
    assert_eq!(
        rt.last_seq().await,
        seq_before + 5,
        "exactly one seq per logical event across failure + retry",
    );
    let durable = durable_since(&mem, seq_before).await;
    assert_eq!(
        durable.len(),
        5,
        "durable log must hold each event exactly once: {durable:#?}",
    );
    let seqs: Vec<u64> = durable.iter().map(|e| e.seq).collect();
    assert_eq!(
        seqs,
        (seq_before + 1..=seq_before + 5).collect::<Vec<u64>>(),
        "durable seqs must be contiguous — no gaps, no duplicates",
    );
}

/// Acceptance test 4a (work item): entries WITHOUT `client_seq` (a
/// pre-upgrade worker) keep today's at-least-once semantics — every
/// send applies. GREEN today; must stay green (no flag day).
#[tokio::test]
async fn unstamped_entries_keep_legacy_semantics() {
    let (app, rt, _mem) = fresh_app().await;
    rt.ingest(job_created("bobby")).await.unwrap();
    let wid = register_one(&app, "bobby").await;

    let batch = serde_json::json!({ "events": [
        delta_event(&wid, 5),
        delta_event(&wid, 3),
    ] });
    for round in 1..=2u64 {
        let (status, resp) = post_json(
            app.clone(),
            &format!("/workers/{wid}/events"),
            batch.clone(),
        )
        .await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(
            resp["seqs"].as_array().unwrap().len(),
            2,
            "unstamped entries apply on every send (round {round}): {resp}",
        );
        assert_eq!(
            resp["deduped"].as_u64().unwrap_or(0),
            0,
            "unstamped entries are never deduped (round {round}): {resp}",
        );
    }
    let job = rt.job_view(&jid("bobby")).await.unwrap();
    assert_eq!(
        job.progress.files_done, 16,
        "at-least-once for pre-upgrade workers: both sends apply",
    );
}

/// Acceptance test 4b (work item): mixed stamped/unstamped batches —
/// resends dedup the stamped entries, re-apply the unstamped ones,
/// and must not corrupt the high-water mark for later stamped
/// traffic.
#[tokio::test]
async fn mixed_stamped_unstamped_batch_dedups_only_stamped() {
    let (app, rt, _mem) = fresh_app().await;
    rt.ingest(job_created("bobby")).await.unwrap();
    let wid = register_one(&app, "bobby").await;

    let batch = serde_json::json!({ "events": [
        stamped_delta(&wid, 1, 1),
        delta_event(&wid, 10), // unstamped rider
        stamped_delta(&wid, 100, 2),
    ] });
    let (status, resp) = post_json(
        app.clone(),
        &format!("/workers/{wid}/events"),
        batch.clone(),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{resp}");
    assert_eq!(resp["seqs"].as_array().unwrap().len(), 3);
    let job = rt.job_view(&jid("bobby")).await.unwrap();
    assert_eq!(job.progress.files_done, 111);

    // Resend: stamped entries dedup, the unstamped rider re-applies
    // (documented at-least-once for unstamped).
    let (status, resp) = post_json(app.clone(), &format!("/workers/{wid}/events"), batch).await;
    assert_eq!(status, StatusCode::OK, "{resp}");
    assert_eq!(
        resp["seqs"].as_array().unwrap().len(),
        1,
        "only the unstamped rider may re-apply: {resp}",
    );
    assert_eq!(
        resp["deduped"].as_u64().unwrap_or(0),
        2,
        "both stamped entries must be skipped: {resp}",
    );
    let job = rt.job_view(&jid("bobby")).await.unwrap();
    assert_eq!(
        job.progress.files_done, 121,
        "resend: stamped deduped (no +101), unstamped re-applied (+10)",
    );

    // The unstamped rider must not have corrupted the HWM: fresh
    // stamped traffic above it still applies exactly once.
    let next = serde_json::json!({ "events": [stamped_delta(&wid, 1000, 3)] });
    let (status, resp) =
        post_json(app.clone(), &format!("/workers/{wid}/events"), next.clone()).await;
    assert_eq!(status, StatusCode::OK, "{resp}");
    assert_eq!(resp["seqs"].as_array().unwrap().len(), 1);
    let (status, resp) = post_json(app, &format!("/workers/{wid}/events"), next).await;
    assert_eq!(status, StatusCode::OK, "{resp}");
    assert_eq!(
        resp["deduped"].as_u64().unwrap_or(0),
        1,
        "the new stamp must dedup on ITS resend: {resp}",
    );
    let job = rt.job_view(&jid("bobby")).await.unwrap();
    assert_eq!(job.progress.files_done, 1121);
}

/// Acceptance test 5 (work item): stamps out of order WITHIN one
/// batch mean a buggy client, not a replay — the whole batch is
/// rejected 400-class and nothing applies. (Cross-batch forward gaps
/// stay legitimate; the other tests cover them.)
#[tokio::test]
async fn non_monotonic_batch_rejected() {
    let (app, rt, _mem) = fresh_app().await;
    rt.ingest(job_created("bobby")).await.unwrap();
    let wid = register_one(&app, "bobby").await;
    let seq_before = rt.last_seq().await;

    // Out of order.
    let (status, body) = post_json(
        app.clone(),
        &format!("/workers/{wid}/events"),
        serde_json::json!({ "events": [
            stamped_delta(&wid, 1, 5),
            stamped_delta(&wid, 2, 3),
        ] }),
    )
    .await;
    assert_eq!(
        status,
        StatusCode::BAD_REQUEST,
        "descending client_seq within a batch must reject the whole batch",
    );
    assert_eq!(body["code"], "client_seq_not_monotonic");

    // Equal stamps are the same bug (strictly increasing required).
    let (status, _body) = post_json(
        app,
        &format!("/workers/{wid}/events"),
        serde_json::json!({ "events": [
            stamped_delta(&wid, 1, 4),
            stamped_delta(&wid, 2, 4),
        ] }),
    )
    .await;
    assert_eq!(
        status,
        StatusCode::BAD_REQUEST,
        "equal client_seq within a batch must reject the whole batch",
    );

    assert_eq!(
        rt.last_seq().await,
        seq_before,
        "rejected batches must never partially apply",
    );
    let job = rt.job_view(&jid("bobby")).await.unwrap();
    assert_eq!(job.progress.files_done, 0, "nothing may have applied");
}

// =============================================================================
// events batch — HWM attribution for stamped kinds without payload
// attribution (F20 residue, docs/work-items/COORD_RUNTIME_BATCH.md
// item 2).
//
// `EventKind::attributed_worker()` is None for ClaimConflictDetected/
// Resolved and VerifyFileMismatch (their worker fields are conflict
// ROLES, not caller attribution), so a stamped entry of those kinds
// at a batch tail could not advance the per-worker high-water mark —
// a resend after a lost 200 silently re-applied it. The envelope's
// `from_worker` caller stamp (set by the coord from the URL id the
// phase-1 trust boundary validated) closes that: every stamped entry
// can advance the mark, live and across replay.
// =============================================================================

/// Item-2 acceptance 1: a batch ENDING in a stamped
/// `ClaimConflictResolved` fully dedups on an identical resend —
/// nothing re-applies, state and durable log identical to a single
/// send. Red before the fix: the tail stamp could not advance the
/// HWM and the resend double-counted `conflicts_resolved`.
#[tokio::test]
async fn stamped_non_attributed_tail_dedups_on_resend() {
    let (app, rt, mem) = fresh_app().await;
    rt.ingest(job_created("bobby")).await.unwrap();
    let wid = register_one(&app, "bobby").await;
    rt.flush_log().await.unwrap();
    let seq_before = rt.last_seq().await;

    let batch = serde_json::json!({ "events": [
        stamped_delta(&wid, 5, 1),
        with_cs(
            entry(&EventKind::ClaimConflictResolved {
                job_id: jid("bobby"),
                shard_id: ShardId("s1".into()),
                winner: wid,
            }),
            2,
        ),
    ] });
    let (status, resp) = post_json(
        app.clone(),
        &format!("/workers/{wid}/events"),
        batch.clone(),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "first send must apply: {resp}");
    assert_eq!(resp["seqs"].as_array().unwrap().len(), 2);
    let job = rt.job_view(&jid("bobby")).await.unwrap();
    assert_eq!(job.progress.conflicts_resolved, 1);
    assert_eq!(job.progress.files_done, 5);
    let durable_once = durable_since(&mem, seq_before).await;
    assert_eq!(durable_once.len(), 2, "flush-before-ack: batch durable");

    // The 200 is lost; the worker resends the identical batch.
    let (status, resp) = post_json(app, &format!("/workers/{wid}/events"), batch).await;
    assert_eq!(status, StatusCode::OK, "resend must succeed: {resp}");
    assert_eq!(
        resp["seqs"].as_array().unwrap().len(),
        0,
        "the stamped non-attributed tail must have advanced the HWM — \
         nothing may re-apply on resend: {resp}",
    );
    assert_eq!(
        resp["deduped"].as_u64().unwrap_or(0),
        2,
        "both entries must be skipped as already-applied: {resp}",
    );
    let job = rt.job_view(&jid("bobby")).await.unwrap();
    assert_eq!(
        job.progress.conflicts_resolved, 1,
        "ClaimConflictResolved must not double-count on resend",
    );
    assert_eq!(job.progress.files_done, 5);
    assert_eq!(
        rt.last_seq().await,
        seq_before + 2,
        "the resend must not grow the event log",
    );
    let durable_twice = durable_since(&mem, seq_before).await;
    assert_eq!(
        durable_twice, durable_once,
        "durable log contents must be identical to a single send",
    );
}

/// Item-2 acceptance 2: the tail stamp survives crash + replay — the
/// caller attribution rides the durable envelope, so a fresh runtime
/// reconstructs the HWM and still dedups the old batch. Red before
/// the fix.
#[tokio::test]
async fn replay_reconstructs_hwm_from_from_worker() {
    let (app, rt, mem, clock) = fresh_app_with_clock().await;
    rt.ingest(job_created("bobby")).await.unwrap();
    let wid = register_one(&app, "bobby").await;
    rt.flush_log().await.unwrap();
    let seq_before = rt.last_seq().await;

    let batch = serde_json::json!({ "events": [
        stamped_delta(&wid, 5, 1),
        with_cs(
            entry(&EventKind::ClaimConflictResolved {
                job_id: jid("bobby"),
                shard_id: ShardId("s1".into()),
                winner: wid,
            }),
            2,
        ),
    ] });
    let (status, _) = post_json(app, &format!("/workers/{wid}/events"), batch.clone()).await;
    assert_eq!(status, StatusCode::OK);
    let durable_once = durable_since(&mem, seq_before).await;

    // Coord crashes; a fresh runtime replays snapshot + log, then the
    // worker (which never saw the lost 200) resends the old batch.
    let rt2 = crash_restart(&mem, &clock).await;
    let app2 = build_router(AppState::new(rt2.clone()));
    let seq_after_replay = rt2.last_seq().await;

    let (status, resp) = post_json(app2, &format!("/workers/{wid}/events"), batch).await;
    assert_eq!(status, StatusCode::OK, "resend must succeed: {resp}");
    assert_eq!(
        resp["seqs"].as_array().unwrap().len(),
        0,
        "replay must reconstruct the HWM from the envelope's caller \
         attribution — the stamped tail stays deduped: {resp}",
    );
    assert_eq!(
        rt2.last_seq().await,
        seq_after_replay,
        "the deduped resend must not grow the event log after replay",
    );
    let job = rt2.job_view(&jid("bobby")).await.unwrap();
    assert_eq!(
        job.progress.conflicts_resolved, 1,
        "conflict count must be identical to the single pre-crash send",
    );
    let durable_twice = durable_since(&mem, seq_before).await;
    assert_eq!(
        durable_twice, durable_once,
        "durable log must be identical to the single pre-crash send",
    );
}

// =============================================================================
// Worker liveness (600M retest follow-up): dead workers must stop
// reading as Copying. Three paths: supersede on re-register from the
// same machine (the default host id is `<hostname>-<pid>`, so the
// literal host comparison never matched), an explicit /leave on
// orderly exit, and the heartbeat-age sweep for everything else.
// =============================================================================

#[tokio::test]
async fn reregister_from_same_machine_with_pid_suffixed_host_id_supersedes() {
    let (app, rt, _store) = fresh_app().await;
    rt.ingest(job_created("bobby")).await.unwrap();

    let (s1, b1) = post_json(
        app.clone(),
        "/workers/register",
        serde_json::json!({
            "job_id": "bobby",
            "host": "k8s-se-2-1203425",
            "pid": 1203425,
            "start_time": "2026-05-29T14:00:00Z",
            "version": "0.6.0",
        }),
    )
    .await;
    assert_eq!(s1, StatusCode::OK);
    let first = WorkerId(Uuid::parse_str(b1["worker_id"].as_str().unwrap()).unwrap());

    // The restarted worker on the same node carries its own pid in
    // the host id.
    let (s2, b2) = post_json(
        app,
        "/workers/register",
        serde_json::json!({
            "job_id": "bobby",
            "host": "k8s-se-2-2620823",
            "pid": 2620823,
            "start_time": "2026-05-29T14:05:00Z",
            "version": "0.6.0",
        }),
    )
    .await;
    assert_eq!(s2, StatusCode::OK);
    let second = WorkerId(Uuid::parse_str(b2["worker_id"].as_str().unwrap()).unwrap());
    let superseded: Vec<String> = b2["superseded"]
        .as_array()
        .unwrap()
        .iter()
        .map(|v| v.as_str().unwrap().to_string())
        .collect();
    assert_eq!(superseded, vec![first.to_string()]);

    let snap = rt.state().await;
    assert_eq!(snap.workers[&first].state, WorkerState::Disconnected);
    assert_eq!(
        snap.workers[&first].last_error.as_deref(),
        Some("reregister")
    );
    assert_eq!(snap.workers[&second].state, WorkerState::Idle);
}

#[tokio::test]
async fn leave_marks_worker_disconnected_and_404s_for_unknown_worker() {
    let (app, rt, _store) = fresh_app().await;
    rt.ingest(job_created("bobby")).await.unwrap();
    let wid = register_one(&app, "bobby").await;

    let (status, body) = post_json(
        app.clone(),
        &format!("/workers/{wid}/leave"),
        serde_json::json!({ "reason": "worker exiting" }),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert!(body["seq"].as_u64().unwrap() > 0);

    let snap = rt.state().await;
    let w = &snap.workers[&wid];
    assert_eq!(w.state, WorkerState::Disconnected);
    assert_eq!(w.last_error.as_deref(), Some("worker exiting"));

    let (status, _) = post_json(
        app,
        &format!("/workers/{}/leave", Uuid::new_v4()),
        serde_json::json!({ "reason": "worker exiting" }),
    )
    .await;
    assert_eq!(status, StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn liveness_sweep_disconnects_only_workers_past_the_timeout() {
    let (app, rt, _mem, clock) = fresh_app_with_clock().await;
    rt.ingest(job_created("bobby")).await.unwrap();

    // `quiet` registers, then goes silent; `chatty` registers a
    // minute later (registration counts as a heartbeat).
    let quiet = register_with_host(&app, "bobby", "quiet-node").await;
    clock.advance(chrono::Duration::seconds(60));
    let chatty = register_with_host(&app, "bobby", "chatty-node").await;
    clock.advance(chrono::Duration::seconds(40));

    let swept = rt
        .sweep_stale_workers(chrono::Duration::seconds(90))
        .await
        .unwrap();
    assert_eq!(swept, vec![quiet]);

    let snap = rt.state().await;
    assert_eq!(snap.workers[&quiet].state, WorkerState::Disconnected);
    assert_eq!(
        snap.workers[&quiet].last_error.as_deref(),
        Some("no heartbeat for 100s")
    );
    assert_eq!(snap.workers[&chatty].state, WorkerState::Idle);

    // Already-Disconnected workers are not swept again.
    let again = rt
        .sweep_stale_workers(chrono::Duration::seconds(90))
        .await
        .unwrap();
    assert!(again.is_empty());
}
