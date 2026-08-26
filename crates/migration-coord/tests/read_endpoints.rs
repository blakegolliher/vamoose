//! Integration tests for the read-only REST endpoints
//! (server::read). Wire layer is axum::Router; tests drive it via
//! `tower::ServiceExt::oneshot` so no real TCP listener is needed.

use http::{Request, StatusCode};
use http_body_util::BodyExt;
use migration_coord::lease::{Identity, LeaseConfig};
use migration_coord::runtime::test_clock::FixedClock;
use migration_coord::runtime::{CoordRuntime, RuntimeConfig};
use migration_coord::schema::{ConfigHash, EventKind, JobId, WorkerId};
use migration_coord::server::{build_router, AppState};
use migration_coord::store::{CoordStore, MemStore};
use serde_json::Value;
use std::sync::Arc;
use tower::ServiceExt;

// =============================================================================
// Helpers
// =============================================================================

fn me(holder: &str) -> Identity {
    Identity {
        holder_id: holder.into(),
        host: "h".into(),
        pid: 1,
    }
}

fn cfg() -> RuntimeConfig {
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

async fn fresh_app() -> (axum::Router, CoordRuntime) {
    let mem = Arc::new(MemStore::new());
    let store: Arc<dyn CoordStore> = mem.clone();
    let clock = FixedClock::new(
        chrono::DateTime::parse_from_rfc3339("2026-05-29T14:32:00Z")
            .unwrap()
            .with_timezone(&chrono::Utc),
    );
    let rt = CoordRuntime::start(store, clock, me("A"), cfg())
        .await
        .unwrap();
    let router = build_router(AppState::new(rt.clone()));
    (router, rt)
}

fn jid(s: &str) -> JobId {
    JobId::new(s).unwrap()
}

fn job_created(j: &str) -> EventKind {
    EventKind::JobCreated {
        job_id: jid(j),
        name: format!("{j}-migration"),
        source: "nfs://src".into(),
        dest: "nfs://dst".into(),
        owner: "test".into(),
        config_hash: ConfigHash("deadbeef".into()),
        total_files: 0,
        total_bytes: 0,
    }
}

fn progress_delta(j: &str) -> EventKind {
    EventKind::ProgressDelta {
        job_id: jid(j),
        worker_id: WorkerId::new(),
        files_delta: 10,
        bytes_delta: 1024,
        errors_delta: 0,
    }
}

fn worker_joined(job: &str, wid: WorkerId) -> EventKind {
    EventKind::WorkerJoined {
        worker_id: wid,
        job_id: jid(job),
        host: "h".into(),
        pid: 42,
        start_time: chrono::DateTime::<chrono::Utc>::from_timestamp(0, 0).unwrap(),
        version: "0.6".into(),
    }
}

fn error_emitted(j: &str) -> EventKind {
    EventKind::ErrorEmitted {
        job_id: jid(j),
        worker_id: WorkerId::new(),
        class: migration_coord::schema::ErrorClass::Nfs3Err(13),
        path: "/a/b/c".into(),
        retryable: true,
        message: "EACCES".into(),
    }
}

async fn get_json(router: axum::Router, uri: &str) -> (StatusCode, Value) {
    let req = Request::builder()
        .uri(uri)
        .body(axum::body::Body::empty())
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
// /healthz
// =============================================================================

#[tokio::test]
async fn healthz_ok_on_fresh_runtime() {
    let (app, _rt) = fresh_app().await;
    let (status, body) = get_json(app, "/healthz").await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["status"], "ok");
    assert_eq!(body["last_seq"], 0);
    assert_eq!(body["lease_lost"], false);
}

#[tokio::test]
async fn healthz_service_unavailable_when_lease_lost() {
    let (app, rt) = fresh_app().await;
    rt.mark_lease_lost().await;
    let (status, body) = get_json(app, "/healthz").await;
    assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
    assert_eq!(body["status"], "lease_lost");
    assert_eq!(body["lease_lost"], true);
}

// =============================================================================
// /jobs
// =============================================================================

#[tokio::test]
async fn list_jobs_returns_empty_array_when_no_jobs() {
    let (app, _rt) = fresh_app().await;
    let (status, body) = get_json(app, "/jobs").await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["jobs"], serde_json::json!([]));
    assert_eq!(body["next_cursor"], Value::Null);
}

#[tokio::test]
async fn list_jobs_returns_all_jobs_sorted_by_id() {
    let (app, rt) = fresh_app().await;
    rt.ingest(job_created("charlie")).await.unwrap();
    rt.ingest(job_created("alice")).await.unwrap();
    rt.ingest(job_created("bob")).await.unwrap();
    let (status, body) = get_json(app, "/jobs").await;
    assert_eq!(status, StatusCode::OK);
    let ids: Vec<&str> = body["jobs"]
        .as_array()
        .unwrap()
        .iter()
        .map(|j| j["id"].as_str().unwrap())
        .collect();
    assert_eq!(ids, vec!["alice", "bob", "charlie"]);
    assert_eq!(body["next_cursor"], Value::Null);
}

#[tokio::test]
async fn list_jobs_paginates_via_cursor() {
    let (app, rt) = fresh_app().await;
    for name in &["alice", "bob", "charlie", "dave"] {
        rt.ingest(job_created(name)).await.unwrap();
    }
    // First page of 2 — expect alice, bob; cursor = "bob".
    let (_, page1) = get_json(app.clone(), "/jobs?limit=2").await;
    let ids1: Vec<&str> = page1["jobs"]
        .as_array()
        .unwrap()
        .iter()
        .map(|j| j["id"].as_str().unwrap())
        .collect();
    assert_eq!(ids1, vec!["alice", "bob"]);
    assert_eq!(page1["next_cursor"], "bob");

    // Second page from cursor — expect charlie, dave; no further cursor.
    let (_, page2) = get_json(app, "/jobs?cursor=bob&limit=2").await;
    let ids2: Vec<&str> = page2["jobs"]
        .as_array()
        .unwrap()
        .iter()
        .map(|j| j["id"].as_str().unwrap())
        .collect();
    assert_eq!(ids2, vec!["charlie", "dave"]);
    assert_eq!(page2["next_cursor"], Value::Null);
}

// =============================================================================
// /jobs/{id}
// =============================================================================

#[tokio::test]
async fn get_job_returns_404_for_unknown_id() {
    let (app, _rt) = fresh_app().await;
    let (status, body) = get_json(app, "/jobs/ghost").await;
    assert_eq!(status, StatusCode::NOT_FOUND);
    assert_eq!(body["code"], "job_not_found");
}

#[tokio::test]
async fn get_job_returns_full_job_view() {
    let (app, rt) = fresh_app().await;
    rt.ingest(job_created("bobby")).await.unwrap();
    rt.ingest(progress_delta("bobby")).await.unwrap();

    let (status, body) = get_json(app, "/jobs/bobby").await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["id"], "bobby");
    assert_eq!(body["progress"]["files_done"], 10);
    assert_eq!(body["progress"]["bytes_done"], 1024);
    // The fixture ingested a ProgressDelta, so the reducer derives
    // Copying (auto phase transition on first delta).
    assert_eq!(body["phase"], "Copying");
}

// =============================================================================
// /jobs/{id}/workers
// =============================================================================

#[tokio::test]
async fn list_workers_returns_404_for_unknown_job() {
    let (app, _rt) = fresh_app().await;
    let (status, body) = get_json(app, "/jobs/ghost/workers").await;
    assert_eq!(status, StatusCode::NOT_FOUND);
    assert_eq!(body["code"], "job_not_found");
}

#[tokio::test]
async fn list_workers_returns_assigned_workers() {
    let (app, rt) = fresh_app().await;
    rt.ingest(job_created("bobby")).await.unwrap();
    let w1 = WorkerId::new();
    let w2 = WorkerId::new();
    rt.ingest(worker_joined("bobby", w1)).await.unwrap();
    rt.ingest(worker_joined("bobby", w2)).await.unwrap();

    let (status, body) = get_json(app, "/jobs/bobby/workers").await;
    assert_eq!(status, StatusCode::OK);
    let workers = body["workers"].as_array().unwrap();
    assert_eq!(workers.len(), 2);
}

// =============================================================================
// /jobs/{id}/errors
// =============================================================================

#[tokio::test]
async fn list_errors_empty_for_quiet_job() {
    let (app, rt) = fresh_app().await;
    rt.ingest(job_created("bobby")).await.unwrap();
    let (status, body) = get_json(app, "/jobs/bobby/errors").await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["buckets"], serde_json::json!([]));
}

#[tokio::test]
async fn list_errors_aggregates_by_class() {
    let (app, rt) = fresh_app().await;
    rt.ingest(job_created("bobby")).await.unwrap();
    for _ in 0..3 {
        rt.ingest(error_emitted("bobby")).await.unwrap();
    }
    let (status, body) = get_json(app, "/jobs/bobby/errors").await;
    assert_eq!(status, StatusCode::OK);
    let buckets = body["buckets"].as_array().unwrap();
    assert_eq!(buckets.len(), 1);
    assert_eq!(buckets[0]["count"], 3);
}

#[tokio::test]
async fn list_errors_404_for_unknown_job() {
    let (app, _rt) = fresh_app().await;
    let (status, _) = get_json(app, "/jobs/ghost/errors").await;
    assert_eq!(status, StatusCode::NOT_FOUND);
}

// =============================================================================
// /jobs/{id}/events
// =============================================================================

#[tokio::test]
async fn list_events_returns_ascending_log_since_param() {
    let (app, rt) = fresh_app().await;
    rt.ingest(job_created("bobby")).await.unwrap();
    rt.ingest(progress_delta("bobby")).await.unwrap();
    rt.ingest(progress_delta("bobby")).await.unwrap();
    rt.flush_log().await.unwrap();

    let (status, body) = get_json(app, "/jobs/bobby/events?since=1").await;
    assert_eq!(status, StatusCode::OK);
    let seqs: Vec<u64> = body["events"]
        .as_array()
        .unwrap()
        .iter()
        .map(|e| e["seq"].as_u64().unwrap())
        .collect();
    assert_eq!(seqs, vec![2, 3]);
    assert_eq!(body["next_since"], 3);
}

#[tokio::test]
async fn list_events_404_for_unknown_job() {
    let (app, _rt) = fresh_app().await;
    let (status, _) = get_json(app, "/jobs/ghost/events").await;
    assert_eq!(status, StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn list_events_respects_limit_cap() {
    let (app, rt) = fresh_app().await;
    rt.ingest(job_created("bobby")).await.unwrap();
    for _ in 0..10 {
        rt.ingest(progress_delta("bobby")).await.unwrap();
    }
    rt.flush_log().await.unwrap();

    let (status, body) = get_json(app, "/jobs/bobby/events?limit=3").await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["events"].as_array().unwrap().len(), 3);
    assert_eq!(body["next_since"], 3);
}

// =============================================================================
// /events (cluster-wide)
// =============================================================================

#[tokio::test]
async fn list_all_events_includes_cluster_and_per_job() {
    let (app, rt) = fresh_app().await;
    rt.ingest(job_created("bobby")).await.unwrap();
    rt.ingest(EventKind::WorkerLeft {
        worker_id: WorkerId::new(),
        reason: "drain".into(),
    })
    .await
    .unwrap();
    rt.flush_log().await.unwrap();

    let (status, body) = get_json(app, "/events").await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["events"].as_array().unwrap().len(), 2);
}

// =============================================================================
// Invalid input
// =============================================================================

#[tokio::test]
async fn invalid_job_id_with_slash_in_path_404s_at_router() {
    // axum routes `/jobs/{id}` as a single segment — a `/` in the id
    // is parsed as a different route. We don't get to the handler.
    let (app, _rt) = fresh_app().await;
    let (status, _) = get_json(app, "/jobs/bad/id").await;
    // Could be 404 (no route) or 405; in either case it's not 200.
    assert_ne!(status, StatusCode::OK);
}

// =============================================================================
// /prepare
// =============================================================================

/// `GET /prepare` never 404s: `progress` is null until `vamoose
/// prepare` has written to the bucket, then it is what the coord last
/// read, with its age by the coord's clock.
#[tokio::test]
async fn prepare_is_null_then_reflects_what_the_coord_read() {
    let (router, rt) = fresh_app().await;
    let (status, body) = get_json(router.clone(), "/prepare").await;
    assert_eq!(status, StatusCode::OK);
    assert!(body["progress"].is_null(), "{body}");
    assert!(body["age_secs"].is_null(), "{body}");

    let updated = chrono::DateTime::parse_from_rfc3339("2026-05-29T14:31:30Z")
        .unwrap()
        .with_timezone(&chrono::Utc);
    rt.set_prepare_progress(Some(migration_coord::schema::PrepareProgress {
        schema_version: migration_coord::schema::PREPARE_PROGRESS_SCHEMA_VERSION,
        run_id: "run-1".into(),
        host: "node1".into(),
        pid: 42,
        source: "nfs://s/x".into(),
        dest: "nfs://d/y/v3".into(),
        phase: migration_coord::schema::PreparePhase::Index,
        started_utc: updated,
        updated_utc: updated,
        scan: migration_coord::schema::PrepareScan {
            files: 603_266_804,
            dirs: 4_208_101,
            errors: 0,
            rate_per_sec: 213_000,
            elapsed_secs: 2829,
            complete: true,
        },
        index: migration_coord::schema::PrepareIndex {
            shards_total: Some(320),
            shards_rewritten: 129,
            shards_uploaded: 128,
            rows_uploaded: 246_528_665,
            bytes_uploaded: 12_400_000_000,
        },
        message: None,
    }))
    .await;
    let (status, body) = get_json(router, "/prepare").await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["progress"]["phase"], "index");
    assert_eq!(body["progress"]["scan"]["files"], 603_266_804u64);
    assert_eq!(body["progress"]["index"]["shards_total"], 320);
    // FixedClock is 14:32:00; the object says 14:31:30.
    assert_eq!(body["age_secs"], 30);
}
