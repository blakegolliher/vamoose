//! Integration tests for the auth middleware. Same in-process
//! harness as the other endpoint test files.

use http::{Request, StatusCode};
use http_body_util::BodyExt;
use migration_coord::lease::{Identity, LeaseConfig};
use migration_coord::runtime::test_clock::FixedClock;
use migration_coord::runtime::{CoordRuntime, RuntimeConfig};
use migration_coord::schema::{ConfigHash, EventKind, JobId};
use migration_coord::server::auth::AuthConfig;
use migration_coord::server::{build_router, AppState};
use migration_coord::store::{CoordStore, MemStore};
use serde_json::Value;
use std::sync::Arc;
use tower::ServiceExt;

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

async fn fresh_app_with(auth: AuthConfig) -> (axum::Router, CoordRuntime, Arc<MemStore>) {
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
    let router = build_router(AppState::with_auth(rt.clone(), auth));
    (router, rt, mem)
}

fn admin_config() -> AuthConfig {
    let mut cfg = AuthConfig::default();
    cfg.admin_tokens.insert("good-token".into(), "blake".into());
    cfg.cluster_secret = Some("cluster-secret".into());
    cfg
}

async fn send(router: axum::Router, req: Request<axum::body::Body>) -> (StatusCode, Value) {
    let resp = router.oneshot(req).await.unwrap();
    let status = resp.status();
    let body_bytes = resp.into_body().collect().await.unwrap().to_bytes();
    let json: Value = if body_bytes.is_empty() {
        Value::Null
    } else {
        serde_json::from_slice(&body_bytes).unwrap_or(Value::Null)
    };
    (status, json)
}

// =============================================================================
// Healthz is always public
// =============================================================================

#[tokio::test]
async fn healthz_reachable_without_credentials() {
    let (app, _rt, _store) = fresh_app_with(admin_config()).await;
    let req = Request::builder()
        .uri("/healthz")
        .body(axum::body::Body::empty())
        .unwrap();
    let (status, body) = send(app, req).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["status"], "ok");
}

// =============================================================================
// Admin endpoints
// =============================================================================

#[tokio::test]
async fn admin_endpoint_rejects_missing_authorization() {
    let (app, _rt, _store) = fresh_app_with(admin_config()).await;
    let req = Request::builder()
        .uri("/jobs")
        .body(axum::body::Body::empty())
        .unwrap();
    let (status, body) = send(app, req).await;
    assert_eq!(status, StatusCode::UNAUTHORIZED);
    assert_eq!(body["code"], "missing_token");
}

#[tokio::test]
async fn admin_endpoint_rejects_unknown_token() {
    let (app, _rt, _store) = fresh_app_with(admin_config()).await;
    let req = Request::builder()
        .uri("/jobs")
        .header("authorization", "Bearer wrong")
        .body(axum::body::Body::empty())
        .unwrap();
    let (status, body) = send(app, req).await;
    assert_eq!(status, StatusCode::UNAUTHORIZED);
    assert_eq!(body["code"], "invalid_token");
}

#[tokio::test]
async fn admin_endpoint_accepts_valid_token() {
    let (app, _rt, _store) = fresh_app_with(admin_config()).await;
    let req = Request::builder()
        .uri("/jobs")
        .header("authorization", "Bearer good-token")
        .body(axum::body::Body::empty())
        .unwrap();
    let (status, _body) = send(app, req).await;
    assert_eq!(status, StatusCode::OK);
}

#[tokio::test]
async fn command_audit_records_token_label() {
    let (app, rt, store) = fresh_app_with(admin_config()).await;
    rt.ingest(job_created("bobby")).await.unwrap();

    let req = Request::builder()
        .method("POST")
        .uri("/jobs/bobby/pause")
        .header("authorization", "Bearer good-token")
        .body(axum::body::Body::empty())
        .unwrap();
    let (status, _) = send(app, req).await;
    assert_eq!(status, StatusCode::OK);

    let audit = store.list("audit/").await.unwrap();
    let (audit_body, _) = store.get(&audit[0].key).await.unwrap().unwrap();
    let line: Value = serde_json::from_slice(audit_body.trim_ascii_end()).unwrap();
    assert_eq!(line["token_label"], "blake");
}

// =============================================================================
// Worker endpoints
// =============================================================================

#[tokio::test]
async fn worker_endpoint_rejects_missing_cluster_secret() {
    let (app, rt, _store) = fresh_app_with(admin_config()).await;
    rt.ingest(job_created("bobby")).await.unwrap();
    let body = serde_json::json!({
        "job_id": "bobby",
        "host": "h",
        "pid": 1,
        "start_time": "2026-01-01T00:00:00Z",
        "version": "0.6",
    });
    let req = Request::builder()
        .method("POST")
        .uri("/workers/register")
        .header("content-type", "application/json")
        .body(axum::body::Body::from(serde_json::to_vec(&body).unwrap()))
        .unwrap();
    let (status, json) = send(app, req).await;
    assert_eq!(status, StatusCode::UNAUTHORIZED);
    assert_eq!(json["code"], "missing_cluster_secret");
}

#[tokio::test]
async fn worker_endpoint_rejects_wrong_cluster_secret() {
    let (app, rt, _store) = fresh_app_with(admin_config()).await;
    rt.ingest(job_created("bobby")).await.unwrap();
    let body = serde_json::json!({
        "job_id": "bobby",
        "host": "h",
        "pid": 1,
        "start_time": "2026-01-01T00:00:00Z",
        "version": "0.6",
    });
    let req = Request::builder()
        .method("POST")
        .uri("/workers/register")
        .header("content-type", "application/json")
        .header("x-cluster-secret", "wrong")
        .body(axum::body::Body::from(serde_json::to_vec(&body).unwrap()))
        .unwrap();
    let (status, json) = send(app, req).await;
    assert_eq!(status, StatusCode::UNAUTHORIZED);
    assert_eq!(json["code"], "invalid_cluster_secret");
}

#[tokio::test]
async fn worker_endpoint_accepts_correct_cluster_secret() {
    let (app, rt, _store) = fresh_app_with(admin_config()).await;
    rt.ingest(job_created("bobby")).await.unwrap();
    let body = serde_json::json!({
        "job_id": "bobby",
        "host": "h",
        "pid": 1,
        "start_time": "2026-01-01T00:00:00Z",
        "version": "0.6",
    });
    let req = Request::builder()
        .method("POST")
        .uri("/workers/register")
        .header("content-type", "application/json")
        .header("x-cluster-secret", "cluster-secret")
        .body(axum::body::Body::from(serde_json::to_vec(&body).unwrap()))
        .unwrap();
    let (status, json) = send(app, req).await;
    assert_eq!(status, StatusCode::OK);
    assert!(json["worker_id"].is_string());
}

// =============================================================================
// Dev mode
// =============================================================================

#[tokio::test]
async fn dev_mode_passes_through_admin_endpoints() {
    let (app, _rt, _store) = fresh_app_with(AuthConfig::default()).await;
    let req = Request::builder()
        .uri("/jobs")
        .body(axum::body::Body::empty())
        .unwrap();
    let (status, _) = send(app, req).await;
    assert_eq!(status, StatusCode::OK);
}

#[tokio::test]
async fn dev_mode_passes_through_worker_endpoints() {
    let (app, rt, _store) = fresh_app_with(AuthConfig::default()).await;
    rt.ingest(job_created("bobby")).await.unwrap();
    let body = serde_json::json!({
        "job_id": "bobby",
        "host": "h",
        "pid": 1,
        "start_time": "2026-01-01T00:00:00Z",
        "version": "0.6",
    });
    let req = Request::builder()
        .method("POST")
        .uri("/workers/register")
        .header("content-type", "application/json")
        .body(axum::body::Body::from(serde_json::to_vec(&body).unwrap()))
        .unwrap();
    let (status, _) = send(app, req).await;
    assert_eq!(status, StatusCode::OK);
}

#[tokio::test]
async fn dev_mode_stamps_audit_label_as_dev_mode() {
    let (app, rt, store) = fresh_app_with(AuthConfig::default()).await;
    rt.ingest(job_created("bobby")).await.unwrap();
    let req = Request::builder()
        .method("POST")
        .uri("/jobs/bobby/pause")
        .body(axum::body::Body::empty())
        .unwrap();
    let (status, _) = send(app, req).await;
    assert_eq!(status, StatusCode::OK);

    let audit = store.list("audit/").await.unwrap();
    let (audit_body, _) = store.get(&audit[0].key).await.unwrap().unwrap();
    let line: Value = serde_json::from_slice(audit_body.trim_ascii_end()).unwrap();
    assert_eq!(line["token_label"], "dev-mode");
}
