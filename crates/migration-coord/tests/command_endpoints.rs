//! Integration tests for the command REST endpoints
//! (server::command). Same in-process harness as
//! tests/read_endpoints.rs.

use http::{Request, StatusCode};
use http_body_util::BodyExt;
use migration_coord::lease::{Identity, LeaseConfig};
use migration_coord::runtime::test_clock::FixedClock;
use migration_coord::runtime::{CoordRuntime, RuntimeConfig};
use migration_coord::schema::{ConfigHash, EventKind, JobId, Phase};
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

async fn post_json(router: axum::Router, uri: &str, body: Option<Value>) -> (StatusCode, Value) {
    let body_bytes = body
        .map(|v| serde_json::to_vec(&v).unwrap())
        .unwrap_or_default();
    let mut builder = Request::builder().method("POST").uri(uri);
    if !body_bytes.is_empty() {
        builder = builder.header("content-type", "application/json");
    }
    let req = builder.body(axum::body::Body::from(body_bytes)).unwrap();
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
// pause
// =============================================================================

#[tokio::test]
async fn pause_unknown_job_404s_and_writes_no_audit() {
    let (app, _rt, store) = fresh_app().await;
    let (status, body) = post_json(app, "/jobs/ghost/pause", None).await;
    assert_eq!(status, StatusCode::NOT_FOUND);
    assert_eq!(body["code"], "job_not_found");
    // No audit row written.
    let audit = store.list("audit/").await.unwrap();
    assert!(audit.is_empty(), "404 must not record an audit line");
}

#[tokio::test]
async fn pause_transitions_phase_and_records_audit() {
    let (app, rt, store) = fresh_app().await;
    rt.ingest(job_created("bobby")).await.unwrap();

    let (status, body) = post_json(
        app,
        "/jobs/bobby/pause",
        Some(serde_json::json!({ "reason": "maintenance" })),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert!(body["command_id"].is_string());

    // Audit line written with the reason and target.
    let audit = store.list("audit/").await.unwrap();
    assert_eq!(audit.len(), 1);
    let (audit_body, _) = store.get(&audit[0].key).await.unwrap().unwrap();
    let line: Value = serde_json::from_slice(audit_body.trim_ascii_end()).unwrap();
    assert_eq!(line["action"], "pause");
    assert_eq!(line["target"], "jobs/bobby");
    assert_eq!(line["args"]["reason"], "maintenance");
    assert_eq!(line["result"]["kind"], "Accepted");
    assert_eq!(line["command_id"], body["command_id"]);

    // Phase actually changed.
    let job = rt.job_view(&jid("bobby")).await.unwrap();
    assert_eq!(job.phase, Phase::Paused);
}

#[tokio::test]
async fn pause_with_no_body_uses_default_reason() {
    let (app, rt, store) = fresh_app().await;
    rt.ingest(job_created("bobby")).await.unwrap();

    let (status, _body) = post_json(app, "/jobs/bobby/pause", None).await;
    assert_eq!(status, StatusCode::OK);

    let audit = store.list("audit/").await.unwrap();
    let (audit_body, _) = store.get(&audit[0].key).await.unwrap().unwrap();
    let line: Value = serde_json::from_slice(audit_body.trim_ascii_end()).unwrap();
    assert_eq!(line["args"]["reason"], "operator");
}

// =============================================================================
// resume
// =============================================================================

#[tokio::test]
async fn pause_then_resume_returns_to_prior_phase_via_rest() {
    let (app, rt, _store) = fresh_app().await;
    rt.ingest(job_created("bobby")).await.unwrap();
    rt.ingest(EventKind::JobPhaseChanged {
        job_id: jid("bobby"),
        from: Phase::Planned,
        to: Phase::Copying,
        reason: "start".into(),
    })
    .await
    .unwrap();

    let (s1, _) = post_json(app.clone(), "/jobs/bobby/pause", None).await;
    assert_eq!(s1, StatusCode::OK);
    assert_eq!(
        rt.job_view(&jid("bobby")).await.unwrap().phase,
        Phase::Paused
    );

    let (s2, _) = post_json(app, "/jobs/bobby/resume", None).await;
    assert_eq!(s2, StatusCode::OK);
    assert_eq!(
        rt.job_view(&jid("bobby")).await.unwrap().phase,
        Phase::Copying,
    );
}

// =============================================================================
// cancel
// =============================================================================

#[tokio::test]
async fn cancel_transitions_to_cancelled() {
    let (app, rt, _store) = fresh_app().await;
    rt.ingest(job_created("bobby")).await.unwrap();
    let (status, _) = post_json(app, "/jobs/bobby/cancel", None).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(
        rt.job_view(&jid("bobby")).await.unwrap().phase,
        Phase::Cancelled,
    );
}

// =============================================================================
// drain
// =============================================================================

#[tokio::test]
async fn drain_pauses_with_drain_reason() {
    let (app, rt, store) = fresh_app().await;
    rt.ingest(job_created("bobby")).await.unwrap();
    let (status, _) = post_json(app, "/jobs/bobby/drain", None).await;
    assert_eq!(status, StatusCode::OK);

    // Phase transitioned to Paused (drain reuses Paused with a distinct reason).
    let job = rt.job_view(&jid("bobby")).await.unwrap();
    assert_eq!(job.phase, Phase::Paused);
    let last = job.phase_history.last().unwrap();
    assert_eq!(last.reason, "drain");

    let audit = store.list("audit/").await.unwrap();
    let (audit_body, _) = store.get(&audit[0].key).await.unwrap().unwrap();
    let line: Value = serde_json::from_slice(audit_body.trim_ascii_end()).unwrap();
    assert_eq!(line["action"], "drain");
}

// =============================================================================
// retry-failed
// =============================================================================

#[tokio::test]
async fn retry_failed_records_audit_only() {
    let (app, rt, store) = fresh_app().await;
    rt.ingest(job_created("bobby")).await.unwrap();
    let last_seq_before = rt.last_seq().await;

    let (status, _) = post_json(app, "/jobs/bobby/retry-failed", None).await;
    assert_eq!(status, StatusCode::OK);

    // No new event ingested — last_seq unchanged.
    assert_eq!(rt.last_seq().await, last_seq_before);
    // But audit row was written.
    let audit = store.list("audit/").await.unwrap();
    assert_eq!(audit.len(), 1);
    let (audit_body, _) = store.get(&audit[0].key).await.unwrap().unwrap();
    let line: Value = serde_json::from_slice(audit_body.trim_ascii_end()).unwrap();
    assert_eq!(line["action"], "retry-failed");
}

// =============================================================================
// Audit sequencing
// =============================================================================

#[tokio::test]
async fn multiple_commands_assign_increasing_audit_seqs_within_a_day() {
    let (app, rt, store) = fresh_app().await;
    rt.ingest(job_created("bobby")).await.unwrap();

    for _ in 0..3 {
        let (s, _) = post_json(app.clone(), "/jobs/bobby/pause", None).await;
        assert_eq!(s, StatusCode::OK);
        let (s, _) = post_json(app.clone(), "/jobs/bobby/resume", None).await;
        assert_eq!(s, StatusCode::OK);
    }
    let audit = store.list("audit/").await.unwrap();
    assert_eq!(audit.len(), 6);
    // Keys are zero-padded seqs under a single date prefix.
    for (i, e) in audit.iter().enumerate() {
        let expected_seq = format!("{:020}", i + 1);
        assert!(
            e.key.contains(&expected_seq),
            "key {} should contain seq {expected_seq}",
            e.key,
        );
    }
}

#[tokio::test]
async fn audit_rolls_over_on_new_utc_day() {
    let mem = Arc::new(MemStore::new());
    let store: Arc<dyn CoordStore> = mem.clone();
    let clock = FixedClock::new(
        chrono::DateTime::parse_from_rfc3339("2026-05-29T23:59:00Z")
            .unwrap()
            .with_timezone(&chrono::Utc),
    );
    let rt = CoordRuntime::start(store, clock.clone(), me("A"), rt_cfg())
        .await
        .unwrap();
    let router = build_router(AppState::new(rt.clone()));

    rt.ingest(job_created("bobby")).await.unwrap();
    let (s, _) = post_json(router.clone(), "/jobs/bobby/pause", None).await;
    assert_eq!(s, StatusCode::OK);

    // Advance past midnight UTC.
    clock.advance(chrono::Duration::minutes(2));
    let (s, _) = post_json(router, "/jobs/bobby/resume", None).await;
    assert_eq!(s, StatusCode::OK);

    let day1 = mem.list("audit/2026-05-29/").await.unwrap();
    let day2 = mem.list("audit/2026-05-30/").await.unwrap();
    assert_eq!(day1.len(), 1);
    assert_eq!(day2.len(), 1);
    // Each day's counter starts at 1.
    assert!(day1[0].key.contains("00000000000000000001"));
    assert!(day2[0].key.contains("00000000000000000001"));
}
