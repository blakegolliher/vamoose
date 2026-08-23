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

// =============================================================================
// Ack durability (ledger F03) — a 200 on a command means the state
// change survives a coord crash. An operator who saw "paused: ok"
// must not find the job silently running again after a failover.
// =============================================================================

#[tokio::test]
async fn command_ack_implies_durable() {
    let (app, rt, mem, clock) = fresh_app_with_clock().await;
    rt.ingest(job_created("bobby")).await.unwrap();

    let (status, body) = post_json(app, "/jobs/bobby/pause", None).await;
    assert_eq!(status, StatusCode::OK);
    assert!(body["command_id"].is_string());

    // Crash (no shutdown, no flush) + takeover: advance past the
    // lease ttl + grace and replay from the same store.
    clock.advance(chrono::Duration::seconds(60));
    let store: Arc<dyn CoordStore> = mem.clone();
    let rt2 = CoordRuntime::start(store, clock.clone(), me("B"), rt_cfg())
        .await
        .unwrap();
    let job = rt2
        .job_view(&jid("bobby"))
        .await
        .expect("job must survive the crash — pause was acked with 200");
    assert_eq!(
        job.phase,
        Phase::Paused,
        "acked pause must still hold after crash-restart",
    );
}

// =============================================================================
// Audit durability across the crash window (ledger F22) — audit keys
// are numbered by an in-memory per-day counter that is only persisted
// via snapshots. A crash before the next snapshot rewinds the counter;
// the restarted coord must NOT clobber rows the previous generation
// already wrote.
// =============================================================================

/// Read every audit object under `audit/` into (key, body) pairs.
async fn audit_rows(store: &MemStore) -> Vec<(String, Value)> {
    let mut out = Vec::new();
    for entry in store.list("audit/").await.unwrap() {
        let (body, _) = store.get(&entry.key).await.unwrap().unwrap();
        let line: Value = serde_json::from_slice(body.trim_ascii_end()).unwrap();
        out.push((entry.key, line));
    }
    out
}

#[tokio::test]
async fn audit_rows_survive_crash_window_counter_reset() {
    let (app, rt, mem, clock) = fresh_app_with_clock().await;
    rt.ingest(job_created("bobby")).await.unwrap();

    // Commands A and B take per-day audit seqs 1 and 2. NO snapshot
    // is written (rt_cfg's max_events_per_chunk is 1000 and nothing
    // calls write_snapshot), so the counter exists only in memory.
    let (s, a) = post_json(app.clone(), "/jobs/bobby/pause", None).await;
    assert_eq!(s, StatusCode::OK);
    let (s, b) = post_json(app.clone(), "/jobs/bobby/resume", None).await;
    assert_eq!(s, StatusCode::OK);

    let before = audit_rows(&mem).await;
    assert_eq!(before.len(), 2, "setup: A and B each wrote one row");

    // Crash (no shutdown, no snapshot) + takeover on the same store.
    clock.advance(chrono::Duration::seconds(60));
    let store: Arc<dyn CoordStore> = mem.clone();
    let rt2 = CoordRuntime::start(store, clock.clone(), me("B"), rt_cfg())
        .await
        .unwrap();
    let app2 = build_router(AppState::new(rt2.clone()));

    // Command C on the restarted coord. Its counter rewound to 0 —
    // it must not overwrite A's row at seq 1.
    let (s, c) = post_json(app2, "/jobs/bobby/pause", None).await;
    assert_eq!(s, StatusCode::OK);

    let after = audit_rows(&mem).await;
    assert_eq!(
        after.len(),
        3,
        "three commands must leave three distinct audit objects",
    );
    // A's and B's bodies are intact.
    for (key, line) in &before {
        let found = after.iter().find(|(k, _)| k == key).unwrap_or_else(|| {
            panic!("pre-crash audit key {key} vanished");
        });
        assert_eq!(
            &found.1, line,
            "pre-crash audit row {key} was overwritten by the restarted coord",
        );
    }
    // All three command_ids are present exactly once.
    for cid in [&a["command_id"], &b["command_id"], &c["command_id"]] {
        assert_eq!(
            after
                .iter()
                .filter(|(_, l)| &l["command_id"] == cid)
                .count(),
            1,
            "command_id {cid} must appear in exactly one audit row",
        );
    }
}

#[tokio::test]
async fn audit_write_never_overwrites() {
    let (app, rt, mem, _clock) = fresh_app_with_clock().await;
    rt.ingest(job_created("bobby")).await.unwrap();

    // A prior coord generation already wrote seq 1 for today.
    let sentinel_key = "audit/2026-05-29/00000000000000000001.jsonl";
    mem.put(sentinel_key, b"{\"sentinel\":true}\n".to_vec())
        .await
        .unwrap();
    mem.clear_ops();

    let (status, _) = post_json(app, "/jobs/bobby/pause", None).await;
    assert_eq!(status, StatusCode::OK);

    // The audit path must never issue a plain PUT — only conditional
    // creates (`PUT_IF_ABSENT`), which cannot clobber.
    assert!(
        mem.ops().iter().all(|op| !op.starts_with("PUT audit/")),
        "audit writes must be put_if_absent, got ops: {:?}",
        mem.ops()
            .iter()
            .filter(|op| op.contains("audit/"))
            .collect::<Vec<_>>(),
    );

    // Both rows survive: the sentinel untouched, the new row on the
    // next free seq.
    let rows = audit_rows(&mem).await;
    assert_eq!(rows.len(), 2, "collision must allocate a new key");
    let (sentinel_body, _) = mem.get(sentinel_key).await.unwrap().unwrap();
    assert_eq!(
        sentinel_body, b"{\"sentinel\":true}\n",
        "existing audit row must not be overwritten",
    );
    let new_row = rows.iter().find(|(k, _)| k != sentinel_key).unwrap();
    assert_eq!(new_row.1["action"], "pause");
}

/// Reorder pin (F22 secondary): the audit row asserts a command that
/// took effect, so the durable event write must precede the audit
/// write — and a command whose ingest fails must not leave a lone
/// `Accepted` audit row behind.
#[tokio::test]
async fn failed_ingest_audits_rejected_not_accepted() {
    // Success path: the audit object lands AFTER the event chunk.
    let (app, rt, mem, _clock) = fresh_app_with_clock().await;
    rt.ingest(job_created("bobby")).await.unwrap();
    rt.flush_log().await.unwrap();
    mem.clear_ops();

    let (status, _) = post_json(app.clone(), "/jobs/bobby/pause", None).await;
    assert_eq!(status, StatusCode::OK);
    let ops = mem.ops();
    let audit_at = ops
        .iter()
        .position(|op| op.contains(" audit/"))
        .expect("command must write an audit row");
    let event_at = ops
        .iter()
        .position(|op| op.starts_with("PUT events/"))
        .expect("command must flush its event");
    assert!(
        event_at < audit_at,
        "audit must be written after the event is durable, got ops: {ops:?}",
    );

    // Failure path: drive ingest failure with the lease fence. The
    // audit trail must not claim the command was accepted.
    rt.mark_lease_lost().await;
    let audit_count_before = mem.list("audit/").await.unwrap().len();
    let (status, _) = post_json(app, "/jobs/bobby/resume", None).await;
    assert_eq!(status, StatusCode::INTERNAL_SERVER_ERROR);
    let rows = audit_rows(&mem).await;
    assert_eq!(
        rows.len(),
        audit_count_before,
        "failed command must not add audit rows",
    );
    assert!(
        !rows
            .iter()
            .any(|(_, l)| l["action"] == "resume" && l["result"]["kind"] == "Accepted"),
        "no lone Accepted row may exist for the failed resume",
    );
}

/// Lease-fence composition (ledger F02 x F03): a fenced coord must
/// fail commands outright — no audit row, no event, no 200.
#[tokio::test]
async fn fenced_coord_never_acks_commands() {
    let (app, rt, mem, _clock) = fresh_app_with_clock().await;
    rt.ingest(job_created("bobby")).await.unwrap();
    rt.flush_log().await.unwrap();

    rt.mark_lease_lost().await;
    let writes_before = mem.write_count();

    let (status, _body) = post_json(app, "/jobs/bobby/pause", None).await;
    assert_eq!(
        status,
        StatusCode::INTERNAL_SERVER_ERROR,
        "a fenced coord must not ack an operator command",
    );
    assert_eq!(
        mem.write_count(),
        writes_before,
        "a fenced coord must not write audit rows or event chunks",
    );
}

// =============================================================================
// Phase legality (ledger F25) — commands that make no sense for the
// job's current phase are rejected with 409 before any side effect:
// no event, no audit row, no phase change.
// =============================================================================

#[tokio::test]
async fn pause_on_terminal_job_is_rejected() {
    let (app, rt, mem) = fresh_app().await;
    rt.ingest(job_created("bobby")).await.unwrap();
    let (s, _) = post_json(app.clone(), "/jobs/bobby/cancel", None).await;
    assert_eq!(s, StatusCode::OK);

    let last_seq_before = rt.last_seq().await;
    let audits_before = mem.list("audit/").await.unwrap().len();

    let (status, body) = post_json(app, "/jobs/bobby/pause", None).await;
    assert_eq!(
        status,
        StatusCode::CONFLICT,
        "pausing a cancelled job must 409, got body {body}",
    );
    assert_eq!(body["code"], "invalid_phase");
    let msg = body["message"].as_str().unwrap();
    assert!(
        msg.contains("Cancelled") && msg.contains("pause"),
        "message must name the current phase and the command: {msg}",
    );

    // No side effects leaked.
    assert_eq!(rt.last_seq().await, last_seq_before, "no event ingested");
    assert_eq!(
        mem.list("audit/").await.unwrap().len(),
        audits_before,
        "no audit row for a rejected command",
    );
    assert_eq!(
        rt.job_view(&jid("bobby")).await.unwrap().phase,
        Phase::Cancelled,
        "phase unchanged",
    );
}

#[tokio::test]
async fn resume_on_non_paused_job_is_rejected() {
    let (app, rt, mem) = fresh_app().await;
    rt.ingest(job_created("bobby")).await.unwrap();
    rt.ingest(EventKind::JobPhaseChanged {
        job_id: jid("bobby"),
        from: Phase::Planned,
        to: Phase::Copying,
        reason: "start".into(),
    })
    .await
    .unwrap();
    let history_before = rt.job_view(&jid("bobby")).await.unwrap().phase_history;
    let audits_before = mem.list("audit/").await.unwrap().len();

    // The rewind bug: resume on a never-paused Copying job used to
    // drive Copying -> Planned.
    let (status, body) = post_json(app, "/jobs/bobby/resume", None).await;
    assert_eq!(
        status,
        StatusCode::CONFLICT,
        "resuming a non-paused job must 409, got body {body}",
    );
    assert_eq!(body["code"], "invalid_phase");

    let job = rt.job_view(&jid("bobby")).await.unwrap();
    assert_eq!(job.phase, Phase::Copying, "phase must not rewind");
    assert_eq!(job.phase_history, history_before, "history unchanged");
    assert_eq!(
        mem.list("audit/").await.unwrap().len(),
        audits_before,
        "no audit row for a rejected command",
    );
}
