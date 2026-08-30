//! End-to-end test for `CoordClient` against an in-process coord.
//!
//! Brings up the real coord router on a loopback port (same pattern
//! as `migration-coord/tests/sse_e2e.rs`) and exercises the four
//! HTTP methods the worker uses:
//!
//! - register → returns WorkerId
//! - heartbeat → returns control envelope (mode tracks job phase)
//! - events_batch → coord assigns seqs
//! - fence → coord emits WorkerFenced
//!
//! Plus a few sad paths: bad cluster secret → 401, unknown worker →
//! 404, retryability classification.

use chrono::{TimeZone, Utc};
use migration_control_protocol::schema::HeartbeatBody;
use migration_control_protocol::schema::{
    ConfigHash, EventEnvelope, EventKind, JobId, WorkerState, SCHEMA_VERSION,
};
use migration_coord::lease::{Identity, LeaseConfig};
use migration_coord::runtime::test_clock::FixedClock;
use migration_coord::runtime::{CoordRuntime, RuntimeConfig};
use migration_coord::server::auth::AuthConfig;
use migration_coord::server::{build_router, AppState};
use migration_coord::store::{CoordStore, MemStore};
use migration_worker::coord_client::{CoordClient, CoordError};
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;
use tokio_util::sync::CancellationToken;

// =============================================================================
// Test scaffolding — keep parallel to sse_e2e.rs intentionally.
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

/// Spin up a coord on a loopback port. `auth` is plumbed verbatim so
/// callers can test dev-mode and the cluster-secret-required mode.
async fn spawn_coord(auth: AuthConfig) -> (CoordRuntime, SocketAddr, CancellationToken) {
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
    let router = build_router(AppState::with_auth(rt.clone(), auth));

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
    tokio::time::sleep(Duration::from_millis(50)).await;
    (rt, addr, shutdown)
}

fn client_for(addr: SocketAddr, cluster_secret: Option<&str>) -> CoordClient {
    CoordClient::new(
        format!("http://{addr}"),
        cluster_secret,
        true, // verify_tls: plain http, ignored
        Duration::from_secs(5),
    )
    .expect("CoordClient::new")
}

// =============================================================================
// Happy path — all four methods end-to-end
// =============================================================================

#[tokio::test]
async fn register_heartbeat_events_fence_end_to_end() {
    let (rt, addr, shutdown) = spawn_coord(AuthConfig::default()).await;
    rt.ingest(job_created("bobby")).await.unwrap();

    let client = client_for(addr, None);
    let job = jid("bobby");

    // 1. Register.
    let reg = client
        .register(
            &job,
            "host-a".into(),
            4242,
            Utc.timestamp_opt(1_700_000_000, 0).unwrap(),
            "0.6.0".into(),
        )
        .await
        .expect("register");
    let worker_id = reg.worker_id;
    assert!(reg.superseded.is_empty());

    // 2. Heartbeat — running job → ControlMode::Run.
    let hb = client
        .heartbeat(
            worker_id,
            HeartbeatBody {
                state: WorkerState::Copying,
                files_per_sec: 5.0,
                bytes_per_sec: 1_000_000.0,
                errors_per_min: 0.0,
                inflight_ops: 2,
                queue_depth: 10,
                latency: None,
            },
        )
        .await
        .expect("heartbeat");
    assert_eq!(
        hb.control.mode,
        migration_worker::coord_client::ControlMode::Run
    );
    // last_seq from heartbeat matches the runtime's view.
    assert_eq!(hb.last_seq, rt.last_seq().await);

    // 3. Events batch — two ProgressDeltas.
    let last_seq_before = rt.last_seq().await;
    let events = vec![
        EventEnvelope {
            seq: 0, // ignored — coord assigns
            at: Utc.timestamp_opt(1_700_000_001, 0).unwrap(),
            schema_version: SCHEMA_VERSION,
            worker_at: None,
            client_seq: None,
            from_worker: None,
            kind: EventKind::ProgressDelta {
                job_id: job.clone(),
                worker_id,
                files_delta: 4,
                bytes_delta: 4096,
                errors_delta: 0,
            },
        },
        EventEnvelope {
            seq: 0,
            at: Utc.timestamp_opt(1_700_000_002, 0).unwrap(),
            schema_version: SCHEMA_VERSION,
            worker_at: None,
            client_seq: None,
            from_worker: None,
            kind: EventKind::ProgressDelta {
                job_id: job.clone(),
                worker_id,
                files_delta: 6,
                bytes_delta: 8192,
                errors_delta: 1,
            },
        },
    ];
    let seqs = client
        .events_batch(worker_id, events)
        .await
        .expect("events_batch");
    assert_eq!(seqs.len(), 2);
    assert_eq!(seqs[0], last_seq_before + 1);
    assert_eq!(seqs[1], last_seq_before + 2);
    // Counters folded.
    let job_view = rt.job_view(&job).await.unwrap();
    assert_eq!(job_view.progress.files_done, 10);
    assert_eq!(job_view.progress.bytes_done, 12_288);

    // 4. Fence.
    let fr = client
        .fence(worker_id, "self-fence R7".into())
        .await
        .expect("fence");
    assert!(fr.seq > 0);
    let snap = rt.state().await;
    let w = &snap.workers[&worker_id];
    assert_eq!(w.state, WorkerState::Fenced);
    assert_eq!(w.fence_reason.as_deref(), Some("self-fence R7"));

    shutdown.cancel();
}

// =============================================================================
// Control envelope tracks job phase across heartbeats
// =============================================================================

#[tokio::test]
async fn heartbeat_control_mode_flips_when_job_pauses() {
    let (rt, addr, shutdown) = spawn_coord(AuthConfig::default()).await;
    rt.ingest(job_created("bobby")).await.unwrap();
    let client = client_for(addr, None);
    let reg = client
        .register(
            &jid("bobby"),
            "host-a".into(),
            42,
            Utc.timestamp_opt(1_700_000_000, 0).unwrap(),
            "0.6.0".into(),
        )
        .await
        .unwrap();

    let body = || HeartbeatBody {
        state: WorkerState::Idle,
        files_per_sec: 0.0,
        bytes_per_sec: 0.0,
        errors_per_min: 0.0,
        inflight_ops: 0,
        queue_depth: 0,
        latency: None,
    };

    let hb1 = client.heartbeat(reg.worker_id, body()).await.unwrap();
    assert_eq!(
        hb1.control.mode,
        migration_worker::coord_client::ControlMode::Run
    );

    rt.ingest(EventKind::JobPaused {
        job_id: jid("bobby"),
        reason: "operator".into(),
    })
    .await
    .unwrap();

    let hb2 = client.heartbeat(reg.worker_id, body()).await.unwrap();
    assert_eq!(
        hb2.control.mode,
        migration_worker::coord_client::ControlMode::Pause
    );
    // last_seq advanced because JobPaused was ingested as an event.
    assert!(hb2.last_seq > hb1.last_seq);

    shutdown.cancel();
}

// =============================================================================
// Auth — cluster secret is required when configured
// =============================================================================

fn cluster_secret_auth() -> AuthConfig {
    AuthConfig {
        admin_tokens: Default::default(),
        cluster_secret: Some("the-secret".into()),
    }
}

#[tokio::test]
async fn missing_cluster_secret_returns_401_unauthorized() {
    let (rt, addr, shutdown) = spawn_coord(cluster_secret_auth()).await;
    rt.ingest(job_created("bobby")).await.unwrap();
    let client = client_for(addr, None);
    let err = client
        .register(
            &jid("bobby"),
            "host-a".into(),
            42,
            Utc.timestamp_opt(1_700_000_000, 0).unwrap(),
            "0.6.0".into(),
        )
        .await
        .expect_err("must reject");
    match err {
        CoordError::Http { status, .. } => {
            assert_eq!(status, reqwest::StatusCode::UNAUTHORIZED);
        }
        other => panic!("unexpected error variant: {other:?}"),
    }
    // 401 is NOT retryable — it's a config bug.
    let err = client
        .register(
            &jid("bobby"),
            "host-a".into(),
            42,
            Utc.timestamp_opt(1_700_000_000, 0).unwrap(),
            "0.6.0".into(),
        )
        .await
        .expect_err("must reject");
    assert!(!err.is_retryable());
    shutdown.cancel();
}

#[tokio::test]
async fn correct_cluster_secret_passes() {
    let (rt, addr, shutdown) = spawn_coord(cluster_secret_auth()).await;
    rt.ingest(job_created("bobby")).await.unwrap();
    let client = client_for(addr, Some("the-secret"));
    let reg = client
        .register(
            &jid("bobby"),
            "host-a".into(),
            42,
            Utc.timestamp_opt(1_700_000_000, 0).unwrap(),
            "0.6.0".into(),
        )
        .await
        .expect("register with correct secret");
    assert!(!reg.worker_id.to_string().is_empty());
    shutdown.cancel();
}

// =============================================================================
// Sad path — unknown worker / unknown job
// =============================================================================

#[tokio::test]
async fn unknown_job_register_returns_404() {
    let (_rt, addr, shutdown) = spawn_coord(AuthConfig::default()).await;
    let client = client_for(addr, None);
    let err = client
        .register(
            &jid("ghost"),
            "host-a".into(),
            42,
            Utc.timestamp_opt(1_700_000_000, 0).unwrap(),
            "0.6.0".into(),
        )
        .await
        .expect_err("must fail");
    match err {
        CoordError::Http { status, .. } => {
            assert_eq!(status, reqwest::StatusCode::NOT_FOUND);
        }
        other => panic!("unexpected: {other:?}"),
    }
    shutdown.cancel();
}

#[tokio::test]
async fn unknown_worker_heartbeat_returns_404() {
    let (_rt, addr, shutdown) = spawn_coord(AuthConfig::default()).await;
    let client = client_for(addr, None);
    let bogus = migration_control_protocol::schema::WorkerId::new();
    let err = client
        .heartbeat(
            bogus,
            HeartbeatBody {
                state: WorkerState::Idle,
                files_per_sec: 0.0,
                bytes_per_sec: 0.0,
                errors_per_min: 0.0,
                inflight_ops: 0,
                queue_depth: 0,
                latency: None,
            },
        )
        .await
        .expect_err("must fail");
    match err {
        CoordError::Http { status, .. } => {
            assert_eq!(status, reqwest::StatusCode::NOT_FOUND);
        }
        other => panic!("unexpected: {other:?}"),
    }
    shutdown.cancel();
}
