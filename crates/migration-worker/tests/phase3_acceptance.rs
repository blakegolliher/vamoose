//! Phase 3 acceptance-gate harness. One file, one purpose: pin
//! every acceptance bullet in `docs/COORD_PLAN.md` §Phase 3 to a
//! reproducible integration test against an in-process coord.
//!
//! Bullets covered here:
//!
//! 1. Workers register, heartbeat, emit events under normal load.
//!    → `full_lifecycle_register_emit_pause_resume_fence`
//! 2. Self-fence event propagates to `/jobs/<id>` (the SSE side
//!    is covered by `migration-coord/tests/sse_e2e.rs`).
//!    → `full_lifecycle_register_emit_pause_resume_fence` (final
//!    fence step)
//! 3. Worker crash → restart re-registers cleanly. Old WorkerId
//!    flipped to `Disconnected` by coord dedup.
//!    → `worker_restart_dedups_prior_worker`
//! 4. Coord restart → workers reconnect and resume event submission
//!    without data loss (bounded buffer covers the gap).
//!    → `coord_restart_buffered_events_drain_after_reconnect`
//! 5. Pause command observed by all workers within one heartbeat
//!    interval.
//!    → `full_lifecycle_register_emit_pause_resume_fence` (pause
//!    step)

use chrono::{TimeZone, Utc};
use migration_control_protocol::schema::{ConfigHash, EventKind, JobId, WorkerState};
use migration_coord::lease::{Identity, LeaseConfig};
use migration_coord::runtime::test_clock::FixedClock;
use migration_coord::runtime::{CoordRuntime, RuntimeConfig};
use migration_coord::server::auth::AuthConfig;
use migration_coord::server::{build_router, AppState};
use migration_coord::store::{CoordStore, MemStore};
use migration_core::fence::Fence;
use migration_worker::config::CoordCfg;
use migration_worker::coord_client::ControlMode;
use migration_worker::coord_driver::{self, DriverInputs, WorkerEventDraft};
use migration_worker::heartbeat::ProgressState;
use migration_worker::throughput::ThroughputCounter;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{mpsc, RwLock};
use tokio_util::sync::CancellationToken;

// =============================================================================
// Scaffolding (kept self-contained — these tests are the single
// integration point for the acceptance gates, no shared helpers)
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

/// Start a coord on a fresh `MemStore` and a loopback port. Returns
/// `(runtime, listener_addr, shutdown_token)`. Cancelling the token
/// graceful-shuts-down the listener (the runtime is independent —
/// callers also invoke `runtime.shutdown` for full state release).
async fn spawn_coord_on_store(
    store: Arc<dyn CoordStore>,
) -> (CoordRuntime, SocketAddr, CancellationToken) {
    let std_listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = std_listener.local_addr().unwrap();
    drop(std_listener);
    let cancel = CancellationToken::new();
    let rt = spawn_coord_listener(store, addr, cancel.clone()).await;
    (rt, addr, cancel)
}

/// Bring up a coord listener at an EXPLICIT address — used by the
/// coord-restart test to bind the SAME port as the prior coord.
async fn spawn_coord_listener(
    store: Arc<dyn CoordStore>,
    addr: SocketAddr,
    cancel: CancellationToken,
) -> CoordRuntime {
    let clock = FixedClock::new(
        chrono::DateTime::parse_from_rfc3339("2026-05-29T14:32:00Z")
            .unwrap()
            .with_timezone(&chrono::Utc),
    );
    let rt = CoordRuntime::start(store, clock, me(), rt_cfg())
        .await
        .unwrap();
    let router = build_router(AppState::with_auth(rt.clone(), AuthConfig::default()));

    // axum_server::Handle is generic in axum-server 0.8 — keep the
    // handle local to this helper to let inference resolve it.
    let handle = axum_server::Handle::new();
    let sh = handle.clone();
    tokio::spawn(async move {
        cancel.cancelled().await;
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
    rt
}

fn driver_cfg(addr: SocketAddr) -> CoordCfg {
    CoordCfg {
        url: format!("http://{addr}"),
        job_id: Some("bobby".into()),
        cluster_secret_env: None,
        heartbeat_sec: 1,
        events_flush_sec: 1,
        buffer_max_bytes: 1024 * 1024,
        verify_tls: false,
        request_timeout_sec: 5,
    }
}

async fn wait_for_register(
    handle: &coord_driver::CoordDriverHandle,
) -> migration_control_protocol::schema::WorkerId {
    let mut wid_rx = handle.worker_id.clone();
    tokio::time::timeout(Duration::from_secs(8), async {
        loop {
            if let Some(id) = *wid_rx.borrow_and_update() {
                return id;
            }
            wid_rx.changed().await.unwrap();
        }
    })
    .await
    .expect("driver registered within 8s")
}

async fn wait_for_progress(rt: &CoordRuntime, job: &JobId, want_files: u64, deadline: Duration) {
    tokio::time::timeout(deadline, async {
        loop {
            let v = rt.job_view(job).await.unwrap();
            if v.progress.files_done >= want_files {
                return;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    })
    .await
    .unwrap_or_else(|_| panic!("coord never observed files_done >= {want_files}"));
}

// =============================================================================
// Gates #1, #2, #5 — full lifecycle
// =============================================================================

#[tokio::test]
async fn full_lifecycle_register_emit_pause_resume_fence() {
    let store = Arc::new(MemStore::new()) as Arc<dyn CoordStore>;
    let (rt, addr, _coord_shutdown) = spawn_coord_on_store(store).await;
    rt.ingest(job_created("bobby")).await.unwrap();

    let (events_tx, events_rx) = mpsc::channel::<WorkerEventDraft>(256);
    let (fence_tx, fence_rx) = mpsc::channel::<String>(1);
    let fence = Fence::new();
    let inputs = DriverInputs {
        progress: Arc::new(RwLock::new(ProgressState::new())),
        live: std::sync::Arc::new(migration_worker::heartbeat::LivePending::default()),
        throughput: ThroughputCounter::new(),
        fence: fence.clone(),
        fence_rx: Some(fence_rx),
        events_rx: Some(events_rx),
    };
    let cancel = CancellationToken::new();
    let handle = coord_driver::spawn(
        &driver_cfg(addr),
        "lifecycle-host".into(),
        4242,
        Utc.timestamp_opt(1_700_000_000, 0).unwrap(),
        "0.6.0-test".into(),
        inputs,
        cancel.clone(),
    )
    .expect("spawn");

    let worker_id = wait_for_register(&handle).await;

    // Gate #1: events flow under normal load.
    for _ in 0..10 {
        events_tx
            .send(WorkerEventDraft::ProgressOk { bytes: 100 })
            .await
            .unwrap();
    }
    wait_for_progress(&rt, &jid("bobby"), 10, Duration::from_secs(5)).await;
    let view = rt.job_view(&jid("bobby")).await.unwrap();
    assert_eq!(view.progress.files_done, 10);
    assert_eq!(view.progress.bytes_done, 1000);

    // Gate #5: pause command observed within one heartbeat tick.
    // The coord's heartbeat response carries control.mode = "pause"
    // as soon as the job phase flips. With heartbeat_sec=1 the
    // driver's RunControl should reflect Pause within ~1.5s.
    rt.ingest(EventKind::JobPaused {
        job_id: jid("bobby"),
        reason: "operator".into(),
    })
    .await
    .unwrap();
    tokio::time::timeout(Duration::from_secs(3), async {
        loop {
            if handle.run_control.mode() == ControlMode::Pause {
                return;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .expect("driver observed pause within heartbeat interval");

    // Drainer is independent of RunControl; events still land even
    // while paused. Push 5 more drafts and verify the progress total
    // climbs to 15.
    for _ in 0..5 {
        events_tx
            .send(WorkerEventDraft::ProgressOk { bytes: 200 })
            .await
            .unwrap();
    }
    wait_for_progress(&rt, &jid("bobby"), 15, Duration::from_secs(5)).await;
    let view = rt.job_view(&jid("bobby")).await.unwrap();
    assert_eq!(view.progress.files_done, 15);
    assert_eq!(view.progress.bytes_done, 1000 + 5 * 200);

    // Resume — RunControl flips back to Run.
    rt.ingest(EventKind::JobResumed {
        job_id: jid("bobby"),
        reason: "operator".into(),
    })
    .await
    .unwrap();
    tokio::time::timeout(Duration::from_secs(3), async {
        loop {
            if handle.run_control.mode() == ControlMode::Run {
                return;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .expect("driver observed resume");

    // Gate #2: self-fence propagates to /jobs/<id>.
    fence_tx
        .send("test-r7-clock-jump".into())
        .await
        .expect("fence signal");
    // Driver task should exit Ok after the fence POST.
    let task_result = tokio::time::timeout(Duration::from_secs(5), handle.task)
        .await
        .expect("driver exits within 5s")
        .expect("task ran");
    assert!(task_result.is_ok());

    // Coord state reflects the fence.
    let snap = rt.state().await;
    let w = snap.workers.get(&worker_id).expect("worker present");
    assert_eq!(w.state, WorkerState::Fenced);
    assert_eq!(w.fence_reason.as_deref(), Some("test-r7-clock-jump"));

    // Job view also reflects the fenced worker's totals as before.
    let view = rt.job_view(&jid("bobby")).await.unwrap();
    assert_eq!(view.progress.files_done, 15);

    drop(events_tx);
    drop(fence_tx);
    drop(fence);
    cancel.cancel();
}

// =============================================================================
// Gate #3 — worker restart re-register supersedes prior
// =============================================================================

#[tokio::test]
async fn worker_restart_dedups_prior_worker() {
    let store = Arc::new(MemStore::new()) as Arc<dyn CoordStore>;
    let (rt, addr, _coord_shutdown) = spawn_coord_on_store(store).await;
    rt.ingest(job_created("bobby")).await.unwrap();

    // Spawn driver A (pid=100, start=T1).
    let cancel_a = CancellationToken::new();
    let (events_tx_a, events_rx_a) = mpsc::channel::<WorkerEventDraft>(64);
    let fence_a = Fence::new();
    let inputs_a = DriverInputs {
        progress: Arc::new(RwLock::new(ProgressState::new())),
        live: std::sync::Arc::new(migration_worker::heartbeat::LivePending::default()),
        throughput: ThroughputCounter::new(),
        fence: fence_a,
        fence_rx: None,
        events_rx: Some(events_rx_a),
    };
    let handle_a = coord_driver::spawn(
        &driver_cfg(addr),
        "restart-host".into(),
        100,
        Utc.timestamp_opt(1_700_000_000, 0).unwrap(),
        "0.6.0-test".into(),
        inputs_a,
        cancel_a.clone(),
    )
    .expect("A spawn");
    let wid_a = wait_for_register(&handle_a).await;

    // A is currently Idle in coord state.
    {
        let snap = rt.state().await;
        let w = snap.workers.get(&wid_a).expect("A present");
        assert_eq!(w.state, WorkerState::Idle);
    }

    // Kill A without a goodbye — an orderly cancel would POST
    // /leave and mark A Disconnected itself, leaving nothing for the
    // re-register dedup to do. The gate is the crash-restart case.
    handle_a.task.abort();
    let _ = tokio::time::timeout(Duration::from_secs(5), handle_a.task).await;
    drop(cancel_a);
    drop(events_tx_a);

    // Spawn driver B from the SAME host (different pid + start_time).
    let cancel_b = CancellationToken::new();
    let (events_tx_b, events_rx_b) = mpsc::channel::<WorkerEventDraft>(64);
    let fence_b = Fence::new();
    let inputs_b = DriverInputs {
        progress: Arc::new(RwLock::new(ProgressState::new())),
        live: std::sync::Arc::new(migration_worker::heartbeat::LivePending::default()),
        throughput: ThroughputCounter::new(),
        fence: fence_b,
        fence_rx: None,
        events_rx: Some(events_rx_b),
    };
    let handle_b = coord_driver::spawn(
        &driver_cfg(addr),
        "restart-host".into(),
        200,
        Utc.timestamp_opt(1_700_000_500, 0).unwrap(),
        "0.6.0-test".into(),
        inputs_b,
        cancel_b.clone(),
    )
    .expect("B spawn");
    let wid_b = wait_for_register(&handle_b).await;
    assert_ne!(wid_a, wid_b, "re-register must mint a new WorkerId");

    // Coord state: A flipped to Disconnected by dedup; B is Idle.
    let snap = rt.state().await;
    let wa = snap.workers.get(&wid_a).expect("A still present");
    assert_eq!(
        wa.state,
        WorkerState::Disconnected,
        "prior worker must be Disconnected after re-register"
    );
    assert_eq!(wa.last_error.as_deref(), Some("reregister"));
    let wb = snap.workers.get(&wid_b).expect("B present");
    assert_eq!(wb.state, WorkerState::Idle);

    cancel_b.cancel();
    let _ = tokio::time::timeout(Duration::from_secs(5), handle_b.task).await;
    drop(events_tx_b);
}

// =============================================================================
// Gate #4 — coord restart, buffered events drain after reconnect
// =============================================================================

#[tokio::test]
async fn coord_restart_buffered_events_drain_after_reconnect() {
    // SHARED store between coord A and coord B — mimics a coord
    // process restart against the same S3 backend. The lease is
    // released on coord A's runtime shutdown so coord B can acquire.
    let store = Arc::new(MemStore::new()) as Arc<dyn CoordStore>;

    let (rt_a, addr, coord_a_shutdown) = spawn_coord_on_store(store.clone()).await;
    rt_a.ingest(job_created("bobby")).await.unwrap();

    // Spawn driver — register against coord A.
    let cancel = CancellationToken::new();
    let (events_tx, events_rx) = mpsc::channel::<WorkerEventDraft>(256);
    let fence = Fence::new();
    let inputs = DriverInputs {
        progress: Arc::new(RwLock::new(ProgressState::new())),
        live: std::sync::Arc::new(migration_worker::heartbeat::LivePending::default()),
        throughput: ThroughputCounter::new(),
        fence,
        fence_rx: None,
        events_rx: Some(events_rx),
    };
    let handle = coord_driver::spawn(
        &driver_cfg(addr),
        "restart-host".into(),
        4242,
        Utc.timestamp_opt(1_700_000_000, 0).unwrap(),
        "0.6.0-test".into(),
        inputs,
        cancel.clone(),
    )
    .expect("driver spawn");
    let _worker_id = wait_for_register(&handle).await;

    // Push some events and wait for them to land at coord A.
    for _ in 0..5 {
        events_tx
            .send(WorkerEventDraft::ProgressOk { bytes: 100 })
            .await
            .unwrap();
    }
    wait_for_progress(&rt_a, &jid("bobby"), 5, Duration::from_secs(5)).await;

    // Bring coord A down: shut down listener AND runtime (which
    // releases the lease so coord B can acquire). Lease release goes
    // through runtime.shutdown.
    coord_a_shutdown.cancel();
    rt_a.shutdown(3).await.expect("coord A shutdown");
    // Give the listener a beat to actually close.
    tokio::time::sleep(Duration::from_millis(500)).await;

    // Push more drafts while coord is down. The drainer's POSTs will
    // fail; EventBuffer holds the materialized ProgressDelta in its
    // requeue path.
    for _ in 0..7 {
        events_tx
            .send(WorkerEventDraft::ProgressOk { bytes: 200 })
            .await
            .unwrap();
    }

    // Bring coord B up on the SAME port with the SAME store. It
    // should replay state and re-acquire the lease.
    let coord_b_shutdown = CancellationToken::new();
    let rt_b = spawn_coord_listener(store.clone(), addr, coord_b_shutdown.clone()).await;

    // Drainer's next tick succeeds; buffered events flow to coord B.
    // Total should reach 5 (already at A) + 7 (queued) = 12.
    wait_for_progress(&rt_b, &jid("bobby"), 12, Duration::from_secs(15)).await;
    let view = rt_b.job_view(&jid("bobby")).await.unwrap();
    assert_eq!(view.progress.files_done, 12);
    assert_eq!(view.progress.bytes_done, 5 * 100 + 7 * 200);

    drop(events_tx);
    cancel.cancel();
    let _ = tokio::time::timeout(Duration::from_secs(5), handle.task).await;
}
