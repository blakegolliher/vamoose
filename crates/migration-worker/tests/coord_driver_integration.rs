//! End-to-end test for the worker-side `coord_driver` against an
//! in-process coord. Exercises the spawn → register → heartbeat →
//! fence-trip-POST → exit lifecycle.
//!
//! Counterpart to `tests/coord_client_integration.rs` (which
//! exercises the HTTP layer in isolation).

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
use migration_worker::coord_driver::{self, DriverInputs, WorkerEventDraft};
use migration_worker::heartbeat::ProgressState;
use migration_worker::throughput::ThroughputCounter;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{mpsc, RwLock};
use tokio_util::sync::CancellationToken;

// =============================================================================
// Scaffolding
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
    let router = build_router(AppState::with_auth(rt.clone(), AuthConfig::default()));

    let std_listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = std_listener.local_addr().unwrap();
    drop(std_listener);
    let shutdown = CancellationToken::new();
    let sc = shutdown.clone();
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
    (rt, addr, shutdown)
}

fn driver_cfg(addr: SocketAddr) -> CoordCfg {
    CoordCfg {
        url: format!("http://{addr}"),
        job_id: Some("bobby".into()),
        cluster_secret_env: None,
        // Sub-second heartbeat for fast tests; non-default but the
        // schema accepts u64 — we just pass it through.
        heartbeat_sec: 1,
        events_flush_sec: 1,
        buffer_max_bytes: 1024 * 1024,
        verify_tls: false,
        request_timeout_sec: 5,
    }
}

fn fresh_inputs(fence: Fence, fence_rx: Option<mpsc::Receiver<String>>) -> DriverInputs {
    DriverInputs {
        progress: Arc::new(RwLock::new(ProgressState::new())),
        live: std::sync::Arc::new(migration_worker::heartbeat::LivePending::default()),
        throughput: ThroughputCounter::new(),
        fence,
        fence_rx,
        events_rx: None,
    }
}

fn fresh_inputs_with_events(
    fence: Fence,
    events_rx: mpsc::Receiver<WorkerEventDraft>,
) -> DriverInputs {
    DriverInputs {
        progress: Arc::new(RwLock::new(ProgressState::new())),
        live: std::sync::Arc::new(migration_worker::heartbeat::LivePending::default()),
        throughput: ThroughputCounter::new(),
        fence,
        fence_rx: None,
        events_rx: Some(events_rx),
    }
}

// =============================================================================
// register → heartbeat → fence POST → exit
// =============================================================================

#[tokio::test]
async fn fence_trip_signal_triggers_fence_post_and_clean_exit() {
    let (rt, addr, shutdown) = spawn_coord().await;
    rt.ingest(job_created("bobby")).await.unwrap();

    let (fence_tx, fence_rx) = mpsc::channel::<String>(1);
    let fence = Fence::new();
    let inputs = fresh_inputs(fence.clone(), Some(fence_rx));
    let cancel = CancellationToken::new();

    let handle = coord_driver::spawn(
        &driver_cfg(addr),
        "host-a".into(),
        4242,
        Utc.timestamp_opt(1_700_000_000, 0).unwrap(),
        "0.6.0-test".into(),
        inputs,
        cancel.clone(),
    )
    .expect("spawn");

    // Wait for register to publish a WorkerId.
    let mut wid_rx = handle.worker_id.clone();
    let worker_id = tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            if let Some(id) = *wid_rx.borrow_and_update() {
                return id;
            }
            wid_rx.changed().await.unwrap();
        }
    })
    .await
    .expect("register completed within 5s");

    // Send a fence reason. The driver's select! should POST /fence
    // and exit Ok(()) on the very next loop iteration.
    fence_tx
        .send("test-fence-r7-clock-jump".into())
        .await
        .expect("send fence reason");

    // Driver task should exit Ok shortly after the fence POST.
    let task_result = tokio::time::timeout(Duration::from_secs(5), handle.task)
        .await
        .expect("driver task exits within 5s")
        .expect("task ran");
    assert!(
        task_result.is_ok(),
        "driver task should exit Ok after fence POST: {task_result:?}"
    );

    // Coord state should now show this worker as Fenced with the
    // exact reason we sent.
    let snap = rt.state().await;
    let w = snap.workers.get(&worker_id).expect("worker present");
    assert_eq!(
        w.state,
        WorkerState::Fenced,
        "coord must record worker as Fenced after the POST"
    );
    assert_eq!(
        w.fence_reason.as_deref(),
        Some("test-fence-r7-clock-jump"),
        "fence reason must round-trip verbatim"
    );

    shutdown.cancel();
    drop(fence); // appease unused warning
}

// =============================================================================
// When fence_rx is None (legacy mode), the driver loop never enters
// the fence arm. Verify normal cancellation still works.
// =============================================================================

#[tokio::test]
async fn driver_without_fence_channel_shuts_down_on_cancel() {
    let (_rt, addr, shutdown) = spawn_coord().await;

    // Pre-create the job.
    let (_, _, _) = (
        // dummy so the compiler doesn't whine about unused let binding
        (),
        (),
        (),
    );
    {
        // ingest in a quick block — keep rt clone lifetime tight.
        let rt = _rt.clone();
        rt.ingest(job_created("bobby")).await.unwrap();
    }

    let fence = Fence::new();
    let inputs = fresh_inputs(fence, None);
    let cancel = CancellationToken::new();
    let handle = coord_driver::spawn(
        &driver_cfg(addr),
        "host-a".into(),
        4242,
        Utc.timestamp_opt(1_700_000_000, 0).unwrap(),
        "0.6.0-test".into(),
        inputs,
        cancel.clone(),
    )
    .expect("spawn");

    // Wait for register to settle so we're cancelling a steady-state
    // driver, not pre-register.
    let mut wid_rx = handle.worker_id.clone();
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            if wid_rx.borrow_and_update().is_some() {
                return;
            }
            wid_rx.changed().await.unwrap();
        }
    })
    .await
    .expect("register");

    cancel.cancel();
    let task_result = tokio::time::timeout(Duration::from_secs(5), handle.task)
        .await
        .expect("task exits within 5s")
        .expect("task ran");
    assert!(task_result.is_ok());

    shutdown.cancel();
}

// =============================================================================
// Events drainer end-to-end — drafts flow through the channel,
// coalesce into one ProgressDelta per window, and land at the coord.
// =============================================================================

#[tokio::test]
async fn events_drainer_coalesces_drafts_and_progressdelta_lands_at_coord() {
    let (rt, addr, shutdown) = spawn_coord().await;
    rt.ingest(job_created("bobby")).await.unwrap();

    let (events_tx, events_rx) = mpsc::channel::<WorkerEventDraft>(256);
    let fence = Fence::new();
    let inputs = fresh_inputs_with_events(fence, events_rx);
    let cancel = CancellationToken::new();

    let handle = coord_driver::spawn(
        &driver_cfg(addr),
        "host-a".into(),
        4242,
        Utc.timestamp_opt(1_700_000_000, 0).unwrap(),
        "0.6.0-test".into(),
        inputs,
        cancel.clone(),
    )
    .expect("spawn");

    // Wait for register.
    let mut wid_rx = handle.worker_id.clone();
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            if wid_rx.borrow_and_update().is_some() {
                return;
            }
            wid_rx.changed().await.unwrap();
        }
    })
    .await
    .expect("register");

    // Push a burst of drafts: 10 OK files at varying sizes, 3 failures,
    // 2 fenced. Drainer should coalesce into one ProgressDelta with
    // files_delta=10, bytes_delta=sum, errors_delta=5.
    let bytes_sizes: [u64; 10] = [100, 200, 300, 400, 500, 600, 700, 800, 900, 1000];
    let bytes_sum: u64 = bytes_sizes.iter().sum();
    for b in bytes_sizes.iter() {
        events_tx
            .send(WorkerEventDraft::ProgressOk { bytes: *b })
            .await
            .unwrap();
    }
    for _ in 0..3 {
        events_tx
            .send(WorkerEventDraft::ProgressFailed)
            .await
            .unwrap();
    }
    for _ in 0..2 {
        events_tx
            .send(WorkerEventDraft::ProgressFenced)
            .await
            .unwrap();
    }

    // Wait up to 5 ticks (5s with events_flush_sec=1) for the coord's
    // view of the job to reflect the totals. The drainer needs at
    // least one tick interval to flush.
    let want_files = 10u64;
    let want_bytes = bytes_sum;
    let want_errors = 5u64;
    tokio::time::timeout(Duration::from_secs(8), async {
        loop {
            let j = rt.job_view(&jid("bobby")).await.unwrap();
            if j.progress.files_done == want_files
                && j.progress.bytes_done == want_bytes
                && j.progress.errors_total == want_errors
            {
                return;
            }
            tokio::time::sleep(Duration::from_millis(200)).await;
        }
    })
    .await
    .expect("coord progress reflects all drafts within 8s");

    // Shutdown — cancel the driver, ensure it joins cleanly.
    drop(events_tx);
    cancel.cancel();
    let _ = tokio::time::timeout(Duration::from_secs(5), handle.task).await;

    shutdown.cancel();
}

#[tokio::test]
async fn events_drainer_emits_at_most_one_progressdelta_per_window() {
    // Direct accumulator check using the public ProgressDelta path:
    // push many drafts faster than the tick, verify the coord sees
    // a SMALL number of events even though many drafts were pushed.
    let (rt, addr, shutdown) = spawn_coord().await;
    rt.ingest(job_created("bobby")).await.unwrap();

    let (events_tx, events_rx) = mpsc::channel::<WorkerEventDraft>(256);
    let fence = Fence::new();
    let inputs = fresh_inputs_with_events(fence, events_rx);
    let cancel = CancellationToken::new();

    let handle = coord_driver::spawn(
        &driver_cfg(addr),
        "host-a".into(),
        4242,
        Utc.timestamp_opt(1_700_000_000, 0).unwrap(),
        "0.6.0-test".into(),
        inputs,
        cancel.clone(),
    )
    .expect("spawn");

    let mut wid_rx = handle.worker_id.clone();
    let worker_id = tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            if let Some(id) = *wid_rx.borrow_and_update() {
                return id;
            }
            wid_rx.changed().await.unwrap();
        }
    })
    .await
    .expect("register");

    // Push 50 drafts as fast as we can. With a 1s flush window the
    // drainer should batch them into ONE event (or two — at most one
    // per tick that elapses while we send).
    let seq_before = rt.last_seq().await;
    for _ in 0..50 {
        events_tx
            .send(WorkerEventDraft::ProgressOk { bytes: 10 })
            .await
            .unwrap();
    }

    // Give the drainer one full tick window plus a margin.
    tokio::time::sleep(Duration::from_millis(1500)).await;

    let seq_after = rt.last_seq().await;
    let events_posted = seq_after - seq_before;
    // 50 raw drafts → at most 2 ProgressDelta events (we allow 2 in
    // case the test crossed a tick boundary mid-send). The point is
    // it's bounded, not 50.
    assert!(
        events_posted <= 2,
        "coalesce must produce at most 2 events for 50 drafts; got {events_posted}"
    );
    assert!(events_posted >= 1, "at least one event must land");

    // Counters still correct.
    let j = rt.job_view(&jid("bobby")).await.unwrap();
    assert_eq!(j.progress.files_done, 50);
    assert_eq!(j.progress.bytes_done, 500);

    // Suppress unused warning in case the test ever skips before
    // using worker_id directly.
    let _ = worker_id;

    drop(events_tx);
    cancel.cancel();
    let _ = tokio::time::timeout(Duration::from_secs(5), handle.task).await;
    shutdown.cancel();
}
