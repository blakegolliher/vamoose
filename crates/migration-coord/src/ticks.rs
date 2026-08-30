//! Background ticks — lease refresh, snapshot cadence, log flush.
//!
//! Three independent loops drive the long-running maintenance the
//! [`crate::runtime::CoordRuntime`] needs:
//!
//! - **Lease refresh** ([`lease_loop`]) — calls [`lease::refresh`]
//!   on `cfg.lease_refresh_interval` (recommended `ttl/3`). If
//!   refresh returns [`crate::Error::LeaseLost`], the loop signals
//!   shutdown — ingest stops, every other tick observes the cancel
//!   token, the process exits.
//!
//! - **Snapshot cadence** ([`snapshot_loop`]) — checks both
//!   thresholds on `cfg.snapshot_check_interval`:
//!   - `now - last_snapshot_at >= snapshot_interval` (default 5min),
//!   - `last_seq - last_snapshot_seq >= snapshot_events` (default
//!     1000).
//!
//!   Either firing writes a snapshot (and history copy, pruned to
//!   `history_keep`). Both counters reset.
//!
//! - **Flush** ([`flush_aged_loop`]) — on `cfg.flush_check_interval`
//!   (default 1s) calls [`CoordRuntime::flush_trailing_progress`]
//!   (trailing-edge re-broadcast for the ProgressDelta wire cap,
//!   ledger F24 residue) and [`CoordRuntime::flush_aged`], which
//!   keeps a long-tail per-job chunk from sitting in memory past
//!   the documented `max_chunk_age`.
//!
//! - **Worker liveness** ([`liveness_loop`]) — on
//!   `cfg.worker_liveness_check_interval` calls
//!   [`CoordRuntime::sweep_stale_workers`] with
//!   `cfg.worker_liveness_timeout`, so a worker that stopped
//!   heartbeating without saying goodbye reads as `Disconnected`
//!   instead of `Copying` forever.
//!
//! All loops are joined under one [`CancellationToken`] so a
//! shutdown request from the caller (or a `LeaseLost` from the
//! lease loop) cleanly stops every loop.

use crate::errors::Result;
use crate::lease::{self, LeaseConfig};
use crate::runtime::CoordRuntime;
use crate::Error;
use std::time::Duration;
use tokio::time::{interval, MissedTickBehavior};
use tokio_util::sync::CancellationToken;

/// Tuning knobs for the tick loops. `default_for_prod` mirrors the
/// build prompt: 30s lease TTL, 1000-event-or-5min snapshot, 24
/// history copies.
#[derive(Debug, Clone)]
pub struct TickerConfig {
    pub lease_refresh_interval: Duration,
    pub snapshot_check_interval: Duration,
    pub snapshot_interval: Duration,
    pub snapshot_events: u64,
    pub history_keep: usize,
    pub flush_check_interval: Duration,
    /// How often the liveness sweep runs.
    pub worker_liveness_check_interval: Duration,
    /// A worker with no heartbeat for longer than this is marked
    /// Disconnected. Workers heartbeat every `coord_heartbeat_sec`
    /// (default 5 s) and back off to 30 s while the coord is
    /// unreachable, so 90 s is three missed backed-off ticks.
    pub worker_liveness_timeout: Duration,
    /// Inherited from the runtime — the lease loop needs the same
    /// TTL the runtime opened the lease with.
    pub lease: LeaseConfig,
}

impl TickerConfig {
    pub fn default_for_prod() -> Self {
        Self {
            lease_refresh_interval: Duration::from_secs(10),
            snapshot_check_interval: Duration::from_secs(30),
            snapshot_interval: Duration::from_secs(5 * 60),
            snapshot_events: 1000,
            history_keep: 24,
            // 1s (was 60s): the flush tick now also delivers the
            // trailing-edge ProgressDelta re-broadcast (F24 residue),
            // whose ~2× PROGRESS_STREAM_MIN_INTERVAL_MS delivery
            // bound needs a tick at or below the 1 Hz cap interval.
            // The aged-chunk check it also drives is a cheap in-
            // memory scan against a 5-minute threshold — ticking it
            // 60× more often costs microseconds.
            flush_check_interval: Duration::from_secs(1),
            worker_liveness_check_interval: Duration::from_secs(10),
            worker_liveness_timeout: Duration::from_secs(90),
            lease: LeaseConfig::default_for_prod(),
        }
    }
}

/// Run every tick loop until shutdown. Returns when the cancel
/// token fires or any loop errors out. Each loop runs in its own
/// task; this function joins them.
pub async fn run_all(
    rt: CoordRuntime,
    cfg: TickerConfig,
    shutdown: CancellationToken,
) -> Result<()> {
    let lease_handle = tokio::spawn(lease_loop(rt.clone(), cfg.clone(), shutdown.clone()));
    let snapshot_handle = tokio::spawn(snapshot_loop(rt.clone(), cfg.clone(), shutdown.clone()));
    let flush_handle = tokio::spawn(flush_aged_loop(rt.clone(), cfg.clone(), shutdown.clone()));
    let liveness_handle = tokio::spawn(liveness_loop(rt, cfg, shutdown));

    // Join everything and surface the first error.
    let (lease_res, snapshot_res, flush_res, liveness_res) =
        tokio::join!(lease_handle, snapshot_handle, flush_handle, liveness_handle);
    lease_res.map_err(|e| Error::Other(anyhow::anyhow!("lease tick panic: {e}")))??;
    snapshot_res.map_err(|e| Error::Other(anyhow::anyhow!("snapshot tick panic: {e}")))??;
    flush_res.map_err(|e| Error::Other(anyhow::anyhow!("flush tick panic: {e}")))??;
    liveness_res.map_err(|e| Error::Other(anyhow::anyhow!("liveness tick panic: {e}")))??;
    Ok(())
}

/// Worker liveness loop. Each tick sweeps workers whose last
/// heartbeat is older than `cfg.worker_liveness_timeout`. A
/// `LeaseLost` from the sweep ends the loop (the lease loop is
/// already shutting everything down); any other error is logged and
/// retried next tick.
pub async fn liveness_loop(
    rt: CoordRuntime,
    cfg: TickerConfig,
    shutdown: CancellationToken,
) -> Result<()> {
    let mut tick = interval(cfg.worker_liveness_check_interval);
    tick.set_missed_tick_behavior(MissedTickBehavior::Delay);
    tick.tick().await;
    let timeout = chrono::Duration::from_std(cfg.worker_liveness_timeout)
        .unwrap_or_else(|_| chrono::Duration::seconds(90));

    loop {
        tokio::select! {
            _ = shutdown.cancelled() => return Ok(()),
            _ = tick.tick() => {
                match rt.sweep_stale_workers(timeout).await {
                    Ok(swept) if !swept.is_empty() => {
                        tracing::info!(count = swept.len(), "liveness sweep marked workers Disconnected");
                    }
                    Ok(_) => {}
                    Err(Error::LeaseLost) => return Ok(()),
                    Err(e) => tracing::warn!(error = %e, "liveness sweep failed; retry next tick"),
                }
            }
        }
    }
}

/// Lease refresh loop. On `LeaseLost`, marks the runtime and
/// triggers shutdown so the other loops bail.
pub async fn lease_loop(
    rt: CoordRuntime,
    cfg: TickerConfig,
    shutdown: CancellationToken,
) -> Result<()> {
    let mut tick = interval(cfg.lease_refresh_interval);
    tick.set_missed_tick_behavior(MissedTickBehavior::Delay);
    // Consume the immediate first tick — we just acquired the lease,
    // no need to refresh on millis 0.
    tick.tick().await;

    loop {
        tokio::select! {
            _ = shutdown.cancelled() => return Ok(()),
            _ = tick.tick() => {
                let handle = rt.lease_handle().await;
                let now = rt.clock().now();
                // A refresh that has not returned by the TTL has
                // forfeited the lease whatever the store eventually
                // says — the object is expired and claimable. Treat
                // it as LeaseLost rather than letting the loop hang
                // (2026-08-29: one hung refresh left the coord running
                // 30 h on an expired lease while every other loop
                // kept writing).
                let ttl = cfg
                    .lease
                    .ttl
                    .to_std()
                    .unwrap_or(Duration::from_secs(30));
                let refreshed = match tokio::time::timeout(
                    ttl,
                    lease::refresh(rt.store().as_ref(), &handle, cfg.lease, now),
                )
                .await
                {
                    Ok(r) => r,
                    Err(_) => {
                        tracing::error!(
                            ttl_secs = ttl.as_secs(),
                            "lease refresh did not return within the TTL"
                        );
                        Err(Error::LeaseLost)
                    }
                };
                match refreshed {
                    Ok(new_handle) => {
                        rt.update_lease(new_handle).await;
                    }
                    Err(Error::LeaseLost) => {
                        tracing::error!("lease lost — shutting down");
                        rt.mark_lease_lost().await;
                        shutdown.cancel();
                        return Ok(());
                    }
                    Err(e) => {
                        tracing::warn!(error = %e, "lease refresh failed; retry next tick");
                    }
                }
            }
        }
    }
}

/// Snapshot cadence loop. Tracks last-snapshot wall-clock and
/// last-snapshot seq locally so a snapshot trigger reflects the
/// runtime's actual cadence rather than a refreshed-at-startup
/// guess.
///
/// Also drives archive-on-completion: every check tick drains
/// [`CoordRuntime::archive_terminal_jobs`] — jobs whose terminal
/// phase a successful snapshot write made durable. Running it here
/// (never inline in ingest or command handlers) keeps ingest latency
/// flat and gives failed archives a natural per-tick retry.
pub async fn snapshot_loop(
    rt: CoordRuntime,
    cfg: TickerConfig,
    shutdown: CancellationToken,
) -> Result<()> {
    let mut tick = interval(cfg.snapshot_check_interval);
    tick.set_missed_tick_behavior(MissedTickBehavior::Delay);
    tick.tick().await;

    let mut last_snapshot_at = rt.clock().now();
    let mut last_snapshot_seq = rt.last_seq().await;

    loop {
        tokio::select! {
            _ = shutdown.cancelled() => return Ok(()),
            _ = tick.tick() => {
                let now = rt.clock().now();
                let cur_seq = rt.last_seq().await;
                let elapsed = now.signed_duration_since(last_snapshot_at);
                let elapsed_std = elapsed.to_std().unwrap_or(Duration::ZERO);
                let events_since = cur_seq.saturating_sub(last_snapshot_seq);

                if elapsed_std >= cfg.snapshot_interval
                    || events_since >= cfg.snapshot_events
                {
                    // Flush the log first so the snapshot reflects a
                    // clean log boundary — any event with seq <=
                    // current last_seq is durable on disk before the
                    // snapshot records last_seq.
                    if let Err(e) = rt.flush_log().await {
                        tracing::warn!(error = %e, "snapshot tick: flush_log failed");
                        continue;
                    }
                    match rt.write_snapshot(cfg.history_keep).await {
                        Ok(()) => {
                            tracing::debug!(
                                cur_seq,
                                events_since,
                                elapsed_secs = elapsed_std.as_secs(),
                                "snapshot written",
                            );
                            last_snapshot_at = now;
                            last_snapshot_seq = cur_seq;
                        }
                        Err(e) => {
                            tracing::warn!(error = %e, "snapshot write failed; retry next tick");
                        }
                    }
                }

                // Archive-on-completion. Eligibility only grows on a
                // successful snapshot write (above), but the drain
                // runs every check tick so a previously failed
                // archive retries without waiting for the next
                // snapshot threshold. Best-effort: per-job failures
                // are logged inside and retried next tick.
                if let Err(e) = rt.archive_terminal_jobs().await {
                    tracing::warn!(error = %e, "archive tick failed; retry next tick");
                }
            }
        }
    }
}

/// Flush-aged loop. Flushes any open chunk older than
/// `EventLogConfig::max_chunk_age` so a low-traffic job's events
/// don't sit in memory indefinitely.
///
/// Also the trailing-edge tick for the ProgressDelta wire cap
/// (ledger F24 residue): each tick re-broadcasts any retained
/// suppressed delta whose cap interval has elapsed
/// ([`CoordRuntime::flush_trailing_progress`] — bus-only, no store
/// I/O), so a quieting burst's final values reach live subscribers
/// within roughly `flush_check_interval` +
/// `PROGRESS_STREAM_MIN_INTERVAL_MS` of the burst's end. Keep
/// `flush_check_interval` at or below the cap interval to hold the
/// documented ~2× bound.
pub async fn flush_aged_loop(
    rt: CoordRuntime,
    cfg: TickerConfig,
    shutdown: CancellationToken,
) -> Result<()> {
    let mut tick = interval(cfg.flush_check_interval);
    tick.set_missed_tick_behavior(MissedTickBehavior::Delay);
    tick.tick().await;

    loop {
        tokio::select! {
            _ = shutdown.cancelled() => return Ok(()),
            _ = tick.tick() => {
                let _ = rt.flush_trailing_progress().await;
                if let Err(e) = rt.emit_progress_syncs().await {
                    tracing::warn!(error = %e, "progress sync emission failed; retry next tick");
                }
                if let Err(e) = rt.flush_aged().await {
                    tracing::warn!(error = %e, "flush_aged failed; retry next tick");
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::lease::Identity;
    use crate::runtime::test_clock::FixedClock;
    use crate::runtime::RuntimeConfig;
    use crate::schema::{EventKind, JobId, WorkerId};
    use crate::store::{CoordStore, MemStore};
    use chrono::{DateTime, TimeZone, Utc};
    use std::sync::Arc;

    fn jid(s: &str) -> JobId {
        JobId::new(s).unwrap()
    }

    fn me(holder: &str) -> Identity {
        Identity {
            holder_id: holder.into(),
            host: "h".into(),
            pid: 1,
        }
    }

    fn at(secs: i64) -> DateTime<Utc> {
        Utc.with_ymd_and_hms(2026, 5, 29, 14, 32, 0).unwrap() + chrono::Duration::seconds(secs)
    }

    fn rt_cfg() -> RuntimeConfig {
        RuntimeConfig {
            lease: LeaseConfig {
                ttl: chrono::Duration::seconds(30),
                grace: chrono::Duration::seconds(5),
            },
            events: crate::events::EventLogConfig {
                max_events_per_chunk: 1000,
                max_chunk_age: chrono::Duration::seconds(60),
            },
            bus_capacity: 16,
            lease_retry_interval: Duration::from_millis(10),
            lease_retry_max_attempts: Some(3),
        }
    }

    fn ticker_cfg_short() -> TickerConfig {
        TickerConfig {
            lease_refresh_interval: Duration::from_millis(20),
            snapshot_check_interval: Duration::from_millis(20),
            snapshot_interval: Duration::from_secs(3600), // disabled for unit tests
            snapshot_events: 1000,                        // disabled by default
            history_keep: 3,
            flush_check_interval: Duration::from_millis(20),
            worker_liveness_check_interval: Duration::from_millis(20),
            worker_liveness_timeout: Duration::from_secs(90),
            lease: LeaseConfig {
                ttl: chrono::Duration::seconds(30),
                grace: chrono::Duration::seconds(5),
            },
        }
    }

    async fn fresh_runtime() -> (CoordRuntime, Arc<FixedClock>, Arc<MemStore>) {
        let mem = Arc::new(MemStore::new());
        let store: Arc<dyn CoordStore> = mem.clone();
        let clock = FixedClock::new(at(0));
        let rt = CoordRuntime::start(store, clock.clone(), me("A"), rt_cfg())
            .await
            .unwrap();
        (rt, clock, mem)
    }

    fn job_created(j: &str) -> EventKind {
        EventKind::JobCreated {
            job_id: jid(j),
            name: format!("{j}-mig"),
            source: "s".into(),
            dest: "d".into(),
            owner: "t".into(),
            config_hash: crate::schema::ConfigHash("ab".into()),
            total_files: 0,
            total_bytes: 0,
        }
    }

    fn progress_delta(j: &str) -> EventKind {
        EventKind::ProgressDelta {
            job_id: jid(j),
            worker_id: WorkerId::new(),
            files_delta: 1,
            bytes_delta: 0,
            errors_delta: 0,
        }
    }

    #[tokio::test]
    async fn lease_refresh_loop_updates_handle() {
        let (rt, clock, _store) = fresh_runtime().await;
        let original = rt.lease_handle().await;

        let shutdown = CancellationToken::new();
        let cfg = ticker_cfg_short();
        let task = tokio::spawn(lease_loop(rt.clone(), cfg, shutdown.clone()));

        // Let the refresh tick fire a couple times.
        clock.advance(chrono::Duration::seconds(1));
        tokio::time::sleep(Duration::from_millis(80)).await;
        shutdown.cancel();
        task.await.unwrap().unwrap();

        let refreshed = rt.lease_handle().await;
        // Etag rotated → refresh actually happened.
        assert_ne!(refreshed.etag, original.etag);
        assert_eq!(refreshed.body.lease_id, original.body.lease_id);
    }

    /// A store whose conditional delete never returns once armed —
    /// the black-holed request of 2026-08-29. Everything else is the
    /// wrapped `MemStore`, so runtime start-up (acquire, snapshot,
    /// log) works normally.
    #[derive(Debug)]
    struct HangingStore {
        inner: Arc<MemStore>,
        hang_delete_if_match: std::sync::atomic::AtomicBool,
    }

    #[async_trait::async_trait]
    impl CoordStore for HangingStore {
        async fn get(&self, key: &str) -> Result<Option<(Vec<u8>, String)>> {
            self.inner.get(key).await
        }
        async fn head(&self, key: &str) -> Result<Option<String>> {
            self.inner.head(key).await
        }
        async fn put(&self, key: &str, body: Vec<u8>) -> Result<String> {
            self.inner.put(key, body).await
        }
        async fn put_if_absent(
            &self,
            key: &str,
            body: Vec<u8>,
        ) -> Result<crate::store::PutOutcome> {
            self.inner.put_if_absent(key, body).await
        }
        async fn delete(&self, key: &str) -> Result<()> {
            self.inner.delete(key).await
        }
        async fn delete_if_match(
            &self,
            key: &str,
            etag: &str,
        ) -> Result<migration_core::claim::DeleteOutcome> {
            if self
                .hang_delete_if_match
                .load(std::sync::atomic::Ordering::SeqCst)
            {
                std::future::pending::<()>().await;
            }
            self.inner.delete_if_match(key, etag).await
        }
        async fn list(&self, prefix: &str) -> Result<Vec<crate::store::ListEntry>> {
            self.inner.list(prefix).await
        }
    }

    /// Regression for the 2026-08-29 coord: one refresh whose store
    /// call never returned parked the lease loop for 30 h while the
    /// other loops kept writing on an expired lease. A refresh that
    /// outlives the TTL is a lost lease — the loop must say so and
    /// shut the coord down, not wait.
    #[tokio::test(start_paused = true)]
    async fn lease_loop_treats_a_hung_refresh_as_lease_lost() {
        let hanging = Arc::new(HangingStore {
            inner: Arc::new(MemStore::new()),
            hang_delete_if_match: std::sync::atomic::AtomicBool::new(false),
        });
        let store: Arc<dyn CoordStore> = hanging.clone();
        let clock = FixedClock::new(at(0));
        let rt = CoordRuntime::start(store, clock, me("A"), rt_cfg())
            .await
            .unwrap();
        hanging
            .hang_delete_if_match
            .store(true, std::sync::atomic::Ordering::SeqCst);

        let shutdown = CancellationToken::new();
        let task = tokio::spawn(lease_loop(rt.clone(), ticker_cfg_short(), shutdown.clone()));

        // Paused time: the 30 s TTL elapses as soon as the runtime is
        // idle on the hung future. Well within it, nothing but the
        // hung refresh is pending; past it, the loop must have bailed.
        tokio::time::timeout(Duration::from_secs(120), shutdown.cancelled())
            .await
            .expect("a refresh hung past the TTL must shut the coord down");
        task.await.unwrap().unwrap();
        assert!(rt.lease_lost().await);
    }

    #[tokio::test]
    async fn lease_loop_shuts_down_on_lease_lost() {
        let (rt, _clock, store) = fresh_runtime().await;

        // Simulate a takeover: nuke the lease object so refresh sees
        // an absent key (etag mismatch → LeaseLost).
        store.delete(crate::layout::LEASE_KEY).await.unwrap();

        let shutdown = CancellationToken::new();
        let cfg = ticker_cfg_short();
        let task = tokio::spawn(lease_loop(rt.clone(), cfg, shutdown.clone()));

        // The lease loop's first refresh tick should detect the loss
        // and cancel shutdown itself.
        tokio::time::timeout(Duration::from_secs(2), shutdown.cancelled())
            .await
            .expect("shutdown should fire");
        task.await.unwrap().unwrap();
        assert!(rt.lease_lost().await);
    }

    #[tokio::test]
    async fn snapshot_loop_fires_on_event_count() {
        let (rt, _clock, store) = fresh_runtime().await;

        let shutdown = CancellationToken::new();
        let mut cfg = ticker_cfg_short();
        cfg.snapshot_events = 5; // trip after 5 events
        cfg.snapshot_interval = Duration::from_secs(3600); // time threshold disabled
        let task = tokio::spawn(snapshot_loop(rt.clone(), cfg, shutdown.clone()));
        // Let the loop run far enough to capture its
        // last_snapshot_seq baseline (= 0 on this fresh runtime)
        // BEFORE the ingests land. yield_now isn't enough because
        // the loop has multiple .await points before it parks at
        // the tick (immediate first tick, then a mutex acquire);
        // a small real sleep ensures the initialization completes.
        tokio::time::sleep(Duration::from_millis(5)).await;

        // Ingest 5 events.
        for _ in 0..5 {
            rt.ingest(progress_delta("bobby")).await.unwrap();
        }
        // Wait for the snapshot tick to fire and write.
        tokio::time::sleep(Duration::from_millis(80)).await;
        shutdown.cancel();
        task.await.unwrap().unwrap();

        let snap = crate::snapshot::load(store.as_ref()).await.unwrap();
        assert!(snap.is_some(), "snapshot should have been written");
        let snap = snap.unwrap();
        assert!(
            snap.last_seq >= 5,
            "snapshot last_seq should reflect ingest"
        );
    }

    #[tokio::test]
    async fn snapshot_loop_fires_on_time_elapsed() {
        let (rt, clock, store) = fresh_runtime().await;
        rt.ingest(job_created("bobby")).await.unwrap();

        let shutdown = CancellationToken::new();
        let mut cfg = ticker_cfg_short();
        cfg.snapshot_events = 10_000; // event threshold disabled
        cfg.snapshot_interval = Duration::from_secs(5);

        // Initialize loop, then jump the clock past snapshot_interval.
        let task = tokio::spawn(snapshot_loop(rt.clone(), cfg, shutdown.clone()));
        // Same reason as the count-test above: brief sleep so the
        // loop captures last_snapshot_at = at(0) before we advance.
        tokio::time::sleep(Duration::from_millis(5)).await;
        clock.advance(chrono::Duration::seconds(10));
        tokio::time::sleep(Duration::from_millis(80)).await;
        shutdown.cancel();
        task.await.unwrap().unwrap();

        let snap = crate::snapshot::load(store.as_ref()).await.unwrap();
        assert!(snap.is_some());
    }

    #[tokio::test]
    async fn snapshot_loop_does_not_fire_below_thresholds() {
        let (rt, _clock, store) = fresh_runtime().await;
        rt.ingest(job_created("bobby")).await.unwrap();

        let shutdown = CancellationToken::new();
        let cfg = ticker_cfg_short(); // 3600s interval, 1000 events
        let task = tokio::spawn(snapshot_loop(rt.clone(), cfg, shutdown.clone()));

        tokio::time::sleep(Duration::from_millis(80)).await;
        shutdown.cancel();
        task.await.unwrap().unwrap();

        let snap = crate::snapshot::load(store.as_ref()).await.unwrap();
        assert!(snap.is_none(), "no snapshot expected below thresholds");
    }

    #[tokio::test]
    async fn run_all_joins_three_loops_and_honors_shutdown() {
        let (rt, _clock, _store) = fresh_runtime().await;
        let shutdown = CancellationToken::new();
        let cfg = ticker_cfg_short();
        let shutdown_clone = shutdown.clone();
        let task = tokio::spawn(run_all(rt, cfg, shutdown_clone));

        tokio::time::sleep(Duration::from_millis(40)).await;
        shutdown.cancel();
        let res = tokio::time::timeout(Duration::from_secs(2), task)
            .await
            .expect("run_all should return after shutdown");
        res.unwrap().unwrap();
    }

    /// F24 residue 3a: the flush tick re-broadcasts a suppressed
    /// trailing ProgressDelta so live subscribers see a quieting
    /// burst's final values without a new event. The frame is a
    /// re-broadcast of the already-ingested envelope — same seq, no
    /// new event minted.
    #[tokio::test]
    async fn flush_loop_broadcasts_trailing_progress() {
        let (rt, clock, _store) = fresh_runtime().await;
        rt.ingest(job_created("bobby")).await.unwrap();
        let w = WorkerId::new();
        let delta = |files: u64| EventKind::ProgressDelta {
            job_id: jid("bobby"),
            worker_id: w,
            files_delta: files,
            bytes_delta: 0,
            errors_delta: 0,
        };
        let mut sub = rt.subscribe();

        // Burst: the first delta broadcasts, the final one is
        // suppressed by the 1 Hz cap.
        rt.ingest(delta(1)).await.unwrap();
        rt.ingest(delta(42)).await.unwrap();
        let first = sub.try_recv().expect("leading delta must broadcast");
        assert!(matches!(
            first.kind,
            EventKind::ProgressDelta { files_delta: 1, .. },
        ));
        assert!(
            matches!(
                sub.try_recv(),
                Err(tokio::sync::broadcast::error::TryRecvError::Empty),
            ),
            "the trailing delta must be suppressed at ingest",
        );
        let last_seq = rt.last_seq().await;

        // Past the cap interval, the running flush loop must deliver
        // the retained trailing values.
        clock.advance(chrono::Duration::seconds(2));
        let shutdown = CancellationToken::new();
        let cfg = ticker_cfg_short();
        let task = tokio::spawn(flush_aged_loop(rt.clone(), cfg, shutdown.clone()));

        let env = tokio::time::timeout(Duration::from_secs(2), sub.recv())
            .await
            .expect("the flush tick must re-broadcast the trailing delta")
            .unwrap();
        assert!(
            matches!(
                env.kind,
                EventKind::ProgressDelta {
                    files_delta: 42,
                    ..
                }
            ),
            "the trailing frame must carry the burst's final values, got {:?}",
            env.kind,
        );
        assert_eq!(env.seq, last_seq, "re-broadcast, not a new event");
        // The tick also mints ProgressSync events for the active job
        // (authoritative absolutes for client-side rates) — so
        // last_seq may advance; the trailing re-broadcast itself
        // still reuses its original envelope.
        assert!(rt.last_seq().await >= last_seq, "seq must never regress",);

        shutdown.cancel();
        task.await.unwrap().unwrap();
    }

    #[tokio::test]
    async fn flush_aged_loop_drains_old_chunks() {
        let (rt, clock, store) = fresh_runtime().await;
        // Ingest one event, then bump the clock past max_chunk_age
        // so the open chunk qualifies for flush.
        rt.ingest(progress_delta("bobby")).await.unwrap();
        clock.advance(chrono::Duration::seconds(90));

        let shutdown = CancellationToken::new();
        let cfg = ticker_cfg_short();
        let task = tokio::spawn(flush_aged_loop(rt.clone(), cfg, shutdown.clone()));

        tokio::time::sleep(Duration::from_millis(80)).await;
        shutdown.cancel();
        task.await.unwrap().unwrap();

        // The bobby chunk should be on disk now.
        let chunks = store.list("events/bobby/").await.unwrap();
        assert_eq!(chunks.len(), 1);
    }
}
