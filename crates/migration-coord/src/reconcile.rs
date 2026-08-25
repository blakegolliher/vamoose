//! Terminal-phase reconciliation from the bucket's shard state.
//!
//! The reducer learns about copy progress only from worker
//! `ProgressDelta` events, and the only phase it derives on its own is
//! `Planned → Copying` on the first delta. Nothing told it when the
//! run was over: after every manifest shard completed on the rig the
//! TUI still read `Copying 98% · 1 run / 0 done` (a worker killed
//! mid-shard took its unsent deltas with it, while the peer that
//! reclaimed the shard reported only its own). `vamoose status`, which
//! reads the claim records under `shards/`, was right the whole time.
//!
//! This module makes the coord read the same records. On a fixed
//! cadence it tallies every shard listed in `manifest.json`:
//!
//! - the rows and bytes of *completed* shards are ingested as a
//!   `ProgressSync` — the reducer floor-merges absolutes, so a lost
//!   delta can no longer leave the job short and the sync is a no-op
//!   while the deltas are ahead of the claims (they normally are: a
//!   shard's rows are counted long before its claim flips);
//! - once every shard's claim is terminal the job transitions —
//!   `JobCompleted` when none failed, `JobFailed` naming the count
//!   otherwise. Terminal phases are absorbing, so the transition is
//!   emitted exactly once per job, and a replayed log reconstructs it
//!   without help.
//!
//! Cost: one LIST of `shards/` per tick plus one GET per claim that
//! was not already known to be completed (completed claims are
//! immutable, so they are fetched once and cached).

use crate::errors::Result;
use crate::runtime::CoordRuntime;
use crate::schema::{EventKind, JobId, Phase};
use crate::store::CoordStore;
use migration_core::layout;
use migration_core::records::{ClaimRecord, ClaimState, Manifest};
use std::collections::{BTreeMap, BTreeSet};
use std::time::Duration;
use tokio_util::sync::CancellationToken;

/// Poll cadence for the shard tally in production. Claims flip at
/// most once per shard, so the tick only has to be quick relative to
/// how long an operator is willing to look at a finished run that
/// still says "Copying".
pub const RECONCILE_INTERVAL: Duration = Duration::from_secs(30);

/// One tick's view of the manifest's shards.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ShardTally {
    pub total_shards: usize,
    pub completed: usize,
    pub failed: usize,
    pub active: usize,
    /// Rows of completed shards — the bucket's absolute for
    /// `files_done` (job totals are seeded from `manifest.total_rows`).
    pub rows_completed: u64,
    pub bytes_completed: u64,
}

impl ShardTally {
    /// Every manifest shard has a terminal claim.
    pub fn all_terminal(&self) -> bool {
        self.completed + self.failed == self.total_shards
    }
}

/// Claims already observed as `Completed`. A completed claim never
/// changes, so it need not be re-fetched. Failed claims are re-read
/// every tick: an operator may clear one to retry the shard.
#[derive(Debug, Default)]
pub struct ShardCache {
    completed: BTreeMap<String, (u64, u64)>,
}

/// Tally the manifest's shards from their claim records.
pub async fn tally_shards(
    store: &dyn CoordStore,
    manifest: &Manifest,
    cache: &mut ShardCache,
) -> Result<ShardTally> {
    let shard_sizes: BTreeMap<&str, (u64, u64)> = manifest
        .shards
        .iter()
        .filter_map(|shard| {
            shard
                .key
                .strip_prefix(layout::INDEX_PREFIX)
                .map(|name| (name, (shard.rows, shard.bytes)))
        })
        .collect();

    let mut tally = ShardTally {
        total_shards: manifest.shards.len(),
        ..ShardTally::default()
    };
    let mut seen: BTreeSet<String> = BTreeSet::new();
    for entry in store.list(layout::SHARDS_PREFIX).await? {
        let Some(name) = layout::shard_from_claim_key(&entry.key) else {
            continue;
        };
        let Some(&(rows, bytes)) = shard_sizes.get(name) else {
            continue; // a claim from another run's shard set
        };
        if !seen.insert(name.to_string()) {
            continue;
        }
        let state = if cache.completed.contains_key(name) {
            ClaimState::Completed
        } else {
            let Some((body, _)) = store.get(&entry.key).await? else {
                continue; // deleted between LIST and GET
            };
            let Ok(record) = serde_json::from_slice::<ClaimRecord>(&body) else {
                tracing::warn!(key = %entry.key, "reconcile: unreadable claim record; skipped");
                continue;
            };
            if record.state == ClaimState::Completed {
                cache.completed.insert(name.to_string(), (rows, bytes));
            }
            record.state
        };
        match state {
            ClaimState::Completed => {
                tally.completed += 1;
                tally.rows_completed = tally.rows_completed.saturating_add(rows);
                tally.bytes_completed = tally.bytes_completed.saturating_add(bytes);
            }
            ClaimState::Failed => tally.failed += 1,
            ClaimState::Active => tally.active += 1,
        }
    }
    Ok(tally)
}

/// What one reconcile pass did to the job.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Reconciled {
    /// The job is already terminal (or unknown); nothing to do — the
    /// caller can stop polling.
    Settled,
    /// Shards still outstanding; a sync may have been ingested.
    Pending { synced: bool },
    /// Every shard terminal: the phase transition was ingested.
    Transitioned(Phase),
}

/// One pass: heal the job's absolutes from the tally and, when every
/// shard is terminal, end the job.
pub async fn reconcile_once(
    rt: &CoordRuntime,
    job_id: &JobId,
    manifest: &Manifest,
    cache: &mut ShardCache,
) -> Result<Reconciled> {
    let Some(job) = rt.job_view(job_id).await else {
        return Ok(Reconciled::Settled);
    };
    if job.phase.is_terminal() {
        return Ok(Reconciled::Settled);
    }
    let tally = tally_shards(rt.store().as_ref(), manifest, cache).await?;

    // Absolutes from the bucket. The reducer max-merges, so this only
    // ever heals upward; skip the event when it would be a no-op.
    let synced = tally.rows_completed > job.progress.files_done
        || tally.bytes_completed > job.progress.bytes_done;
    if synced {
        rt.ingest(EventKind::ProgressSync {
            job_id: job_id.clone(),
            files_done: tally.rows_completed,
            bytes_done: tally.bytes_completed,
            workers: Vec::new(),
        })
        .await?;
    }

    if !tally.all_terminal() {
        return Ok(Reconciled::Pending { synced });
    }
    let (kind, phase) = if tally.failed == 0 {
        (
            EventKind::JobCompleted {
                job_id: job_id.clone(),
            },
            Phase::Completed,
        )
    } else {
        (
            EventKind::JobFailed {
                job_id: job_id.clone(),
                reason: format!("{} of {} shards failed", tally.failed, tally.total_shards),
            },
            Phase::Failed,
        )
    };
    let seq = rt.ingest(kind).await?;
    tracing::info!(
        job = %job_id,
        seq,
        ?phase,
        completed = tally.completed,
        failed = tally.failed,
        rows = tally.rows_completed,
        "reconcile: every manifest shard is terminal; job ended",
    );
    Ok(Reconciled::Transitioned(phase))
}

async fn load_manifest(store: &dyn CoordStore) -> Result<Option<Manifest>> {
    let Some((body, _etag)) = store.get(layout::MANIFEST_KEY).await? else {
        return Ok(None);
    };
    Ok(Some(serde_json::from_slice(&body).map_err(|e| {
        crate::Error::Other(anyhow::anyhow!("parse {}: {e}", layout::MANIFEST_KEY))
    })?))
}

/// Poll the bucket every `interval` until the job is terminal or
/// `shutdown` fires. The manifest is read until it exists (a job
/// seeded by explicit id may precede `vamoose prepare`) and then
/// kept: a published manifest is immutable. Store errors are logged
/// and retried next tick — a coord that serves the TUI is more useful
/// than one that exits over an S3 hiccup.
pub async fn run(rt: CoordRuntime, job_id: JobId, interval: Duration, shutdown: CancellationToken) {
    let mut manifest: Option<Manifest> = None;
    let mut cache = ShardCache::default();
    let mut announced_wait = false;
    loop {
        if manifest.is_none() {
            match load_manifest(rt.store().as_ref()).await {
                Ok(Some(m)) => manifest = Some(m),
                Ok(None) => {
                    if !announced_wait {
                        announced_wait = true;
                        tracing::info!(
                            job = %job_id,
                            "reconcile: no manifest.json yet; will track shard state once one \
                             is published",
                        );
                    }
                }
                Err(e) => tracing::warn!(error = %e, "reconcile: manifest read failed; retrying"),
            }
        }
        if let Some(m) = &manifest {
            match reconcile_once(&rt, &job_id, m, &mut cache).await {
                Ok(Reconciled::Settled) | Ok(Reconciled::Transitioned(_)) => return,
                Ok(Reconciled::Pending { .. }) => {}
                Err(e) => tracing::warn!(error = %e, "reconcile: shard tally failed; retrying"),
            }
        }
        tokio::select! {
            biased;
            _ = shutdown.cancelled() => return,
            _ = tokio::time::sleep(interval) => {}
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::lease::{Identity, LeaseConfig};
    use crate::runtime::test_clock::FixedClock;
    use crate::runtime::RuntimeConfig;
    use crate::schema::WorkerId;
    use crate::store::MemStore;
    use chrono::{TimeZone, Utc};
    use migration_core::records::{Endpoint, EndpointKind, MigrationOptions, ShardEntry};
    use migration_core::time::UtcTime;
    use std::sync::Arc;

    fn jid(s: &str) -> JobId {
        JobId::new(s).unwrap()
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

    async fn fresh_runtime() -> (CoordRuntime, Arc<MemStore>) {
        let mem = Arc::new(MemStore::new());
        let store: Arc<dyn CoordStore> = mem.clone();
        let clock = FixedClock::new(Utc.with_ymd_and_hms(2026, 8, 25, 0, 0, 0).unwrap());
        let me = Identity {
            holder_id: "A".into(),
            host: "h".into(),
            pid: 1,
        };
        let rt = CoordRuntime::start(store, clock, me, rt_cfg())
            .await
            .unwrap();
        (rt, mem)
    }

    fn manifest(shards: &[(&str, u64, u64)]) -> Manifest {
        Manifest {
            format_version: 2,
            run_id: "run-2026".into(),
            created_utc: UtcTime(Utc.with_ymd_and_hms(2026, 8, 24, 0, 0, 0).unwrap()),
            shards: shards
                .iter()
                .map(|(name, rows, bytes)| ShardEntry {
                    key: layout::index_key(name),
                    rows: *rows,
                    bytes: *bytes,
                    etag: "e".into(),
                })
                .collect(),
            total_rows: shards.iter().map(|(_, rows, _)| rows).sum(),
            source: Endpoint {
                kind: EndpointKind::Nfs,
                url: "nfs://src/export".into(),
                root: "/".into(),
            },
            dest: Endpoint {
                kind: EndpointKind::Nfs,
                url: "nfs://dst/export".into(),
                root: "/".into(),
            },
            options: MigrationOptions::default(),
        }
    }

    async fn seed(rt: &CoordRuntime, m: &Manifest) -> JobId {
        let id = jid(&m.run_id);
        rt.ingest(EventKind::JobCreated {
            job_id: id.clone(),
            name: "run".into(),
            source: "s".into(),
            dest: "d".into(),
            owner: "t".into(),
            config_hash: crate::schema::ConfigHash("ab".into()),
            total_files: m.total_rows,
            total_bytes: m.shards.iter().map(|s| s.bytes).sum(),
        })
        .await
        .unwrap();
        id
    }

    async fn put_claim(store: &MemStore, shard: &str, state: ClaimState) {
        let record = ClaimRecord {
            host: "w1".into(),
            claimed_utc: UtcTime(Utc.with_ymd_and_hms(2026, 8, 25, 0, 0, 0).unwrap()),
            epoch: 1,
            state,
        };
        store
            .put(
                &layout::claim_key(shard),
                serde_json::to_vec(&record).unwrap(),
            )
            .await
            .unwrap();
    }

    async fn put_manifest(store: &MemStore, m: &Manifest) {
        store
            .put(layout::MANIFEST_KEY, serde_json::to_vec(m).unwrap())
            .await
            .unwrap();
    }

    /// The phase-ending events durably logged for `job`.
    async fn phase_events(rt: &CoordRuntime, job: &JobId) -> Vec<EventKind> {
        rt.flush_log().await.unwrap();
        crate::state::read_job_events(rt.store().as_ref(), job, 0)
            .await
            .unwrap()
            .into_iter()
            .filter(|env| {
                matches!(
                    env.kind,
                    EventKind::JobCompleted { .. } | EventKind::JobFailed { .. }
                )
            })
            .map(|env| env.kind)
            .collect()
    }

    const SHARDS: &[(&str, u64, u64)] = &[
        ("part-0000.parquet", 10, 100),
        ("part-0001.parquet", 5, 50),
        ("part-0002.parquet", 7, 70),
    ];

    #[tokio::test]
    async fn tally_counts_only_manifest_shards_and_caches_completed() {
        let (rt, store) = fresh_runtime().await;
        let m = manifest(SHARDS);
        put_claim(&store, "part-0000.parquet", ClaimState::Completed).await;
        put_claim(&store, "part-0001.parquet", ClaimState::Active).await;
        put_claim(&store, "stranger.parquet", ClaimState::Completed).await;
        let mut cache = ShardCache::default();

        let t = tally_shards(rt.store().as_ref(), &m, &mut cache)
            .await
            .unwrap();
        assert_eq!(
            t,
            ShardTally {
                total_shards: 3,
                completed: 1,
                failed: 0,
                active: 1,
                rows_completed: 10,
                bytes_completed: 100,
            }
        );
        assert!(!t.all_terminal());
        assert_eq!(cache.completed.len(), 1, "completed claims are remembered");

        // A cached completed claim is not re-read: corrupting its
        // object must not change the tally.
        store
            .put(&layout::claim_key("part-0000.parquet"), b"garbage".to_vec())
            .await
            .unwrap();
        let again = tally_shards(rt.store().as_ref(), &m, &mut cache)
            .await
            .unwrap();
        assert_eq!(again, t);
    }

    #[tokio::test]
    async fn pending_run_heals_absolutes_without_ending_the_job() {
        let (rt, store) = fresh_runtime().await;
        let m = manifest(SHARDS);
        let job = seed(&rt, &m).await;
        // One delta reached the coord; a second worker's did not.
        rt.ingest(EventKind::ProgressDelta {
            job_id: job.clone(),
            worker_id: WorkerId::new(),
            files_delta: 4,
            bytes_delta: 40,
            errors_delta: 0,
        })
        .await
        .unwrap();
        put_claim(&store, "part-0000.parquet", ClaimState::Completed).await;
        put_claim(&store, "part-0001.parquet", ClaimState::Active).await;
        let mut cache = ShardCache::default();

        let r = reconcile_once(&rt, &job, &m, &mut cache).await.unwrap();
        assert_eq!(r, Reconciled::Pending { synced: true });
        let view = rt.job_view(&job).await.unwrap();
        assert_eq!(view.phase, Phase::Copying);
        assert_eq!(
            view.progress.files_done, 10,
            "healed to the bucket's absolute"
        );
        assert_eq!(view.progress.bytes_done, 100);

        // Deltas ahead of the claims: the sync is skipped, not regressed.
        rt.ingest(EventKind::ProgressDelta {
            job_id: job.clone(),
            worker_id: WorkerId::new(),
            files_delta: 3,
            bytes_delta: 30,
            errors_delta: 0,
        })
        .await
        .unwrap();
        let r = reconcile_once(&rt, &job, &m, &mut cache).await.unwrap();
        assert_eq!(r, Reconciled::Pending { synced: false });
        assert_eq!(rt.job_view(&job).await.unwrap().progress.files_done, 13);
        assert!(phase_events(&rt, &job).await.is_empty());
    }

    #[tokio::test]
    async fn all_completed_ends_the_job_exactly_once_at_full_totals() {
        let (rt, store) = fresh_runtime().await;
        let m = manifest(SHARDS);
        let job = seed(&rt, &m).await;
        // Lost deltas: the coord only ever saw 2 of 22 rows.
        rt.ingest(EventKind::ProgressDelta {
            job_id: job.clone(),
            worker_id: WorkerId::new(),
            files_delta: 2,
            bytes_delta: 20,
            errors_delta: 0,
        })
        .await
        .unwrap();
        for (name, _, _) in SHARDS {
            put_claim(&store, name, ClaimState::Completed).await;
        }
        let mut cache = ShardCache::default();

        let r = reconcile_once(&rt, &job, &m, &mut cache).await.unwrap();
        assert_eq!(r, Reconciled::Transitioned(Phase::Completed));
        let view = rt.job_view(&job).await.unwrap();
        assert_eq!(view.phase, Phase::Completed);
        assert_eq!(view.progress.files_done, view.progress.files_total);
        assert_eq!(view.progress.bytes_done, view.progress.bytes_total);

        // Absorbing: a second pass (and a coord restart replaying the
        // log) emits nothing more.
        let r = reconcile_once(&rt, &job, &m, &mut cache).await.unwrap();
        assert_eq!(r, Reconciled::Settled);
        assert_eq!(phase_events(&rt, &job).await.len(), 1);
        rt.flush_log().await.unwrap();
        let replayed = crate::state::replay(rt.store().as_ref(), Utc::now())
            .await
            .unwrap();
        assert_eq!(replayed.state.jobs[&job].phase, Phase::Completed);
        crate::state::assert_replay_is_idempotent(rt.store().as_ref(), Utc::now())
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn a_failed_shard_ends_the_job_as_failed() {
        let (rt, store) = fresh_runtime().await;
        let m = manifest(SHARDS);
        let job = seed(&rt, &m).await;
        put_claim(&store, "part-0000.parquet", ClaimState::Completed).await;
        put_claim(&store, "part-0001.parquet", ClaimState::Failed).await;
        put_claim(&store, "part-0002.parquet", ClaimState::Completed).await;
        let mut cache = ShardCache::default();

        let r = reconcile_once(&rt, &job, &m, &mut cache).await.unwrap();
        assert_eq!(r, Reconciled::Transitioned(Phase::Failed));
        let view = rt.job_view(&job).await.unwrap();
        assert_eq!(view.phase, Phase::Failed);
        assert_eq!(view.progress.files_done, 17, "only completed shards count");
        assert_eq!(
            view.phase_history.last().unwrap().reason,
            "1 of 3 shards failed"
        );
        assert_eq!(phase_events(&rt, &job).await.len(), 1);
    }

    #[tokio::test]
    async fn paused_job_with_all_shards_done_still_completes() {
        let (rt, store) = fresh_runtime().await;
        let m = manifest(SHARDS);
        let job = seed(&rt, &m).await;
        rt.ingest(EventKind::JobPaused {
            job_id: job.clone(),
            reason: "operator".into(),
        })
        .await
        .unwrap();
        for (name, _, _) in SHARDS {
            put_claim(&store, name, ClaimState::Completed).await;
        }
        let mut cache = ShardCache::default();
        let r = reconcile_once(&rt, &job, &m, &mut cache).await.unwrap();
        assert_eq!(r, Reconciled::Transitioned(Phase::Completed));
    }

    #[tokio::test]
    async fn run_loop_waits_for_manifest_then_ends_job_and_returns() {
        let (rt, store) = fresh_runtime().await;
        let m = manifest(SHARDS);
        let job = seed(&rt, &m).await;
        let shutdown = CancellationToken::new();
        let task = tokio::spawn(run(
            rt.clone(),
            job.clone(),
            Duration::from_millis(10),
            shutdown.clone(),
        ));
        tokio::time::sleep(Duration::from_millis(40)).await;
        assert_eq!(
            rt.job_view(&job).await.unwrap().phase,
            Phase::Planned,
            "no manifest, no shard state, nothing to conclude"
        );

        put_manifest(&store, &m).await;
        for (name, _, _) in SHARDS {
            put_claim(&store, name, ClaimState::Completed).await;
        }
        tokio::time::timeout(Duration::from_secs(2), task)
            .await
            .expect("loop must return once the job is terminal")
            .unwrap();
        assert_eq!(rt.job_view(&job).await.unwrap().phase, Phase::Completed);
        assert!(!shutdown.is_cancelled());
    }

    #[tokio::test]
    async fn run_loop_honors_shutdown_while_pending() {
        let (rt, store) = fresh_runtime().await;
        let m = manifest(SHARDS);
        let job = seed(&rt, &m).await;
        put_manifest(&store, &m).await;
        let shutdown = CancellationToken::new();
        let task = tokio::spawn(run(
            rt.clone(),
            job.clone(),
            Duration::from_millis(10),
            shutdown.clone(),
        ));
        tokio::time::sleep(Duration::from_millis(30)).await;
        shutdown.cancel();
        tokio::time::timeout(Duration::from_secs(2), task)
            .await
            .expect("loop must observe shutdown")
            .unwrap();
        assert_eq!(rt.job_view(&job).await.unwrap().phase, Phase::Planned);
    }
}
