//! VAST S3 integration smoke for `migration-coord`.
//!
//! Marked `#[ignore]` so it does not run in CI without an environment.
//! **Should** be run before merging any change to the lease,
//! snapshot, event-log, or archive layers — it is the only place
//! the production VAST S3 quirks (no `PUT If-Match`, etag formatting,
//! self-signed TLS) actually get exercised against the real
//! endpoint.
//!
//! ## Required env
//!
//! ```text
//! VAMOOSE_TEST_S3_ENDPOINT=https://main.selab-var204.selab.vastdata.com
//! VAMOOSE_TEST_S3_REGION=us-east-1                # optional, default us-east-1
//! VAMOOSE_TEST_S3_BUCKET=vamoose
//! VAMOOSE_TEST_S3_PROFILE=var204                  # optional
//! VAMOOSE_TEST_S3_VERIFY_TLS=0                    # optional, default 0 (matches var204)
//! ```
//!
//! ## Run manually
//!
//! ```bash
//! cargo test -p migration-coord --test var204_smoke -- --ignored --nocapture
//! ```
//!
//! ## Isolation
//!
//! Every test mints a fresh UUID and routes all coord keys under
//! `coord-test-runs/<uuid>/` via [`PrefixedStore`]. Tests can run
//! concurrently and cleanup is best-effort per test. If a test
//! crashes mid-run, a manual sweep is:
//!
//! ```bash
//! aws s3 rm --recursive --profile var204 s3://vamoose/coord-test-runs/
//! ```

use async_trait::async_trait;
use chrono::{Duration, Utc};
use migration_coord::archive::{archive_job, list_archived_chunks};
use migration_coord::events::{EventLogConfig, EventLogWriter};
use migration_coord::lease::{self, AcquireOutcome, Identity, LeaseConfig};
use migration_coord::schema::{
    EventEnvelope, EventKind, JobId, Snapshot, WorkerId, SCHEMA_VERSION,
};
use migration_coord::snapshot;
use migration_coord::state;
use migration_coord::store::{CoordStore, ListEntry, PutOutcome, S3Store};
use migration_coord::Result;
use migration_core::claim::DeleteOutcome;
use migration_core::s3::S3Client;
use uuid::Uuid;

// =============================================================================
// Test environment
// =============================================================================

fn env_or_skip(name: &str) -> Option<String> {
    match std::env::var(name) {
        Ok(v) if !v.is_empty() => Some(v),
        _ => {
            eprintln!("skipping: {name} is not set");
            None
        }
    }
}

/// Build an `S3Store` from env vars. Returns `None` if the required
/// env is missing — callers `return Ok(())` so the run is silent
/// rather than red.
async fn make_store() -> Option<PrefixedStore<S3Store>> {
    let endpoint = env_or_skip("VAMOOSE_TEST_S3_ENDPOINT")?;
    let bucket = env_or_skip("VAMOOSE_TEST_S3_BUCKET")?;
    let region = std::env::var("VAMOOSE_TEST_S3_REGION").unwrap_or_else(|_| "us-east-1".into());
    let profile = std::env::var("VAMOOSE_TEST_S3_PROFILE").ok();
    let verify_tls = matches!(
        std::env::var("VAMOOSE_TEST_S3_VERIFY_TLS").as_deref(),
        Ok("1") | Ok("true")
    );

    let client = S3Client::from_config(&endpoint, &region, &bucket, profile.as_deref(), verify_tls)
        .await
        .expect("S3Client::from_config");
    let s3 = S3Store::new(client);
    let prefix = format!("coord-test-runs/{}/", Uuid::new_v4());
    Some(PrefixedStore::new(s3, prefix))
}

/// Recursively delete every key the test wrote under the prefix.
/// Best-effort: errors are logged but don't fail the test (the
/// manual sweep instruction in the module doc covers any straggler).
async fn cleanup<S: CoordStore>(store: &S, prefix: &str) {
    match store.list(prefix).await {
        Ok(entries) => {
            for e in entries {
                if let Err(err) = store.delete(&e.key).await {
                    eprintln!("cleanup: delete {key} failed: {err}", key = e.key);
                }
            }
        }
        Err(err) => eprintln!("cleanup: list {prefix} failed: {err}"),
    }
}

// =============================================================================
// PrefixedStore — scopes every key under `prefix`. Test-only adapter.
// =============================================================================

#[derive(Debug)]
struct PrefixedStore<S> {
    inner: S,
    prefix: String,
}

impl<S> PrefixedStore<S> {
    fn new(inner: S, prefix: String) -> Self {
        assert!(prefix.ends_with('/'), "prefix must end with '/'");
        Self { inner, prefix }
    }

    fn key(&self, k: &str) -> String {
        format!("{}{}", self.prefix, k)
    }

    fn prefix(&self) -> &str {
        &self.prefix
    }
}

#[async_trait]
impl<S: CoordStore> CoordStore for PrefixedStore<S> {
    async fn get(&self, key: &str) -> Result<Option<(Vec<u8>, String)>> {
        self.inner.get(&self.key(key)).await
    }
    async fn head(&self, key: &str) -> Result<Option<String>> {
        self.inner.head(&self.key(key)).await
    }
    async fn put(&self, key: &str, body: Vec<u8>) -> Result<String> {
        self.inner.put(&self.key(key), body).await
    }
    async fn put_if_absent(&self, key: &str, body: Vec<u8>) -> Result<PutOutcome> {
        self.inner.put_if_absent(&self.key(key), body).await
    }
    async fn delete(&self, key: &str) -> Result<()> {
        self.inner.delete(&self.key(key)).await
    }
    async fn delete_if_match(&self, key: &str, etag: &str) -> Result<DeleteOutcome> {
        self.inner.delete_if_match(&self.key(key), etag).await
    }
    async fn list(&self, prefix: &str) -> Result<Vec<ListEntry>> {
        let entries = self.inner.list(&self.key(prefix)).await?;
        // Strip the test prefix from returned keys so callers see the
        // same shape as a real coord rooted at the bucket.
        let strip = self.prefix.clone();
        Ok(entries
            .into_iter()
            .map(|mut e| {
                e.key = e.key.strip_prefix(&strip).unwrap_or(&e.key).to_string();
                e
            })
            .collect())
    }
}

// =============================================================================
// Tests
// =============================================================================

fn me(holder: &str) -> Identity {
    Identity {
        holder_id: holder.to_string(),
        host: "var204-test".to_string(),
        pid: std::process::id(),
    }
}

fn cfg() -> LeaseConfig {
    LeaseConfig {
        ttl: Duration::seconds(30),
        grace: Duration::seconds(5),
    }
}

fn cluster_env(seq: u64) -> EventEnvelope {
    EventEnvelope {
        seq,
        at: Utc::now(),
        schema_version: SCHEMA_VERSION,
        worker_at: None,
        client_seq: None,
        from_worker: None,
        kind: EventKind::WorkerLeft {
            worker_id: WorkerId::new(),
            reason: "test".into(),
        },
    }
}

fn job_env(seq: u64, job: &str) -> EventEnvelope {
    EventEnvelope {
        seq,
        at: Utc::now(),
        schema_version: SCHEMA_VERSION,
        worker_at: None,
        client_seq: None,
        from_worker: None,
        kind: EventKind::VerifyStarted {
            job_id: JobId::new(job).unwrap(),
        },
    }
}

#[tokio::test]
#[ignore]
async fn lease_acquire_refresh_takeover_release() {
    let store = match make_store().await {
        Some(s) => s,
        None => return,
    };
    let prefix = store.prefix().to_string();

    // 1. Cold acquire.
    let now = Utc::now();
    let acquired = match lease::try_acquire(&store, &me("A"), cfg(), now)
        .await
        .unwrap()
    {
        AcquireOutcome::Acquired(h) => h,
        AcquireOutcome::Held { .. } => panic!("fresh prefix should not be Held"),
    };
    assert_eq!(acquired.body.holder_id, "A");

    // 2. Refresh.
    let refreshed = lease::refresh(&store, &acquired, cfg(), now + Duration::seconds(5))
        .await
        .expect("refresh");
    assert_eq!(refreshed.body.lease_id, acquired.body.lease_id);
    assert!(refreshed.body.expires_at > acquired.body.expires_at);

    // 3. Takeover before grace fails.
    let during_grace = refreshed.body.expires_at + Duration::seconds(2);
    match lease::try_acquire(&store, &me("B"), cfg(), during_grace)
        .await
        .unwrap()
    {
        AcquireOutcome::Held { holder_id, .. } => assert_eq!(holder_id, "A"),
        AcquireOutcome::Acquired(_) => panic!("B took over during grace window"),
    }

    // 4. Takeover after grace succeeds.
    let after_grace = refreshed.body.expires_at + Duration::seconds(10);
    let b = match lease::try_acquire(&store, &me("B"), cfg(), after_grace)
        .await
        .unwrap()
    {
        AcquireOutcome::Acquired(h) => h,
        AcquireOutcome::Held { .. } => panic!("B should have taken over"),
    };
    assert_eq!(b.body.holder_id, "B");
    assert_ne!(b.body.lease_id, refreshed.body.lease_id);

    // 5. A's stale release is silent.
    lease::release(&store, &refreshed)
        .await
        .expect("stale release");
    // B's lease is still there.
    assert!(store
        .get(migration_coord::layout::LEASE_KEY)
        .await
        .unwrap()
        .is_some());

    // 6. B releases cleanly.
    lease::release(&store, &b).await.expect("clean release");
    assert!(store
        .get(migration_coord::layout::LEASE_KEY)
        .await
        .unwrap()
        .is_none());

    cleanup(&store, &prefix).await;
}

#[tokio::test]
#[ignore]
async fn snapshot_write_load_round_trip() {
    let store = match make_store().await {
        Some(s) => s,
        None => return,
    };
    let prefix = store.prefix().to_string();

    let mut snap = Snapshot::empty(Utc::now());
    snap.last_seq = 42;
    snapshot::write(&store, &snap, 3, Utc::now())
        .await
        .expect("write");

    let loaded = snapshot::load(&store).await.expect("load").expect("Some");
    assert_eq!(loaded.last_seq, 42);

    // Second write drops a history copy; history list reflects that.
    snap.last_seq = 100;
    snapshot::write(&store, &snap, 3, Utc::now() + Duration::seconds(1))
        .await
        .expect("write 2");
    let history = snapshot::list_history(&store).await.expect("history");
    assert!(!history.is_empty());

    cleanup(&store, &prefix).await;
}

#[tokio::test]
#[ignore]
async fn replay_snapshot_plus_log_round_trip() {
    let store = match make_store().await {
        Some(s) => s,
        None => return,
    };
    let prefix = store.prefix().to_string();

    // Seed: JobCreated, two ProgressDeltas, Completed.
    let job = JobId::new("test-bobby").unwrap();
    let envs = vec![
        EventEnvelope {
            seq: 1,
            at: Utc::now(),
            schema_version: SCHEMA_VERSION,
            worker_at: None,
            client_seq: None,
            from_worker: None,
            kind: EventKind::JobCreated {
                job_id: job.clone(),
                name: "test-bobby".into(),
                source: "nfs://src".into(),
                dest: "nfs://dst".into(),
                owner: "test".into(),
                config_hash: migration_coord::schema::ConfigHash("ab".into()),
                total_files: 0,
                total_bytes: 0,
            },
        },
        EventEnvelope {
            seq: 2,
            at: Utc::now(),
            schema_version: SCHEMA_VERSION,
            worker_at: None,
            client_seq: None,
            from_worker: None,
            kind: EventKind::ProgressDelta {
                job_id: job.clone(),
                worker_id: WorkerId::new(),
                files_delta: 10,
                bytes_delta: 1024,
                errors_delta: 0,
            },
        },
        EventEnvelope {
            seq: 3,
            at: Utc::now(),
            schema_version: SCHEMA_VERSION,
            worker_at: None,
            client_seq: None,
            from_worker: None,
            kind: EventKind::JobCompleted {
                job_id: job.clone(),
            },
        },
    ];

    let mut writer = EventLogWriter::new(EventLogConfig::default());
    let mut state = Snapshot::empty(Utc::now());
    // Snapshot at seq 1 (after JobCreated), then append remaining.
    writer.append(&store, envs[0].clone()).await.unwrap();
    state.apply(&envs[0]);
    writer.flush_all(&store).await.unwrap();
    snapshot::write(&store, &state, 3, Utc::now())
        .await
        .unwrap();

    writer.append(&store, envs[1].clone()).await.unwrap();
    writer.append(&store, envs[2].clone()).await.unwrap();
    writer.flush_all(&store).await.unwrap();

    // Replay should reach the same final state as a single-pass reduce.
    let mut single_pass = Snapshot::empty(Utc::now());
    for e in &envs {
        single_pass.apply(e);
    }

    let replayed = state::replay(&store, Utc::now()).await.expect("replay");
    assert_eq!(replayed.state.jobs[&job].progress.files_done, 10);
    assert_eq!(
        replayed.state.jobs[&job].phase,
        single_pass.jobs[&job].phase
    );
    assert_eq!(replayed.state.last_seq, 3);
    assert_eq!(replayed.snapshot_last_seq, 1);
    assert_eq!(replayed.events_applied, 2);

    // Idempotency.
    let again = state::replay(&store, Utc::now()).await.expect("replay 2");
    assert_eq!(again.state.jobs[&job], replayed.state.jobs[&job]);

    cleanup(&store, &prefix).await;
}

#[tokio::test]
#[ignore]
async fn archive_on_completion_moves_event_chunks() {
    let store = match make_store().await {
        Some(s) => s,
        None => return,
    };
    let prefix = store.prefix().to_string();

    let job = JobId::new("test-archive").unwrap();
    let mut writer = EventLogWriter::new(EventLogConfig {
        max_events_per_chunk: 2,
        max_chunk_age: Duration::seconds(60),
    });
    for seq in 1..=5 {
        writer
            .append(&store, job_env(seq, "test-archive"))
            .await
            .unwrap();
    }
    writer.flush_all(&store).await.unwrap();
    // 3 chunks (2 + 2 + 1).
    assert_eq!(store.list("events/test-archive/").await.unwrap().len(), 3);

    let out = archive_job(&store, &job).await.expect("archive");
    assert_eq!(out.chunks_moved, 3);
    assert!(store.list("events/test-archive/").await.unwrap().is_empty());
    let archived = list_archived_chunks(&store, &job)
        .await
        .expect("archived list");
    assert_eq!(archived.len(), 3);

    cleanup(&store, &prefix).await;
}

#[tokio::test]
#[ignore]
async fn concurrent_acquire_yields_exactly_one_winner() {
    // Two contenders racing on the *same* prefix. The PrefixedStore
    // serializes through one S3 bucket key, so VAST's PUT
    // If-None-Match adjudicates the race for real.
    let store = match make_store().await {
        Some(s) => s,
        None => return,
    };
    let prefix = store.prefix().to_string();

    let now = Utc::now();
    let me_a = me("A");
    let me_b = me("B");
    let (a, b) = tokio::join!(
        lease::try_acquire(&store, &me_a, cfg(), now),
        lease::try_acquire(&store, &me_b, cfg(), now),
    );
    let a = a.unwrap();
    let b = b.unwrap();
    let acquired = [&a, &b]
        .iter()
        .filter(|o| matches!(o, AcquireOutcome::Acquired(_)))
        .count();
    assert_eq!(acquired, 1, "exactly one contender must win");

    cleanup(&store, &prefix).await;
}

// Unused import suppressor — the `_` binds are local to enabled
// (ignored) test bodies; tests that the env-gate skips never touch
// them.
#[allow(dead_code)]
fn _unused() {
    let _ = (cluster_env, job_env);
}
