//! F42 acceptance tests — `docs/work-items/WORKER_RESILIENCE.md`
//! item 3: transient S3 errors on the scan / manifest / acquire paths
//! must retry with jittered backoff under a bounded budget instead of
//! `?`-propagating to process exit (a sustained S3 blip used to kill
//! every worker in the fleet simultaneously). Download failures flow
//! through F13's existing release-and-skip once they are typed
//! `Error::S3`.
//!
//! Tests 8 and 10 (per the work item's numbering) were written first
//! and observed red: `retry_transient` / `fetch_shard_index` did not
//! exist, and `download_to` errors were typed `Error::Other`.
//!
//! Follows the F13 test template (`worker_error_classification.rs`):
//! drive the extracted fns against the canonical `FakeStore` under
//! paused tokio time; assert elapsed time and op counts.

use std::collections::HashSet;
use std::time::Duration;

use migration_core::claim::test_util::{FakeStore, OpKind, RiggedFailureKind};
use migration_core::claim::{self, AcquireOutcome, ClaimStore};
use migration_core::errors::Error;
use migration_core::layout;
use migration_core::records::{
    Endpoint, EndpointKind, Manifest, MigrationOptions, ShardEntry, RUN_FORMAT_VERSION,
};
use migration_core::time::UtcTime;
use migration_worker::heartbeat::HeldClaim;
use migration_worker::orchestrator::{
    fetch_shard_index, handle_process_error, retry_transient, scan_shards, transient_retry_budget,
    ClaimBodyCache, ProcessErrorContext, ShardErrorClass,
};
use tokio::sync::Mutex;

const HB_SEC: u64 = 30;
const LEASE_SEC: u64 = 180;

/// The contention-backoff base the retry wrapper reuses
/// (`backoff_after_lost_race`): `heartbeat_sec / 4`, jitter on top.
fn backoff_base() -> Duration {
    Duration::from_millis(HB_SEC * 1000 / 4)
}

fn shard_name(i: usize) -> String {
    format!("part-{i:04}.parquet")
}

fn fixture_etag(i: usize) -> String {
    format!("etag-fixture-{i:04}")
}

fn manifest_with_shards(n: usize) -> Manifest {
    Manifest {
        format_version: RUN_FORMAT_VERSION,
        run_id: "f42-transient-retry".into(),
        created_utc: UtcTime::now(),
        shards: (1..=n)
            .map(|i| ShardEntry {
                key: format!("index/{}", shard_name(i)),
                rows: 10,
                bytes: 4096,
                etag: fixture_etag(i),
            })
            .collect(),
        total_rows: (n as u64) * 10,
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

/// A typed transient-S3 error of the shape `download_to` now returns
/// (F42 re-type). The concrete variant is irrelevant — F13 classifies
/// every `Error::S3` WorkerLocal.
fn s3_error() -> Error {
    Error::S3(aws_sdk_s3::Error::NoSuchKey(
        aws_sdk_s3::types::error::NoSuchKey::builder().build(),
    ))
}

// ---------------------------------------------------------------------------
// Budget shape — R6's `max(1, lease/heartbeat)` consecutive failures.
// ---------------------------------------------------------------------------

#[test]
fn budget_shape_mirrors_r6() {
    // Same numbers as the heartbeat R6 tests: lease 40 / interval 10.
    assert_eq!(transient_retry_budget(40, 10), 4);
    assert_eq!(transient_retry_budget(LEASE_SEC, HB_SEC), 6);
    // Floors for absurd configs — never 0.
    assert_eq!(transient_retry_budget(0, 30), 1);
    assert_eq!(transient_retry_budget(5, 30), 1);
    assert_eq!(transient_retry_budget(180, 0), 180);
}

// ---------------------------------------------------------------------------
// Test 8 (red before fix): transient LIST failures inside a scan pass
// retry with backoff and do NOT exit.
// ---------------------------------------------------------------------------

#[tokio::test(start_paused = true)]
async fn scan_transient_list_error_retries_with_backoff() {
    let store = FakeStore::new();
    let manifest = manifest_with_shards(2);
    let lease = Duration::from_secs(LEASE_SEC);
    let skip: HashSet<String> = HashSet::new();
    let mut cache = ClaimBodyCache::default();
    let budget = transient_retry_budget(LEASE_SEC, HB_SEC);

    store.rig_next_lists_to_fail(2, RiggedFailureKind::Transient);

    let start = tokio::time::Instant::now();
    let mut st = (&store, &manifest, &mut cache, &skip);
    let scan = retry_transient("shard scan", budget, HB_SEC, &mut st, |st| {
        Box::pin(scan_shards(st.0, st.1, lease, HB_SEC, st.2, st.3))
    })
    .await
    .expect("sub-budget transient LIST failures must not exit the worker");
    let elapsed = start.elapsed();

    assert!(
        scan.next_target.is_some(),
        "post-retry scan must see the free shard",
    );
    assert_eq!(
        store.list_calls(),
        3,
        "expected 2 failed LIST attempts + 1 success",
    );
    assert!(
        elapsed >= backoff_base() * 2,
        "each retry must wait at least the backoff base; 2 retries took {elapsed:?} \
         (expected >= {:?})",
        backoff_base() * 2,
    );
}

// ---------------------------------------------------------------------------
// Test 9: sustained failure past the budget surfaces the error so
// `run()` exits — a worker must not spin forever on a dead bucket.
// ---------------------------------------------------------------------------

#[tokio::test(start_paused = true)]
async fn scan_budget_exhausted_exits_with_error() {
    let store = FakeStore::new();
    let manifest = manifest_with_shards(1);
    let lease = Duration::from_secs(LEASE_SEC);
    let skip: HashSet<String> = HashSet::new();
    let mut cache = ClaimBodyCache::default();
    let budget = transient_retry_budget(LEASE_SEC, HB_SEC);

    store.rig_next_lists_to_fail(u32::MAX, RiggedFailureKind::Transient);

    let mut st = (&store, &manifest, &mut cache, &skip);
    let err = retry_transient("shard scan", budget, HB_SEC, &mut st, |st| {
        Box::pin(scan_shards(st.0, st.1, lease, HB_SEC, st.2, st.3))
    })
    .await
    .expect_err("sustained store failure past the budget must surface as Err");

    assert!(
        format!("{err:#}").contains("rigged transient LIST failure"),
        "the surfaced error must be the store's, got: {err:#}",
    );
    assert_eq!(
        store.list_calls(),
        budget,
        "exactly `budget` attempts, then give up",
    );
}

// ---------------------------------------------------------------------------
// Test 10 (red before fix): a transient error from try_acquire backs
// off and retries — it neither exits the worker nor touches the skip
// set (structurally: the retry wrapper has no access to it — the
// shard is not at fault).
// ---------------------------------------------------------------------------

#[tokio::test(start_paused = true)]
async fn acquire_transient_error_backs_off_not_exit() {
    let store = FakeStore::new();
    let shard = shard_name(1);
    let budget = transient_retry_budget(LEASE_SEC, HB_SEC);

    store.rig_next_puts_to_fail(1, RiggedFailureKind::Transient);

    let start = tokio::time::Instant::now();
    let mut st = (&store, shard.as_str());
    let outcome = retry_transient("claim acquire", budget, HB_SEC, &mut st, |st| {
        Box::pin(claim::try_acquire(st.0, st.1, "host-A"))
    })
    .await
    .expect("one transient acquire failure must not exit the worker");

    assert!(
        matches!(outcome, AcquireOutcome::Acquired { .. }),
        "retry must complete the acquire, got {outcome:?}",
    );
    assert!(
        start.elapsed() >= backoff_base(),
        "acquire retry must back off first (elapsed {:?})",
        start.elapsed(),
    );
    // Exactly 2 PUT attempts: the rigged failure + the success.
    let puts = store
        .op_log()
        .iter()
        .filter(|op| matches!(op.kind, OpKind::PutIfAbsent { .. }))
        .count();
    assert_eq!(puts, 2, "expected one failed + one successful PUT");
}

// ---------------------------------------------------------------------------
// Test 11: with the re-typed Error::S3, a download failure flows
// through F13's existing WorkerLocal path — release + skip + backoff.
// No new machinery. (`download_sdk_error_is_s3_typed` in
// migration-core/src/s3.rs pins the re-type itself.)
// ---------------------------------------------------------------------------

#[tokio::test(start_paused = true)]
async fn download_transient_error_releases_and_retries_later() {
    let store = FakeStore::new();
    let manifest = manifest_with_shards(1);
    let shard = shard_name(1);
    let key = layout::claim_key(&shard);

    // Hold the claim exactly like run() does at the download point.
    let AcquireOutcome::Acquired { etag, record } = claim::try_acquire(&store, &shard, "host-A")
        .await
        .expect("acquire")
    else {
        panic!("first acquire must succeed");
    };
    let current = Mutex::new(Some(HeldClaim {
        shard: shard.clone(),
        etag: etag.clone(),
        epoch: record.epoch,
    }));
    let mut skip: HashSet<String> = HashSet::new();

    // The download seam yields a typed S3 transport error.
    let err = fetch_shard_index(async { Err(s3_error()) }, &manifest, &shard)
        .await
        .expect_err("download failure must surface");

    let start = tokio::time::Instant::now();
    let class = handle_process_error(
        ProcessErrorContext {
            store: &store,
            host_id: "host-A",
            shard_filename: &shard,
            current: &current,
            fallback_etag: &etag,
            fallback_epoch: record.epoch,
            heartbeat_sec: HB_SEC,
        },
        &err,
        &mut skip,
    )
    .await;

    // Exactly F13's WorkerLocal path.
    assert_eq!(class, ShardErrorClass::WorkerLocal);
    assert!(
        store.head_object(&key).await.unwrap().is_none(),
        "claim must be released (absent) for a healthy peer, not Failed",
    );
    assert!(skip.contains(&shard), "shard must be locally skipped");
    assert!(
        start.elapsed() >= backoff_base(),
        "release path must back off before the next scan",
    );
}

/// Good-path pin for the download seam: a download etag matching the
/// manifest passes F40 verification through `fetch_shard_index`.
#[tokio::test]
async fn fetch_shard_index_good_path_verifies() {
    let manifest = manifest_with_shards(1);
    let shard = shard_name(1);
    fetch_shard_index(async { Ok(fixture_etag(1)) }, &manifest, &shard)
        .await
        .expect("matching etag must verify");

    // And a mismatched one surfaces the typed ManifestChanged.
    let err = fetch_shard_index(async { Ok("etag-swapped".to_string()) }, &manifest, &shard)
        .await
        .expect_err("mismatched etag must fail F40 verification");
    assert!(matches!(
        err.downcast_ref::<Error>(),
        Some(Error::ManifestChanged { .. })
    ));
}

// ---------------------------------------------------------------------------
// Test 12: the budget covers CONSECUTIVE failures only — mirror of
// heartbeat.rs `refresh_transient_errors_within_budget_no_trip`. A
// success leaves the next pass with a full budget; sub-budget bursts
// across successful passes must never accumulate into an exit.
// ---------------------------------------------------------------------------

#[tokio::test(start_paused = true)]
async fn budget_resets_on_success() {
    let store = FakeStore::new();
    let manifest = manifest_with_shards(1);
    let lease = Duration::from_secs(LEASE_SEC);
    let skip: HashSet<String> = HashSet::new();
    let mut cache = ClaimBodyCache::default();
    let budget = transient_retry_budget(LEASE_SEC, HB_SEC); // 6

    for pass in 0..2u32 {
        // budget-1 failures then success — under budget on every pass.
        // If the counter accumulated across passes, the second pass
        // would exceed the budget and error out.
        store.rig_next_lists_to_fail(budget as u32 - 1, RiggedFailureKind::Transient);
        let mut st = (&store, &manifest, &mut cache, &skip);
        retry_transient("shard scan", budget, HB_SEC, &mut st, |st| {
            Box::pin(scan_shards(st.0, st.1, lease, HB_SEC, st.2, st.3))
        })
        .await
        .unwrap_or_else(|e| panic!("pass {pass}: sub-budget burst must not exit: {e:?}"));
    }

    assert_eq!(
        store.list_calls(),
        2 * budget,
        "each pass: (budget-1) failures + 1 success",
    );
}
