//! Regression tests for the 2026-08-22 live-claim theft.
//!
//! `claimed_utc` is written once at acquire and never advances
//! (heartbeat refresh is HEAD-and-compare), so lease-age alone marks
//! EVERY long-held shard "stale". On the 600M rig run a fresh-host_id
//! worker scanned while a peer had held its shard for >lease and stole
//! it mid-copy — the dual-writer window the design exists to prevent,
//! closed only by the victim's self-fence.
//!
//! The fix makes the per-host progress heartbeat authoritative for
//! Active claims: a fresh, matching-`held_etag` progress record vetoes
//! reclaim regardless of claim age; lease age decides only when the
//! progress object cannot be read.

use std::collections::HashSet;
use std::time::Duration;

use chrono::{Duration as ChronoDuration, Utc};
use migration_core::claim::test_util::{FakeStore, RiggedFailureKind};
use migration_core::claim::ClaimStore;
use migration_core::layout;
use migration_core::records::{
    ClaimRecord, ClaimState, Endpoint, EndpointKind, Manifest, MigrationOptions, ProgressRecord,
    ShardEntry, RUN_FORMAT_VERSION,
};
use migration_core::time::UtcTime;
use migration_worker::orchestrator::{scan_shards, ClaimBodyCache, ClaimTarget};

const HB_SEC: u64 = 30;
const LEASE_SEC: u64 = 180;
const SHARD: &str = "part-0001.parquet";
const OWNER: &str = "owner-host";

fn manifest_one_shard() -> Manifest {
    Manifest {
        format_version: RUN_FORMAT_VERSION,
        run_id: "lease-liveness-regression".into(),
        created_utc: UtcTime::now(),
        shards: vec![ShardEntry {
            key: format!("index/{SHARD}"),
            rows: 10,
            bytes: 1024,
            etag: "shard-etag".into(),
        }],
        total_rows: 10,
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

/// Seed an Active claim whose `claimed_utc` is far beyond the lease
/// window — the normal state of any long-running shard. Returns the
/// claim's store etag.
async fn seed_ancient_active_claim(store: &FakeStore) -> String {
    let record = ClaimRecord {
        host: OWNER.into(),
        claimed_utc: UtcTime(Utc::now() - ChronoDuration::seconds(10 * LEASE_SEC as i64)),
        epoch: 3,
        state: ClaimState::Active,
    };
    store
        .put_unconditional(
            &layout::claim_key(SHARD),
            serde_json::to_vec(&record).unwrap(),
        )
        .await
        .unwrap()
}

async fn seed_progress(store: &FakeStore, held_etag: &str, heartbeat_age_sec: i64) {
    let p = ProgressRecord {
        host: OWNER.into(),
        started_utc: UtcTime::now(),
        heartbeat_utc: UtcTime(Utc::now() - ChronoDuration::seconds(heartbeat_age_sec)),
        current_shard: Some(SHARD.into()),
        shard_rows_total: 10,
        shard_rows_done: 1,
        shard_bytes_done: 100,
        files_ok: 1,
        files_failed: 0,
        files_fenced: 0,
        throughput_mb_s_1m: 1.0,
        status: "active".into(),
        held_etag: Some(held_etag.into()),
        heartbeat_sec: HB_SEC,
        latency: None,
    };
    store
        .put_unconditional(
            &layout::progress_key(OWNER),
            serde_json::to_vec(&p).unwrap(),
        )
        .await
        .unwrap();
}

async fn scan(store: &FakeStore, cache: &mut ClaimBodyCache) -> Option<ClaimTarget> {
    scan_shards(
        store,
        &manifest_one_shard(),
        Duration::from_secs(LEASE_SEC),
        HB_SEC,
        cache,
        &HashSet::new(),
    )
    .await
    .unwrap()
    .next_target
}

/// THE regression: lease long expired, but the owner's progress
/// heartbeat is fresh and carries the claim's etag. The shard must
/// NOT be a reclaim target.
#[tokio::test]
async fn live_heartbeat_vetoes_lease_expiry() {
    let store = FakeStore::new();
    let etag = seed_ancient_active_claim(&store).await;
    seed_progress(&store, &etag, 2).await;

    let mut cache = ClaimBodyCache::new();
    assert!(
        scan(&store, &mut cache).await.is_none(),
        "an actively-heartbeating owner's shard was offered for reclaim",
    );
}

/// Owner genuinely dead: heartbeat far past the 2×hb_sec freshness
/// window. Reclaim proceeds (and does not need to wait out the lease).
#[tokio::test]
async fn stale_heartbeat_is_reclaimable() {
    let store = FakeStore::new();
    let etag = seed_ancient_active_claim(&store).await;
    seed_progress(&store, &etag, (HB_SEC * 4) as i64).await;

    let mut cache = ClaimBodyCache::new();
    match scan(&store, &mut cache).await {
        Some(ClaimTarget::Stale {
            shard, stale_etag, ..
        }) => {
            assert_eq!(shard, SHARD);
            assert_eq!(stale_etag, etag);
        }
        other => panic!("expected Stale target, got {other:?}"),
    }
}

/// Progress object unreadable (S3 GET error): fall back to the lease
/// window. Lease expired → reclaimable; this is the only path where
/// lease age alone still decides.
#[tokio::test]
async fn progress_error_falls_back_to_lease() {
    let store = FakeStore::new();
    let etag = seed_ancient_active_claim(&store).await;
    seed_progress(&store, &etag, 2).await;

    // Pre-warm the claim-body cache so the scan's only GET is the
    // progress fetch, then rig that one GET to fail.
    let mut cache = ClaimBodyCache::new();
    let record = ClaimRecord {
        host: OWNER.into(),
        claimed_utc: UtcTime(Utc::now() - ChronoDuration::seconds(10 * LEASE_SEC as i64)),
        epoch: 3,
        state: ClaimState::Active,
    };
    cache.insert(layout::claim_key(SHARD), (etag.clone(), record));
    store.rig_next_gets_to_fail(1, RiggedFailureKind::Transient);

    match scan(&store, &mut cache).await {
        Some(ClaimTarget::Stale { stale_etag, .. }) => assert_eq!(stale_etag, etag),
        other => panic!("expected lease-fallback Stale target, got {other:?}"),
    }
}
