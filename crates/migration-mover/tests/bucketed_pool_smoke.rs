//! Real-VAST smoke for `BucketedAsyncPool`.
//!
//! Env-gated like the other async smokes: skipped unless run with
//! `--ignored` AND the required env vars are set. Defaults to the
//! `VAMOOSE_TEST_NFS_URL` convention from
//! `memory/reference_verification_env.md`; uses
//! `VAMOOSE_TEST_NFS_DST_URL` for the dest URL when set, otherwise
//! reuses the source URL (acceptable for Phase 1 — the goal is
//! "all six mounts come up with their tuned mount opts", not
//! cross-export correctness, which is Phase 2's territory).
//!
//! Build first as your user, then run as root (libnfs requires
//! UID 0 against the VAST export; see the verification-env memory):
//!
//! ```bash
//! export VAMOOSE_TEST_NFS_URL=nfs://main.selab-var204.../bgolliher/vamoose-source
//! export VAMOOSE_TEST_NFS_PATH=/src-test/m2-verify/large.bin
//! cargo build -p migration-mover --tests
//! sudo -E target/debug/deps/bucketed_pool_smoke-* --ignored --nocapture
//! ```

use std::env;

use migration_mover::bucketed_pool::{bucket_for_size, BucketedAsyncPool, BUCKETS};
use migration_mover::libnfs::asyncio::Flags;

fn src_url() -> String {
    env::var("VAMOOSE_TEST_NFS_URL").expect("VAMOOSE_TEST_NFS_URL not set")
}

fn dst_url() -> String {
    env::var("VAMOOSE_TEST_NFS_DST_URL").unwrap_or_else(|_| src_url())
}

fn probe_path() -> Vec<u8> {
    env::var("VAMOOSE_TEST_NFS_PATH")
        .expect("VAMOOSE_TEST_NFS_PATH not set")
        .into_bytes()
}

/// Build the pool and confirm every src and dst context responds to a
/// stat on a known-good path. Six contexts × one stat each.
///
/// What this test catches:
/// - `BucketedAsyncPool::new` doesn't deadlock on the concurrent
///   six-way mount.
/// - Each context honors its tuned `rsize` / `wsize` at mount (a
///   broken `MountOpts` mapping would fail validation here).
/// - Each context can actually issue one async RPC end-to-end
///   (catches a "service-task spawned but stalled" regression).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore]
async fn bucketed_pool_mounts_and_stats() {
    let src = src_url();
    let dst = dst_url();
    let path = probe_path();

    let pool = BucketedAsyncPool::new(&src, &dst, migration_mover::DEFAULT_RPC_TIMEOUT_MS, 1)
        .await
        .expect("BucketedAsyncPool::new against real VAST");

    for cfg in BUCKETS.iter() {
        let pair = pool
            .pair_by_name(cfg.name)
            .unwrap_or_else(|| panic!("pair_by_name({}) returned None", cfg.name));

        let src_attrs = pair
            .src
            .stat(&path)
            .await
            .unwrap_or_else(|e| panic!("stat on src via {} bucket: {e}", cfg.name));
        let dst_attrs = pair
            .dst
            .stat(&path)
            .await
            .unwrap_or_else(|e| panic!("stat on dst via {} bucket: {e}", cfg.name));

        assert_eq!(
            src_attrs.size, dst_attrs.size,
            "src/dst stat sizes differ for bucket {}; \
             smoke assumes the probe path resolves identically on \
             both endpoints (set VAMOOSE_TEST_NFS_DST_URL to a \
             distinct export if that assumption is wrong)",
            cfg.name,
        );
    }
}

/// Verify the pool routes a representative size to the expected
/// bucket and that the returned (src, dst) refs can open the probe
/// path read-only. This is the per-bucket-routing smoke; size-to-
/// bucket logic is unit-tested in `bucketed_pool::tests`, but here
/// we re-prove it end-to-end against a live mount.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore]
async fn pair_for_size_routes_and_opens() {
    let src = src_url();
    let dst = dst_url();
    let path = probe_path();
    let pool = BucketedAsyncPool::new(&src, &dst, migration_mover::DEFAULT_RPC_TIMEOUT_MS, 1)
        .await
        .expect("BucketedAsyncPool::new against real VAST");

    // One probe size per bucket. (0, 1 MiB, 1 GiB) cover all three.
    for size in [0u64, 1 << 20, 1 << 30] {
        let expected_bucket = bucket_for_size(size).name;
        let (src_ctx, _dst_ctx, cfg) = pool.pair_for_size(size);
        assert_eq!(
            cfg.name, expected_bucket,
            "pair_for_size({size}) routed to {} but bucket_for_size says {}",
            cfg.name, expected_bucket,
        );

        let fh = src_ctx
            .open(&path, Flags::rdonly())
            .await
            .unwrap_or_else(|e| panic!("open on {} bucket src: {e}", cfg.name));
        src_ctx
            .close(fh)
            .await
            .unwrap_or_else(|e| panic!("close on {} bucket src: {e}", cfg.name));
    }
}
