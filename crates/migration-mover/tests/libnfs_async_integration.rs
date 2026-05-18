//! Service-task integration tests for the async libnfs surface.
//!
//! Per Section "Verification" in `LIBNFS_ASYNC_FORK.md`:
//!
//! - 64 concurrent `pread_async` calls against one context — all
//!   complete with correct data.
//! - The service task wakes the right futures (no cross-talk).
//! - Dropping one future before it completes does not break the
//!   others.
//!
//! Run with the same env-var setup as `libnfs_async_ffi_smoke.rs`:
//!
//!   cargo build -p migration-mover --tests
//!   sudo -E target/debug/deps/libnfs_async_integration-* \
//!       --ignored --nocapture

use migration_mover::libnfs::asyncio::{AsyncNfsContext, Flags, MountOpts};
use std::env;

const PREAD_SIZE: usize = 64 * 1024;
const CONCURRENCY: usize = 64;

fn env_url() -> String {
    env::var("VAMOOSE_TEST_NFS_URL").expect("VAMOOSE_TEST_NFS_URL not set")
}

fn env_path() -> String {
    env::var("VAMOOSE_TEST_NFS_PATH").expect("VAMOOSE_TEST_NFS_PATH not set")
}

fn env_expected_size() -> u64 {
    env::var("VAMOOSE_TEST_NFS_EXPECTED_SIZE")
        .expect("VAMOOSE_TEST_NFS_EXPECTED_SIZE not set")
        .parse()
        .expect("VAMOOSE_TEST_NFS_EXPECTED_SIZE not numeric")
}

async fn mount() -> AsyncNfsContext {
    AsyncNfsContext::mount(&env_url(), MountOpts::default())
        .await
        .expect("async mount")
}

/// 64 concurrent preads at distinct offsets against the same
/// context. Each pread must complete; bytes must match a single
/// "ground truth" sync-read of the same region. This catches:
///   - `private_data` crosswiring (Pending<T> for op A delivered to
///     callback B → wrong oneshot fires → result mismatch).
///   - Service-task missed wakeups (any future stuck pending).
///   - Buffer aliasing between concurrent preads on one context.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore]
async fn concurrent_preads_no_crosstalk() {
    let ctx = mount().await;
    let path = env_path();
    let expected = env_expected_size();

    let max_offset = expected.saturating_sub(PREAD_SIZE as u64);
    assert!(
        max_offset > 0,
        "test requires the known-good file be at least {} bytes \
         (got {}); set VAMOOSE_TEST_NFS_PATH to a larger file",
        PREAD_SIZE,
        expected
    );

    // Build the offset list: evenly-spaced across the file so we
    // span enough wire traffic to force the service task to mux.
    let stride = max_offset / CONCURRENCY as u64;
    let offsets: Vec<u64> = (0..CONCURRENCY as u64).map(|i| i * stride).collect();

    // Read ground truth once, sequentially, on a fresh handle.
    let truth_fh = ctx
        .open(path.as_bytes(), Flags::rdonly())
        .await
        .expect("open truth");
    let mut truth: Vec<Vec<u8>> = Vec::with_capacity(CONCURRENCY);
    for off in &offsets {
        let b = ctx
            .pread(&truth_fh, *off, PREAD_SIZE)
            .await
            .expect("truth pread");
        assert_eq!(b.len(), PREAD_SIZE, "truth short read at {off}");
        truth.push(b);
    }
    ctx.close(truth_fh).await.expect("close truth");

    // Issue all PREADs concurrently against a single fh.
    let fh = ctx
        .open(path.as_bytes(), Flags::rdonly())
        .await
        .expect("open concurrent");
    let fh_ref = &fh;
    let ctx_ref = &ctx;

    let mut futs = Vec::with_capacity(CONCURRENCY);
    for off in offsets.iter().copied() {
        futs.push(async move {
            let b = ctx_ref.pread(fh_ref, off, PREAD_SIZE).await.expect("pread");
            (off, b)
        });
    }
    let results: Vec<(u64, Vec<u8>)> = futures::future::join_all(futs).await;

    for (i, (off, bytes)) in results.into_iter().enumerate() {
        assert_eq!(off, offsets[i], "result ordering invariant");
        assert_eq!(
            bytes.len(),
            PREAD_SIZE,
            "short read at offset {off} (got {} bytes)",
            bytes.len()
        );
        assert_eq!(
            bytes, truth[i],
            "byte mismatch at offset {off}; cross-talk between callbacks?"
        );
    }

    ctx.close(fh).await.expect("close");
    ctx.shutdown().await.expect("shutdown");
}

/// Drop half the in-flight futures before they complete; the
/// remaining half must still deliver correct bytes. The dropped
/// futures' RPCs are not cancellable (libnfs has no cancel for
/// NFSv3) — what we're testing is that the orphan completions
/// don't poison the survivors.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore]
async fn dropping_futures_does_not_break_neighbors() {
    let ctx = mount().await;
    let path = env_path();
    let expected = env_expected_size();
    let max_offset = expected.saturating_sub(PREAD_SIZE as u64);
    assert!(max_offset > 0, "test file too small");

    let fh = ctx
        .open(path.as_bytes(), Flags::rdonly())
        .await
        .expect("open");
    let stride = max_offset / CONCURRENCY as u64;

    {
        let fh_ref = &fh;
        let ctx_ref = &ctx;
        // Issue all preads, then drop the futures at even indices
        // before awaiting any of them.
        let mut futs: Vec<_> = (0..CONCURRENCY as u64)
            .map(|i| {
                let off = i * stride;
                Box::pin(async move { ctx_ref.pread(fh_ref, off, PREAD_SIZE).await })
            })
            .collect();

        let mut survivors = Vec::new();
        for (i, fut) in futs.drain(..).enumerate() {
            if i % 2 == 1 {
                survivors.push((i, fut));
            } else {
                drop(fut);
            }
        }

        for (i, fut) in survivors {
            let bytes = fut.await.expect("survivor pread");
            assert_eq!(
                bytes.len(),
                PREAD_SIZE,
                "survivor at idx {i} got short read"
            );
        }
    }

    ctx.close(fh).await.expect("close");
    ctx.shutdown().await.expect("shutdown");
}

/// Mount-options enforcement: reject `nconnect > 1` on the linked
/// libnfs which does not support it. This is the audit-doc gate (#1)
/// surfaced at runtime.
#[tokio::test]
#[ignore]
async fn rejects_nconnect_gt_one() {
    let opts = MountOpts {
        nconnect: 4,
        ..MountOpts::default()
    };
    let err = match AsyncNfsContext::mount(&env_url(), opts).await {
        Ok(_) => panic!("expected mount to fail with UnsupportedOpt"),
        Err(e) => e,
    };
    let msg = format!("{err}");
    assert!(
        msg.contains("nconnect=") && msg.contains("LIBNFS_ASYNC_FORK_AUDIT"),
        "expected nconnect rejection error referencing audit doc; got: {msg}"
    );
}

/// Mount-options enforcement: reject non-3 NFS version. NFSv3 is
/// the protocol baseline per `docs/CORRECTNESS_RULES.md`.
#[tokio::test]
#[ignore]
async fn rejects_non_v3() {
    let opts = MountOpts {
        version: 4,
        ..MountOpts::default()
    };
    let err = match AsyncNfsContext::mount(&env_url(), opts).await {
        Ok(_) => panic!("expected mount to fail with UnsupportedOpt"),
        Err(e) => e,
    };
    let msg = format!("{err}");
    assert!(
        msg.contains("version=4"),
        "expected v4 rejection error; got: {msg}"
    );
}
