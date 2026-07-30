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

/// F11 hardware case (PROTECTED_FFI_BATCH.md Item 2): force an error
/// return from `pipelined_copy` while reads are genuinely in flight,
/// and assert the context remains usable afterwards — the drain
/// awaited the orphan RPC completions, so the (caller-issued) closes
/// act on quiescent fhs and later opens/reads on the same context
/// still work; the process doesn't crash.
///
/// Error injection: the "dst" fh is opened RDONLY on the same
/// context/file, so the first pwrite is rejected
/// (`nfs_pwrite_async` refuses read-only fhs) after the read pump
/// has already loaded the pipeline — reads are in flight at the
/// moment the error surfaces. Needs the known-good file to span
/// several rsize chunks (the standard perf-smoke file does).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore]
async fn error_path_leaves_context_usable_after_drain() {
    use migration_mover::{bucket_for_size, pipelined_copy};

    let ctx = mount().await;
    let path = env_path();
    let expected = env_expected_size();
    assert!(
        expected > 4 * 1024 * 1024,
        "test wants a file large enough to keep reads in flight \
         when the first write fails (got {expected} bytes)"
    );

    let src_fh = ctx
        .open(path.as_bytes(), Flags::rdonly())
        .await
        .expect("open src");
    let dst_fh = ctx
        .open(path.as_bytes(), Flags::rdonly())
        .await
        .expect("open dst (deliberately RDONLY)");

    let cfg = bucket_for_size(expected);
    let res = pipelined_copy(&ctx, &src_fh, &ctx, &dst_fh, expected, cfg).await;
    assert!(
        res.is_err(),
        "pwrite against an RDONLY fh must fail the copy"
    );

    // The fhs must be closeable (the drain already quiesced them —
    // this mirrors copy_regular's unconditional closes)...
    ctx.close(src_fh).await.expect("close src after drain");
    ctx.close(dst_fh).await.expect("close dst after drain");

    // ...and the context must still service fresh RPCs.
    let fh = ctx
        .open(path.as_bytes(), Flags::rdonly())
        .await
        .expect("re-open after error path");
    let bytes = ctx
        .pread(&fh, 0, 4096)
        .await
        .expect("pread after error path");
    assert!(!bytes.is_empty(), "context wedged after drained error");
    ctx.close(fh).await.expect("close");
    ctx.shutdown().await.expect("shutdown");
}

/// F12 hardware case (PROTECTED_FFI_BATCH.md Item 1): a context
/// created with an explicit `rpc_timeout_ms` must fail a mount
/// against an unreachable (blackholed) address within ~2× the
/// configured timeout, not hang toward the 60 s library default —
/// the hang shape `parallel_mounts_in_one_runtime_dont_collide`
/// below documents. The timeout is applied at context creation,
/// before `nfs_mount_async`, so the mount's own RPCs are bounded;
/// with libnfs's `retrans = 0` default even never-sent outqueue
/// pdus time out (`lib/socket.c:rpc_timeout_scan`, which runs at
/// most once per second — hence the +1.5 s slack below).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore]
async fn mount_unreachable_addr_fails_within_2x_rpc_timeout() {
    // TEST-NET-1 (RFC 5737): reserved for documentation, never
    // routed — SYNs blackhole. Override if the lab network differs.
    let url = env::var("VAMOOSE_TEST_NFS_UNREACHABLE_URL")
        .unwrap_or_else(|_| "nfs://192.0.2.1/timeout-probe".to_string());
    const TIMEOUT_MS: u64 = 3_000;

    let opts = MountOpts {
        rpc_timeout_ms: TIMEOUT_MS as u32,
        ..MountOpts::default()
    };
    let started = std::time::Instant::now();
    let res = AsyncNfsContext::mount(&url, opts).await;
    let elapsed = started.elapsed();

    assert!(
        res.is_err(),
        "mount against unreachable {url} unexpectedly succeeded"
    );
    assert!(
        elapsed < std::time::Duration::from_millis(2 * TIMEOUT_MS + 1_500),
        "mount failure took {elapsed:?} with rpc_timeout_ms={TIMEOUT_MS}; \
         expected ~2x the configured timeout — is nfs_set_timeout \
         applied at context creation?"
    );
}

/// Regression for the 2026-05-18 async-mount fd-swap bug:
/// `nfs_mount_async` on NFSv3 walks portmap → mountd → portmap →
/// nfsd, disconnecting and reconnecting at each step (each transition
/// changes `rpc->fd`). The original `driver::run` captured the fd
/// once and registered `AsyncFd` on it; after the first reconnect,
/// the registration pointed at a closed fd that the kernel could
/// recycle to a sibling libnfs context in the same process. When
/// several mounts raced inside one process, libnfs got cross-wired
/// events and the mount failed with `RPC_STATUS_CANCEL`
/// ("Command was cancelled") or stalled out at the 60 s RPC
/// timeout ("Command timed out").
///
/// This test asserts that several concurrent mounts in a single
/// runtime all complete promptly. Failure mode if the fix regresses:
/// hangs near the 60 s RPC timeout.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore]
async fn parallel_mounts_in_one_runtime_dont_collide() {
    const N: usize = 4;
    let url = env_url();

    let started = std::time::Instant::now();
    let mut handles = Vec::with_capacity(N);
    for _ in 0..N {
        let url = url.clone();
        handles.push(tokio::spawn(async move {
            AsyncNfsContext::mount(&url, MountOpts::default()).await
        }));
    }
    let mut ctxs = Vec::with_capacity(N);
    for h in handles {
        let ctx = h
            .await
            .expect("join")
            .expect("mount must complete (fd-swap regression?)");
        ctxs.push(ctx);
    }
    let elapsed = started.elapsed();

    // Mount against var204 is ~50 ms steady state. 10 s is generous
    // headroom for cluster noise while still catching a regression
    // that would otherwise hit the 60 s RPC timeout.
    assert!(
        elapsed < std::time::Duration::from_secs(10),
        "parallel mounts took {elapsed:?} — suggests fd-swap drift"
    );

    for ctx in ctxs {
        ctx.shutdown().await.expect("shutdown");
    }
}
