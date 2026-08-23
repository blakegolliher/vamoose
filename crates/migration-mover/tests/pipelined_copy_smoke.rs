//! Real-VAST smoke for `pipelined_copy`.
//!
//! Env-gated like the other async smokes. The test copies the
//! `VAMOOSE_TEST_NFS_PATH` source file through the pipeline into a
//! per-run `.partial` file under `VAMOOSE_TEST_NFS_WRITE_DIR`, then
//! re-reads the destination and asserts that:
//!
//! - `bytes_copied` matches the source size (no early-EOF on a
//!   stable file).
//! - `torn` is `false` (source is stable for the duration).
//! - The hash computed inline by the pipeline matches the hash of
//!   the destination bytes recomputed via a fresh read.
//!
//! This is the integration gate for `pipelined_copy` — the unit
//! tests in `pipelined_copy::tests` cover only the trivial pieces
//! (hash constant, error wrapping); the real loop logic is
//! verifiable only against a live cluster (per Phase 2 NOTES §5
//! Option B, no `AsyncNfsOps` trait extraction → no mocks).
//!
//! ```bash
//! export VAMOOSE_TEST_NFS_URL=nfs://main.selab-var204.../bgolliher/vamoose-source
//! export VAMOOSE_TEST_NFS_PATH=/src-test/m2-verify/large.bin
//! export VAMOOSE_TEST_NFS_EXPECTED_SIZE=104857600
//! export VAMOOSE_TEST_NFS_WRITE_URL=nfs://main.selab-var204.../bgolliher/vamoose-dest
//! export VAMOOSE_TEST_NFS_WRITE_DIR=/async-smoke   # must pre-exist
//! cargo build -p migration-mover --tests
//! sudo -E target/debug/deps/pipelined_copy_smoke-* --ignored --nocapture
//! ```

use std::env;
use std::time::SystemTime;

use migration_mover::bucketed_pool::BucketedAsyncPool;
use migration_mover::libnfs::asyncio::Flags;
use migration_mover::pipelined_copy::pipelined_copy;
use xxhash_rust::xxh3::Xxh3;

fn src_url() -> String {
    env::var("VAMOOSE_TEST_NFS_URL").expect("VAMOOSE_TEST_NFS_URL not set")
}

fn dst_url() -> String {
    env::var("VAMOOSE_TEST_NFS_WRITE_URL").expect("VAMOOSE_TEST_NFS_WRITE_URL not set")
}

fn src_path() -> Vec<u8> {
    env::var("VAMOOSE_TEST_NFS_PATH")
        .expect("VAMOOSE_TEST_NFS_PATH not set")
        .into_bytes()
}

fn expected_size() -> u64 {
    env::var("VAMOOSE_TEST_NFS_EXPECTED_SIZE")
        .expect("VAMOOSE_TEST_NFS_EXPECTED_SIZE not set")
        .parse()
        .expect("VAMOOSE_TEST_NFS_EXPECTED_SIZE not numeric")
}

fn write_dir() -> String {
    env::var("VAMOOSE_TEST_NFS_WRITE_DIR").expect("VAMOOSE_TEST_NFS_WRITE_DIR not set")
}

fn timestamp_suffix() -> String {
    let n = SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .map(|d| d.as_nanos())
        .unwrap_or(0);
    format!("{n}")
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore]
async fn pipelined_copy_roundtrip_byte_perfect() {
    let pool = BucketedAsyncPool::new(
        &src_url(),
        &dst_url(),
        migration_mover::DEFAULT_RPC_TIMEOUT_MS,
        1,
    )
    .await
    .expect("BucketedAsyncPool::new");

    let size = expected_size();
    assert!(size > 0, "test file must be non-empty");

    let (src_ctx, dst_ctx, cfg) = pool.pair_for_size(size);

    let src_p = src_path();
    let dst_p = format!(
        "{}/pipelined-copy-smoke-{}.partial",
        write_dir(),
        timestamp_suffix()
    )
    .into_bytes();

    let src_fh = src_ctx
        .open(&src_p, Flags::rdonly())
        .await
        .expect("open src");
    let dst_fh = dst_ctx
        .create(&dst_p, Flags::wronly().with_create(), 0o600)
        .await
        .expect("create dst .partial");

    let result = pipelined_copy(src_ctx, &src_fh, dst_ctx, &dst_fh, size, cfg)
        .await
        .expect("pipelined_copy");

    src_ctx.close(src_fh).await.expect("close src");
    dst_ctx.close(dst_fh).await.expect("close dst");

    // Steady-state assertions for a stable file.
    assert_eq!(
        result.bytes_copied, size,
        "bytes_copied {} != source size {} (early EOF?)",
        result.bytes_copied, size,
    );
    assert!(!result.torn, "torn read flagged on a stable test file");
    assert_eq!(
        result.post_stat.size, size,
        "post-stat size {} != source size {}",
        result.post_stat.size, size,
    );

    // Re-open dst and hash the bytes we just wrote; compare to the
    // inline-hashed value. Catches any out-of-order write delivery
    // bug that would have torn the file on disk without being
    // visible to the read pipeline's sequential walk.
    let dst_verify_fh = dst_ctx
        .open(&dst_p, Flags::rdonly())
        .await
        .expect("open dst for verify");
    let mut verify_hasher = Xxh3::new();
    let chunk = 1 << 20;
    let mut off = 0u64;
    while off < size {
        let len = std::cmp::min(chunk, size - off) as usize;
        let buf = dst_ctx
            .pread(&dst_verify_fh, off, len)
            .await
            .expect("verify pread");
        assert!(
            !buf.is_empty(),
            "unexpected EOF on dst verify at offset {off}"
        );
        verify_hasher.update(&buf);
        off += buf.len() as u64;
    }
    dst_ctx
        .close(dst_verify_fh)
        .await
        .expect("close dst verify");
    let verify_hash = verify_hasher.digest128().to_le_bytes();
    assert_eq!(
        verify_hash, result.file_hash,
        "dst content hash differs from inline hash — pipeline lost or reordered bytes",
    );

    // Best-effort cleanup; not load-bearing.
    let _ = dst_ctx.unlink(&dst_p).await;
}
