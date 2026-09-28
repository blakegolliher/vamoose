//! Smoke test for libnfs FFI signatures. This test runs against a
//! real libnfs instance and a real NFS export; it cannot be mocked,
//! because the bug it guards against — parameter-order mismatch
//! between the FFI declarations and the actually-linked `.so` — is
//! invisible to any Rust-level mock. See `docs/CORRECTNESS_RULES.md`
//! "Cross-check C library FFI" and `M2_NOTES.md` "M2/M3 verification
//! incidents".
//!
//! Marked `#[ignore]` so it does not run in CI without an
//! environment, but MUST be run before any change to libnfs FFI
//! declarations or wrapper code.
//!
//! Run manually with (build first as a normal user, then run the
//! test binary as root — libnfs sends RPCs with the calling UID and
//! the verification VAST export only accepts root, same as the
//! production worker which runs under sudo):
//!
//!   cargo build -p migration-mover --tests
//!   sudo -E target/debug/deps/libnfs_ffi_smoke-*  --ignored --nocapture
//!
//! Required environment (export before sudo so `-E` carries them):
//!   VAMOOSE_TEST_NFS_URL=nfs://server/export
//!   VAMOOSE_TEST_NFS_PATH=/path/to/known-good-file
//!   VAMOOSE_TEST_NFS_EXPECTED_SIZE=<bytes>

use migration_core::records::FailurePhase;
use migration_mover::libnfs::ops;
use migration_mover::{LibnfsContextPool, SimplePool};
use std::env;

#[tokio::test]
#[ignore]
async fn nfs_pread_returns_actual_bytes() {
    let url = env::var("VAMOOSE_TEST_NFS_URL").expect("VAMOOSE_TEST_NFS_URL not set");
    let path = env::var("VAMOOSE_TEST_NFS_PATH").expect("VAMOOSE_TEST_NFS_PATH not set");
    let expected_size: u64 = env::var("VAMOOSE_TEST_NFS_EXPECTED_SIZE")
        .expect("VAMOOSE_TEST_NFS_EXPECTED_SIZE not set")
        .parse()
        .expect("VAMOOSE_TEST_NFS_EXPECTED_SIZE not a number");

    assert!(expected_size > 0, "test requires a non-empty file");

    // Mount source and dest both pointing at the same export — we
    // only exercise reads, so dest is unused. SimplePool is the
    // single-pair specialization of MultiPool, which is what the
    // worker uses at runtime; sharing the pool surface is part of
    // what makes this an FFI-signature smoke test rather than a
    // narrower unit test.
    let pool = SimplePool::build(&url, &url, migration_mover::DEFAULT_RPC_TIMEOUT_MS)
        .expect("SimplePool::build");
    let mut pair = pool.acquire().await.expect("acquire pair");

    let path_bytes = path.as_bytes();
    let fh = ops::open_read(pair.src(), path_bytes)
        .expect("nfs_open_read failed against known-good file");

    // Read the first 1 KiB. For any non-empty file this must
    // return more than zero bytes. The buggy FFI signature would
    // return Ok(0) here — which is exactly the silent data-loss
    // failure mode this test exists to catch.
    let mut buf = vec![0u8; 1024];
    let n = ops::pread(pair.src(), &fh, 0, &mut buf).expect("nfs_pread returned an error");

    ops::close_quietly(pair.src(), fh);

    assert!(
        n > 0,
        "pread returned 0 bytes from a {} byte file; FFI signature \
         likely mismatches the linked library",
        expected_size,
    );
    assert!(
        n as u64 <= expected_size,
        "pread returned more bytes ({}) than the file contains ({})",
        n,
        expected_size,
    );
}

/// The verifier's content bracket: `stat64 / open / fstat64 / read /
/// fstat64 / stat64 / close`. Guards the audited `nfs_fstat64` binding
/// the same way `nfs_pread_returns_actual_bytes` guards `nfs_pread`: a
/// signature mismatch against the linked library would surface as a
/// zeroed or garbage snapshot, which the identity assertions below catch.
/// Same environment and root requirement as the case above.
#[tokio::test]
#[ignore]
async fn stat_open_fstat_read_fstat_stat_bracket_is_stable() {
    let url = env::var("VAMOOSE_TEST_NFS_URL").expect("VAMOOSE_TEST_NFS_URL not set");
    let path = env::var("VAMOOSE_TEST_NFS_PATH").expect("VAMOOSE_TEST_NFS_PATH not set");
    let expected_size: u64 = env::var("VAMOOSE_TEST_NFS_EXPECTED_SIZE")
        .expect("VAMOOSE_TEST_NFS_EXPECTED_SIZE not set")
        .parse()
        .expect("VAMOOSE_TEST_NFS_EXPECTED_SIZE not a number");

    let pool = SimplePool::build(&url, &url, migration_mover::DEFAULT_RPC_TIMEOUT_MS)
        .expect("SimplePool::build");
    let mut pair = pool.acquire().await.expect("acquire pair");
    let ctx = pair.src();
    let path_bytes = path.as_bytes();

    let before_path = ops::stat_snapshot(ctx, path_bytes).expect("nfs_stat64 before open");
    assert_eq!(before_path.size, expected_size, "path stat size");
    assert_ne!(before_path.ino, 0, "path stat fileid must be populated");

    let fh = ops::open_read(ctx, path_bytes).expect("nfs_open");
    let before = ops::fstat_snapshot(ctx, &fh).expect("nfs_fstat64 before read");
    assert_eq!(
        before, before_path,
        "handle and path observations must agree"
    );

    let mut buf = vec![0u8; 1024];
    let n = ops::pread(ctx, &fh, 0, &mut buf).expect("nfs_pread");
    assert!(n > 0 || expected_size == 0, "read returned no data");

    let after = ops::fstat_snapshot(ctx, &fh).expect("nfs_fstat64 after read");
    assert_eq!(after, before, "handle identity changed across the read");
    let after_path = ops::stat_snapshot(ctx, path_bytes).expect("nfs_stat64 after read");
    assert_eq!(
        after_path, before,
        "path no longer names the file that was read"
    );

    ops::close_fh(ctx, fh, FailurePhase::Read).expect("nfs_close");
}
