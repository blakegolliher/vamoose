//! Smoke test for the async libnfs FFI surface.
//!
//! Mirrors `libnfs_ffi_smoke.rs` (the sync FFI smoke gate) and is
//! load-bearing for the same reason: parameter-order mismatch
//! between the Rust async bindings and the actually-linked libnfs
//! `.so` would produce silent data loss that no Rust-level mock
//! catches. See `docs/CORRECTNESS_RULES.md` "Cross-check C library
//! FFI" and `docs/work-items/LIBNFS_ASYNC_FORK_AUDIT.md`.
//!
//! Marked `#[ignore]` so it does not run in CI without an
//! environment. **MUST** be run before merging any change to the
//! async FFI declarations or callback bridging.
//!
//! Run manually (build first, then run the test binary as root —
//! libnfs sends RPCs with the calling UID and the VAST export only
//! accepts root, same as the production worker under sudo):
//!
//!   cargo build -p migration-mover --tests
//!   sudo -E target/debug/deps/libnfs_async_ffi_smoke-* \
//!       --ignored --nocapture
//!
//! Required environment (export before sudo so `-E` carries them):
//!   VAMOOSE_TEST_NFS_URL=nfs://server/source-export
//!   VAMOOSE_TEST_NFS_PATH=/path/to/known-good-file
//!   VAMOOSE_TEST_NFS_EXPECTED_SIZE=<bytes>
//!   VAMOOSE_TEST_NFS_WRITE_URL=nfs://server/dest-export   (separate writable mount)
//!   VAMOOSE_TEST_NFS_WRITE_DIR=/path/to/writable/dir      (relative to write export)

use migration_mover::libnfs::asyncio::{AsyncNfsContext, Flags, MountOpts};
use std::env;
use std::time::SystemTime;

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

fn env_write_dir() -> String {
    env::var("VAMOOSE_TEST_NFS_WRITE_DIR").expect("VAMOOSE_TEST_NFS_WRITE_DIR not set")
}

fn env_write_url() -> String {
    // Fall back to the read URL if a separate write export wasn't
    // configured. The variant tests that write expect a writable
    // destination; the dst export at var204 is the natural target.
    env::var("VAMOOSE_TEST_NFS_WRITE_URL").unwrap_or_else(|_| env_url())
}

async fn mount_write() -> AsyncNfsContext {
    AsyncNfsContext::mount(&env_write_url(), MountOpts::default())
        .await
        .expect("async mount (write)")
}

fn timestamp_suffix() -> String {
    let n = SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .map(|d| d.as_nanos())
        .unwrap_or(0);
    format!("{n}")
}

async fn mount() -> AsyncNfsContext {
    AsyncNfsContext::mount(&env_url(), MountOpts::default())
        .await
        .expect("async mount")
}

/// The single most important assertion of this work item. Mirrors
/// the sync FFI smoke test: if the async pread binding has reversed
/// arguments (the M2 silent-zero-byte mode), this is the call that
/// catches it. A successful call must return >0 bytes — Ok(empty) is
/// the failure mode.
#[tokio::test]
#[ignore]
async fn async_pread_returns_actual_bytes() {
    let ctx = mount().await;
    let path = env_path();
    let expected = env_expected_size();
    assert!(expected > 0, "test requires a non-empty file");

    let fh = ctx
        .open(path.as_bytes(), Flags::rdonly())
        .await
        .expect("open");
    let bytes = ctx.pread(&fh, 0, 1024).await.expect("pread");
    ctx.close(fh).await.expect("close");

    assert!(
        !bytes.is_empty(),
        "pread returned 0 bytes from a {expected}-byte file; \
         the async FFI signature likely mismatches the linked library"
    );
    assert!(
        bytes.len() as u64 <= expected,
        "pread returned more bytes ({}) than the file contains ({})",
        bytes.len(),
        expected
    );

    ctx.shutdown().await.expect("shutdown");
}

/// Round-trip test for pwrite + pread: write a known pattern,
/// fsync, re-open, read back, byte-compare. Catches the same class
/// of FFI mismatch as the sync smoke test, but on the write path.
#[tokio::test]
#[ignore]
async fn async_write_then_read_roundtrip() {
    let ctx = mount_write().await;
    let dir = env_write_dir();
    let target = format!("{dir}/vamoose-async-smoke-{}.bin", timestamp_suffix());

    // 4 KiB of pseudo-random pattern; small enough to be one RPC.
    let pattern: Vec<u8> = (0..4096).map(|i| ((i * 31 + 7) & 0xff) as u8).collect();

    let fh = ctx
        .create(target.as_bytes(), Flags::wronly().with_create(), 0o600)
        .await
        .expect("create");
    let n = ctx.pwrite(&fh, 0, pattern.clone()).await.expect("pwrite");
    assert_eq!(n, pattern.len(), "short write");
    ctx.fsync(&fh).await.expect("fsync");
    ctx.close(fh).await.expect("close write fh");

    let fh = ctx
        .open(target.as_bytes(), Flags::rdonly())
        .await
        .expect("re-open");
    let readback = ctx.pread(&fh, 0, pattern.len()).await.expect("pread");
    ctx.close(fh).await.expect("close read fh");

    assert_eq!(readback, pattern, "round-trip byte mismatch");

    // Cleanup; treat unlink failure as a test issue, not a hard fail
    // (smoke tests are leaky by nature against an external export).
    let _ = ctx.unlink(target.as_bytes()).await;
    ctx.shutdown().await.expect("shutdown");
}

/// stat / fstat parity. Catches `nfs_stat_64` layout drift between
/// the bound struct and the linked libnfs's actual layout — a
/// stat-shape mismatch would either crash or surface garbage field
/// values, neither of which is caught by Rust-level mocks.
#[tokio::test]
#[ignore]
async fn async_stat_and_fstat_match() {
    let ctx = mount().await;
    let path = env_path();
    let expected = env_expected_size();

    let s = ctx.stat(path.as_bytes()).await.expect("stat");
    assert_eq!(s.size, expected, "stat size mismatch");
    assert!(s.mode != 0, "stat mode should be non-zero");

    let fh = ctx
        .open(path.as_bytes(), Flags::rdonly())
        .await
        .expect("open for fstat");
    let fs = ctx.fstat(&fh).await.expect("fstat");
    ctx.close(fh).await.expect("close");

    assert_eq!(s.size, fs.size, "stat vs fstat size differ");
    assert_eq!(s.ino, fs.ino, "stat vs fstat ino differ");
    assert_eq!(s.mode, fs.mode, "stat vs fstat mode differ");

    ctx.shutdown().await.expect("shutdown");
}

/// Exercise rename, chmod, chown, utimes, mkdir, unlink as a chain.
/// One assertion per call; if any libnfs `*_async` symbol has a
/// reversed parameter or wrong arity, the chain breaks on that step.
#[tokio::test]
#[ignore]
async fn async_attribute_and_namespace_ops_round_trip() {
    let ctx = mount_write().await;
    let dir = env_write_dir();
    let suffix = timestamp_suffix();

    let scratch = format!("{dir}/vamoose-async-scratch-{suffix}");
    let renamed = format!("{dir}/vamoose-async-renamed-{suffix}");
    let subdir = format!("{dir}/vamoose-async-subdir-{suffix}");

    // mkdir → chmod → chown → utimes → create file → rename → unlink → rmdir (via unlink fails for dir, so we leave it for the export's GC).
    ctx.mkdir(subdir.as_bytes(), 0o755).await.expect("mkdir");
    ctx.chmod(subdir.as_bytes(), 0o750)
        .await
        .expect("chmod dir");
    ctx.chown(subdir.as_bytes(), 0, 0).await.expect("chown dir");
    ctx.utimes(subdir.as_bytes(), 1_700_000_000, 0, 1_700_000_000, 0)
        .await
        .expect("utimes");

    let fh = ctx
        .create(scratch.as_bytes(), Flags::wronly().with_create(), 0o600)
        .await
        .expect("create scratch");
    let _ = ctx.pwrite(&fh, 0, vec![0u8; 16]).await.expect("write");
    ctx.fsync(&fh).await.expect("fsync");
    ctx.close(fh).await.expect("close");

    ctx.rename(scratch.as_bytes(), renamed.as_bytes())
        .await
        .expect("rename");
    ctx.unlink(renamed.as_bytes()).await.expect("unlink");

    // queue_length must be a non-blocking probe.
    let q = ctx.queue_length().await.expect("queue_length");
    assert!(q < 1024, "queue length suspiciously large: {q}");

    ctx.shutdown().await.expect("shutdown");
}

/// Symlink + readlink round-trip — catches missing or reordered
/// arg pair on `nfs_symlink_async`. Also exercises `lutimes_async`
/// (mtime-on-link-itself) — see MTIME_PARITY_FIX.md slice 2.
#[tokio::test]
#[ignore]
async fn async_symlink_readlink_roundtrip() {
    let ctx = mount_write().await;
    let dir = env_write_dir();
    let suffix = timestamp_suffix();
    let target_str = format!("vamoose-target-{suffix}");
    let link_path = format!("{dir}/vamoose-async-link-{suffix}");

    ctx.symlink(target_str.as_bytes(), link_path.as_bytes())
        .await
        .expect("symlink");
    let got = ctx.readlink(link_path.as_bytes()).await.expect("readlink");
    assert_eq!(got.as_slice(), target_str.as_bytes());

    // lutimes on the symlink itself — must not error and must not
    // follow the link to a (likely non-existent) target.
    ctx.lutimes(link_path.as_bytes(), 1_700_000_000, 0, 1_700_000_000, 0)
        .await
        .expect("lutimes on symlink");

    ctx.unlink(link_path.as_bytes()).await.expect("unlink");
    ctx.shutdown().await.expect("shutdown");
}
