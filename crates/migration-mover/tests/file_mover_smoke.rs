//! End-to-end smoke for `AsyncBucketedFileMover` against real VAST.
//!
//! Constructs the full stack — sync `SimplePool`, `BucketedAsyncPool`,
//! `Mover`, `AsyncBucketedFileMover` — and drives a single regular
//! file through `FileMover::move_one`. Verifies the result.
//!
//! Builds on the env contract from `pipelined_copy_smoke.rs`. Add
//! `VAMOOSE_TEST_NFS_WRITE_DIR` (must pre-exist under the write
//! export).
//!
//! ```bash
//! sudo -E target/debug/deps/file_mover_smoke-* --ignored --nocapture
//! ```

use std::env;
use std::sync::Arc;
use std::time::SystemTime;

use migration_core::fence::Fence;
use migration_core::records::MigrationOptions;
use migration_core::schema::FileTypeTag;
use migration_core::shard::RowView;
use migration_mover::attrs::AttrPolicy;
use migration_mover::batch::InflightProfile;
use migration_mover::bucketed_pool::BucketedAsyncPool;
use migration_mover::file_mover::AsyncBucketedFileMover;
use migration_mover::libnfs::SimplePool;
use migration_mover::{DowngradeSink, FileMover, Mover, MoverConfig, DEFAULT_RPC_TIMEOUT_MS};

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
async fn async_bucketed_mover_copies_regular_file_end_to_end() {
    let src_url_s = src_url();
    let dst_url_s = dst_url();

    // The path under the source export to copy.
    let src_relative_inside_export = src_path();
    let size = expected_size();
    assert!(size > 0, "test file must be non-empty");

    // Choose a destination path under VAMOOSE_TEST_NFS_WRITE_DIR.
    // This becomes the row's relative-to-dst-root path. We'll set
    // both source_root and dest_root to "" so join_root just returns
    // the absolute paths we encode in row.path. But row.path is the
    // *same* on both sides, so the source path under src_root and
    // dest path under dest_root must match exactly.
    //
    // To make this work with a fixed row.path = "/foo/bar/large.bin",
    // we set source_root="" and dest_root=write_dir so the source
    // path is /foo/bar/large.bin (we'll temp-stage the source) and
    // dest is write_dir/foo/bar/large.bin. That's clumsy. Simpler:
    // set source_root and dest_root to "" and use the absolute path
    // for the row, and append a per-run suffix to the dest by using
    // a different layout for each side via a unique row.path that
    // happens to resolve to both the existing src test file (via
    // source_root prefix) AND a writable dst path (via dest_root
    // prefix). Easiest: stage source_root = "/" then row.path is
    // /src-test/m2-verify/large.bin which exists; dest_root =
    // write_dir + "/file-mover-smoke-{ts}/" so the row's path falls
    // under a per-run dir.
    //
    // We need source_root + row.path == /src-test/m2-verify/large.bin
    //          dest_root  + row.path == {write_dir}/{suffix}/{rel}
    //
    // Set source_root = "" and dest_root = "" and put the *full*
    // src absolute path in row.path. Then handle the dest separately:
    // sad, doesn't fit join_root's API since src and dst share row.path.
    //
    // Simpler still: make the row path absolute on dest by setting
    // source_root = "" and dest_root = write_dir/{suffix}, with the
    // row.path = "/{basename}" e.g. "/large.bin". Source side: the
    // test file's absolute path is /src-test/m2-verify/large.bin.
    // join_root("", "/large.bin") → "/large.bin" which does NOT
    // resolve to /src-test/m2-verify/large.bin.
    //
    // Cleanest: stage a fresh source file at write_dir to copy from.
    // Both src and dst URLs are the same export (the write export),
    // and we can write a fresh file under {write_dir}/in/, then
    // mover copies it to {write_dir}/out/. Source and destination
    // are the same export but different parent dirs, so the overlap
    // guard doesn't trip.
    //
    // For this smoke we keep src_url and dst_url as the test
    // configured (typically source-export and dest-export) and use
    // separate root prefixes so the row's relative path maps to the
    // actual test file on src and to a per-run path on dst.

    let suffix = timestamp_suffix();
    // Source path on src side: stage by treating the configured
    // VAMOOSE_TEST_NFS_PATH as `{source_root}{row.path}`. We split
    // off the last segment as row.path and use the leading portion
    // as source_root. That way join_root reconstructs the actual
    // file path on src.
    let last_slash = src_relative_inside_export
        .iter()
        .rposition(|&b| b == b'/')
        .expect("VAMOOSE_TEST_NFS_PATH must be absolute");
    let source_root = String::from_utf8(src_relative_inside_export[..last_slash].to_vec())
        .expect("source root utf8");
    let basename = src_relative_inside_export[last_slash..].to_vec(); // starts with /

    // Dest root: a fresh per-run directory under write_dir so the
    // rename produces {write_dir}/file-mover-smoke-{suffix}/{basename}.
    let dest_root = format!("{}/file-mover-smoke-{suffix}", write_dir());

    let cfg = Arc::new(MoverConfig {
        source_url: src_url_s.clone(),
        dest_url: dst_url_s.clone(),
        source_root: source_root.clone(),
        dest_root: dest_root.clone(),
        policy: AttrPolicy::from_options(&MigrationOptions::default()),
        inflight: InflightProfile::default(),
        require_chown: false, // running as root in the test but be defensive
        require_unchanged_size: false,
        use_raw_fh: false,
        direct_commit: false,
        rpc_timeout_ms: DEFAULT_RPC_TIMEOUT_MS,
    });

    let downgrades = DowngradeSink::new();
    let fence = Fence::new();

    let sync_pool = SimplePool::build(&src_url_s, &dst_url_s, DEFAULT_RPC_TIMEOUT_MS)
        .expect("SimplePool::build");
    let sync_mover = Mover::new(
        (*cfg).clone(),
        sync_pool,
        "test-host",
        downgrades.clone(),
        fence.clone(),
    );

    let async_pool = Arc::new(
        BucketedAsyncPool::new(&src_url_s, &dst_url_s, DEFAULT_RPC_TIMEOUT_MS, 1)
            .await
            .expect("BucketedAsyncPool::new"),
    );

    let file_mover = AsyncBucketedFileMover::new(
        async_pool,
        sync_mover,
        cfg.clone(),
        fence.clone(),
        "test-host",
        downgrades.clone(),
    );

    // Build the row.
    let row = RowView {
        row_id: 1,
        path: basename.clone(),
        size,
        mtime_sec: Some(1_700_000_000),
        mtime_nsec: Some(0),
        atime_sec: Some(1_700_000_000),
        atime_nsec: Some(0),
        mode: 0o644,
        uid: Some(0),
        gid: Some(0),
        nlink: Some(1),
        inode: None,
        fsid: None,
        xattr_blob: None,
        symlink_target: None,
        file_type: FileTypeTag::Regular,
    };

    let outcome = file_mover.move_one(&row).await;

    assert!(
        outcome.result.is_ok(),
        "move_one failed: {:?}",
        outcome.result
    );
    assert_eq!(
        outcome.bytes_moved, size,
        "bytes_moved {} != size {}",
        outcome.bytes_moved, size
    );

    // Verify the file landed at the expected path on dst with the
    // expected size. We reuse the bucketed pool's small bucket
    // (any bucket's dst ctx sees the same namespace) for the stat.
    let pool = BucketedAsyncPool::new(&src_url_s, &dst_url_s, DEFAULT_RPC_TIMEOUT_MS, 1)
        .await
        .expect("verify pool");
    let dst_full = format!("{dest_root}{}", String::from_utf8_lossy(&basename));
    let stat = pool
        .pair_by_name("small")
        .expect("small bucket")
        .dst
        .stat(dst_full.as_bytes())
        .await
        .expect("stat dst");
    assert_eq!(stat.size, size, "dst stat size {} != {}", stat.size, size);
    assert_eq!(
        stat.mode & 0o777,
        0o644,
        "dst mode != 0o644 (got {:o})",
        stat.mode & 0o777,
    );
    assert_eq!(
        stat.mtime, 1_700_000_000,
        "dst mtime != 1_700_000_000 (got {})",
        stat.mtime
    );

    // Best-effort cleanup. Leaves the per-run dir; one path each so
    // the export doesn't accumulate over many runs but the dirs
    // hang around for postmortem if a test fails partway.
    let _ = pool
        .pair_by_name("small")
        .expect("small bucket")
        .dst
        .unlink(dst_full.as_bytes())
        .await;
}

/// F10 hardlink replay idempotency: copy a file, `move_hardlink` a
/// second path to it, then call `move_hardlink` AGAIN for the same
/// row (simulating at-least-once redelivery after a worker died
/// post-link-pre-ack). The replay must succeed — nfs_link's EEXIST
/// resolves to Ok because both paths already share a fileid — and
/// both paths must share an inode afterwards. VAST rig pass only.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore]
async fn hardlink_replay_is_idempotent() {
    let src_url_s = src_url();
    let dst_url_s = dst_url();

    let src_relative_inside_export = src_path();
    let size = expected_size();
    assert!(size > 0, "test file must be non-empty");

    // Same env contract and root layout as the end-to-end smoke
    // above: split the configured src path into source_root +
    // row.path, land dest under a per-run dir.
    let suffix = timestamp_suffix();
    let last_slash = src_relative_inside_export
        .iter()
        .rposition(|&b| b == b'/')
        .expect("VAMOOSE_TEST_NFS_PATH must be absolute");
    let source_root = String::from_utf8(src_relative_inside_export[..last_slash].to_vec())
        .expect("source root utf8");
    let basename = src_relative_inside_export[last_slash..].to_vec(); // starts with /
    let dest_root = format!("{}/hardlink-replay-smoke-{suffix}", write_dir());

    let cfg = Arc::new(MoverConfig {
        source_url: src_url_s.clone(),
        dest_url: dst_url_s.clone(),
        source_root: source_root.clone(),
        dest_root: dest_root.clone(),
        policy: AttrPolicy::from_options(&MigrationOptions::default()),
        inflight: InflightProfile::default(),
        require_chown: false,
        require_unchanged_size: false,
        use_raw_fh: false,
        direct_commit: false,
        rpc_timeout_ms: DEFAULT_RPC_TIMEOUT_MS,
    });

    let downgrades = DowngradeSink::new();
    let fence = Fence::new();

    let sync_pool = SimplePool::build(&src_url_s, &dst_url_s, DEFAULT_RPC_TIMEOUT_MS)
        .expect("SimplePool::build");
    let sync_mover = Mover::new(
        (*cfg).clone(),
        sync_pool,
        "test-host",
        downgrades.clone(),
        fence.clone(),
    );

    let async_pool = Arc::new(
        BucketedAsyncPool::new(&src_url_s, &dst_url_s, DEFAULT_RPC_TIMEOUT_MS, 1)
            .await
            .expect("BucketedAsyncPool::new"),
    );

    let file_mover = AsyncBucketedFileMover::new(
        async_pool,
        sync_mover,
        cfg.clone(),
        fence.clone(),
        "test-host",
        downgrades.clone(),
    );

    // 1. Copy the file — this becomes the hardlink group's "first
    //    path" (the final, post-rename link target — R5).
    let row = RowView {
        row_id: 1,
        path: basename.clone(),
        size,
        mtime_sec: Some(1_700_000_000),
        mtime_nsec: Some(0),
        atime_sec: Some(1_700_000_000),
        atime_nsec: Some(0),
        mode: 0o644,
        uid: Some(0),
        gid: Some(0),
        nlink: Some(2),
        inode: None,
        fsid: None,
        xattr_blob: None,
        symlink_target: None,
        file_type: FileTypeTag::Regular,
    };
    let copy_outcome = file_mover.move_one(&row).await;
    assert!(
        copy_outcome.result.is_ok(),
        "copy move_one failed: {:?}",
        copy_outcome.result
    );

    // 2. Hardlink a second path to it.
    let mut link_basename = basename.clone();
    link_basename.extend_from_slice(b".link");
    let link_row = RowView {
        row_id: 2,
        path: link_basename.clone(),
        ..row.clone()
    };
    let first = file_mover.move_hardlink(&link_row, &basename).await;
    assert!(
        first.result.is_ok(),
        "first move_hardlink failed: {:?}",
        first.result
    );

    // 3. Replay the SAME hardlink row — simulates redelivery after a
    //    died-post-link-pre-ack worker. Must succeed, not EEXIST-fail.
    let replay = file_mover.move_hardlink(&link_row, &basename).await;
    assert!(
        replay.result.is_ok(),
        "replayed move_hardlink must be idempotent, got: {:?}",
        replay.result
    );
    assert_eq!(replay.bytes_moved, 0, "hardlink rows move no file data");

    // 4. Both paths must share an inode on the destination.
    let pool = BucketedAsyncPool::new(&src_url_s, &dst_url_s, DEFAULT_RPC_TIMEOUT_MS, 1)
        .await
        .expect("verify pool");
    let dst_full = format!("{dest_root}{}", String::from_utf8_lossy(&basename));
    let link_full = format!("{dest_root}{}", String::from_utf8_lossy(&link_basename));
    let pair = pool.pair_by_name("small").expect("small bucket");
    let target_stat = pair.dst.stat(dst_full.as_bytes()).await.expect("stat dst");
    let link_stat = pair
        .dst
        .stat(link_full.as_bytes())
        .await
        .expect("stat link");
    assert_eq!(
        target_stat.ino, link_stat.ino,
        "target and linkpath must share an inode after replay"
    );
    assert_eq!(target_stat.nlink, 2, "link count must be exactly 2");

    // Best-effort cleanup, same policy as the smoke above.
    let _ = pair.dst.unlink(link_full.as_bytes()).await;
    let _ = pair.dst.unlink(dst_full.as_bytes()).await;
}

/// F10 symlink replay idempotency: `move_one` a symlink row, then
/// `move_one` the SAME row again (simulating at-least-once redelivery
/// after a worker died post-symlink-pre-ack). The replay must succeed
/// — nfs_symlink's EEXIST resolves to Ok because the dst symlink
/// already points at the intended target — and the destination
/// readlink must still equal the intended target afterwards. VAST rig
/// pass only.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore]
async fn symlink_replay_is_idempotent() {
    let src_url_s = src_url();
    let dst_url_s = dst_url();

    let src_relative_inside_export = src_path();

    // Same env contract and root layout as the end-to-end smoke
    // above: split the configured src path into source_root +
    // row.path, land dest under a per-run dir. The symlink target
    // comes from the row (`symlink_target`), so the source file is
    // never read — the split is kept for env-contract symmetry.
    let suffix = timestamp_suffix();
    let last_slash = src_relative_inside_export
        .iter()
        .rposition(|&b| b == b'/')
        .expect("VAMOOSE_TEST_NFS_PATH must be absolute");
    let source_root = String::from_utf8(src_relative_inside_export[..last_slash].to_vec())
        .expect("source root utf8");
    let mut link_basename = src_relative_inside_export[last_slash..].to_vec(); // starts with /
    link_basename.extend_from_slice(b".sym");
    let dest_root = format!("{}/symlink-replay-smoke-{suffix}", write_dir());

    // The intended target: the source file's absolute path. It need
    // not resolve on the destination export — symlinks may dangle —
    // what matters is that the byte string round-trips.
    let target = src_relative_inside_export.clone();

    let cfg = Arc::new(MoverConfig {
        source_url: src_url_s.clone(),
        dest_url: dst_url_s.clone(),
        source_root: source_root.clone(),
        dest_root: dest_root.clone(),
        policy: AttrPolicy::from_options(&MigrationOptions::default()),
        inflight: InflightProfile::default(),
        require_chown: false,
        require_unchanged_size: false,
        use_raw_fh: false,
        direct_commit: false,
        rpc_timeout_ms: DEFAULT_RPC_TIMEOUT_MS,
    });

    let downgrades = DowngradeSink::new();
    let fence = Fence::new();

    let sync_pool = SimplePool::build(&src_url_s, &dst_url_s, DEFAULT_RPC_TIMEOUT_MS)
        .expect("SimplePool::build");
    let sync_mover = Mover::new(
        (*cfg).clone(),
        sync_pool,
        "test-host",
        downgrades.clone(),
        fence.clone(),
    );

    let async_pool = Arc::new(
        BucketedAsyncPool::new(&src_url_s, &dst_url_s, DEFAULT_RPC_TIMEOUT_MS, 1)
            .await
            .expect("BucketedAsyncPool::new"),
    );

    let file_mover = AsyncBucketedFileMover::new(
        async_pool,
        sync_mover,
        cfg.clone(),
        fence.clone(),
        "test-host",
        downgrades.clone(),
    );

    // 1. Create the symlink row — symlink IS the commit point (R8).
    let row = RowView {
        row_id: 1,
        path: link_basename.clone(),
        size: 0,
        mtime_sec: Some(1_700_000_000),
        mtime_nsec: Some(0),
        atime_sec: Some(1_700_000_000),
        atime_nsec: Some(0),
        mode: 0o777,
        uid: Some(0),
        gid: Some(0),
        nlink: Some(1),
        inode: None,
        fsid: None,
        xattr_blob: None,
        symlink_target: Some(target.clone()),
        file_type: FileTypeTag::Symlink,
    };
    let first = file_mover.move_one(&row).await;
    assert!(
        first.result.is_ok(),
        "first symlink move_one failed: {:?}",
        first.result
    );

    // 2. Replay the SAME symlink row — simulates redelivery after a
    //    died-post-symlink-pre-ack worker. Must succeed, not
    //    EEXIST-fail.
    let replay = file_mover.move_one(&row).await;
    assert!(
        replay.result.is_ok(),
        "replayed symlink move_one must be idempotent, got: {:?}",
        replay.result
    );
    assert_eq!(replay.bytes_moved, 0, "symlink rows move no file data");

    // 3. The destination readlink must still equal the intended
    //    target, byte for byte.
    let pool = BucketedAsyncPool::new(&src_url_s, &dst_url_s, DEFAULT_RPC_TIMEOUT_MS, 1)
        .await
        .expect("verify pool");
    let link_full = format!("{dest_root}{}", String::from_utf8_lossy(&link_basename));
    let pair = pool.pair_by_name("small").expect("small bucket");
    let read_back = pair
        .dst
        .readlink(link_full.as_bytes())
        .await
        .expect("readlink dst");
    assert_eq!(
        read_back, target,
        "destination symlink target must equal the intended target after replay"
    );

    // Best-effort cleanup, same policy as the smokes above.
    let _ = pair.dst.unlink(link_full.as_bytes()).await;
}

/// F09 hardware case (PROTECTED_FFI_BATCH.md Item 3): sync
/// write → `ops::fsync` (whole-file NFS COMMIT via the new
/// `nfs_fsync` binding) → read-back. Exercises the new wrapper
/// end-to-end against real VAST: the COMMIT must succeed on a dirty
/// write fh, and the committed bytes must read back identical
/// through a fresh fh. CI cannot cover the COMMIT itself (FFI + a
/// real server); this smoke is the verification vehicle.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore]
async fn sync_write_fsync_commit_readback() {
    use migration_core::records::FailurePhase;
    use migration_mover::libnfs::ops;
    use migration_mover::LibnfsContextPool;

    let url = dst_url();
    let dir = write_dir();

    let pool = SimplePool::build(&url, &url, DEFAULT_RPC_TIMEOUT_MS).expect("SimplePool::build");
    let mut pair = pool.acquire().await.expect("acquire pair");

    let path = format!("{dir}/fsync-smoke-{}.bin", timestamp_suffix()).into_bytes();
    let payload: Vec<u8> = b"vamoose F09 COMMIT smoke payload / "
        .iter()
        .copied()
        .cycle()
        .take(96 * 1024)
        .collect();

    // Write UNSTABLE (create_write opens without O_SYNC), then COMMIT.
    let fh = ops::create_write(pair.dst(), &path, 0o600).expect("create_write");
    let mut off = 0usize;
    while off < payload.len() {
        let n = ops::pwrite(pair.dst(), &fh, off as u64, &payload[off..]).expect("pwrite");
        assert!(n > 0, "pwrite wrote 0 bytes at offset {off}");
        off += n;
    }
    ops::fsync(pair.dst(), &fh).expect("ops::fsync (whole-file NFS COMMIT) must succeed");
    ops::close_fh(pair.dst(), fh, FailurePhase::Write).expect("close after commit");

    // Read back through a fresh fh: committed bytes must be intact.
    let fh = ops::open_read(pair.dst(), &path).expect("open_read for read-back");
    let mut buf = vec![0u8; payload.len()];
    let mut off = 0usize;
    while off < buf.len() {
        let n = ops::pread(pair.dst(), &fh, off as u64, &mut buf[off..]).expect("pread");
        assert!(n > 0, "EOF at {off} before full read-back");
        off += n;
    }
    ops::close_quietly(pair.dst(), fh);
    assert_eq!(buf, payload, "committed bytes must read back identical");

    // Best-effort cleanup, same policy as the smokes above.
    let _ = ops::unlink(pair.dst(), &path);
}
