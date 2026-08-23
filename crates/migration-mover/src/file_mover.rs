//! `FileMover` trait + impls.
//!
//! Single, async per-row entry surface that the worker uses to copy
//! one file. Two impls:
//!
//! - [`Mover`] itself (the existing sync-libnfs path) — see `mover.rs`.
//! - [`AsyncBucketedFileMover`] — routes regular files through the
//!   bucketed async pool ([`BucketedAsyncPool`]) and
//!   [`pipelined_copy`]; falls back to the wrapped sync `Mover` for
//!   symlinks / hardlinks / dirs / empty files / skip-everything-else
//!   rows.
//!
//! The worker holds `Arc<dyn FileMover>`; configuration or the
//! `--use-bucketed-pool` CLI override picks the implementation at startup.
//!
//! ## R8 placement
//!
//! Both impls preserve R8 (fence check immediately before the commit-
//! point op, no work in between). In `AsyncBucketedFileMover::copy_regular`
//! the order is:
//!
//! 1. `pipelined_copy(...)` — durabilizes bytes via whole-file fsync.
//! 2. close src + dst fhs (post-fsync cleanup; bytes are durable).
//! 3. `apply_async_attrs(...)` — chown / chmod / utimes on `.partial`.
//! 4. `check_fence()` — R8 gate.
//! 5. `dst.rename(.partial, final)` — atomic commit point.
//!
//! Steps 1–3 may take seconds (large file + fsync + per-RPC attrs);
//! step 5 must follow step 4 with no intervening await. This mirrors
//! the sync mover's flow (`mover.rs:521-526` in this tree).

use std::sync::Arc;

use async_trait::async_trait;
use migration_core::fence::Fence;
use migration_core::records::{DowngradeKind, FailurePhase};
use migration_core::shard::RowView;

use crate::attr_plan::{self, AttrOp};
use crate::bucketed_pool::BucketedAsyncPool;
use crate::downgrade::DowngradeSink;
use crate::error::MoveError;
use crate::libnfs::asyncio::{AsyncNfsContext, Flags};
use crate::mover::check_self_target;
use crate::paths::{join_root, partial_path};
use crate::pipelined_copy::{pipelined_copy, FileCopyResult};
use crate::strategy::{self, Strategy, StrategyContext};
use crate::{MoveOutcome, Mover, MoverConfig};

/// What to do with a completed [`pipelined_copy`] result. Every
/// variant commits — a torn copy is NOT a failure (at-least-once
/// semantics; the source remains intact), it just carries an
/// operator-visible downgrade record alongside the commit. There is
/// deliberately no failure variant here: re-copying a live file can
/// tear again, so remediation belongs to the future multi-pass
/// driver (`MULTI_PASS_MOVER.md`), not this path.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CopyDisposition {
    /// Clean copy: commit via rename, nothing to record.
    Commit,
    /// Torn copy: still commit via rename, and emit `downgrade` to
    /// the downgrade sink for the row.
    CommitAndRecord {
        row_id: u64,
        downgrade: DowngradeKind,
    },
}

impl CopyDisposition {
    /// Always true — the disposition never aborts the commit. Exists
    /// to make the contract explicit at call sites and in tests.
    pub fn commits(&self) -> bool {
        match self {
            CopyDisposition::Commit | CopyDisposition::CommitAndRecord { .. } => true,
        }
    }

    /// The downgrade record to emit, if any.
    pub fn downgrade(&self) -> Option<&DowngradeKind> {
        match self {
            CopyDisposition::Commit => None,
            CopyDisposition::CommitAndRecord { downgrade, .. } => Some(downgrade),
        }
    }
}

/// Pure classifier for a completed [`pipelined_copy`] result — the
/// seam where torn detection becomes an operator-visible outcome
/// (F05, `docs/work-items/MOVER_TORN_COPY_SURFACE.md`). A `torn`
/// result classifies to commit-and-record with a
/// [`DowngradeKind::TornCopy`] carrying the pre/post
/// `(size, mtime_sec, ctime_sec)` stat-bracket triples; a clean
/// result is a plain commit. No I/O, no side effects — the caller
/// (`AsyncBucketedFileMover::copy_regular`) emits the record,
/// bumps counters, and warns.
pub fn classify_copy(result: &FileCopyResult, row: &RowView) -> CopyDisposition {
    if !result.torn {
        return CopyDisposition::Commit;
    }
    let pre = (
        result.pre_stat.size,
        result.pre_stat.mtime as i64,
        result.pre_stat.ctime as i64,
    );
    let post = (
        result.post_stat.size,
        result.post_stat.mtime as i64,
        result.post_stat.ctime as i64,
    );
    CopyDisposition::CommitAndRecord {
        row_id: row.row_id,
        downgrade: DowngradeKind::TornCopy { pre, post },
    }
}

/// Unified per-row mover surface. Two impls live in this module.
#[async_trait]
pub trait FileMover: Send + Sync {
    /// Move one file. Same contract as `Mover::move_one`: pick strategy
    /// from row, execute, return outcome.
    async fn move_one(&self, row: &RowView) -> MoveOutcome;

    /// Hardlink an already-copied dest path to `link_target`. The async pool
    /// does not handle this path; it delegates to the wrapped sync mover.
    async fn move_hardlink(&self, row: &RowView, link_target: &[u8]) -> MoveOutcome;

    /// Borrow the downgrade sink for the shard processor's
    /// `FsidUngrouped` recording path.
    fn downgrade_sink(&self) -> &DowngradeSink;
}

#[async_trait]
impl FileMover for Mover {
    async fn move_one(&self, row: &RowView) -> MoveOutcome {
        Mover::move_one(self, row).await
    }
    async fn move_hardlink(&self, row: &RowView, link_target: &[u8]) -> MoveOutcome {
        Mover::move_hardlink(self, row, link_target).await
    }
    fn downgrade_sink(&self) -> &DowngradeSink {
        Mover::downgrade_sink(self)
    }
}

/// Per-directory single-flight guards keyed by destination path.
type AsyncDirLocks =
    tokio::sync::Mutex<std::collections::HashMap<Vec<u8>, Arc<tokio::sync::Mutex<()>>>>;

/// Routes regular-file rows through the bucketed async pool +
/// [`pipelined_copy`]. Everything else delegates to a wrapped sync [`Mover`];
/// the async pipe handles regular files only. The two impls share the same
/// fence so the worker observes one unified observability surface
/// regardless of which path a row took.
pub struct AsyncBucketedFileMover {
    pool: Arc<BucketedAsyncPool>,
    sync: Mover,
    cfg: Arc<MoverConfig>,
    fence: Fence,
    host_id: Arc<str>,
    pid: u32,
    downgrades: DowngradeSink,
    /// Destination directories confirmed present + per-dir single-flight
    /// guards. Same rationale as `Mover::dirs_known` on the sync path:
    /// without both pieces every in-flight file of a directory re-probes
    /// the full ancestor mkdir chain (~7 EEXIST round-trips per file on
    /// a depth-8 tree). Entries are only added after a successful
    /// mkdir chain and nothing removes destination dirs during a run.
    dirs_known: Arc<tokio::sync::Mutex<std::collections::HashSet<Vec<u8>>>>,
    dir_locks: Arc<AsyncDirLocks>,
}

impl AsyncBucketedFileMover {
    /// Construct. The `cfg` / `fence` / `host_id` / `downgrades`
    /// arguments MUST match those the wrapped `sync` Mover was
    /// constructed with — the bucketed-async fast path consults them
    /// directly for src_root/dest_root/policy and shares the same
    /// fence semantics as the sync fallback.
    pub fn new(
        pool: Arc<BucketedAsyncPool>,
        sync: Mover,
        cfg: Arc<MoverConfig>,
        fence: Fence,
        host_id: impl Into<Arc<str>>,
        downgrades: DowngradeSink,
    ) -> Self {
        Self {
            pool,
            sync,
            cfg,
            fence,
            host_id: host_id.into(),
            pid: std::process::id(),
            downgrades,
            dirs_known: Arc::new(tokio::sync::Mutex::new(std::collections::HashSet::new())),
            dir_locks: Arc::new(tokio::sync::Mutex::new(std::collections::HashMap::new())),
        }
    }

    /// Cached, single-flight `mkdir -p` of `file_path`'s parent. See
    /// the `dirs_known` field docs and the sync twin
    /// `Mover::ensure_parent_dir`.
    async fn ensure_parent_dir(
        &self,
        ctx: &AsyncNfsContext,
        file_path: &[u8],
    ) -> Result<(), MoveError> {
        let last_slash = match file_path.iter().rposition(|&b| b == b'/') {
            Some(0) | None => return Ok(()), // root or no parent; root exists
            Some(i) => i,
        };
        let parent = &file_path[..last_slash];
        if parent.is_empty() {
            return Ok(());
        }
        if self.dirs_known.lock().await.contains(parent) {
            return Ok(());
        }
        let dir_lock = {
            let mut locks = self.dir_locks.lock().await;
            Arc::clone(
                locks
                    .entry(parent.to_vec())
                    .or_insert_with(|| Arc::new(tokio::sync::Mutex::new(()))),
            )
        };
        let _flight = dir_lock.lock().await;
        if self.dirs_known.lock().await.contains(parent) {
            return Ok(());
        }
        async_mkdir_p(ctx, parent).await?;
        {
            let mut known = self.dirs_known.lock().await;
            let mut end = parent.len();
            loop {
                known.insert(parent[..end].to_vec());
                match parent[..end].iter().rposition(|&b| b == b'/') {
                    Some(i) if i > 0 => end = i,
                    _ => break,
                }
            }
        }
        self.dir_locks.lock().await.remove(parent);
        Ok(())
    }

    /// R8 gate. Same contract as `Mover::check_fence`.
    fn check_fence(&self) -> Result<(), MoveError> {
        if self.fence.is_valid() {
            Ok(())
        } else {
            Err(MoveError::new(FailurePhase::Fenced, "FENCE_TRIPPED"))
        }
    }

    /// Async fast path for regular files. Mirrors the sync
    /// `Mover::do_libnfs_copy` flow in order and error semantics; the
    /// only differences are (a) data goes through `pipelined_copy`
    /// (with whole-file fsync inside) instead of the sync streaming
    /// loop, and (b) attrs/rename go through the bucketed async ctx
    /// instead of the sync pool.
    ///
    /// Returns `(bytes_copied, torn)`. `torn = true` means the source
    /// changed under the copy ([`classify_copy`]): the file was still
    /// committed, a [`DowngradeKind::TornCopy`] record went to the
    /// downgrade sink, and the caller surfaces it on
    /// [`MoveOutcome::torn`] so the shard summary can count it.
    async fn copy_regular(&self, row: &RowView) -> Result<(u64, bool), MoveError> {
        let src = join_root(self.cfg.source_root.as_bytes(), &row.path);
        let dst = join_root(self.cfg.dest_root.as_bytes(), &row.path);
        let dst_partial = partial_path(&dst, &self.host_id, self.pid)?;

        check_self_target(
            &self.cfg.source_url,
            &self.cfg.dest_url,
            &src,
            &dst,
            &dst_partial,
        )?;

        let (src_ctx, dst_ctx, cfg) = self.pool.pair_for_size(row.size);

        // Ensure the dst parent dir exists (cached + single-flight).
        // Uses the bucket's dst ctx — any bucket's dst ctx would
        // resolve to the same NFS server view.
        self.ensure_parent_dir(dst_ctx, &dst).await?;

        let src_fh = src_ctx
            .open(&src, Flags::rdonly())
            .await
            .map_err(|e| nfs_err(FailurePhase::Open, format!("open src: {e}")))?;
        let dst_fh = match dst_ctx
            .create(&dst_partial, Flags::wronly().with_create(), 0o600)
            .await
        {
            Ok(fh) => fh,
            Err(e) => {
                // libnfs handle leak protection: close the src fh we
                // already opened before propagating.
                let _ = src_ctx.close(src_fh).await;
                return Err(nfs_err(FailurePhase::Open, format!("create dst: {e}")));
            }
        };

        let copy_result = pipelined_copy(src_ctx, &src_fh, dst_ctx, &dst_fh, row.size, cfg).await;

        // Close fhs whether or not the copy succeeded. The bytes are
        // either durable (fsync inside pipelined_copy completed) or
        // they're orphan in the .partial — either way the close just
        // frees libnfs-side state.
        let _ = src_ctx.close(src_fh).await;
        let _ = dst_ctx.close(dst_fh).await;

        let copy = copy_result?;

        if self.cfg.require_unchanged_size && copy.bytes_copied != row.size {
            return Err(MoveError::new(FailurePhase::Open, "SIZE_CHANGED"));
        }

        if copy.bytes_copied < row.size {
            self.downgrades
                .record(row.row_id, &row.path, DowngradeKind::EarlyEof);
        }

        // F05: torn-copy surface. A torn result still commits
        // (at-least-once; source intact) but must leave an
        // operator-visible trace — until the multi-pass driver
        // exists, this record is the only remediation.
        let disposition = classify_copy(&copy, row);
        let torn = match &disposition {
            CopyDisposition::Commit => false,
            CopyDisposition::CommitAndRecord { row_id, downgrade } => {
                tracing::warn!(
                    path = %String::from_utf8_lossy(&row.path),
                    row_id,
                    "source modified during copy (torn); committing and \
                     recording TORN_COPY downgrade",
                );
                self.downgrades.record(*row_id, &row.path, *downgrade);
                true
            }
        };

        self.apply_async_attrs(dst_ctx, &dst_partial, row).await?;

        // R8: fence check immediately before the commit-point rename.
        // Nothing between check_fence and rename — no logging, no
        // metric updates, no close ops.
        self.check_fence()?;
        dst_ctx.rename(&dst_partial, &dst).await.map_err(|e| {
            nfs_err(
                FailurePhase::Rename,
                format!("rename .partial → final: {e}"),
            )
        })?;

        // Post-commit observability. Outside the R8 critical section —
        // the rename has already published the file. Matches the sync
        // mover's `commit: rename` line so M5/M5-partition harnesses
        // (which poll worker stdout for that string) work unchanged
        // against the bucketed-async path.
        tracing::debug!(
            dest = %String::from_utf8_lossy(&dst),
            host = %self.host_id,
            pid = self.pid,
            row_id = row.row_id,
            "commit: rename .partial → final",
        );

        Ok((copy.bytes_copied, torn))
    }

    /// Apply owner / mode / mtime+atime through the async ctx in the
    /// order planned by [`attr_plan::plan_attr_ops`], the same plan
    /// the sync `Mover::apply_attrs` executes: chown → chmod → utimes
    /// (F08 — owner before mode so NFSv3 kill-priv semantics can't
    /// strip S_ISUID/S_ISGID the chmod just applied; utimes strictly
    /// last). Honors the same downgrade rules: null source attrs the
    /// user asked to preserve get a `DowngradeKind` record; chown
    /// EPERM is degraded to a `NullOwner` downgrade when
    /// `require_chown` is false, and the plan continues to chmod.
    async fn apply_async_attrs(
        &self,
        dst: &AsyncNfsContext,
        dst_partial: &[u8],
        row: &RowView,
    ) -> Result<(), MoveError> {
        let policy = self.cfg.policy;

        if policy.preserve_owner && (row.uid.is_none() || row.gid.is_none()) {
            self.downgrades
                .record(row.row_id, &row.path, DowngradeKind::NullOwner);
        }
        if policy.preserve_times && row.mtime_sec.is_none() {
            self.downgrades
                .record(row.row_id, &row.path, DowngradeKind::NullMtime);
        }
        if policy.preserve_times && row.mtime_sec.is_some() && row.atime_sec.is_none() {
            self.downgrades
                .record(row.row_id, &row.path, DowngradeKind::NullAtime);
        }

        // Async twin of the sync `execute_plan` loop (closures can't
        // await, so the plan is iterated inline). Each op maps to the
        // pre-existing call; the chown-EPERM degraded-mode policy is
        // unchanged — only its position in the sequence moved.
        for op in attr_plan::plan_attr_ops(row, policy) {
            match op {
                AttrOp::Chown { uid, gid } => match dst.chown(dst_partial, uid, gid).await {
                    Ok(()) => {}
                    Err(e)
                        if matches!(e.errno(), Some(eno) if eno == libc::EPERM)
                            && !self.cfg.require_chown =>
                    {
                        tracing::debug!(uid, gid, "chown EPERM in degraded mode; skipping");
                        self.downgrades
                            .record(row.row_id, &row.path, DowngradeKind::NullOwner);
                    }
                    Err(e) => return Err(nfs_err(FailurePhase::Setattr, format!("chown: {e}"))),
                },
                AttrOp::Chmod { mode } => {
                    dst.chmod(dst_partial, mode)
                        .await
                        .map_err(|e| nfs_err(FailurePhase::Setattr, format!("chmod: {e}")))?;
                }
                AttrOp::Utimes {
                    atime: (at_s, at_n),
                    mtime: (mt_s, mt_n),
                } => {
                    dst.utimes(dst_partial, at_s, at_n, mt_s, mt_n)
                        .await
                        .map_err(|e| nfs_err(FailurePhase::Setattr, format!("utimes: {e}")))?;
                }
            }
        }
        Ok(())
    }
}

#[async_trait]
impl FileMover for AsyncBucketedFileMover {
    async fn move_one(&self, row: &RowView) -> MoveOutcome {
        // Strategy selection mirrors the sync mover. The async path
        // is only the regular-file fast path; everything else falls
        // back to the wrapped sync impl.
        let strategy = strategy_for_move_one(row);
        if !uses_bucketed_async_path(strategy) {
            return self.sync.move_one(row).await;
        }

        let result = self.copy_regular(row).await;
        let (bytes_moved, torn) = match &result {
            Ok((n, torn)) => (*n, *torn),
            Err(_) => (0, false),
        };
        MoveOutcome {
            row_id: row.row_id,
            strategy,
            bytes_moved,
            torn,
            result: result.map(|_| ()),
        }
    }

    async fn move_hardlink(&self, row: &RowView, link_target: &[u8]) -> MoveOutcome {
        // Per Decision #3 — hardlinks go through the sync path.
        self.sync.move_hardlink(row, link_target).await
    }

    fn downgrade_sink(&self) -> &DowngradeSink {
        &self.downgrades
    }
}

fn strategy_for_move_one(row: &RowView) -> Strategy {
    strategy::pick(
        row,
        &StrategyContext {
            already_copied_inode: false,
        },
    )
}

fn uses_bucketed_async_path(strategy: Strategy) -> bool {
    strategy == Strategy::LibnfsIoUring
}

async fn async_mkdir_p(dst: &AsyncNfsContext, path: &[u8]) -> Result<(), MoveError> {
    if path.is_empty() {
        return Ok(());
    }
    let mut acc: Vec<u8> = Vec::with_capacity(path.len());
    for component in path.split(|&b| b == b'/') {
        if component.is_empty() {
            acc.push(b'/');
            continue;
        }
        acc.push(b'/');
        acc.extend_from_slice(component);
        match dst.mkdir(&acc, 0o755).await {
            Ok(()) => {}
            Err(e) if matches!(e.errno(), Some(eno) if eno == libc::EEXIST) => {}
            Err(e) => return Err(nfs_err(FailurePhase::Write, format!("mkdir: {e}"))),
        }
    }
    Ok(())
}

fn nfs_err(phase: FailurePhase, msg: String) -> MoveError {
    MoveError::new(phase, msg)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::libnfs::asyncio::NfsStat64;
    use crate::pipelined_copy::FileCopyResult;
    use migration_core::schema::FileTypeTag;

    #[test]
    fn trait_is_dyn_safe() {
        // Smoke that `dyn FileMover` actually works — caught a
        // signature mistake in an earlier draft. Mover is Send+Sync
        // and implements the trait via its inherent move_* methods.
        fn _accepts_dyn(_m: Arc<dyn FileMover>) {}
    }

    // ---- F05: torn-copy classification ---------------------------
    //
    // The FFI copy path is hardware-gated, so the torn-copy surface
    // is tested at the seam: `classify_copy` is the pure function
    // `copy_regular` consults after `pipelined_copy` returns. See
    // docs/work-items/MOVER_TORN_COPY_SURFACE.md.

    fn stat(size: u64, mtime: u64, ctime: u64) -> NfsStat64 {
        NfsStat64 {
            dev: 0,
            ino: 1,
            mode: 0o100644,
            nlink: 1,
            uid: 0,
            gid: 0,
            rdev: 0,
            size,
            blksize: 4096,
            blocks: 0,
            atime: 0,
            mtime,
            ctime,
            atime_nsec: 0,
            mtime_nsec: 0,
            ctime_nsec: 0,
            used: 0,
        }
    }

    fn copy_result(torn: bool, pre: NfsStat64, post: NfsStat64) -> FileCopyResult {
        FileCopyResult {
            file_hash: [0; 16],
            bytes_copied: post.size,
            torn,
            pre_stat: pre,
            post_stat: post,
        }
    }

    fn test_row(row_id: u64, path: &[u8], size: u64) -> RowView {
        test_row_with_type(row_id, path, size, FileTypeTag::Regular)
    }

    fn test_row_with_type(row_id: u64, path: &[u8], size: u64, file_type: FileTypeTag) -> RowView {
        RowView {
            row_id,
            path: path.to_vec(),
            size,
            mtime_sec: None,
            mtime_nsec: None,
            atime_sec: None,
            atime_nsec: None,
            mode: 0o100644,
            uid: None,
            gid: None,
            nlink: None,
            inode: None,
            fsid: None,
            xattr_blob: None,
            symlink_target: None,
            file_type,
        }
    }

    #[test]
    fn bucketed_async_route_is_regular_nonempty_only() {
        let cases = [
            (FileTypeTag::Regular, 1, true),
            (FileTypeTag::Regular, 0, false),
            (FileTypeTag::Symlink, 1, false),
            (FileTypeTag::Dir, 1, false),
            (FileTypeTag::Fifo, 1, false),
            (FileTypeTag::Socket, 1, false),
            (FileTypeTag::BlockDev, 1, false),
            (FileTypeTag::CharDev, 1, false),
            (FileTypeTag::Unknown, 1, false),
        ];

        for (file_type, size, expected) in cases {
            let row = test_row_with_type(1, b"/route", size, file_type);
            let strategy = strategy_for_move_one(&row);
            assert_eq!(
                uses_bucketed_async_path(strategy),
                expected,
                "file_type={file_type:?} size={size} strategy={strategy:?}",
            );
        }
    }

    /// A `torn = true` copy result must classify to commit-and-record
    /// with a `DowngradeKind::TornCopy` carrying the pre/post
    /// `(size, mtime, ctime)` triples from the stat brackets.
    #[test]
    fn torn_result_produces_downgrade_record() {
        let row = test_row(42, b"/data/hot.bin", 1024);
        let result = copy_result(true, stat(1024, 100, 100), stat(2048, 200, 300));

        let d = classify_copy(&result, &row);

        match d {
            CopyDisposition::CommitAndRecord { row_id, downgrade } => {
                assert_eq!(row_id, 42);
                assert_eq!(
                    downgrade,
                    DowngradeKind::TornCopy {
                        pre: (1024, 100, 100),
                        post: (2048, 200, 300),
                    },
                );
            }
            other => panic!("torn result must be CommitAndRecord, got {other:?}"),
        }
    }

    /// `torn = false` → no record; plain commit.
    #[test]
    fn clean_result_produces_no_record() {
        let row = test_row(7, b"/data/cold.bin", 512);
        let result = copy_result(false, stat(512, 100, 100), stat(512, 100, 100));

        let d = classify_copy(&result, &row);

        assert_eq!(d, CopyDisposition::Commit);
        assert!(
            d.downgrade().is_none(),
            "clean copy must not carry a record"
        );
    }

    /// Torn must NOT become a failure: at-least-once semantics, the
    /// source remains intact, and the row still counts as copied. The
    /// classification marks commit-and-record — there is no failure
    /// arm for torn at all.
    #[test]
    fn torn_file_still_commits() {
        let row = test_row(9, b"/data/live.bin", 4096);
        let result = copy_result(true, stat(4096, 1, 1), stat(4096, 2, 2));

        let d = classify_copy(&result, &row);

        assert!(
            d.commits(),
            "torn disposition must commit-and-record, never fail",
        );
        assert!(d.downgrade().is_some(), "torn commit must carry the record");
    }
}
