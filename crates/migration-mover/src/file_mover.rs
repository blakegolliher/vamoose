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
//! The worker holds `Arc<dyn FileMover>`; the `--use-bucketed-pool`
//! CLI flag (Phase 2 T3) picks which impl is constructed at startup.
//!
//! ## R8 placement
//!
//! Both impls preserve R8 (fence check immediately before the commit-
//! point op, no work in between). In `AsyncBucketedFileMover::copy_regular`
//! the order is:
//!
//! 1. `pipelined_copy(...)` — durabilizes bytes via whole-file fsync.
//! 2. close src + dst fhs (post-fsync cleanup; bytes are durable).
//! 3. `apply_async_attrs(...)` — chmod / chown / utimes on `.partial`.
//! 4. `fence.check_pre_rename()` — R8 gate.
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

use crate::attrs;
use crate::bucketed_pool::BucketedAsyncPool;
use crate::downgrade::DowngradeSink;
use crate::error::MoveError;
use crate::libnfs::asyncio::{AsyncNfsContext, Flags};
use crate::mover::check_self_target;
use crate::paths::{join_root, partial_path};
use crate::pipelined_copy::pipelined_copy;
use crate::strategy::{self, Strategy, StrategyContext};
use crate::{MoveOutcome, Mover, MoverConfig};

/// Unified per-row mover surface. Two impls live in this module.
#[async_trait]
pub trait FileMover: Send + Sync {
    /// Move one file. Same contract as `Mover::move_one`: pick strategy
    /// from row, execute, return outcome.
    async fn move_one(&self, row: &RowView) -> MoveOutcome;

    /// Hardlink an already-copied dest path to `link_target`. Per
    /// Phase 2 Decision #3 the async pool does not handle this path —
    /// it always delegates to the wrapped sync mover.
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

/// Routes regular-file rows through the bucketed async pool +
/// [`pipelined_copy`]. Everything else delegates to a wrapped sync
/// [`Mover`] (Phase 2 Decision #3 — async pipe handles regular files
/// only). The two impls share the same downgrade sink and the same
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
        }
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
    async fn copy_regular(&self, row: &RowView) -> Result<u64, MoveError> {
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

        // Ensure the dst parent dir exists. mkdir -p style; ignore
        // EEXIST per component. Uses the bucket's dst ctx — any
        // bucket's dst ctx would resolve to the same NFS server view.
        async_mkdir_p_for_file(dst_ctx, &dst).await?;

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

        Ok(copy.bytes_copied)
    }

    /// Apply mode / owner / mtime+atime through the async ctx in the
    /// same order as `Mover::apply_attrs` (chmod → chown → utimes).
    /// Honors the same downgrade rules: null source attrs the user
    /// asked to preserve get a `DowngradeKind` record; chown EPERM
    /// is degraded to a `NullOwner` downgrade when `require_chown` is
    /// false.
    async fn apply_async_attrs(
        &self,
        dst: &AsyncNfsContext,
        dst_partial: &[u8],
        row: &RowView,
    ) -> Result<(), MoveError> {
        let a = attrs::build(row, self.cfg.policy);
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

        if let Some(mode) = a.mode {
            dst.chmod(dst_partial, mode)
                .await
                .map_err(|e| nfs_err(FailurePhase::Setattr, format!("chmod: {e}")))?;
        }

        if let (Some(uid), Some(gid)) = (a.uid, a.gid) {
            match dst.chown(dst_partial, uid, gid).await {
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
            }
        }

        if let Some((mt_s, mt_n)) = a.mtime {
            let (at_s, at_n) = a.atime.unwrap_or((mt_s, mt_n));
            dst.utimes(dst_partial, at_s, at_n as i32, mt_s, mt_n as i32)
                .await
                .map_err(|e| nfs_err(FailurePhase::Setattr, format!("utimes: {e}")))?;
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
        let strat_ctx = StrategyContext {
            server_side_copy_policy: self.cfg.server_side_copy,
            same_server_v42: self.cfg.same_server_v42,
            server_side_copy_min_bytes: self.cfg.server_side_copy_min_bytes,
            already_copied_inode: false,
        };
        let strategy = strategy::pick(row, &strat_ctx);
        if strategy != Strategy::LibnfsIoUring {
            return self.sync.move_one(row).await;
        }

        let result = self.copy_regular(row).await;
        let bytes_moved = match &result {
            Ok(n) => *n,
            Err(_) => 0,
        };
        MoveOutcome {
            row_id: row.row_id,
            strategy,
            bytes_moved,
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

/// Async `mkdir -p` for the parent dir of `file_path`. Mirrors
/// `crate::libnfs::ops::mkdir_p_for_file` (sync) — walk the path,
/// mkdir each component, ignore `EEXIST`.
async fn async_mkdir_p_for_file(dst: &AsyncNfsContext, file_path: &[u8]) -> Result<(), MoveError> {
    let last_slash = match file_path.iter().rposition(|&b| b == b'/') {
        Some(0) => return Ok(()), // file is at root; root exists
        Some(i) => i,
        None => return Ok(()),
    };
    let parent = &file_path[..last_slash];
    if parent.is_empty() {
        return Ok(());
    }
    async_mkdir_p(dst, parent).await
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

    #[test]
    fn trait_is_dyn_safe() {
        // Smoke that `dyn FileMover` actually works — caught a
        // signature mistake in an earlier draft. Mover is Send+Sync
        // and implements the trait via its inherent move_* methods.
        fn _accepts_dyn(_m: Arc<dyn FileMover>) {}
    }
}
