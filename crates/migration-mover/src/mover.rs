//! The `Mover` orchestrates one file copy from start to finish:
//! strategy selection → data movement → attribute application → atomic
//! rename. The shard processor calls `move_one` per row (and
//! `move_hardlink` for rows that have already been copied earlier in
//! the shard).
//!
//! ## Threading model (M3)
//!
//! libnfs is a userspace transport whose ops *block* the calling
//! thread on the network socket. The mover's public `move_*` entry
//! points are therefore split:
//!
//! 1. **Async prologue** — pick the strategy (cheap, sync) and
//!    `pool.acquire().await` (channel recv, properly async).
//! 2. **Blocking body** — `tokio::task::spawn_blocking` runs the
//!    libnfs work on the runtime's blocking pool so the worker
//!    threads stay free for other tasks.
//!
//! This is what makes the M3 concurrent shard dispatch viable. With
//! M2's "everything async" shape, N concurrent libnfs copies would
//! pin N tokio workers; routing through the blocking pool removes
//! that ceiling.
//!
//! ## Order on commit (R4)
//!
//! 1. data WRITE
//! 2. close write fh
//! 3. chown (uid/gid) — skipped or downgraded if `require_chown`
//!    not set and EPERM is observed; runs *before* chmod because
//!    NFSv3 SETATTR of uid/gid clears S_ISUID/S_ISGID on regular
//!    files (kill-priv semantics) — see `attr_plan` (F08)
//! 4. chmod (mode)
//! 5. utimes (atime/mtime) — last, because some servers update mtime
//!    as a side effect of mode/owner changes
//! 6. rename `.partial` → final — **commit point**

use crate::attr_plan::{self, AttrExec, ChownOutcome};
use crate::attrs::AttrPolicy;
use crate::batch::InflightProfile;
use crate::downgrade::DowngradeSink;
use crate::error::MoveError;
use crate::libnfs::{ops, ContextPair, LibnfsContextPool, NfsContext};
use crate::paths::{join_root, partial_path};
use crate::strategy::{self, Strategy, StrategyContext};
use crate::uring::{FixedBufferPool, UringConfig};
use migration_core::fence::Fence;
use migration_core::records::{DowngradeKind, FailurePhase, MigrationOptions, ServerSideCopy};
use migration_core::shard::RowView;
use std::sync::Arc;

/// Conventional symlink mode on POSIX (`rwxrwxrwx`). When source
/// reports this, no mode-on-symlink work is needed — that's already
/// what `nfs_symlink` produces.
const SYMLINK_DEFAULT_MODE: u32 = 0o0777;

/// Streaming buffer size for the libnfs READ→WRITE path. Per-task
/// allocation is a few microseconds; keeping this simple while M3 is
/// new (true fixed-buffer registration is M3.5+ — see `M3_NOTES.md`).
const STREAM_BUF_SIZE: usize = 1 << 20; // 1 MiB

/// Outcome of attempting to move one file.
#[derive(Debug, Clone)]
pub struct MoveOutcome {
    pub row_id: u64,
    pub strategy: Strategy,
    /// Bytes actually written for this row (F41) — the streaming
    /// copy's byte count for regular files (less than `row.size` on
    /// an EarlyEof short copy), and 0 for failures and for rows that
    /// move no file data (skip / symlink / hardlink / dir-attrs /
    /// empty). Never `row.size` taken on faith: this value feeds the
    /// throughput sample that gates backpressure, progress records,
    /// and coord aggregation.
    pub bytes_moved: u64,
    /// True iff the copy committed but the source changed under it
    /// (`FileCopyResult::torn` → `file_mover::classify_copy`). The
    /// row still counts as copied; a `DowngradeKind::TornCopy` record
    /// was emitted, and the shard processor bumps `files_torn`.
    /// Detection is async-path-only: the sync path
    /// (`do_libnfs_copy`) has no pre/post stat bracket and always
    /// reports `false`.
    pub torn: bool,
    pub result: Result<(), MoveError>,
}

#[derive(Clone)]
pub struct MoverConfig {
    pub source_url: String,
    pub dest_url: String,
    /// `endpoint.root` from the manifest's `source` block. Joined with
    /// `row.path` via [`join_root`] to produce the absolute path used
    /// by every source-side libnfs op. See SCHEMA_CONTRACT.md "Path
    /// encoding" and BUGFIX_PLAN.md.
    pub source_root: String,
    /// `endpoint.root` from the manifest's `dest` block. Joined with
    /// `row.path` via [`join_root`] for every dest-side libnfs op.
    pub dest_root: String,
    /// **Currently unused.** Strategy selection in this build never
    /// returns `Strategy::ServerSideCopy` because the system targets
    /// NFSv3 as the protocol baseline; see `strategy.rs`. Kept on the
    /// struct for forward compatibility — when an NFSv4.2 fast path
    /// is reintroduced it will read this flag again.
    pub same_server_v42: bool,
    pub policy: AttrPolicy,
    pub server_side_copy: ServerSideCopy,
    pub server_side_copy_min_bytes: u64,
    pub uring: UringConfig,
    pub inflight: InflightProfile,
    /// True if the worker has CAP_CHOWN (or `require_chown_capability`
    /// is set). Controls whether `chown` EPERM is fatal or degraded
    /// to a recorded warning.
    pub require_chown: bool,
    /// When true, verify the bytes written equal `row.size` and fail
    /// the row with `SIZE_CHANGED` on mismatch. See
    /// `SCHEMA_CONTRACT.md` "Size semantics". Default false: source
    /// truth wins over walker's stale `size`.
    pub require_unchanged_size: bool,
    /// F12: per-RPC timeout in milliseconds, applied to every libnfs
    /// context (sync pools and the bucketed async pool) at creation.
    /// `0` = leave the libnfs built-in default untouched. Seeded to
    /// [`crate::libnfs::DEFAULT_RPC_TIMEOUT_MS`] by `from_options`;
    /// the orchestrator overrides it from `[mover] rpc_timeout_ms`.
    pub rpc_timeout_ms: u32,
}

impl MoverConfig {
    pub fn from_options(
        source_url: String,
        dest_url: String,
        source_root: String,
        dest_root: String,
        same_server_v42: bool,
        opts: &MigrationOptions,
    ) -> Self {
        Self {
            source_url,
            dest_url,
            source_root,
            dest_root,
            same_server_v42,
            policy: AttrPolicy::from_options(opts),
            server_side_copy: opts.server_side_copy,
            server_side_copy_min_bytes: 64 * 1024,
            uring: UringConfig::default(),
            inflight: InflightProfile::default(),
            require_chown: true,
            require_unchanged_size: false,
            rpc_timeout_ms: crate::libnfs::DEFAULT_RPC_TIMEOUT_MS,
        }
    }
}

/// The mover. Holds long-lived resources: libnfs context pool, the
/// host id and pid (used to construct `.partial` names), the buffer
/// pool placeholder (M3.5 wires it in), the downgrade sink, the
/// fence (consulted immediately before each commit-point op per R8),
/// and policy. Cloning is cheap (Arc inside) and required because
/// concurrent shard dispatch hands a clone to each spawned task.
/// The fence is Arc-backed; all clones share the same atomic flag.
#[derive(Clone)]
pub struct Mover {
    cfg: Arc<MoverConfig>,
    pool: Arc<dyn LibnfsContextPool>,
    host_id: Arc<str>,
    pid: u32,
    downgrades: DowngradeSink,
    fence: Fence,
    _buffers: Arc<FixedBufferPool>,
}

impl Mover {
    pub fn new(
        cfg: MoverConfig,
        pool: Arc<dyn LibnfsContextPool>,
        host_id: impl Into<Arc<str>>,
        downgrades: DowngradeSink,
        fence: Fence,
    ) -> Self {
        let buffers = FixedBufferPool::new(cfg.uring);
        Self {
            cfg: Arc::new(cfg),
            pool,
            host_id: host_id.into(),
            pid: std::process::id(),
            downgrades,
            fence,
            _buffers: buffers,
        }
    }

    /// Borrow the downgrade sink. Used by the orchestrator to drain
    /// JSONL between shards and to update the current shard name, and
    /// by the processor to record FsidUngrouped fallbacks.
    pub fn downgrade_sink(&self) -> &DowngradeSink {
        &self.downgrades
    }

    // =========================================================================
    // Public entry points used by the shard processor.
    // =========================================================================

    /// Move a single file. Picks a strategy from `(row, ctx)`, executes
    /// it, and returns the outcome. Hardlinks-to-already-copied-inodes
    /// are *not* dispatched here — call [`Self::move_hardlink`] for
    /// those (the shard processor's per-group logic is the authority).
    pub async fn move_one(&self, row: &RowView) -> MoveOutcome {
        let strat_ctx = StrategyContext {
            server_side_copy_policy: self.cfg.server_side_copy,
            same_server_v42: self.cfg.same_server_v42,
            server_side_copy_min_bytes: self.cfg.server_side_copy_min_bytes,
            already_copied_inode: false,
        };
        let strategy = strategy::pick(row, &strat_ctx);
        let row_owned = row.clone();
        self.run_with_pair(row, strategy, move |me, pair| {
            me.execute(pair, &row_owned, strategy)
        })
        .await
    }

    /// Hardlink an already-copied dest path to a new linkpath. Caller
    /// (the shard processor) holds the per-group "first path" state
    /// and is responsible for passing the *final* (post-rename) path
    /// — see R5.
    pub async fn move_hardlink(&self, row: &RowView, link_target: &[u8]) -> MoveOutcome {
        let target = link_target.to_vec();
        let path = row.path.clone();
        self.run_with_pair(row, Strategy::HardlinkExisting, move |me, pair| {
            // F41: a hardlink writes no file data — report 0 bytes.
            me.do_hardlink(pair, &target, &path).map(|()| 0)
        })
        .await
    }

    /// Common framing: pick-strategy → acquire-pair → spawn_blocking →
    /// build MoveOutcome. The closure receives the cloned mover and a
    /// mutable borrow of the pair so it can drive any of the
    /// strategy-specific sync paths; on success it returns the bytes
    /// it actually wrote (F41), which becomes `MoveOutcome::bytes_moved`.
    async fn run_with_pair<F>(&self, row: &RowView, strategy: Strategy, work: F) -> MoveOutcome
    where
        F: FnOnce(&Mover, &mut ContextPair) -> Result<u64, MoveError> + Send + 'static,
    {
        let row_id = row.row_id;

        let pair = match self.pool.acquire().await {
            Ok(p) => p,
            Err(e) => {
                return MoveOutcome {
                    row_id,
                    strategy,
                    bytes_moved: 0,
                    torn: false,
                    result: Err(MoveError::new(FailurePhase::Open, format!("pool: {e}"))),
                };
            }
        };

        // Move-into-blocking. The pair must live for the whole sync
        // body; on completion (or panic) it Drops, which sends the
        // contexts back to the pool.
        let me = self.clone();
        let result = tokio::task::spawn_blocking(move || {
            let mut pair = pair;
            work(&me, &mut pair)
        })
        .await
        .unwrap_or_else(|join_err| {
            Err(MoveError::new(
                FailurePhase::Open,
                format!("spawn_blocking join: {join_err}"),
            ))
        });

        MoveOutcome {
            row_id,
            strategy,
            // F41: the bytes the body actually wrote — never row.size
            // taken on faith. 0 on failure (pre-existing contract).
            bytes_moved: *result.as_ref().unwrap_or(&0),
            // Sync paths have no torn detection (see do_libnfs_copy).
            torn: false,
            result: result.map(|_| ()),
        }
    }

    // =========================================================================
    // Sync strategy dispatch (called from inside spawn_blocking).
    // =========================================================================

    /// Dispatch one strategy body and report the bytes it actually
    /// wrote (F41). Only the streaming copy moves file data; symlink,
    /// hardlink, dir-attrs, empty, and skip rows write no file bytes
    /// and report 0 — `row.size` is never reported on faith.
    fn execute(
        &self,
        pair: &mut ContextPair,
        row: &RowView,
        strategy: Strategy,
    ) -> Result<u64, MoveError> {
        match strategy {
            Strategy::ServerSideCopy => self.do_server_side_copy(pair, row).map(|()| 0),
            Strategy::LibnfsIoUring => self.do_libnfs_copy(pair, row),
            Strategy::KernelCopyFileRange => self.do_kernel_cfr(pair, row).map(|()| 0),
            Strategy::Symlink => self.do_symlink(pair, row).map(|()| 0),
            Strategy::HardlinkExisting => Err(MoveError::new(FailurePhase::Hardlink, "EINVAL")),
            Strategy::Empty => self.do_empty(pair, row).map(|()| 0),
            Strategy::DirAttrs => self.do_dir_attrs(pair, row).map(|()| 0),
            Strategy::Skip => Ok(0),
        }
    }

    /// Apply mode/owner/mtime to a directory whose row appeared in
    /// the index. Ensures the dir exists first (mkdir-on-demand may
    /// not have created it if no child file landed in it). Caller
    /// (the shard processor) MUST schedule this strategy after all
    /// non-dir rows in the same shard are committed, otherwise file
    /// commits inside the dir will restamp its mtime.
    ///
    /// Cross-shard caveat: if a child file's row lands in a later
    /// shard than its parent dir's row, that child's commit will
    /// still restamp the parent's mtime. Documented in M3_NOTES.md.
    fn do_dir_attrs(&self, pair: &mut ContextPair, row: &RowView) -> Result<(), MoveError> {
        let dst = self.dst_path(row);
        ops::mkdir_p(pair.dst(), &dst)?;
        self.apply_attrs(pair.dst(), &dst, row)?;
        Ok(())
    }

    // =========================================================================
    // Strategy implementations (all sync, all called from inside
    // spawn_blocking with an owned ContextPair).
    // =========================================================================

    /// Compose the source-side absolute path for `row.path`. Source
    /// libnfs ops MUST go through this — never `&row.path` directly.
    /// See BUGFIX_PLAN.md and SCHEMA_CONTRACT.md "Path encoding".
    fn src_path(&self, row: &RowView) -> Vec<u8> {
        join_root(self.cfg.source_root.as_bytes(), &row.path)
    }

    /// Compose the destination-side absolute path for `row.path`.
    /// All dest libnfs ops MUST go through this.
    fn dst_path(&self, row: &RowView) -> Vec<u8> {
        join_root(self.cfg.dest_root.as_bytes(), &row.path)
    }

    /// Per-file self-target check (Fix 2). Refuses to write when the
    /// computed dest path would collide with the source — either
    /// directly (same path) or through the `.partial` sibling living
    /// next to the source file. Both forms can zero a source file
    /// when `nfs_create` opens with `O_TRUNC`.
    ///
    /// Returns Err with tag `SELF_TARGET` on collision. Only meaningful
    /// when source and dest URLs match — different servers can never
    /// collide regardless of path. Belt-and-suspenders against the
    /// startup overlap guard in the worker; either alone is
    /// insufficient.
    fn check_self_target(
        &self,
        src: &[u8],
        dst: &[u8],
        dst_partial: &[u8],
    ) -> Result<(), MoveError> {
        check_self_target(
            &self.cfg.source_url,
            &self.cfg.dest_url,
            src,
            dst,
            dst_partial,
        )
    }

    /// R8: consult the fence immediately before issuing a commit-point
    /// op (`rename` / `link` / `symlink`). If the fence has tripped
    /// since the shard processor's last between-row check, bail out
    /// with `FailurePhase::Fenced` rather than commit. The shard
    /// processor recognizes that phase and routes the row back to
    /// claimable (via the shard's claim terminating) instead of
    /// recording a per-file failure.
    ///
    /// Note: there is no fence check inside the per-byte READ→WRITE
    /// loop. Once a row's commit op is in flight (mid-syscall) we
    /// accept it — that's the residual at-least-once tolerance the
    /// `.partial`-stamped + atomic-rename safety argument relies on
    /// (see CLAIM_PROTOCOL.md "What's NOT enforced" / R8).
    fn check_fence(&self) -> Result<(), MoveError> {
        if self.fence.is_valid() {
            Ok(())
        } else {
            Err(MoveError::new(FailurePhase::Fenced, "FENCE_TRIPPED"))
        }
    }

    /// **M4** — NFSv4.2 server-side COPY. Stubbed.
    fn do_server_side_copy(
        &self,
        _pair: &mut ContextPair,
        _row: &RowView,
    ) -> Result<(), MoveError> {
        Err(MoveError::new(FailurePhase::ServerSideCopy, "ENOSYS"))
    }

    /// **Escape hatch** — kernel `copy_file_range` over already-mounted
    /// kernel NFS. Stubbed.
    fn do_kernel_cfr(&self, _pair: &mut ContextPair, _row: &RowView) -> Result<(), MoveError> {
        Err(MoveError::new(FailurePhase::Write, "ENOSYS"))
    }

    /// Symlink — preserve `target` byte-for-byte from the index column
    /// if present (R8), otherwise readlink from the source.
    ///
    /// Per SCHEMA_CONTRACT.md "Symlink mode preservation": NFSv3 has
    /// no lchmod-equivalent (`nfs_chmod` follows symlinks), so when
    /// `preserve_mode = true` and the source mode bits differ from
    /// the conventional `0o0777`, the mover writes a
    /// `SYMLINK_MODE_NFSV3` downgrade record and counts the row as
    /// success. The destination symlink ends up with whatever default
    /// mode the server assigns. See BUGFIX_PLAN.md "Fix 5".
    ///
    /// Symlink is the commit point (R8), and replay is idempotent
    /// (F10): under at-least-once delivery a worker can die
    /// post-symlink-pre-ack and the row comes back. When `nfs_symlink`
    /// reports `EEXIST`, the destination is readlink'd on the dst
    /// context and [`resolve_symlink_eexist`] treats a byte-equal
    /// target as our own committed work (`Ok`, continuing with the
    /// idempotent post-commit steps), a mismatch as a real conflict
    /// (fail, naming both targets), and a readlink failure as the
    /// original `EEXIST` failure. Unlink-then-create was rejected as
    /// the recovery strategy: it destroys pre-existing data at the
    /// destination and opens a crash window with the link missing.
    fn do_symlink(&self, pair: &mut ContextPair, row: &RowView) -> Result<(), MoveError> {
        let src = self.src_path(row);
        let dst = self.dst_path(row);

        let target = match &row.symlink_target {
            Some(t) => t.clone(),
            None => ops::readlink(pair.src(), &src)?,
        };

        if let Err(e) = ops::mkdir_p_for_file(pair.dst(), &dst) {
            return Err(MoveError::new(FailurePhase::Symlink, e.error));
        }
        // R8: symlink IS the commit point for symlink rows — there is
        // no .partial + rename pattern (NFSv3 has no atomic
        // symlink-replace primitive). Fence-check immediately before
        // issuing it.
        self.check_fence()?;
        if let Err(e) = ops::symlink(pair.dst(), &target, &dst) {
            if !is_eexist(&e) {
                return Err(e);
            }
            // EEXIST recovery — readlink the destination on the dst
            // context and let `resolve_symlink_eexist` decide replay
            // vs conflict. Runs strictly after `ops::symlink`
            // returned, so the R8 fence check above still guards the
            // commit point. On Ok, fall THROUGH to the post-commit
            // steps below (mode-downgrade record, best-effort
            // lutimes): they are identical to the first run and
            // idempotent under at-least-once replay.
            let dst_readlink = ops::readlink(pair.dst(), &dst);
            resolve_symlink_eexist(e, &target, dst_readlink)?;
        }

        if self.cfg.policy.preserve_mode {
            let link_mode = row.mode & 0o7777;
            if link_mode != SYMLINK_DEFAULT_MODE {
                self.downgrades
                    .record(row.row_id, &row.path, DowngradeKind::SymlinkModeNfsV3);
            }
        }

        // Symlink mtime — best-effort post-commit. libnfs 1.16 does
        // export `nfs_lutimes` (µs precision, the symlink-itself
        // counterpart to `nfs_utimes`). If the row has no mtime,
        // record `NullMtime` consistent with the regular-file path.
        // If `lutimes` itself errors, log + downgrade rather than fail
        // the row — the symlink is already committed.
        if self.cfg.policy.preserve_times {
            match (row.mtime_sec, row.mtime_nsec) {
                (Some(mt_s), mt_n_opt) => {
                    let mt_n = mt_n_opt.unwrap_or(0);
                    let (at_s, at_n) = match (row.atime_sec, row.atime_nsec) {
                        (Some(a_s), a_n_opt) => (a_s, a_n_opt.unwrap_or(0)),
                        _ => {
                            self.downgrades
                                .record(row.row_id, &row.path, DowngradeKind::NullAtime);
                            (mt_s, mt_n)
                        }
                    };
                    if let Err(e) = ops::lutimes(pair.dst(), &dst, at_s, at_n, mt_s, mt_n) {
                        tracing::warn!(
                            row_id = row.row_id,
                            error = %e.error,
                            "lutimes on symlink failed; recording SymlinkTimeNfsV3 downgrade",
                        );
                        self.downgrades.record(
                            row.row_id,
                            &row.path,
                            DowngradeKind::SymlinkTimeNfsV3,
                        );
                    }
                }
                (None, _) => {
                    self.downgrades
                        .record(row.row_id, &row.path, DowngradeKind::NullMtime);
                }
            }
        }

        Ok(())
    }

    /// Hardlink — link an already-copied final path to a new path
    /// within the same shard. Both `target` and `linkpath` are
    /// `row.path`-style (relative to the export root); the mover
    /// composes the absolute dest paths via [`Self::dst_path`].
    ///
    /// Link is the commit point (R8), and replay is idempotent (F10):
    /// under at-least-once delivery a worker can die post-link-pre-ack
    /// and the row comes back. When `nfs_link` reports `EEXIST`, both
    /// sides are stat'd on the dest context and
    /// [`resolve_hardlink_eexist`] treats a fileid match as our own
    /// committed work (`Ok`), a mismatch as a real conflict (fail,
    /// naming both fileids), and a stat failure as the original
    /// `EEXIST` failure. Unlink-then-create was rejected as the
    /// recovery strategy: it destroys pre-existing data at the
    /// linkpath and opens a crash window with the link missing.
    fn do_hardlink(
        &self,
        pair: &mut ContextPair,
        target: &[u8],
        linkpath: &[u8],
    ) -> Result<(), MoveError> {
        let target_abs = join_root(self.cfg.dest_root.as_bytes(), target);
        let linkpath_abs = join_root(self.cfg.dest_root.as_bytes(), linkpath);

        if let Err(e) = ops::mkdir_p_for_file(pair.dst(), &linkpath_abs) {
            return Err(MoveError::new(FailurePhase::Hardlink, e.error));
        }
        // R8: link IS the commit point for hardlink rows — there is no
        // .partial + rename pattern, the linkpath is the final dest.
        // Fence-check immediately before issuing it.
        self.check_fence()?;
        if let Err(e) = ops::link(pair.dst(), &target_abs, &linkpath_abs) {
            if !is_eexist(&e) {
                return Err(e);
            }
            // EEXIST recovery — stat both sides on the dest context
            // and let `resolve_hardlink_eexist` decide replay vs
            // conflict. Runs strictly after `ops::link` returned, so
            // the R8 fence check above still guards the commit point.
            let target_stat = ops::stat_fileid(pair.dst(), &target_abs);
            let linkpath_stat = ops::stat_fileid(pair.dst(), &linkpath_abs);
            return resolve_hardlink_eexist(e, target_stat, linkpath_stat);
        }
        Ok(())
    }

    /// Empty file (`size == 0`) — no read/write loop, just CREATE +
    /// attrs + rename. R7: keeps the data-loop entirely off the
    /// zero-byte path.
    fn do_empty(&self, pair: &mut ContextPair, row: &RowView) -> Result<(), MoveError> {
        let src = self.src_path(row);
        let dst = self.dst_path(row);
        let dst_partial = partial_path(&dst, &self.host_id, self.pid)?;
        self.check_self_target(&src, &dst, &dst_partial)?;

        ops::mkdir_p_for_file(pair.dst(), &dst)?;

        let fh = ops::create_write(pair.dst(), &dst_partial, 0o600)?;
        ops::close_fh(pair.dst(), fh, FailurePhase::Write)?;

        self.apply_attrs(pair.dst(), &dst_partial, row)?;
        // R8: see do_libnfs_copy — fence check immediately before rename.
        self.check_fence()?;
        tracing::debug!(
            dest = %String::from_utf8_lossy(&dst),
            host = %self.host_id,
            pid = self.pid,
            row_id = row.row_id,
            "commit: rename .partial → final",
        );
        ops::rename(pair.dst(), &dst_partial, &dst)?;
        Ok(())
    }

    /// Default path: libnfs READ → libnfs WRITE through a 1 MiB
    /// streaming buffer, single-fiber within the call. Concurrency
    /// across files comes from the shard processor's JoinSet.
    /// Returns the bytes actually written (F41) — on an EarlyEof
    /// short copy this is less than `row.size` and the row still
    /// commits `Ok` with a `DowngradeKind::EarlyEof` record.
    ///
    /// Torn-copy detection is async-path-only for now: this sync path
    /// has no pre/post source-stat bracket, so a file modified during
    /// the copy commits here with no `DowngradeKind::TornCopy` record
    /// and `MoveOutcome::torn` stays `false`. The bucketed async path
    /// (`pipelined_copy` + `file_mover::classify_copy`) is the one
    /// that detects and records tears; see
    /// docs/work-items/MOVER_TORN_COPY_SURFACE.md (F05).
    fn do_libnfs_copy(&self, pair: &mut ContextPair, row: &RowView) -> Result<u64, MoveError> {
        let src = self.src_path(row);
        let dst = self.dst_path(row);
        let dst_partial = partial_path(&dst, &self.host_id, self.pid)?;
        self.check_self_target(&src, &dst, &dst_partial)?;

        ops::mkdir_p_for_file(pair.dst(), &dst)?;

        let src_fh = ops::open_read(pair.src(), &src)?;
        let dst_fh = match ops::create_write(pair.dst(), &dst_partial, 0o600) {
            Ok(fh) => fh,
            Err(e) => {
                ops::close_quietly(pair.src(), src_fh);
                return Err(e);
            }
        };

        let result = stream_copy(pair, &src_fh, &dst_fh, row.row_id, row.size);

        let close_src = ops::close_fh(pair.src(), src_fh, FailurePhase::Read);
        let close_dst = ops::close_fh(pair.dst(), dst_fh, FailurePhase::Write);

        let written = result?;
        close_src?;
        close_dst?;

        if self.cfg.require_unchanged_size && written != row.size {
            return Err(MoveError::new(FailurePhase::Open, "SIZE_CHANGED"));
        }

        // Per SCHEMA_CONTRACT.md "Size semantics" / decision #11, a
        // short read is *not* a failure on the default path — the
        // file is committed. But surface the discrepancy so the
        // operator sees that actual bytes copied differ from the
        // indexed size. (If we'd had this in place during the M2 FFI
        // verification incident, every non-empty regular file would
        // have produced an EARLY_EOF record; see M2_NOTES.md
        // "M2/M3 verification incidents".)
        if written < row.size {
            self.downgrades
                .record(row.row_id, &row.path, DowngradeKind::EarlyEof);
        }

        self.apply_attrs(pair.dst(), &dst_partial, row)?;
        // R8: last-ditch fence check immediately before the commit-point
        // rename. The shard processor only checks between rows; without
        // this guard, every row already inside spawn_blocking at fence
        // trip time still commits.
        self.check_fence()?;
        tracing::debug!(
            dest = %String::from_utf8_lossy(&dst),
            host = %self.host_id,
            pid = self.pid,
            row_id = row.row_id,
            "commit: rename .partial → final",
        );
        ops::rename(pair.dst(), &dst_partial, &dst)?;
        Ok(written)
    }

    // =========================================================================
    // Helpers.
    // =========================================================================

    /// Apply uid/gid + mode + atime/mtime on the still-`.partial`
    /// destination, in the order planned by
    /// [`attr_plan::plan_attr_ops`]: chown → chmod → utimes (F08 —
    /// owner before mode so NFSv3 kill-priv semantics can't strip
    /// S_ISUID/S_ISGID the chmod just applied; utimes strictly last).
    /// Honors `cfg.policy` and `cfg.require_chown` (chown EPERM in
    /// degraded mode records `NullOwner` and continues to chmod).
    /// Records downgrades for null source attrs the user asked to
    /// preserve, per SCHEMA_CONTRACT.md "Null attribute semantics".
    fn apply_attrs(
        &self,
        ctx: &mut NfsContext,
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

        let plan = attr_plan::plan_attr_ops(row, policy);
        let mut exec = SyncAttrExec {
            ctx,
            dst_partial,
            row,
            downgrades: &self.downgrades,
            require_chown: self.cfg.require_chown,
        };
        attr_plan::execute_plan(&plan, &mut exec)
    }
}

/// [`AttrExec`] over the sync libnfs context — each op maps to the
/// pre-existing `ops::` call. The chown-EPERM degraded-mode policy
/// lives here unchanged (record `NullOwner`, report `SkippedDegraded`
/// so the plan continues); only its position in the sequence moved.
struct SyncAttrExec<'a> {
    ctx: &'a mut NfsContext,
    dst_partial: &'a [u8],
    row: &'a RowView,
    downgrades: &'a DowngradeSink,
    require_chown: bool,
}

impl AttrExec for SyncAttrExec<'_> {
    type Err = MoveError;

    fn chown(&mut self, uid: u32, gid: u32) -> Result<ChownOutcome, MoveError> {
        match ops::chown(self.ctx, self.dst_partial, uid, gid) {
            Ok(()) => Ok(ChownOutcome::Applied),
            Err(e) if e.error == "EPERM" && !self.require_chown => {
                tracing::debug!(uid, gid, "chown EPERM in degraded mode; skipping");
                self.downgrades
                    .record(self.row.row_id, &self.row.path, DowngradeKind::NullOwner);
                Ok(ChownOutcome::SkippedDegraded)
            }
            Err(e) => Err(e),
        }
    }

    fn chmod(&mut self, mode: u32) -> Result<(), MoveError> {
        ops::chmod(self.ctx, self.dst_partial, mode)
    }

    fn utimes(&mut self, atime: (i64, i32), mtime: (i64, i32)) -> Result<(), MoveError> {
        ops::utimes(
            self.ctx,
            self.dst_partial,
            atime.0,
            atime.1,
            mtime.0,
            mtime.1,
        )
    }
}

/// Return the parent directory portion of an absolute byte path,
/// keeping the trailing slash so two parents compare equal even when
/// only one originally had a slash. Empty input maps to empty.
fn parent_dir(p: &[u8]) -> &[u8] {
    match p.iter().rposition(|&b| b == b'/') {
        Some(0) => b"/",
        Some(i) => &p[..i],
        None => &[],
    }
}

/// Free-function form of the per-file self-target check, factored out
/// of `Mover` so it's unit-testable without spinning up a libnfs pool.
/// See `Mover::check_self_target` for behavior; this is the body.
pub(crate) fn check_self_target(
    source_url: &str,
    dest_url: &str,
    src: &[u8],
    dst: &[u8],
    dst_partial: &[u8],
) -> Result<(), MoveError> {
    if source_url != dest_url {
        return Ok(());
    }
    if src == dst {
        return Err(MoveError::new(FailurePhase::Open, "SELF_TARGET"));
    }
    if parent_dir(src) == parent_dir(dst_partial) {
        return Err(MoveError::new(FailurePhase::Open, "SELF_TARGET"));
    }
    Ok(())
}

/// The READ→WRITE loop. Sync; runs inside `spawn_blocking`. Returns
/// the total bytes written. EOF before `size` is *not* an error in
/// the default mode — `size` is advisory per SCHEMA_CONTRACT.md —
/// but is surfaced as a tracing warning so the operator can spot a
/// short copy without grep'ing for downgrade records. The caller
/// (`do_libnfs_copy`) writes the corresponding `EARLY_EOF` downgrade
/// record after this returns.
///
/// `row_id` is threaded through purely for the warning's structured
/// fields.
fn stream_copy(
    pair: &mut ContextPair,
    src_fh: &ops::NfsFh,
    dst_fh: &ops::NfsFh,
    row_id: u64,
    size: u64,
) -> Result<u64, MoveError> {
    let (src_ctx, dst_ctx) = pair.split();
    stream_copy_inner(
        size,
        |off, buf| ops::pread(src_ctx, src_fh, off, buf),
        |off, buf| ops::pwrite(dst_ctx, dst_fh, off, buf),
        |off, remaining| {
            tracing::warn!(
                row_id,
                indexed_size = size,
                actual_size = off,
                short = remaining,
                "pread returned 0 with remaining bytes; treating as EOF \
                 (contract: size is advisory)",
            );
        },
    )
}

/// Loop body of `stream_copy`, factored out so it can be exercised
/// against in-memory closures (no libnfs context, no real fhs).
/// Production calls it from `stream_copy` with closures that hit the
/// libnfs FFI; the unit tests below call it with closures that drive
/// pre-canned read returns to reproduce the FFI-bug failure mode
/// (silent zero-byte reads).
fn stream_copy_inner<R, W, S>(
    size: u64,
    mut read_at: R,
    mut write_at: W,
    mut on_short_eof: S,
) -> Result<u64, MoveError>
where
    R: FnMut(u64, &mut [u8]) -> Result<usize, MoveError>,
    W: FnMut(u64, &[u8]) -> Result<usize, MoveError>,
    S: FnMut(u64, u64),
{
    if size == 0 {
        return Ok(0);
    }

    let mut buf = vec![0u8; STREAM_BUF_SIZE];
    let mut off = 0u64;
    let mut remaining = size;

    while remaining > 0 {
        let want = remaining.min(STREAM_BUF_SIZE as u64) as usize;
        let n = read_at(off, &mut buf[..want])?;
        if n == 0 {
            on_short_eof(off, remaining);
            break;
        }
        let mut written_in_chunk = 0;
        while written_in_chunk < n {
            let w = write_at(off + written_in_chunk as u64, &buf[written_in_chunk..n])?;
            if w == 0 {
                return Err(MoveError::new(FailurePhase::Write, "EIO"));
            }
            written_in_chunk += w;
        }
        off += n as u64;
        remaining -= n as u64;
    }
    Ok(off)
}

/// True iff a link-layer error is `EEXIST`. Same errno-name
/// convention as the rest of the safe-wrapper layer (`MoveError.error`
/// carries the errno name from `libnfs::errno_name` — see
/// `ops::mkdir`'s EEXIST handling). `do_hardlink` and `do_symlink`
/// enter EEXIST recovery only behind this guard; every other
/// link/symlink error passes through unchanged.
fn is_eexist(err: &MoveError) -> bool {
    err.error == "EEXIST"
}

/// Decide the outcome of a hardlink EEXIST: `Ok(())` iff the existing
/// linkpath already IS the target (same fileid). `target_stat` /
/// `linkpath_stat` are the results of statting the target and the
/// linkpath on the destination context during recovery.
fn resolve_hardlink_eexist(
    link_err: MoveError,
    target_stat: Result<u64, MoveError>,
    linkpath_stat: Result<u64, MoveError>,
) -> Result<(), MoveError> {
    match (target_stat, linkpath_stat) {
        // Same fileid: the linkpath already IS the target — our own
        // committed link replayed after a died-post-link-pre-ack
        // worker. Idempotent success.
        (Ok(target_id), Ok(linkpath_id)) if target_id == linkpath_id => Ok(()),
        // Different fileid: a real conflict — some other file
        // occupies the linkpath. Name both fileids so the operator
        // can tell conflict from replay in the failure log.
        (Ok(target_id), Ok(linkpath_id)) => Err(MoveError::new(
            FailurePhase::Hardlink,
            format!(
                "EEXIST: linkpath fileid {linkpath_id} != target fileid \
                 {target_id} (conflict, not a replay)"
            ),
        )),
        // Either stat failed: recovery must never mask the primary
        // failure — surface the ORIGINAL EEXIST link error.
        _ => Err(link_err),
    }
}

/// Decide the outcome of a symlink EEXIST: `Ok(())` iff the existing
/// dst entry is a symlink whose target byte-equals `intended`.
/// `dst_readlink` is the result of readlink-ing the destination path
/// on the destination context during recovery.
fn resolve_symlink_eexist(
    link_err: MoveError,
    intended: &[u8],
    dst_readlink: Result<Vec<u8>, MoveError>,
) -> Result<(), MoveError> {
    match dst_readlink {
        // Matching target bytes: the dst symlink already points at
        // the intended target — our own committed symlink replayed
        // after a died-post-symlink-pre-ack worker. Idempotent
        // success.
        Ok(existing) if existing == intended => Ok(()),
        // Different target: a real conflict — some other symlink
        // occupies the destination. Name both targets
        // (lossy-rendered) so the operator can tell conflict from
        // replay in the failure log.
        Ok(existing) => Err(MoveError::new(
            FailurePhase::Symlink,
            format!(
                "EEXIST: dst symlink target \"{}\" != intended target \
                 \"{}\" (conflict, not a replay)",
                String::from_utf8_lossy(&existing),
                String::from_utf8_lossy(intended),
            ),
        )),
        // readlink failed (including EINVAL: the existing entry is
        // not a symlink at all): recovery must never mask the primary
        // failure — surface the ORIGINAL EEXIST symlink error.
        Err(_) => Err(link_err),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // ---- parent_dir ----------------------------------------------

    #[test]
    fn parent_dir_root_level_file() {
        assert_eq!(parent_dir(b"/foo.txt"), b"/");
    }

    #[test]
    fn parent_dir_nested() {
        assert_eq!(parent_dir(b"/a/b/c"), b"/a/b");
    }

    #[test]
    fn parent_dir_no_slash() {
        assert_eq!(parent_dir(b"foo"), b"");
    }

    // ---- self-target check ---------------------------------------
    //
    // Belt-and-suspenders against the startup overlap guard. These
    // tests are the regression test for the data-loss bug
    // recovered in M2 verification.

    #[test]
    fn self_target_check_blocks_same_path() {
        let url = "nfs://host/exp";
        let r = check_self_target(url, url, b"/foo/bar", b"/foo/bar", b"/foo/.bar.h.1.partial");
        let e = r.expect_err("identical src and dst must fail SELF_TARGET");
        assert_eq!(e.error, "SELF_TARGET");
        assert_eq!(e.phase, FailurePhase::Open);
    }

    #[test]
    fn self_target_check_blocks_same_parent_dir() {
        // dst path differs but its .partial parent equals src parent —
        // create-with-O_TRUNC would still trash the source file.
        let url = "nfs://host/exp";
        let r = check_self_target(url, url, b"/foo/bar", b"/foo/baz", b"/foo/.bar.h.1.partial");
        assert_eq!(r.unwrap_err().error, "SELF_TARGET");
    }

    #[test]
    fn self_target_check_allows_different_url() {
        // Different servers — paths can collide all they want.
        let r = check_self_target(
            "nfs://srcA/exp",
            "nfs://srcB/exp",
            b"/foo/bar",
            b"/foo/bar",
            b"/foo/.bar.h.1.partial",
        );
        assert!(r.is_ok());
    }

    #[test]
    fn self_target_check_allows_disjoint_dirs() {
        let url = "nfs://host/exp";
        let r = check_self_target(
            url,
            url,
            b"/src/file",
            b"/dst/file",
            b"/dst/.file.h.1.partial",
        );
        assert!(r.is_ok());
    }

    // ---- stream_copy_inner: short-read behavior ------------------
    //
    // Regression coverage for the M2 libnfs FFI bug. The buggy FFI
    // signature caused pread to return 0 on every call against a
    // real export, but the stream_copy loop used to swallow that
    // silently and report success. The contract still says "size is
    // advisory" so the loop must NOT fail — it must return Ok(off)
    // with off == bytes-actually-read, leaving the caller to record
    // the EARLY_EOF downgrade. These tests pin that behavior so the
    // surface can't regress.

    #[test]
    fn stream_copy_short_read_returns_ok_with_partial_off() {
        // Mock pread that always returns 0 — the exact failure mode
        // of the M2 libnfs FFI bug. write should never be called.
        let size: u64 = 4096;
        let mut on_short_called = false;
        let result = stream_copy_inner(
            size,
            |_off, _buf| Ok(0usize),
            |_off, _buf| -> Result<usize, MoveError> {
                panic!("write_at must not be called when read returns 0");
            },
            |off, remaining| {
                on_short_called = true;
                assert_eq!(off, 0);
                assert_eq!(remaining, size);
            },
        );

        let off = result.expect("short read is not an error in default mode");
        assert_eq!(off, 0, "off must equal bytes-actually-read");
        assert!(off < size, "off ({off}) must be < indexed size ({size})");
        assert!(
            on_short_called,
            "on_short_eof must fire so caller can record EARLY_EOF"
        );
    }

    #[test]
    fn stream_copy_short_read_after_partial_progress() {
        // Variant: pread returns one full chunk then 0. off should
        // equal the chunk that did land; the loop still returns Ok.
        let size: u64 = (STREAM_BUF_SIZE as u64) * 4;
        let mut reads = 0;
        let mut writes = 0;
        let result = stream_copy_inner(
            size,
            |_off, buf| {
                reads += 1;
                if reads == 1 {
                    Ok(buf.len()) // first chunk: full read
                } else {
                    Ok(0) // then EOF
                }
            },
            |_off, buf| {
                writes += 1;
                Ok(buf.len())
            },
            |_off, _remaining| {},
        );

        let off = result.expect("short read after progress is not an error");
        assert_eq!(off as usize, STREAM_BUF_SIZE);
        assert!(off < size);
        assert_eq!(reads, 2, "expected one full read then one short read");
        assert_eq!(writes, 1);
    }

    #[test]
    fn stream_copy_size_zero_is_no_op() {
        let result = stream_copy_inner(
            0,
            |_, _| -> Result<usize, MoveError> {
                panic!("read_at must not be called for size 0");
            },
            |_, _| -> Result<usize, MoveError> {
                panic!("write_at must not be called for size 0");
            },
            |_, _| panic!("on_short_eof must not fire for size 0"),
        );
        assert_eq!(result.unwrap(), 0);
    }

    // ---- R8 fence check ------------------------------------------
    //
    // Building a Mover requires a `LibnfsContextPool` to plug into the
    // pre-commit acquire path. The fence check itself doesn't touch
    // the pool — it just reads the atomic flag — so the tests below
    // use a stub pool that never hands out a pair. The strategy
    // bodies all funnel through `check_fence()` immediately before
    // their commit-point op; confirming `check_fence()` honors the
    // flag is sufficient to prove the fence guard fires for each
    // strategy (verified by code inspection at the patch sites).

    use crate::libnfs::{ContextPair, LibnfsContextPool};
    use async_trait::async_trait;

    struct StubPool;
    #[async_trait]
    impl LibnfsContextPool for StubPool {
        async fn acquire(&self) -> anyhow::Result<ContextPair> {
            // The fence-check tests construct the Mover but never
            // actually call into a strategy body — driving libnfs from
            // a unit test would require a real NFS context. We only
            // need the Mover type to exist; acquire is never invoked.
            anyhow::bail!("StubPool::acquire is not implemented for fence tests")
        }
    }

    fn build_mover_with_fence(fence: Fence) -> Mover {
        build_mover(Arc::new(StubPool) as Arc<dyn LibnfsContextPool>, fence)
    }

    fn build_mover(pool: Arc<dyn LibnfsContextPool>, fence: Fence) -> Mover {
        let cfg = MoverConfig {
            source_url: "nfs://srcA/exp".to_string(),
            dest_url: "nfs://srcB/exp".to_string(),
            source_root: "/".to_string(),
            dest_root: "/".to_string(),
            same_server_v42: false,
            policy: AttrPolicy {
                preserve_mode: true,
                preserve_owner: true,
                preserve_times: true,
                preserve_xattr: false,
            },
            server_side_copy: ServerSideCopy::Off,
            server_side_copy_min_bytes: 0,
            uring: UringConfig::default(),
            inflight: InflightProfile::default(),
            require_chown: false,
            require_unchanged_size: false,
            rpc_timeout_ms: crate::libnfs::DEFAULT_RPC_TIMEOUT_MS,
        };
        Mover::new(
            cfg,
            pool,
            "test-host",
            crate::downgrade::DowngradeSink::new(),
            fence,
        )
    }

    /// F12: `MoverConfig::from_options` seeds the explicit per-RPC
    /// timeout default (60_000 ms); the orchestrator overrides it
    /// from `[mover] rpc_timeout_ms` before mounting any pool.
    #[test]
    fn from_options_defaults_rpc_timeout_to_60000() {
        let cfg = MoverConfig::from_options(
            "nfs://src/exp".into(),
            "nfs://dst/exp".into(),
            "/".into(),
            "/".into(),
            false,
            &MigrationOptions::default(),
        );
        assert_eq!(cfg.rpc_timeout_ms, crate::libnfs::DEFAULT_RPC_TIMEOUT_MS);
    }

    // ---- F41: honest byte counts ----------------------------------
    //
    // `MoveOutcome::bytes_moved` must report the bytes actually
    // written, never `row.size` taken on faith. Two sync-path `Ok`
    // outcomes used to inflate it: `Strategy::Skip` (copies nothing)
    // and an EarlyEof short copy (commits `written < row.size`).
    // See docs/work-items/WORKER_RESILIENCE.md item 2.

    use migration_core::schema::FileTypeTag;
    use migration_core::shard::RowView;

    /// Pool that hands out unmounted pairs — valid for strategy arms
    /// and stubbed bodies that never touch the contexts.
    struct DummyPairPool;
    #[async_trait]
    impl LibnfsContextPool for DummyPairPool {
        async fn acquire(&self) -> anyhow::Result<ContextPair> {
            Ok(ContextPair::unmounted_for_tests())
        }
    }

    fn test_row(size: u64, file_type: FileTypeTag) -> RowView {
        RowView {
            row_id: 7,
            path: b"/data/file".to_vec(),
            size,
            mtime_sec: None,
            mtime_nsec: None,
            atime_sec: None,
            atime_nsec: None,
            mode: 0o644,
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

    /// F41 acceptance test 5 (red before fix): a Skip row (fifo /
    /// socket / dev) copies nothing and must report 0 bytes while
    /// still counting as a success. Before the fix it reported
    /// `row.size` — inflating throughput, backpressure inputs, and
    /// coord aggregation.
    #[tokio::test]
    async fn skip_reports_zero_bytes() {
        let mover = build_mover(
            Arc::new(DummyPairPool) as Arc<dyn LibnfsContextPool>,
            Fence::new(),
        );
        let row = test_row(4096, FileTypeTag::Fifo);
        let outcome = mover.move_one(&row).await;
        assert_eq!(outcome.strategy, Strategy::Skip);
        assert!(
            outcome.result.is_ok(),
            "Skip must stay a success: {:?}",
            outcome.result,
        );
        assert_eq!(
            outcome.bytes_moved, 0,
            "Skip copies nothing and must report 0 bytes, not row.size",
        );
    }

    /// F41 acceptance test 6 (red before fix — a type-level red: the
    /// sync copy bodies returned `()`, so a stubbed body could not
    /// even express a written count). `do_libnfs_copy` is FFI-coupled,
    /// so this drives `run_with_pair`'s outcome assembly with a
    /// stubbed body that commits fewer bytes than `row.size` — the
    /// EarlyEof shape (`stream_copy` hit EOF early; the row still
    /// commits `Ok`, with the downgrade recorded by the real body).
    /// The outcome must report the actual written count.
    #[tokio::test]
    async fn early_eof_reports_written_bytes() {
        let mover = build_mover(
            Arc::new(DummyPairPool) as Arc<dyn LibnfsContextPool>,
            Fence::new(),
        );
        let row = test_row(4096, FileTypeTag::Regular);
        let outcome = mover
            .run_with_pair(&row, Strategy::LibnfsIoUring, |_, _| Ok(500))
            .await;
        assert!(outcome.result.is_ok(), "EarlyEof stays a committed success");
        assert_eq!(
            outcome.bytes_moved, 500,
            "outcome must report the bytes actually written, not row.size",
        );
    }

    /// Regression guard (hardware-free analog of file_mover_smoke's
    /// `bytes_moved == size` assertion): a full clean copy still
    /// reports the full size.
    #[tokio::test]
    async fn full_copy_reports_full_size() {
        let mover = build_mover(
            Arc::new(DummyPairPool) as Arc<dyn LibnfsContextPool>,
            Fence::new(),
        );
        let row = test_row(4096, FileTypeTag::Regular);
        let outcome = mover
            .run_with_pair(&row, Strategy::LibnfsIoUring, |_, _| Ok(4096))
            .await;
        assert!(outcome.result.is_ok());
        assert_eq!(outcome.bytes_moved, 4096);
    }

    /// Failed rows keep reporting 0 bytes (pre-F41 behavior pin).
    #[tokio::test]
    async fn failed_copy_reports_zero_bytes() {
        let mover = build_mover(
            Arc::new(DummyPairPool) as Arc<dyn LibnfsContextPool>,
            Fence::new(),
        );
        let row = test_row(4096, FileTypeTag::Regular);
        let outcome = mover
            .run_with_pair(&row, Strategy::LibnfsIoUring, |_, _| {
                Err(MoveError::new(FailurePhase::Write, "EIO"))
            })
            .await;
        assert!(outcome.result.is_err());
        assert_eq!(outcome.bytes_moved, 0);
    }

    /// Pins the F41 plumbing by type: the sync copy body returns the
    /// actual written count (`u64`), not `()`. Never called — the
    /// body is FFI-coupled; the count itself comes from `stream_copy`,
    /// whose short-read behavior is pinned by the tests above.
    #[allow(dead_code)]
    fn _pin_do_libnfs_copy_returns_written(
        m: &Mover,
        p: &mut ContextPair,
        r: &RowView,
    ) -> Result<u64, MoveError> {
        m.do_libnfs_copy(p, r)
    }

    #[test]
    fn check_fence_passes_when_fence_valid() {
        let fence = Fence::new();
        let mover = build_mover_with_fence(fence);
        assert!(mover.check_fence().is_ok());
    }

    #[test]
    fn check_fence_returns_fenced_when_fence_tripped() {
        let fence = Fence::new();
        fence.trip("test trip");
        let mover = build_mover_with_fence(fence);
        let err = mover
            .check_fence()
            .expect_err("tripped fence must short-circuit commit");
        assert_eq!(err.phase, FailurePhase::Fenced);
        assert_eq!(err.error, "FENCE_TRIPPED");
    }

    #[test]
    fn fence_clones_share_state_with_mover() {
        // The Mover's fence is held by-value (Fence is Clone, Arc
        // internally). Tripping the original handle after building
        // the Mover must still cause check_fence() to return Fenced.
        // This pins the "Arc-backed atomic flag" contract the mover
        // relies on per docs/CLAIM_PROTOCOL.md "Self-fencing".
        let fence = Fence::new();
        let mover = build_mover_with_fence(fence.clone());
        assert!(mover.check_fence().is_ok());
        fence.trip("late trip after Mover constructed");
        let err = mover.check_fence().expect_err("late trip must propagate");
        assert_eq!(err.phase, FailurePhase::Fenced);
    }

    // ---- hardlink EEXIST recovery (F10) ---------------------------
    //
    // At-least-once replay of a committed hardlink row: the worker
    // died post-link-pre-ack, the row is redelivered, and nfs_link
    // reports EEXIST. Same fileid on both sides means the linkpath
    // already IS the target — our own committed work — and the row
    // must resolve Ok instead of landing in the failure sink. See
    // docs/work-items/HARDLINK_REPLAY_IDEMPOTENCY.md.

    fn eexist_link_err() -> MoveError {
        MoveError::new(FailurePhase::Hardlink, "EEXIST")
    }

    #[test]
    fn eexist_same_fileid_is_success() {
        let r = resolve_hardlink_eexist(eexist_link_err(), Ok(42), Ok(42));
        assert!(
            r.is_ok(),
            "same fileid = replay of committed work, must be Ok: {r:?}"
        );
    }

    #[test]
    fn eexist_different_fileid_stays_failure() {
        let e = resolve_hardlink_eexist(eexist_link_err(), Ok(111), Ok(222))
            .expect_err("different fileids are a real conflict, not a replay");
        assert_eq!(e.phase, FailurePhase::Hardlink);
        assert!(
            e.error.contains("111") && e.error.contains("222"),
            "conflict message must name both fileids so the operator \
             can tell conflict from replay, got: {}",
            e.error
        );
    }

    #[test]
    fn eexist_stat_failure_preserves_original_error() {
        // Recovery must never mask the primary failure: whichever
        // stat fails, the returned error is the ORIGINAL EEXIST link
        // error, not the stat error.
        let stat_err = || MoveError::new(FailurePhase::Hardlink, "EACCES");
        let cases: [(Result<u64, MoveError>, Result<u64, MoveError>); 3] = [
            (Err(stat_err()), Ok(42)),
            (Ok(42), Err(stat_err())),
            (Err(stat_err()), Err(stat_err())),
        ];
        for (target_stat, linkpath_stat) in cases {
            let e = resolve_hardlink_eexist(eexist_link_err(), target_stat, linkpath_stat)
                .expect_err("stat failure during recovery must stay a failure");
            assert_eq!(e.phase, FailurePhase::Hardlink);
            assert_eq!(
                e.error, "EEXIST",
                "must return the ORIGINAL link error, not the stat error"
            );
        }
    }

    #[test]
    fn non_eexist_errors_pass_through() {
        // Wiring-level guarantee: `do_hardlink` enters the recovery
        // arm only behind `is_eexist` (an early `return Err(e)`
        // otherwise), so `resolve_hardlink_eexist` is unreachable for
        // any other link error. Pin the guard's classification here.
        assert!(is_eexist(&MoveError::new(FailurePhase::Hardlink, "EEXIST")));
        for name in ["ENOSPC", "EACCES", "EIO", "ENOENT", "errno=999"] {
            assert!(
                !is_eexist(&MoveError::new(FailurePhase::Hardlink, name)),
                "{name} must pass through, not enter EEXIST recovery"
            );
        }
    }

    // ---- symlink EEXIST recovery (F10, symlink half) ---------------
    //
    // At-least-once replay of a committed symlink row: the worker
    // died post-symlink-pre-ack, the row is redelivered, and
    // nfs_symlink reports EEXIST. A dst symlink whose target
    // byte-equals the intended target IS our own committed work — the
    // row must resolve Ok instead of landing in the failure sink. See
    // docs/work-items/SYMLINK_REPLAY_IDEMPOTENCY.md.

    fn eexist_symlink_err() -> MoveError {
        MoveError::new(FailurePhase::Symlink, "EEXIST")
    }

    #[test]
    fn symlink_eexist_matching_target_is_success() {
        // Byte-compare, not string-compare: the second target is not
        // valid UTF-8 and must still match.
        let targets: [&[u8]; 2] = [b"/t/plain", b"/t/\xff\xfe"];
        for intended in targets {
            let r = resolve_symlink_eexist(eexist_symlink_err(), intended, Ok(intended.to_vec()));
            assert!(
                r.is_ok(),
                "matching target = replay of committed work, must be Ok: {r:?}"
            );
        }
    }

    #[test]
    fn symlink_eexist_different_target_stays_failure() {
        let e = resolve_symlink_eexist(
            eexist_symlink_err(),
            b"/t/intended",
            Ok(b"/t/existing".to_vec()),
        )
        .expect_err("different targets are a real conflict, not a replay");
        assert_eq!(e.phase, FailurePhase::Symlink);
        assert!(
            e.error.contains("/t/intended") && e.error.contains("/t/existing"),
            "conflict message must name both targets (lossy-rendered) so \
             the operator can tell conflict from replay, got: {}",
            e.error
        );
    }

    #[test]
    fn symlink_eexist_readlink_failure_preserves_original_error() {
        // Recovery must never mask the primary failure. EINVAL is the
        // entry-is-not-a-symlink shape (readlink on a non-symlink
        // entry); the returned error must be the ORIGINAL EEXIST
        // symlink error, not the readlink error.
        let readlink_err = MoveError::new(FailurePhase::Symlink, "EINVAL");
        let e = resolve_symlink_eexist(eexist_symlink_err(), b"/t/intended", Err(readlink_err))
            .expect_err("readlink failure during recovery must stay a failure");
        assert_eq!(e.phase, FailurePhase::Symlink);
        assert_eq!(
            e.error, "EEXIST",
            "must return the ORIGINAL symlink error, not the readlink error"
        );
    }

    #[test]
    fn symlink_non_eexist_errors_pass_through() {
        // Wiring-level guarantee: `do_symlink` enters the recovery
        // arm only behind the shared `is_eexist` guard (an early
        // `return Err(e)` otherwise), so `resolve_symlink_eexist` is
        // unreachable for any other symlink error. The guard
        // classifies by errno name alone; pin that it holds for
        // Symlink-phase errors exactly as for Hardlink-phase ones.
        assert!(is_eexist(&MoveError::new(FailurePhase::Symlink, "EEXIST")));
        for name in ["ENOSPC", "EACCES", "EIO", "ENOENT", "EINVAL", "errno=999"] {
            assert!(
                !is_eexist(&MoveError::new(FailurePhase::Symlink, name)),
                "{name} must pass through, not enter EEXIST recovery"
            );
        }
    }
}
