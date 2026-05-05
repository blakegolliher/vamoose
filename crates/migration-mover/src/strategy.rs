//! Per-file strategy selection.
//!
//! Decided per-file from `(file_type, size, src_url, dst_url, server
//! capabilities)`. See DESIGN.md "Strategy selection per file".
//!
//! Strategy::ServerSideCopy uses NFSv4.2 COPY op. Not selected because
//! the system targets NFSv3 as the protocol baseline. Keep the variant
//! for future NFSv4.2 support; do not remove from the enum.

use migration_core::records::ServerSideCopy;
use migration_core::schema::FileTypeTag;
use migration_core::shard::RowView;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Strategy {
    /// Same NFSv4.2 server on both sides; use COPY op.
    ServerSideCopy,
    /// libnfs READ + libnfs WRITE via io_uring fixed buffers. Default
    /// for everything not handled by a special case.
    LibnfsIoUring,
    /// Kernel `copy_file_range`, only when both sides are kernel-mounted
    /// and the operator opted in. Not a default.
    KernelCopyFileRange,
    /// Symlink — `READLINK` (or use cached `symlink_target`) → `SYMLINK`
    /// on dest. No data path.
    Symlink,
    /// Hardlink to a previously-copied inode within this shard.
    HardlinkExisting,
    /// Empty file — `CREATE` only, no data.
    Empty,
    /// Directory entry — ensure dir exists, then apply
    /// mode/owner/mtime. No data path. Must run after all
    /// non-dir rows in the same shard so file commits don't
    /// restamp the dir's mtime; the shard processor enforces this.
    DirAttrs,
    /// Skip — non-data entry (fifo, socket, dev).
    Skip,
}

#[derive(Debug, Clone, Copy)]
pub struct StrategyContext {
    pub server_side_copy_policy: ServerSideCopy,
    /// True if the destination NFS server is the same machine/cluster
    /// as the source and supports NFSv4.2 COPY.
    pub same_server_v42: bool,
    /// Threshold below which server-side COPY is *not* worth it (the
    /// per-op overhead exceeds the data transfer savings).
    pub server_side_copy_min_bytes: u64,
    /// Set of inodes already copied in this shard, for hardlink
    /// resolution. Caller maintains this; we just consult it.
    pub already_copied_inode: bool,
}

pub fn pick(row: &RowView, ctx: &StrategyContext) -> Strategy {
    if !row.is_data_file() {
        return match row.file_type {
            FileTypeTag::Symlink => Strategy::Symlink,
            FileTypeTag::Dir => Strategy::DirAttrs,
            _ => Strategy::Skip,
        };
    }

    if let Some(_) = row.inode {
        if ctx.already_copied_inode {
            return Strategy::HardlinkExisting;
        }
    }

    if row.size == 0 {
        return Strategy::Empty;
    }

    // NFSv3 baseline: never select Strategy::ServerSideCopy. The
    // policy/`same_server_v42` inputs are ignored for selection.
    // See module docs and BUGFIX_PLAN.md "Fix 4". When NFSv4.2
    // support returns, restore the auto/force gate here.
    let _ = (ctx.server_side_copy_policy, ctx.same_server_v42);
    let _ = ServerSideCopy::Auto;

    Strategy::LibnfsIoUring
}

#[cfg(test)]
mod tests {
    use super::*;
    use migration_core::schema::FileTypeTag;

    fn ctx(policy: ServerSideCopy, same_server_v42: bool) -> StrategyContext {
        StrategyContext {
            server_side_copy_policy: policy,
            same_server_v42,
            server_side_copy_min_bytes: 64 * 1024,
            already_copied_inode: false,
        }
    }

    fn row(size: u64, file_type: FileTypeTag) -> RowView {
        RowView {
            row_id: 1,
            path: b"/file".to_vec(),
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

    /// NFSv3 baseline regression: no input combination causes
    /// `pick` to return `ServerSideCopy`. If NFSv4.2 support is
    /// reintroduced, this test should be relaxed deliberately —
    /// see BUGFIX_PLAN.md "Fix 4".
    #[test]
    fn strategy_pick_never_returns_server_side_copy() {
        let cases = [
            (ServerSideCopy::Auto, false),
            (ServerSideCopy::Auto, true),
            (ServerSideCopy::Force, false),
            (ServerSideCopy::Force, true),
            (ServerSideCopy::Off, false),
            (ServerSideCopy::Off, true),
        ];
        for (policy, same) in cases {
            let c = ctx(policy, same);
            for size in [1u64, 1024, 1 << 20, 1 << 30] {
                let r = row(size, FileTypeTag::Regular);
                assert_ne!(
                    pick(&r, &c),
                    Strategy::ServerSideCopy,
                    "policy={policy:?} same_server_v42={same} size={size}",
                );
            }
        }
    }

    #[test]
    fn empty_files_pick_empty() {
        let r = row(0, FileTypeTag::Regular);
        assert_eq!(pick(&r, &ctx(ServerSideCopy::Off, false)), Strategy::Empty);
    }

    #[test]
    fn symlinks_pick_symlink() {
        let r = row(0, FileTypeTag::Symlink);
        assert_eq!(pick(&r, &ctx(ServerSideCopy::Off, false)), Strategy::Symlink);
    }

    #[test]
    fn dirs_pick_dir_attrs() {
        let r = row(0, FileTypeTag::Dir);
        assert_eq!(pick(&r, &ctx(ServerSideCopy::Off, false)), Strategy::DirAttrs);
    }

    #[test]
    fn regular_files_pick_libnfs() {
        let r = row(1 << 20, FileTypeTag::Regular);
        assert_eq!(
            pick(&r, &ctx(ServerSideCopy::Auto, true)),
            Strategy::LibnfsIoUring,
            "auto + same_server_v42=true must still pick LibnfsIoUring under NFSv3 baseline",
        );
    }
}
