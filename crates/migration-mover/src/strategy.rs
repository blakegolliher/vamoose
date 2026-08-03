//! Per-file strategy selection.
//!
//! Decided per-file from the row kind, size, and hardlink state. Regular
//! non-empty files use the libnfs data path; special rows use their existing
//! metadata-only or skip paths.

use migration_core::schema::FileTypeTag;
use migration_core::shard::RowView;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Strategy {
    /// Regular-file libnfs READ/WRITE.
    ///
    /// The historical variant name is retained because it appears in mover
    /// outcomes and debug logging. The active sync and bucketed async paths do
    /// not use io_uring fixed buffers.
    LibnfsIoUring,
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

    if row.inode.is_some() && ctx.already_copied_inode {
        return Strategy::HardlinkExisting;
    }

    if row.size == 0 {
        return Strategy::Empty;
    }

    Strategy::LibnfsIoUring
}

#[cfg(test)]
mod tests {
    use super::*;
    use migration_core::schema::FileTypeTag;

    fn ctx(already_copied_inode: bool) -> StrategyContext {
        StrategyContext {
            already_copied_inode,
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

    #[test]
    fn empty_files_pick_empty() {
        let r = row(0, FileTypeTag::Regular);
        assert_eq!(pick(&r, &ctx(false)), Strategy::Empty);
    }

    #[test]
    fn symlinks_pick_symlink() {
        let r = row(0, FileTypeTag::Symlink);
        assert_eq!(pick(&r, &ctx(false)), Strategy::Symlink);
    }

    #[test]
    fn dirs_pick_dir_attrs() {
        let r = row(0, FileTypeTag::Dir);
        assert_eq!(pick(&r, &ctx(false)), Strategy::DirAttrs);
    }

    #[test]
    fn regular_files_pick_libnfs() {
        let r = row(1 << 20, FileTypeTag::Regular);
        assert_eq!(pick(&r, &ctx(false)), Strategy::LibnfsIoUring);
    }

    #[test]
    fn regular_strategy_keeps_compatibility_debug_label() {
        assert_eq!(format!("{:?}", Strategy::LibnfsIoUring), "LibnfsIoUring");
    }

    #[test]
    fn copied_inodes_pick_hardlink() {
        let mut r = row(1 << 20, FileTypeTag::Regular);
        r.inode = Some(42);
        assert_eq!(pick(&r, &ctx(true)), Strategy::HardlinkExisting);
        assert_eq!(pick(&r, &ctx(false)), Strategy::LibnfsIoUring);
    }

    #[test]
    fn non_data_rows_pick_skip() {
        for file_type in [
            FileTypeTag::Unknown,
            FileTypeTag::Fifo,
            FileTypeTag::Socket,
            FileTypeTag::BlockDev,
            FileTypeTag::CharDev,
        ] {
            let r = row(4096, file_type);
            assert_eq!(pick(&r, &ctx(false)), Strategy::Skip);
        }
    }
}
