//! Parquet index schema.
//!
//! This module is the single source of truth for the column names,
//! types, and ordering the mover expects to find in an index parquet
//! shard. The walker (`nfs-walker`) writes shards conforming to this
//! schema; the mover reads them.
//!
//! Authoritative spec lives in `SCHEMA_CONTRACT.md` at the workspace
//! root — vendored byte-identical in `nfs-walker`. When code disagrees
//! with the contract, the contract wins.

use arrow::datatypes::{DataType, Field, Schema};
use std::sync::Arc;

// =============================================================================
// Versioning. Bumped when SCHEMA_CONTRACT.md bumps.
// =============================================================================

/// Schema format version. Walker stamps `migration.format_version`
/// into the parquet KV footer; mover refuses any other value.
pub const FORMAT_VERSION: u32 = 1;

/// Operational contract version. Mismatch is a WARN, not an error —
/// the parquet schema can be unchanged while the contract document
/// gains a clarification.
pub const CONTRACT_VERSION: u32 = 1;

// =============================================================================
// Parquet KV footer key constants. See SCHEMA_CONTRACT.md
// "Parquet file metadata (KV footer)".
// =============================================================================

pub const KV_FORMAT_VERSION: &str = "migration.format_version";
pub const KV_CONTRACT_VERSION: &str = "migration.contract_version";
pub const KV_SHARD_INDEX: &str = "migration.shard_index";
pub const KV_WALKER_VERSION: &str = "migration.walker_version";
pub const KV_ROW_COUNT: &str = "migration.row_count";

// =============================================================================
// Column names — use these constants everywhere, never literal strings.
// =============================================================================

pub const COL_ROW_ID: &str = "row_id";
pub const COL_PATH: &str = "path";
pub const COL_SIZE: &str = "size";
pub const COL_MTIME_SEC: &str = "mtime_sec";
pub const COL_MTIME_NSEC: &str = "mtime_nsec";
pub const COL_ATIME_SEC: &str = "atime_sec";
pub const COL_ATIME_NSEC: &str = "atime_nsec";
pub const COL_MODE: &str = "mode";
pub const COL_UID: &str = "uid";
pub const COL_GID: &str = "gid";
pub const COL_NLINK: &str = "nlink";
pub const COL_INODE: &str = "inode";
pub const COL_FSID: &str = "fsid";
pub const COL_XATTR_BLOB: &str = "xattr_blob";
pub const COL_SYMLINK_TARGET: &str = "symlink_target";
pub const COL_FILE_TYPE: &str = "file_type";

/// Columns the mover *requires* to function. If any are missing from a
/// shard's actual schema, the mover refuses the shard with
/// `Error::MissingColumn`.
pub const REQUIRED_COLUMNS: &[&str] = &[COL_ROW_ID, COL_PATH, COL_SIZE, COL_MODE, COL_FILE_TYPE];

// =============================================================================
// File-type tag values — must match what the walker emits.
// =============================================================================
//
// These mirror POSIX d_type / S_IFMT values but as a small enum so the
// parquet column is a UINT8 rather than the full mode bits. The mover
// uses these to fast-skip non-data entries (sockets, fifos) without
// looking at `mode`.

#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FileTypeTag {
    Unknown = 0,
    Regular = 1,
    Dir = 2,
    Symlink = 3,
    Fifo = 4,
    Socket = 5,
    BlockDev = 6,
    CharDev = 7,
}

impl FileTypeTag {
    pub fn from_u8(v: u8) -> Self {
        match v {
            1 => Self::Regular,
            2 => Self::Dir,
            3 => Self::Symlink,
            4 => Self::Fifo,
            5 => Self::Socket,
            6 => Self::BlockDev,
            7 => Self::CharDev,
            _ => Self::Unknown,
        }
    }

    /// True if this entry has data the mover should copy.
    pub fn has_data(self) -> bool {
        matches!(self, Self::Regular)
    }
}

// =============================================================================
// Arrow schema constructor — what we expect a shard to look like.
// =============================================================================

/// Construct the canonical Arrow schema. Any shard's actual schema must
/// be a superset of `REQUIRED_COLUMNS` with matching types; extra
/// columns are allowed and ignored.
pub fn canonical_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new(COL_ROW_ID, DataType::UInt64, false),
        // Raw bytes — POSIX paths are not guaranteed UTF-8.
        Field::new(COL_PATH, DataType::Binary, false),
        Field::new(COL_SIZE, DataType::UInt64, false),
        Field::new(COL_MTIME_SEC, DataType::Int64, true),
        Field::new(COL_MTIME_NSEC, DataType::Int32, true),
        Field::new(COL_ATIME_SEC, DataType::Int64, true),
        Field::new(COL_ATIME_NSEC, DataType::Int32, true),
        Field::new(COL_MODE, DataType::UInt32, false),
        Field::new(COL_UID, DataType::UInt32, true),
        Field::new(COL_GID, DataType::UInt32, true),
        Field::new(COL_NLINK, DataType::UInt32, true),
        Field::new(COL_INODE, DataType::UInt64, true),
        // Source filesystem identifier; combined with `inode` to
        // disambiguate hardlinks across underlying filesystems within
        // an export. Nullable per contract; mover falls back to
        // grouping by inode alone with a one-time WARN.
        Field::new(COL_FSID, DataType::UInt64, true),
        // Reserved for future xattr support; NULL until walker emits it.
        Field::new(COL_XATTR_BLOB, DataType::Binary, true),
        Field::new(COL_SYMLINK_TARGET, DataType::Binary, true),
        Field::new(COL_FILE_TYPE, DataType::UInt8, false),
    ]))
}

/// Compose a `row_id` from `(shard_index, row_offset_within_shard)`.
///
/// Shard index occupies the high 24 bits, row offset the low 40. This
/// gives us up to 16M shards × ~1T rows/shard. Materialized at write
/// time by the walker — never derived at read time, because predicate
/// pushdown can reorder rows within a row group.
pub fn make_row_id(shard_index: u32, row_in_shard: u64) -> u64 {
    debug_assert!(shard_index < (1 << 24), "shard index overflow");
    debug_assert!(row_in_shard < (1 << 40), "row offset overflow");
    ((shard_index as u64) << 40) | (row_in_shard & ((1 << 40) - 1))
}

pub fn split_row_id(row_id: u64) -> (u32, u64) {
    let shard_index = (row_id >> 40) as u32;
    let row_in_shard = row_id & ((1 << 40) - 1);
    (shard_index, row_in_shard)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn row_id_round_trip() {
        let cases = [
            (0u32, 0u64),
            (1, 1),
            (42, 1_000_000),
            ((1 << 24) - 1, (1 << 40) - 1),
        ];
        for (shard, row) in cases {
            let id = make_row_id(shard, row);
            assert_eq!(split_row_id(id), (shard, row));
        }
    }
}
