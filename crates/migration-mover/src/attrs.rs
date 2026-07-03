//! POSIX attribute application post-data.
//!
//! After data is written to the destination, attributes are applied
//! once per file to avoid extra round-trips. Order matters:
//!
//! 1. `WRITE` last data block (in mover proper, not here).
//! 2. Per-attribute sequence: `chown` → `chmod` → `utimes` (order
//!    planned by `attr_plan::plan_attr_ops` — owner before mode so
//!    NFSv3 kill-priv semantics can't strip S_ISUID/S_ISGID; F08).
//!    Each is a distinct libnfs call. **There is no NFSv4
//!    batched-SETATTR fast path in this build.** The system targets
//!    NFSv3 as the protocol baseline (BUGFIX_PLAN.md "Fix 4"), and
//!    the per-attribute sequence is the only path. If NFSv4 batching
//!    is revived later it would short-circuit this sequence; until
//!    then there is one code path and `mover.rs::Mover::apply_attrs`
//!    is the implementer.
//! 3. `SETXATTR` per name/value pair if `xattr_blob` is non-null.
//!    **Currently dead code** — walker doesn't emit xattrs yet. The
//!    apply path is wired up so it lights up automatically.
//!
//! `utimes` happens last so a successful copy is always reflected
//! in the dest file's mtime matching the source.

use migration_core::shard::RowView;

#[derive(Debug, Clone)]
pub struct AttrSet {
    pub mode: Option<u32>,
    pub uid: Option<u32>,
    pub gid: Option<u32>,
    pub mtime: Option<(i64, i32)>,
    pub atime: Option<(i64, i32)>,
    pub xattrs: Vec<(Vec<u8>, Vec<u8>)>,
}

#[derive(Debug, Clone, Copy)]
pub struct AttrPolicy {
    pub preserve_mode: bool,
    pub preserve_owner: bool,
    pub preserve_times: bool,
    pub preserve_xattr: bool,
}

impl AttrPolicy {
    pub fn from_options(opts: &migration_core::records::MigrationOptions) -> Self {
        Self {
            preserve_mode: opts.preserve_mode,
            preserve_owner: opts.preserve_owner,
            preserve_times: opts.preserve_times,
            preserve_xattr: opts.preserve_xattr,
        }
    }
}

/// Build the `AttrSet` we'll apply to a destination file from the
/// row's recorded attributes and the policy.
pub fn build(row: &RowView, policy: AttrPolicy) -> AttrSet {
    AttrSet {
        mode: policy.preserve_mode.then_some(row.mode),
        uid: policy.preserve_owner.then_some(row.uid).flatten(),
        gid: policy.preserve_owner.then_some(row.gid).flatten(),
        mtime: policy
            .preserve_times
            .then_some(match (row.mtime_sec, row.mtime_nsec) {
                (Some(s), Some(ns)) => Some((s, ns)),
                (Some(s), None) => Some((s, 0)),
                _ => None,
            })
            .flatten(),
        atime: policy
            .preserve_times
            .then_some(match (row.atime_sec, row.atime_nsec) {
                (Some(s), Some(ns)) => Some((s, ns)),
                (Some(s), None) => Some((s, 0)),
                _ => None,
            })
            .flatten(),
        xattrs: if policy.preserve_xattr {
            parse_xattr_blob(row.xattr_blob.as_deref())
        } else {
            Vec::new()
        },
    }
}

/// Parse the on-disk xattr blob format. **Stub** — walker doesn't emit
/// these yet, so this always returns empty in v1. When walker support
/// lands, the format must match what `nfs-walker` writes.
///
/// Proposed format (length-prefixed, big-endian):
///
/// ```text
/// repeat:
///   u32 name_len
///   <name_len> bytes name
///   u32 value_len
///   <value_len> bytes value
/// ```
pub fn parse_xattr_blob(blob: Option<&[u8]>) -> Vec<(Vec<u8>, Vec<u8>)> {
    let Some(_blob) = blob else { return Vec::new() };
    // TODO: implement once format is finalized in migration-core.
    // Until walker emits xattrs, this will never be called with Some.
    Vec::new()
}
