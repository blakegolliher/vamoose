//! Walker / rewrite invocation helpers. The implementation moved to
//! [`migration_core::prepare_tools`] so `mongoose` (the single-host
//! mover) can drive the same external stages; this module keeps the
//! `tools::` names the prepare command and `doctor` already use.

pub(crate) use migration_core::prepare_tools::{
    check_walker_flags, find_sibling, find_walker, resolve_scan_dir, scan_url, spawn_stage,
    walker_lock, walker_version, RewriteInvocation, WalkerInvocation, PACKAGED_WALKER,
    REWRITE_SOURCE_ROOT,
};
