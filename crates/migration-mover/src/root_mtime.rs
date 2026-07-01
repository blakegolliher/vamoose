//! End-of-run helper that propagates the source-root directory's
//! atime/mtime onto the dest-root. The migration root sits above
//! every walker-emitted row, so the per-shard `Strategy::DirAttrs`
//! path never touches it — yet every file commit, mkdir, and rename
//! inside the dest tree bumps the dest root's mtime.
//!
//! Call once, after the orchestrator's main shard loop reports
//! `all_terminal` and before shutdown. Multi-worker safe by
//! construction: every worker writes the same source-derived value,
//! so the last writer wins with the same result.
//!
//! Best-effort. A failure is logged and discarded — the bytes are
//! durable, the mtime drift is cosmetic, and we don't want a stat
//! glitch on the source root to wedge worker shutdown.

use crate::libnfs::{ops, LibnfsContextPool};
use std::sync::Arc;

pub async fn restore_root_mtime(
    pool: Arc<dyn LibnfsContextPool>,
    src_root: &[u8],
    dst_root: &[u8],
) -> anyhow::Result<()> {
    let src_root = src_root.to_vec();
    let dst_root = dst_root.to_vec();

    let mut pair = pool.acquire().await?;
    // libnfs calls are blocking — move off the runtime so we don't
    // stall any other shutdown-time tasks.
    tokio::task::spawn_blocking(move || -> anyhow::Result<()> {
        let (at_s, at_n, mt_s, mt_n) = ops::stat_times(pair.src(), &src_root).map_err(|e| {
            anyhow::anyhow!(
                "stat source root {:?}: phase={:?} err={}",
                String::from_utf8_lossy(&src_root),
                e.phase,
                e.error,
            )
        })?;
        ops::utimes(pair.dst(), &dst_root, at_s, at_n, mt_s, mt_n).map_err(|e| {
            anyhow::anyhow!(
                "utimes dest root {:?}: phase={:?} err={}",
                String::from_utf8_lossy(&dst_root),
                e.phase,
                e.error,
            )
        })?;
        Ok(())
    })
    .await??;
    Ok(())
}
