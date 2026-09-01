//! Building the configured libnfs file mover.
//!
//! Extracted from the orchestrator so the single-host `mongoose` CLI
//! and the distributed worker construct the mover identically: the
//! same pool sizing rules (bucketed-async caps the sync pool at 16
//! pairs because every context pair costs two reserved ports), the
//! same `MoverConfig` projection, and the same sync-vs-bucketed-async
//! wiring behind the [`FileMover`] trait.

use migration_core::fence::Fence;
use migration_core::records::MigrationOptions;
use migration_mover::batch::InflightProfile;
use migration_mover::{
    AsyncBucketedFileMover, BucketedAsyncPool, DowngradeSink, FileMover, LibnfsContextPool, Mover,
    MoverConfig, MultiPool,
};
use std::sync::Arc;

/// Everything that shapes the mover, resolved by the caller (worker
/// config + manifest, or mongoose CLI + local manifest).
#[derive(Debug, Clone)]
pub struct MoverParams {
    pub source_url: String,
    pub dest_url: String,
    pub source_root: String,
    pub dest_root: String,
    pub options: MigrationOptions,
    /// Requested libnfs context pairs (`[mover] nfs_connections`).
    /// Clamped to at least 1; additionally capped at 16 for the sync
    /// pool when `use_bucketed_pool` is set (the async pool needs the
    /// reserved-port headroom).
    pub nfs_connections: usize,
    pub use_bucketed_pool: bool,
    pub use_raw_fh: bool,
    pub direct_commit: bool,
    /// Per-RPC timeout in ms; 0 = leave the libnfs default untouched.
    pub rpc_timeout_ms: u32,
    /// Whether chown EPERM is a per-file failure (true) or a recorded
    /// degradation (false). Callers pass `require_chown_capability &&
    /// has_cap_chown` (worker) or `has_cap_chown` (mongoose).
    pub require_chown: bool,
    pub require_unchanged_size: bool,
    pub inflight: InflightProfile,
    pub host_id: String,
}

/// The built mover plus the sync context pool, which callers keep for
/// the end-of-run root-mtime restore.
pub struct BuiltMover {
    pub mover: Arc<dyn FileMover>,
    pub pool: Arc<dyn LibnfsContextPool>,
}

/// Project [`MoverParams`] onto a [`MoverConfig`]. Pure; split out so
/// the projection is unit-testable without mounting libnfs.
pub fn mover_config(p: &MoverParams) -> MoverConfig {
    let mut cfg = MoverConfig::from_options(
        p.source_url.clone(),
        p.dest_url.clone(),
        p.source_root.clone(),
        p.dest_root.clone(),
        &p.options,
    );
    cfg.require_chown = p.require_chown;
    cfg.require_unchanged_size = p.require_unchanged_size;
    cfg.use_raw_fh = p.use_raw_fh;
    cfg.direct_commit = p.direct_commit;
    cfg.rpc_timeout_ms = p.rpc_timeout_ms;
    cfg.inflight = p.inflight;
    cfg
}

/// Size of the sync `MultiPool` for a requested pair count. In
/// bucketed-async mode the sync pool only serves fallback rows
/// (symlink/hardlink/dir/empty), so it is capped — every context pair
/// costs two reserved ports (libnfs as root binds ports < 1024; ~111
/// pairs is the observed per-host ceiling) and the async pool needs
/// that headroom for its small-bucket pairs.
pub fn sync_pool_size(nfs_connections: usize, use_bucketed_pool: bool) -> usize {
    let requested = nfs_connections.max(1);
    if use_bucketed_pool {
        requested.min(16)
    } else {
        requested
    }
}

/// Mount the libnfs pools and build the configured [`FileMover`].
///
/// The caller owns the `DowngradeSink` and `Fence` so it can share
/// them with the shard processor and drain the sink per shard.
pub async fn build(
    params: &MoverParams,
    downgrades: DowngradeSink,
    fence: Fence,
) -> anyhow::Result<BuiltMover> {
    let requested = params.nfs_connections.max(1);
    migration_core::latency::set_pairs(requested as u32);
    let pool_size = sync_pool_size(params.nfs_connections, params.use_bucketed_pool);
    let pool: Arc<dyn LibnfsContextPool> = MultiPool::build(
        &params.source_url,
        &params.dest_url,
        pool_size,
        // F12: explicit per-RPC timeout at every context creation;
        // 0 = leave the libnfs default untouched.
        params.rpc_timeout_ms,
    )?;
    tracing::info!(
        pool_size,
        src = %params.source_url,
        dst = %params.dest_url,
        "libnfs pool mounted",
    );

    let cfg = mover_config(params);
    let mover: Arc<dyn FileMover> = if params.use_bucketed_pool {
        let async_pool = Arc::new(
            BucketedAsyncPool::new(
                &params.source_url,
                &params.dest_url,
                cfg.rpc_timeout_ms,
                requested,
            )
            .await?,
        );
        tracing::info!(
            src = %params.source_url,
            dst = %params.dest_url,
            small_pairs = async_pool.small_pairs(),
            "bucketed async libnfs pool mounted",
        );
        let sync_mover = Mover::new(
            cfg.clone(),
            Arc::clone(&pool),
            params.host_id.clone(),
            downgrades.clone(),
            fence.clone(),
        );
        Arc::new(AsyncBucketedFileMover::new(
            async_pool,
            sync_mover,
            Arc::new(cfg),
            fence,
            params.host_id.clone(),
            downgrades,
        ))
    } else {
        Arc::new(Mover::new(
            cfg,
            Arc::clone(&pool),
            params.host_id.clone(),
            downgrades,
            fence,
        ))
    };
    Ok(BuiltMover { mover, pool })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn params() -> MoverParams {
        MoverParams {
            source_url: "nfs://src/export".into(),
            dest_url: "nfs://dst/export".into(),
            source_root: "/data".into(),
            dest_root: "/copy".into(),
            options: MigrationOptions::default(),
            nfs_connections: 32,
            use_bucketed_pool: false,
            use_raw_fh: true,
            direct_commit: false,
            rpc_timeout_ms: 45_000,
            require_chown: false,
            require_unchanged_size: true,
            inflight: InflightProfile {
                small: 128,
                medium: 8,
                large: 2,
                large_stripe_size: 4 * 1024 * 1024,
                large_stripe_depth: 32,
            },
            host_id: "host-t".into(),
        }
    }

    #[test]
    fn mover_config_projects_every_knob() {
        let p = params();
        let cfg = mover_config(&p);
        assert_eq!(cfg.source_url, "nfs://src/export");
        assert_eq!(cfg.dest_url, "nfs://dst/export");
        assert_eq!(cfg.source_root, "/data");
        assert_eq!(cfg.dest_root, "/copy");
        assert!(!cfg.require_chown);
        assert!(cfg.require_unchanged_size);
        assert!(cfg.use_raw_fh);
        assert!(!cfg.direct_commit);
        assert_eq!(cfg.rpc_timeout_ms, 45_000);
        assert_eq!(cfg.inflight.small, 128);
        assert_eq!(cfg.inflight.medium, 8);
        assert_eq!(cfg.inflight.large, 2);
    }

    #[test]
    fn sync_pool_is_capped_only_in_bucketed_mode() {
        assert_eq!(sync_pool_size(32, false), 32);
        assert_eq!(sync_pool_size(32, true), 16);
        assert_eq!(sync_pool_size(8, true), 8);
        assert_eq!(sync_pool_size(0, false), 1, "clamped to at least one pair");
        assert_eq!(sync_pool_size(0, true), 1);
    }
}
