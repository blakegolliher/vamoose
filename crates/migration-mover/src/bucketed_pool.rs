//! Bucketed async libnfs pool for the opt-in regular-file mover.
//!
//! Builds three `AsyncNfsContext` pairs — one per file-size bucket —
//! with per-bucket `rsize` / `wsize` tuned to the bucket's workload.
//! See `docs/work-items/MULTI_PASS_MOVER.md` "Bucketed async libnfs
//! pool" for the design and `LIBNFS_ASYNC_FORK.md` (closed 2026-05-18)
//! for the async-FFI surface the pool composes.
//!
//! ## What lives here
//!
//! - [`BucketConfig`] — the per-bucket tuning record.
//! - [`BUCKETS`] — the three statically-known buckets (small / medium
//!   / large).
//! - [`AsyncNfsContextPair`] — one (src, dst) pair of mounted contexts.
//! - [`BucketedAsyncPool`] — owns all three pairs; routes
//!   [`BucketedAsyncPool::pair_for_size`] selection by file size.
//!
//! ## What does NOT live here
//!
//! - Per-file pipelined copy: `pipelined_copy.rs`.
//! - Inflight concurrency budgets across files: `batch.rs::InflightLimiter`.
//! - Mover integration and sync fallback: `file_mover.rs`, selected by
//!   `[mover] use_bucketed_pool` or `--use-bucketed-pool`.
//!
//! ## Deltas vs the work-item sketch
//!
//! The shipped `MountOpts` rejects `nconnect > 1` (linked libnfs v6
//! lacks support) and has no `readahead` knob. The original sketch's
//! `BucketConfig::nconnect` / `libnfs_readahead` fields are therefore
//! gone; the pool unconditionally passes `nconnect: 1, version: 3` to
//! `AsyncNfsContext::mount`. `pair_for_size` returns the bucket
//! config by value (`BucketConfig: Copy`) rather than by `&'static`
//! reference to keep call-site lifetimes uncluttered; the sketch's
//! signature is otherwise preserved.

use crate::libnfs::asyncio::{AsyncNfsContext, MountOpts, NfsError};

const KIB_U32: u32 = 1024;
const MIB_U32: u32 = 1024 * 1024;
const MIB_U64: u64 = 1024 * 1024;
const GIB_U64: u64 = 1024 * 1024 * 1024;

/// Per-bucket tuning. Selected by file size; all sizes are inclusive.
///
/// Rationale (kept in sync with `MULTI_PASS_MOVER.md`):
///
/// - **Large** (≥1 GiB): throughput-bound. 32 concurrent 4 MiB RPCs
///   = 128 MiB streaming window over the single TCP connection
///   `AsyncNfsContext` gives us.
/// - **Medium** (1 MiB – 1 GiB): balanced. 16 MiB window covers
///   small-to-medium files in one batch and pipelines on larger ones.
/// - **Small** (<1 MiB): latency-bound. Per-file depth of 2 is
///   defensive (256 KiB file = 2 RPCs total). Parallelism comes from
///   many concurrent *files*, not pipelining inside one — scale by
///   raising the per-class `InflightLimiter` budget.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BucketConfig {
    pub name: &'static str,
    pub min_size: u64,
    pub max_size: u64,
    pub rsize: u32,
    pub wsize: u32,
    pub read_pipeline_depth: u32,
    pub write_pipeline_depth: u32,
}

/// The three statically-defined buckets. Ordered large → medium →
/// small purely so visual inspection matches "thickest pipe first";
/// [`bucket_for_size`] does not depend on the order — it picks by
/// `(min_size, max_size)` containment.
pub const BUCKETS: [BucketConfig; 3] = [
    BucketConfig {
        name: "large",
        min_size: GIB_U64,
        max_size: u64::MAX,
        rsize: 4 * MIB_U32,
        wsize: 4 * MIB_U32,
        read_pipeline_depth: 32,
        write_pipeline_depth: 32,
    },
    BucketConfig {
        name: "medium",
        min_size: MIB_U64,
        max_size: GIB_U64 - 1,
        rsize: 2 * MIB_U32,
        wsize: 2 * MIB_U32,
        read_pipeline_depth: 8,
        write_pipeline_depth: 8,
    },
    BucketConfig {
        name: "small",
        min_size: 0,
        max_size: MIB_U64 - 1,
        rsize: 128 * KIB_U32,
        wsize: 128 * KIB_U32,
        read_pipeline_depth: 2,
        write_pipeline_depth: 2,
    },
];

/// One (src, dst) pair of mounted async contexts. `AsyncNfsContext`
/// is `Clone` (it's an `Arc<Inner>` internally) so cloning the pair
/// is a refcount bump on each side; no contexts are duplicated.
#[derive(Clone, Debug)]
pub struct AsyncNfsContextPair {
    pub src: AsyncNfsContext,
    pub dst: AsyncNfsContext,
}

/// Three (src, dst) async-NFS pairs, one per bucket. Each worker
/// process owns exactly one of these; the pool is not shared across
/// workers (consistent with the sync `LibnfsContextPool` model).
#[derive(Debug)]
pub struct BucketedAsyncPool {
    large: AsyncNfsContextPair,
    medium: AsyncNfsContextPair,
    /// N pairs, checked out round-robin. One pair per bucket was the
    /// original design; on small-file trees a single connection pair
    /// becomes the whole worker's throughput (every in-flight file
    /// multiplexes one socket and serializes on its context lock —
    /// measured ~350 files/s regardless of `inflight_small`). Small
    /// files are RPC-latency-bound, so they scale with connection
    /// count; large/medium are bandwidth-bound and keep one pair.
    small: Vec<AsyncNfsContextPair>,
    small_rr: std::sync::atomic::AtomicUsize,
}

impl BucketedAsyncPool {
    /// Mount `2 + 2 + 2 * small_pairs` contexts. Returns once every
    /// mount has completed.
    ///
    /// `rpc_timeout_ms` (F12): per-RPC timeout applied to all
    /// contexts at creation; `0` = leave the libnfs default. See
    /// [`crate::libnfs::DEFAULT_RPC_TIMEOUT_MS`].
    ///
    /// On any single mount failure the partial result drops and the
    /// already-mounted contexts wind down through `AsyncNfsContext`'s
    /// own Drop (which signals the service task to shut down). No
    /// extra cleanup is needed at this layer.
    pub async fn new(
        src_url: &str,
        dst_url: &str,
        rpc_timeout_ms: u32,
        small_pairs: usize,
    ) -> Result<Self, NfsError> {
        let small_pairs = small_pairs.max(1);
        let (large, medium) = tokio::try_join!(
            mount_pair(src_url, dst_url, BUCKETS[0], rpc_timeout_ms),
            mount_pair(src_url, dst_url, BUCKETS[1], rpc_timeout_ms),
        )?;
        let small = futures::future::try_join_all(
            (0..small_pairs).map(|_| mount_pair(src_url, dst_url, BUCKETS[2], rpc_timeout_ms)),
        )
        .await?;
        Ok(Self {
            large,
            medium,
            small,
            small_rr: std::sync::atomic::AtomicUsize::new(0),
        })
    }

    /// Number of small-bucket pairs mounted (for startup logging).
    pub fn small_pairs(&self) -> usize {
        self.small.len()
    }

    /// Return the (src ctx, dst ctx, bucket config) for the bucket
    /// whose `[min_size, max_size]` contains `size`. `size: u64`
    /// always matches exactly one bucket given the small bucket has
    /// `min_size = 0` and the large bucket has `max_size = u64::MAX`.
    pub fn pair_for_size(&self, size: u64) -> (&AsyncNfsContext, &AsyncNfsContext, BucketConfig) {
        let cfg = bucket_for_size(size);
        let pair = self.pair_for_bucket(cfg);
        (&pair.src, &pair.dst, cfg)
    }

    /// Borrow the (src, dst) pair for a given bucket name. Mostly for
    /// tests and observability; production code goes through
    /// [`Self::pair_for_size`].
    pub fn pair_by_name(&self, name: &str) -> Option<&AsyncNfsContextPair> {
        match name {
            "large" => Some(&self.large),
            "medium" => Some(&self.medium),
            "small" => self.small.first(),
            _ => None,
        }
    }

    fn pair_for_bucket(&self, cfg: BucketConfig) -> &AsyncNfsContextPair {
        match cfg.name {
            "large" => &self.large,
            "medium" => &self.medium,
            "small" => {
                let idx = self
                    .small_rr
                    .fetch_add(1, std::sync::atomic::Ordering::Relaxed)
                    % self.small.len();
                &self.small[idx]
            }
            other => unreachable!(
                "bucket_for_size returned unknown bucket name {other:?}; \
                 BUCKETS and pair_for_bucket are out of sync",
            ),
        }
    }
}

/// Pick the bucket whose `[min_size, max_size]` contains `size`. The
/// three buckets form a non-overlapping partition of `0..=u64::MAX`
/// (asserted by the `buckets_form_non_overlapping_partition` unit
/// test), so this returns exactly one match.
pub fn bucket_for_size(size: u64) -> BucketConfig {
    for b in BUCKETS.iter() {
        if size >= b.min_size && size <= b.max_size {
            return *b;
        }
    }
    unreachable!(
        "BUCKETS does not cover size={size}; the small bucket's \
         min_size must be 0 and the large bucket's max_size must be u64::MAX",
    )
}

async fn mount_pair(
    src_url: &str,
    dst_url: &str,
    cfg: BucketConfig,
    rpc_timeout_ms: u32,
) -> Result<AsyncNfsContextPair, NfsError> {
    let opts = MountOpts {
        rsize: cfg.rsize,
        wsize: cfg.wsize,
        nconnect: 1,
        version: 3,
        rpc_timeout_ms,
    };
    let (src, dst) = tokio::try_join!(
        AsyncNfsContext::mount(src_url, opts.clone()),
        AsyncNfsContext::mount(dst_url, opts),
    )?;
    Ok(AsyncNfsContextPair { src, dst })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn size_zero_routes_to_small() {
        assert_eq!(bucket_for_size(0).name, "small");
    }

    #[test]
    fn one_byte_below_one_mib_routes_to_small() {
        assert_eq!(bucket_for_size((1 << 20) - 1).name, "small");
    }

    #[test]
    fn exactly_one_mib_routes_to_medium() {
        assert_eq!(bucket_for_size(1 << 20).name, "medium");
    }

    #[test]
    fn one_byte_above_one_mib_routes_to_medium() {
        assert_eq!(bucket_for_size((1 << 20) + 1).name, "medium");
    }

    #[test]
    fn one_byte_below_one_gib_routes_to_medium() {
        assert_eq!(bucket_for_size((1 << 30) - 1).name, "medium");
    }

    #[test]
    fn exactly_one_gib_routes_to_large() {
        assert_eq!(bucket_for_size(1 << 30).name, "large");
    }

    #[test]
    fn one_byte_above_one_gib_routes_to_large() {
        assert_eq!(bucket_for_size((1 << 30) + 1).name, "large");
    }

    #[test]
    fn u64_max_routes_to_large() {
        assert_eq!(bucket_for_size(u64::MAX).name, "large");
    }

    #[test]
    fn buckets_form_non_overlapping_partition() {
        // Sort ascending by min_size and assert: small.min == 0,
        // large.max == u64::MAX, and every adjacent pair touches
        // (prev.max + 1 == next.min) with no gap and no overlap.
        let mut sorted: Vec<BucketConfig> = BUCKETS.to_vec();
        sorted.sort_by_key(|b| b.min_size);

        assert_eq!(
            sorted.first().expect("at least one bucket").min_size,
            0,
            "smallest bucket must cover size=0",
        );
        assert_eq!(
            sorted.last().expect("at least one bucket").max_size,
            u64::MAX,
            "largest bucket must cover size=u64::MAX",
        );
        for w in sorted.windows(2) {
            assert_eq!(
                w[0].max_size + 1,
                w[1].min_size,
                "gap or overlap between {} (max={}) and {} (min={})",
                w[0].name,
                w[0].max_size,
                w[1].name,
                w[1].min_size,
            );
            assert!(
                w[0].max_size < w[1].min_size,
                "buckets {} and {} overlap",
                w[0].name,
                w[1].name,
            );
        }
    }

    #[test]
    fn each_bucket_has_a_unique_name() {
        let mut names: Vec<&str> = BUCKETS.iter().map(|b| b.name).collect();
        names.sort();
        let original_len = names.len();
        names.dedup();
        assert_eq!(names.len(), original_len, "duplicate bucket name");
    }

    #[test]
    fn bucket_for_size_returns_full_config() {
        let cfg = bucket_for_size(2 * 1024 * 1024);
        assert_eq!(cfg.name, "medium");
        assert_eq!(cfg.rsize, 2 * 1024 * 1024);
        assert_eq!(cfg.wsize, 2 * 1024 * 1024);
        assert_eq!(cfg.read_pipeline_depth, 8);
        assert_eq!(cfg.write_pipeline_depth, 8);
    }

    #[test]
    fn rsize_wsize_within_libnfs_safe_range() {
        // libnfs/NFSv3 caps READ/WRITE at 1 MiB by default on many
        // servers; >1 MiB requires server cooperation. The large
        // bucket configures 4 MiB which matches the perf-smoke value
        // recorded in LIBNFS_ASYNC_FORK.md; bake a sanity floor here.
        for b in BUCKETS.iter() {
            assert!(
                b.rsize >= 4096,
                "bucket {} rsize={} too small",
                b.name,
                b.rsize
            );
            assert!(
                b.wsize >= 4096,
                "bucket {} wsize={} too small",
                b.name,
                b.wsize
            );
            assert!(b.read_pipeline_depth >= 1);
            assert!(b.write_pipeline_depth >= 1);
        }
    }
}
