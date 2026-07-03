//! io_uring helpers for the mover.
//!
//! libnfs handles the NFS protocol; io_uring handles everything else:
//!
//! - **Fixed-buffer pool** — pre-allocated and registered with the
//!   ring once at startup, handed out to libnfs READ/WRITE without
//!   per-op allocation.
//! - **Batched submission** of attribute syscalls (`fchownat`,
//!   `fchmodat`, `utimensat`, `setxattr`) at end-of-file.
//! - **Local tmpfs operations** for the mmap'd parquet shard.
//!
//! Requires Linux 5.10+ for io_uring; 5.15+ recommended for fixed
//! buffer registration.

use std::sync::Arc;

#[derive(Debug, Clone, Copy)]
pub struct UringConfig {
    pub queue_depth: u32,
    pub fixed_buffer_count: u32,
    pub fixed_buffer_size: usize,
}

impl Default for UringConfig {
    fn default() -> Self {
        Self {
            queue_depth: 256,
            fixed_buffer_count: 256,
            fixed_buffer_size: 1024 * 1024,
        }
    }
}

/// Pre-registered fixed buffer pool. Buffers are recycled across copy
/// operations to avoid per-op allocation churn — critical for the
/// small-file IOPS-bound case where allocation cost would dominate.
pub struct FixedBufferPool {
    cfg: UringConfig,
    // TODO: real pool with free-list + arc handles.
    _private: (),
}

impl FixedBufferPool {
    pub fn new(cfg: UringConfig) -> Arc<Self> {
        Arc::new(Self { cfg, _private: () })
    }

    pub fn config(&self) -> &UringConfig {
        &self.cfg
    }

    /// Acquire a fixed buffer from the pool.
    ///
    /// Not implemented yet (M3.5). This returns `Err` rather than
    /// `todo!()` because under the release profile's `panic = "abort"`
    /// a `todo!()` here would kill the whole worker process the moment
    /// any future code path reached it (F35). The signature keeps the
    /// M3.5 shape; the real implementation will await a free buffer
    /// and return a lease that recycles it on drop.
    pub async fn acquire(&self) -> Result<BufferLease, UringError> {
        Err(UringError::Unimplemented)
    }
}

/// Errors from the io_uring fixed-buffer machinery.
#[derive(Debug, thiserror::Error)]
pub enum UringError {
    /// The fixed-buffer pool is an M3.5 stub — nothing hands out
    /// buffers yet. Callers should stay on the default 1 MiB buffered
    /// copy path.
    #[error(
        "io_uring fixed-buffer pool is not implemented yet (M3.5); \
         use the default buffered copy path"
    )]
    Unimplemented,
}

#[derive(Debug)]
pub struct BufferLease {
    // Buffer index into the registered pool, used directly by io_uring
    // SQEs as `buf_index` rather than as a userspace pointer.
    pub index: u32,
    pub size: usize,
    // TODO: pointer back to pool for return-on-drop.
}

#[cfg(test)]
mod tests {
    use super::*;

    /// F35: `acquire` was `todo!()` — under the release profile's
    /// `panic = "abort"` any future caller reaching it would kill the
    /// whole worker process mid-migration. The M3.5 stub must be a
    /// plain `Err` the caller can handle, not a panic.
    #[tokio::test]
    async fn acquire_returns_unimplemented_error() {
        let pool = FixedBufferPool::new(UringConfig::default());
        let err = pool
            .acquire()
            .await
            .expect_err("M3.5 stub must return Err, never panic/abort");
        let msg = err.to_string();
        assert!(
            msg.contains("M3.5") && msg.contains("not implemented"),
            "error must clearly say the pool is an M3.5 stub, got: {msg}",
        );
    }
}
