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

    pub async fn acquire(&self) -> BufferLease {
        todo!("await a free buffer; return a lease that returns it on drop")
    }
}

pub struct BufferLease {
    // Buffer index into the registered pool, used directly by io_uring
    // SQEs as `buf_index` rather than as a userspace pointer.
    pub index: u32,
    pub size: usize,
    // TODO: pointer back to pool for return-on-drop.
}
