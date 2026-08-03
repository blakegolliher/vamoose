//! migration-mover
//!
//! The data plane. Given a `RowView` from a parquet shard, the mover
//! decides which strategy applies and copies the file from source to
//! destination, preserving POSIX attributes.
//!
//! Regular files have two implemented libnfs paths:
//!
//! 1. **Sync libnfs READ → WRITE** through [`MultiPool`], run on Tokio's
//!    blocking pool.
//! 2. **Bucketed async libnfs** through [`AsyncBucketedFileMover`] and the
//!    pipelined copy implementation, enabled explicitly by worker config or
//!    CLI override.
//!
//! Symlinks, hardlinks, empty files, directory attributes, and skipped rows
//! retain their dedicated paths. NFSv4.2 server-side COPY, kernel
//! `copy_file_range`, and true io_uring integration are not implemented.
//! [`strategy::Strategy::LibnfsIoUring`] remains the compatibility name used
//! in mover outcomes for regular-file libnfs copies.

pub mod attr_plan;
pub mod attrs;
pub mod batch;
pub mod bucketed_pool;
pub mod downgrade;
pub mod error;
pub mod failure;
pub mod file_mover;
pub mod libnfs;
mod mover;
pub mod paths;
pub mod pipelined_copy;
pub mod reorder;
pub mod root_mtime;
pub mod strategy;
pub use bucketed_pool::{
    bucket_for_size, AsyncNfsContextPair, BucketConfig, BucketedAsyncPool, BUCKETS,
};
pub use downgrade::DowngradeSink;
pub use error::MoveError;
pub use failure::FailureSink;
pub use file_mover::{AsyncBucketedFileMover, FileMover};
pub use libnfs::{
    ContextPair, LibnfsContextPool, MultiPool, NfsContext, SimplePool, DEFAULT_RPC_TIMEOUT_MS,
};
pub use mover::{MoveOutcome, Mover, MoverConfig};
pub use paths::join_root;
pub use pipelined_copy::{pipelined_copy, FileCopyResult};
pub use root_mtime::restore_root_mtime;
