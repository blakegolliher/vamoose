//! migration-mover
//!
//! The data plane. Given a `RowView` from a parquet shard, the mover
//! decides which strategy applies and copies the file from source to
//! destination, preserving POSIX attributes.
//!
//! Three strategies, picked per-file:
//!
//! 1. **NFSv4.2 server-side COPY** when source and destination are the
//!    same NFSv4.2 server. The mover does almost no data-plane work.
//!    *Wired up in M4.*
//! 2. **libnfs READ → libnfs WRITE driven by io_uring fixed buffers**
//!    (default). M2 implements the libnfs side single-threaded with a
//!    plain 1 MiB buffer; io_uring lands in M3.
//! 3. **Kernel `copy_file_range`** as an escape hatch for environments
//!    where libnfs can't be used. *Stubbed.*
//!
//! See DESIGN.md "Mover" and the M2 working spec.

pub mod attrs;
pub mod batch;
pub mod bucketed_pool;
pub mod downgrade;
pub mod error;
pub mod failure;
pub mod file_mover;
pub mod libnfs;
pub mod paths;
pub mod pipelined_copy;
pub mod root_mtime;
pub mod strategy;
pub mod uring;

mod mover;
pub use bucketed_pool::{
    bucket_for_size, AsyncNfsContextPair, BucketConfig, BucketedAsyncPool, BUCKETS,
};
pub use downgrade::DowngradeSink;
pub use error::MoveError;
pub use failure::FailureSink;
pub use file_mover::{AsyncBucketedFileMover, FileMover};
pub use libnfs::{ContextPair, LibnfsContextPool, MultiPool, NfsContext, SimplePool};
pub use mover::{MoveOutcome, Mover, MoverConfig};
pub use paths::join_root;
pub use pipelined_copy::{pipelined_copy, FileCopyResult};
pub use root_mtime::restore_root_mtime;
