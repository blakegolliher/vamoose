//! `MoveError` — the unified error type returned by every operation
//! on the data path. Pairs a phase tag (used by retry tooling and the
//! per-file failure log) with a short error string (typically an
//! errno name like `"ENOSPC"`).
//!
//! Lives in its own small module so the FFI wrappers (`libnfs::ops`),
//! the path helpers (`paths`), and the high-level mover all share one
//! definition without depending on each other.

use migration_core::records::FailurePhase;

#[derive(Debug, Clone, thiserror::Error)]
#[error("[{phase:?}] {error}")]
pub struct MoveError {
    pub phase: FailurePhase,
    /// Short tag, typically an errno name (`"ENOSPC"`, `"EPERM"`,
    /// `"EINVAL"`). Falls back to `errno=<n>` for unknown codes so the
    /// failure log is never lossy. See `libnfs::errno_name`.
    pub error: String,
}

impl MoveError {
    pub fn new(phase: FailurePhase, error: impl Into<String>) -> Self {
        Self {
            phase,
            error: error.into(),
        }
    }
}
