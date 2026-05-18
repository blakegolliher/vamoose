//! Error type for the async libnfs surface.
//!
//! Distinct from `MoveError` (which is the mover's per-row failure
//! shape) because the async FFI lives a layer below: it has no notion
//! of `FailurePhase`. Callers map `NfsError` → `MoveError` themselves
//! at the boundary, the way ops.rs does for the sync FFI.

use std::fmt;

#[derive(Debug, Clone)]
pub enum NfsError {
    /// libnfs returned a negative errno on the queue step or via the
    /// callback's `err` argument. The positive errno value is what
    /// libnfs encoded; callers map this to a stable name via
    /// [`crate::libnfs::errno_name`].
    Errno {
        errno: i32,
        /// Optional libnfs detail string (drained via `nfs_get_error`
        /// at issue time, or carried in the callback's `data` slot).
        detail: String,
    },
    /// Path contained an interior NUL or otherwise could not be
    /// converted to a C string.
    InvalidPath(String),
    /// `nfs_init_context` returned NULL, `nfs_get_fd` returned -1, or
    /// some other one-shot init step failed.
    Init(String),
    /// The service task has shut down (e.g. context destroyed) so no
    /// new requests can be issued or completed.
    Closed,
    /// A `MountOpts` field is incompatible with the linked libnfs.
    /// See `docs/work-items/LIBNFS_ASYNC_FORK_AUDIT.md`.
    UnsupportedOpt(String),
    /// Generic protocol-level surprise: a callback fired with shape we
    /// don't recognize, or the libnfs queue rejected the request.
    Protocol(String),
}

impl fmt::Display for NfsError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            NfsError::Errno { errno, detail } => {
                let name = crate::libnfs::errno_name(*errno);
                write!(f, "libnfs {name}: {detail}")
            }
            NfsError::InvalidPath(p) => write!(f, "invalid path: {p}"),
            NfsError::Init(s) => write!(f, "libnfs init: {s}"),
            NfsError::Closed => f.write_str("async libnfs context closed"),
            NfsError::UnsupportedOpt(s) => write!(f, "unsupported mount option: {s}"),
            NfsError::Protocol(s) => write!(f, "libnfs protocol surprise: {s}"),
        }
    }
}

impl std::error::Error for NfsError {}

impl NfsError {
    /// Build an `Errno` variant from libnfs's negative-errno
    /// convention. `rc < 0` means errno = `-rc`. Any non-negative rc
    /// at an error site is `Protocol`.
    pub(crate) fn from_neg_rc(rc: i32, detail: impl Into<String>) -> Self {
        if rc < 0 {
            NfsError::Errno {
                errno: -rc,
                detail: detail.into(),
            }
        } else {
            NfsError::Protocol(format!("unexpected rc={rc}: {}", detail.into()))
        }
    }

    /// Convenience for callers that want to match on a specific errno
    /// without pulling in the detail string.
    pub fn errno(&self) -> Option<i32> {
        match self {
            NfsError::Errno { errno, .. } => Some(*errno),
            _ => None,
        }
    }
}
