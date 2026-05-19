//! Request types passed from caller tasks to the service task.
//!
//! Every public `AsyncNfsContext` method builds a `Request`, queues
//! it on the service-task mpsc, and awaits the matching
//! `oneshot::Receiver`. The service task drains the mpsc, issues the
//! corresponding libnfs `*_async` call, and stuffs the `Sender` half
//! into a heap-allocated `Pending<T>` whose pointer is the libnfs
//! callback's `private_data`.

use super::error::NfsError;
use crate::libnfs::nfsfh;
use std::ffi::CString;
use std::os::raw::c_int;
use tokio::sync::oneshot;

pub(crate) type NfsResult<T> = Result<T, NfsError>;

/// Raw libnfs file handle. Lifetime is enforced by `AsyncNfsFh` on
/// the public side; this type is what crosses the mpsc.
#[derive(Debug)]
pub(crate) struct RawFh(pub *mut nfsfh);

unsafe impl Send for RawFh {}

/// Decoded stat record. Subset of `nfs_stat_64`; trim further if
/// callers need only specific fields.
#[derive(Debug, Clone, Copy)]
pub struct NfsStat64 {
    pub dev: u64,
    pub ino: u64,
    pub mode: u64,
    pub nlink: u64,
    pub uid: u64,
    pub gid: u64,
    pub rdev: u64,
    pub size: u64,
    pub blksize: u64,
    pub blocks: u64,
    pub atime: u64,
    pub mtime: u64,
    pub ctime: u64,
    pub atime_nsec: u64,
    pub mtime_nsec: u64,
    pub ctime_nsec: u64,
    pub used: u64,
}

/// Service-task command surface. One variant per libnfs entry point
/// the async surface binds, plus `Shutdown` for graceful teardown.
///
/// All `CString`s are owned to ensure the C-side path pointer remains
/// valid for the duration of the call.
pub(crate) enum Request {
    Mount {
        server: CString,
        export: CString,
        tx: oneshot::Sender<NfsResult<()>>,
    },
    Open {
        path: CString,
        flags: c_int,
        tx: oneshot::Sender<NfsResult<RawFh>>,
    },
    Open2 {
        path: CString,
        flags: c_int,
        mode: c_int,
        tx: oneshot::Sender<NfsResult<RawFh>>,
    },
    Close {
        fh: RawFh,
        tx: oneshot::Sender<NfsResult<()>>,
    },
    Pread {
        fh: RawFh,
        offset: u64,
        len: usize,
        tx: oneshot::Sender<NfsResult<Vec<u8>>>,
    },
    Pwrite {
        fh: RawFh,
        offset: u64,
        buf: Vec<u8>,
        tx: oneshot::Sender<NfsResult<usize>>,
    },
    Fsync {
        fh: RawFh,
        tx: oneshot::Sender<NfsResult<()>>,
    },
    Stat64 {
        path: CString,
        tx: oneshot::Sender<NfsResult<NfsStat64>>,
    },
    Fstat64 {
        fh: RawFh,
        tx: oneshot::Sender<NfsResult<NfsStat64>>,
    },
    Unlink {
        path: CString,
        tx: oneshot::Sender<NfsResult<()>>,
    },
    Rename {
        oldpath: CString,
        newpath: CString,
        tx: oneshot::Sender<NfsResult<()>>,
    },
    Utimes {
        path: CString,
        times: [libc::timeval; 2],
        tx: oneshot::Sender<NfsResult<()>>,
    },
    /// Symlink-aware utimes — acts on the link itself, not the target.
    Lutimes {
        path: CString,
        times: [libc::timeval; 2],
        tx: oneshot::Sender<NfsResult<()>>,
    },
    Chmod {
        path: CString,
        mode: c_int,
        tx: oneshot::Sender<NfsResult<()>>,
    },
    Chown {
        path: CString,
        uid: c_int,
        gid: c_int,
        tx: oneshot::Sender<NfsResult<()>>,
    },
    Symlink {
        target: CString,
        linkname: CString,
        tx: oneshot::Sender<NfsResult<()>>,
    },
    Link {
        oldpath: CString,
        newpath: CString,
        tx: oneshot::Sender<NfsResult<()>>,
    },
    Mkdir2 {
        path: CString,
        mode: c_int,
        tx: oneshot::Sender<NfsResult<()>>,
    },
    Readlink {
        path: CString,
        tx: oneshot::Sender<NfsResult<Vec<u8>>>,
    },
    QueueLen {
        tx: oneshot::Sender<usize>,
    },
    /// Asks the service task to stop accepting new work and drain
    /// in-flight callbacks before exiting.
    Shutdown,
}
