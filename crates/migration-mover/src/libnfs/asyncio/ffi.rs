//! `extern "C"` declarations for the async libnfs surface.
//!
//! Every signature here was cross-checked against
//! `/usr/local/include/nfsc/libnfs.h` (the header that ships with the
//! linked `.so` at `/usr/local/lib/libnfs.so.16.0.2`) on 2026-05-18.
//! See `docs/work-items/LIBNFS_ASYNC_FORK_AUDIT.md` for the per-symbol
//! audit.
//!
//! Parameter order is what causes silent data loss with libnfs (see
//! `M2_NOTES.md` M2/M3 verification incidents). Do not "tidy up" any
//! declaration here without re-running the audit + the three async
//! test binaries against real hardware per
//! `docs/CORRECTNESS_RULES.md` "Pre-merge runbook: async libnfs FFI
//! changes".

#![allow(non_camel_case_types, dead_code)]

use crate::libnfs::{nfs_context, nfsfh};
use std::os::raw::{c_char, c_int, c_void};

/// libnfs callback signature. Used by every `*_async` entry point.
///
/// - `err`: result code. For success, `>= 0` (often a byte count, sometimes 0).
///   For failure, negative `-errno`.
/// - `nfs`: the context the call was issued on.
/// - `data`: result payload, semantics depend on the call.
/// - `private_data`: the cookie we passed at issue time.
pub type nfs_cb = unsafe extern "C" fn(
    err: c_int,
    nfs: *mut nfs_context,
    data: *mut c_void,
    private_data: *mut c_void,
);

// The `nfs_stat64` shape libnfs delivers on stat callbacks. Field
// ordering and types are libnfs-specific (see `libnfs.h` ~line 460).
#[repr(C)]
#[derive(Debug, Clone, Copy)]
pub struct nfs_stat_64 {
    pub nfs_dev: u64,
    pub nfs_ino: u64,
    pub nfs_mode: u64,
    pub nfs_nlink: u64,
    pub nfs_uid: u64,
    pub nfs_gid: u64,
    pub nfs_rdev: u64,
    pub nfs_size: u64,
    pub nfs_blksize: u64,
    pub nfs_blocks: u64,
    pub nfs_atime: u64,
    pub nfs_mtime: u64,
    pub nfs_ctime: u64,
    pub nfs_atime_nsec: u64,
    pub nfs_mtime_nsec: u64,
    pub nfs_ctime_nsec: u64,
    pub nfs_used: u64,
}

extern "C" {
    // ----- driver -----
    pub fn nfs_get_fd(nfs: *mut nfs_context) -> c_int;
    pub fn nfs_which_events(nfs: *mut nfs_context) -> c_int;
    pub fn nfs_service(nfs: *mut nfs_context, revents: c_int) -> c_int;
    pub fn nfs_queue_length(nfs: *mut nfs_context) -> c_int;

    // ----- tunables -----
    pub fn nfs_set_readmax(nfs: *mut nfs_context, readmax: usize);
    pub fn nfs_set_writemax(nfs: *mut nfs_context, writemax: usize);

    // ----- mount -----
    pub fn nfs_mount_async(
        nfs: *mut nfs_context,
        server: *const c_char,
        exportname: *const c_char,
        cb: nfs_cb,
        private_data: *mut c_void,
    ) -> c_int;

    // ----- file ops -----
    pub fn nfs_open_async(
        nfs: *mut nfs_context,
        path: *const c_char,
        flags: c_int,
        cb: nfs_cb,
        private_data: *mut c_void,
    ) -> c_int;
    pub fn nfs_open2_async(
        nfs: *mut nfs_context,
        path: *const c_char,
        flags: c_int,
        mode: c_int,
        cb: nfs_cb,
        private_data: *mut c_void,
    ) -> c_int;
    pub fn nfs_close_async(
        nfs: *mut nfs_context,
        fh: *mut nfsfh,
        cb: nfs_cb,
        private_data: *mut c_void,
    ) -> c_int;
    pub fn nfs_pread_async(
        nfs: *mut nfs_context,
        fh: *mut nfsfh,
        buf: *mut c_void,
        count: usize,
        offset: u64,
        cb: nfs_cb,
        private_data: *mut c_void,
    ) -> c_int;
    pub fn nfs_pwrite_async(
        nfs: *mut nfs_context,
        fh: *mut nfsfh,
        buf: *const c_void,
        count: usize,
        offset: u64,
        cb: nfs_cb,
        private_data: *mut c_void,
    ) -> c_int;
    pub fn nfs_fsync_async(
        nfs: *mut nfs_context,
        fh: *mut nfsfh,
        cb: nfs_cb,
        private_data: *mut c_void,
    ) -> c_int;
    pub fn nfs_stat64_async(
        nfs: *mut nfs_context,
        path: *const c_char,
        cb: nfs_cb,
        private_data: *mut c_void,
    ) -> c_int;
    pub fn nfs_fstat64_async(
        nfs: *mut nfs_context,
        fh: *mut nfsfh,
        cb: nfs_cb,
        private_data: *mut c_void,
    ) -> c_int;
    pub fn nfs_unlink_async(
        nfs: *mut nfs_context,
        path: *const c_char,
        cb: nfs_cb,
        private_data: *mut c_void,
    ) -> c_int;
    pub fn nfs_rename_async(
        nfs: *mut nfs_context,
        oldpath: *const c_char,
        newpath: *const c_char,
        cb: nfs_cb,
        private_data: *mut c_void,
    ) -> c_int;
    pub fn nfs_chmod_async(
        nfs: *mut nfs_context,
        path: *const c_char,
        mode: c_int,
        cb: nfs_cb,
        private_data: *mut c_void,
    ) -> c_int;
    pub fn nfs_chown_async(
        nfs: *mut nfs_context,
        path: *const c_char,
        uid: c_int,
        gid: c_int,
        cb: nfs_cb,
        private_data: *mut c_void,
    ) -> c_int;
    pub fn nfs_utimes_async(
        nfs: *mut nfs_context,
        path: *const c_char,
        times: *mut libc::timeval,
        cb: nfs_cb,
        private_data: *mut c_void,
    ) -> c_int;
    /// Symlink-aware utimes_async — sets times on the link itself, not
    /// the target. Same µs-precision timeval layout as the non-`l`
    /// form. libnfs has no ns-precision variant (see
    /// `docs/work-items/MTIME_PARITY_FIX.md`).
    pub fn nfs_lutimes_async(
        nfs: *mut nfs_context,
        path: *const c_char,
        times: *mut libc::timeval,
        cb: nfs_cb,
        private_data: *mut c_void,
    ) -> c_int;
    pub fn nfs_symlink_async(
        nfs: *mut nfs_context,
        target: *const c_char,
        linkname: *const c_char,
        cb: nfs_cb,
        private_data: *mut c_void,
    ) -> c_int;
    pub fn nfs_link_async(
        nfs: *mut nfs_context,
        oldpath: *const c_char,
        newpath: *const c_char,
        cb: nfs_cb,
        private_data: *mut c_void,
    ) -> c_int;
    pub fn nfs_mkdir2_async(
        nfs: *mut nfs_context,
        path: *const c_char,
        mode: c_int,
        cb: nfs_cb,
        private_data: *mut c_void,
    ) -> c_int;
    pub fn nfs_readlink_async(
        nfs: *mut nfs_context,
        path: *const c_char,
        cb: nfs_cb,
        private_data: *mut c_void,
    ) -> c_int;
}
