//! `extern "C"` callbacks that libnfs invokes when an async RPC
//! completes. Each callback is a thin shim: it reconstitutes the
//! boxed `Pending<T>` from `private_data`, decodes the result, and
//! sends it down the oneshot.
//!
//! ## Reentrancy / threading
//!
//! libnfs fires these callbacks from inside `nfs_service`, which our
//! service task calls on its own tokio worker thread. Callbacks must
//! not call back into libnfs from this thread — see the "service
//! task model" section of `LIBNFS_ASYNC_FORK.md`. The bridge does
//! nothing here except decode and `tx.send()` (non-blocking).
//!
//! ## What happens if the receiver was dropped
//!
//! `oneshot::Sender::send` returns the value back as `Err(_)`. We
//! drop it; the in-flight RPC has already completed by definition
//! (we're in its callback), so there's no resource leak. This is
//! how "drop the future to cancel" degenerates to "drop the result"
//! per the spec's cancellation note.

use super::error::NfsError;
use super::ffi::nfs_stat_64;
use super::request::{NfsResult, NfsStat64, RawFh};
use crate::libnfs::{last_error, nfs_context, nfsfh};
use std::ffi::CStr;
use std::os::raw::{c_char, c_int, c_void};
use tokio::sync::oneshot;

/// `private_data` heap box for the "result is `()`" callback shape.
/// Used by close, unlink, rename, chmod, chown, symlink, link,
/// mkdir2, utimes, fsync, mount.
pub(crate) struct PendingUnit {
    pub tx: oneshot::Sender<NfsResult<()>>,
}

/// `private_data` heap box for opens (returns a raw file handle).
pub(crate) struct PendingFh {
    pub tx: oneshot::Sender<NfsResult<RawFh>>,
}

/// `private_data` heap box for `readlink` (callback delivers a
/// NUL-terminated `char *` we copy into a `Vec<u8>`).
pub(crate) struct PendingBytes {
    pub tx: oneshot::Sender<NfsResult<Vec<u8>>>,
}

/// `private_data` heap box for `nfs_pread_async`. libnfs writes the
/// bytes directly into `buf` (see `lib/nfs_v3.c:nfs3_pread_cb` —
/// the user callback receives `data == NULL`; bytes live in the
/// pre-allocated read buffer). The `Pending` owns the Vec for the
/// duration of the RPC and ships it on the oneshot at completion.
pub(crate) struct PendingPread {
    pub tx: oneshot::Sender<NfsResult<Vec<u8>>>,
    pub buf: Vec<u8>,
}

/// `private_data` heap box for `pwrite` (callback delivers a byte
/// count via `err`). `_buf` owns the write buffer for the lifetime
/// of the RPC — dropped here after callback fires.
pub(crate) struct PendingWrite {
    pub tx: oneshot::Sender<NfsResult<usize>>,
    pub _buf: Vec<u8>,
}

/// `private_data` heap box for `stat` / `fstat` (callback delivers
/// `struct nfs_stat_64 *`).
pub(crate) struct PendingStat {
    pub tx: oneshot::Sender<NfsResult<NfsStat64>>,
}

/// Decode the libnfs error string from the `data` slot on a failure
/// callback. `data` is a `char *` to an internal libnfs buffer.
unsafe fn detail_from_data(data: *mut c_void) -> String {
    if data.is_null() {
        return String::new();
    }
    let s = unsafe { CStr::from_ptr(data as *const c_char) };
    s.to_string_lossy().into_owned()
}

/// Build an `Errno` for a callback whose `err` field is negative.
/// `data` is interpreted as the libnfs detail string on failure.
unsafe fn err_from_cb(err: c_int, _nfs: *mut nfs_context, data: *mut c_void) -> NfsError {
    NfsError::from_neg_rc(err, unsafe { detail_from_data(data) })
}

pub(crate) unsafe extern "C" fn unit_cb(
    err: c_int,
    nfs: *mut nfs_context,
    data: *mut c_void,
    private_data: *mut c_void,
) {
    // SAFETY: every site that issues a unit-shaped RPC passes a Box<PendingUnit>
    // raw pointer as private_data. libnfs returns it untouched.
    let p: Box<PendingUnit> = unsafe { Box::from_raw(private_data as *mut PendingUnit) };
    let result = if err < 0 {
        Err(unsafe { err_from_cb(err, nfs, data) })
    } else {
        Ok(())
    };
    let _ = p.tx.send(result);
}

pub(crate) unsafe extern "C" fn fh_cb(
    err: c_int,
    nfs: *mut nfs_context,
    data: *mut c_void,
    private_data: *mut c_void,
) {
    let p: Box<PendingFh> = unsafe { Box::from_raw(private_data as *mut PendingFh) };
    let result = if err < 0 {
        Err(unsafe { err_from_cb(err, nfs, data) })
    } else {
        // For open / open2 / creat, libnfs delivers the file handle
        // pointer in `data`.
        let raw = data as *mut nfsfh;
        Ok(RawFh(raw))
    };
    let _ = p.tx.send(result);
}

pub(crate) unsafe extern "C" fn pread_cb(
    err: c_int,
    nfs: *mut nfs_context,
    data: *mut c_void,
    private_data: *mut c_void,
) {
    let mut p: Box<PendingPread> = unsafe { Box::from_raw(private_data as *mut PendingPread) };
    let result = if err < 0 {
        Err(unsafe { err_from_cb(err, nfs, data) })
    } else {
        // err >= 0 is the actual byte count read. libnfs has already
        // written the bytes directly into `p.buf` (see
        // `nfs3_pread_cb` in lib/nfs_v3.c — the user callback fires
        // with `data == NULL` and bytes in the user buffer). Truncate
        // to the real count before handing back; the Vec's capacity
        // is the requested `len`.
        let n = (err as usize).min(p.buf.len());
        p.buf.truncate(n);
        Ok(std::mem::take(&mut p.buf))
    };
    let _ = p.tx.send(result);
}

pub(crate) unsafe extern "C" fn pwrite_cb(
    err: c_int,
    nfs: *mut nfs_context,
    data: *mut c_void,
    private_data: *mut c_void,
) {
    let p: Box<PendingWrite> = unsafe { Box::from_raw(private_data as *mut PendingWrite) };
    let result = if err < 0 {
        Err(unsafe { err_from_cb(err, nfs, data) })
    } else {
        Ok(err as usize)
    };
    // p._buf drops here, after libnfs is definitely done with it.
    let _ = p.tx.send(result);
}

pub(crate) unsafe extern "C" fn readlink_cb(
    err: c_int,
    nfs: *mut nfs_context,
    data: *mut c_void,
    private_data: *mut c_void,
) {
    let p: Box<PendingBytes> = unsafe { Box::from_raw(private_data as *mut PendingBytes) };
    let result = if err < 0 {
        Err(unsafe { err_from_cb(err, nfs, data) })
    } else {
        // Success: data is a NUL-terminated symlink target string
        // owned by libnfs.
        if data.is_null() {
            Ok(Vec::new())
        } else {
            let s = unsafe { CStr::from_ptr(data as *const c_char) };
            Ok(s.to_bytes().to_vec())
        }
    };
    let _ = p.tx.send(result);
}

pub(crate) unsafe extern "C" fn stat_cb(
    err: c_int,
    nfs: *mut nfs_context,
    data: *mut c_void,
    private_data: *mut c_void,
) {
    let p: Box<PendingStat> = unsafe { Box::from_raw(private_data as *mut PendingStat) };
    let result = if err < 0 {
        Err(unsafe { err_from_cb(err, nfs, data) })
    } else if data.is_null() {
        Err(NfsError::Protocol(
            "stat callback delivered NULL data".into(),
        ))
    } else {
        let st = unsafe { &*(data as *const nfs_stat_64) };
        Ok(NfsStat64 {
            dev: st.nfs_dev,
            ino: st.nfs_ino,
            mode: st.nfs_mode,
            nlink: st.nfs_nlink,
            uid: st.nfs_uid,
            gid: st.nfs_gid,
            rdev: st.nfs_rdev,
            size: st.nfs_size,
            blksize: st.nfs_blksize,
            blocks: st.nfs_blocks,
            atime: st.nfs_atime,
            mtime: st.nfs_mtime,
            ctime: st.nfs_ctime,
            atime_nsec: st.nfs_atime_nsec,
            mtime_nsec: st.nfs_mtime_nsec,
            ctime_nsec: st.nfs_ctime_nsec,
            used: st.nfs_used,
        })
    };
    let _ = p.tx.send(result);
}

/// Build an error after libnfs returned `rc < 0` on the *issue* step
/// (i.e. the request never made it onto the wire and the callback
/// will never fire). Collect the detail string from `nfs_get_error`.
pub(crate) fn err_from_issue(ctx: *mut nfs_context, rc: c_int) -> NfsError {
    NfsError::from_neg_rc(rc, last_error(ctx).to_string())
}
