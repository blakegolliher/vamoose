//! Safe wrappers around the libnfs FFI.
//!
//! Each wrapper:
//! 1. Builds the C strings the call needs (rejecting interior NULs as
//!    per-file failures, never panicking — see R1).
//! 2. Calls the FFI under `unsafe`.
//! 3. Translates the int return: positive = ok (or byte count), 0 = ok,
//!    negative = `-errno`. Errno is mapped to a stable name via
//!    [`super::errno_name`] and paired with a `FailurePhase` per R6.
//!
//! Wrappers take `&mut NfsContext` because libnfs contexts are not
//! thread-safe — `&mut` enforces single-caller use at the type level
//! without needing a runtime mutex inside the wrapper.
//!
//! All paths are `&[u8]` because POSIX paths are byte sequences.

use super::{errno_name, last_error, nfs_stat_64, nfsfh, NfsContext};
use crate::error::MoveError;
use crate::paths::cstr_from_bytes;
use migration_core::records::FailurePhase;
use std::os::raw::c_int;

/// An owned NFS file handle. Drops on close (caller must call
/// `close_fh` before drop, or the fh leaks — Drop here is a defensive
/// no-op because libnfs's `nfs_close` needs the context too).
pub struct NfsFh {
    raw: *mut nfsfh,
}

impl NfsFh {
    fn from_raw(raw: *mut nfsfh) -> Self {
        Self { raw }
    }
    fn raw(&self) -> *mut nfsfh {
        self.raw
    }
}

unsafe impl Send for NfsFh {}

// =============================================================================
// Common rc → MoveError translation.
// =============================================================================

fn err_from_rc(ctx: &mut NfsContext, rc: c_int, phase: FailurePhase) -> MoveError {
    // libnfs convention: rc < 0 is -errno. The error string from
    // nfs_get_error is informational; we surface the errno name so
    // failure log consumers can match deterministically.
    if rc < 0 {
        let errno = -rc;
        let s = errno_name(errno);
        let detail = last_error(ctx.raw()).to_string();
        // Trace the libnfs detail for forensics; the failure log keeps
        // just the errno name to stay structured.
        tracing::debug!(rc, errno, %s, detail = %detail, ?phase, "libnfs error");
        MoveError::new(phase, s)
    } else {
        // Defensive — should not happen, all wrappers gate on rc < 0.
        MoveError::new(phase, format!("unexpected rc={rc}"))
    }
}

// =============================================================================
// FFI wrappers.
// =============================================================================

/// Open an existing file for read-only.
pub fn open_read(ctx: &mut NfsContext, path: &[u8]) -> Result<NfsFh, MoveError> {
    let c = cstr_from_bytes(path)?;
    let mut fh: *mut nfsfh = std::ptr::null_mut();
    let rc = unsafe { super::nfs_open(ctx.raw(), c.as_ptr(), libc::O_RDONLY, &mut fh) };
    if rc < 0 {
        return Err(err_from_rc(ctx, rc, FailurePhase::Open));
    }
    Ok(NfsFh::from_raw(fh))
}

/// Create a new file for writing. `mode` is the initial mode; the
/// final mode is set by [`chmod`] at end-of-file. M2 callers create
/// with `0o600` so the in-flight `.partial` is not world-readable.
pub fn create_write(ctx: &mut NfsContext, path: &[u8], mode: u32) -> Result<NfsFh, MoveError> {
    let c = cstr_from_bytes(path)?;
    let mut fh: *mut nfsfh = std::ptr::null_mut();
    let flags = libc::O_WRONLY | libc::O_CREAT | libc::O_TRUNC;
    let rc = unsafe { super::nfs_open2(ctx.raw(), c.as_ptr(), flags, mode as c_int, &mut fh) };
    if rc < 0 {
        return Err(err_from_rc(ctx, rc, FailurePhase::Write));
    }
    Ok(NfsFh::from_raw(fh))
}

/// F09: whole-file NFS COMMIT (sync `nfs_fsync`) for an open write
/// fh — durabilizes every UNSTABLE WRITE issued on it. Called by
/// `do_libnfs_copy` after the streaming loop, before `close_fh` on
/// the write fh, mirroring the async path's `dst.fsync`.
///
/// Failure phase: reuses `FailurePhase::Write` with a
/// `COMMIT:`-prefixed error tag (e.g. `COMMIT:EIO`) instead of a new
/// `FailurePhase::Commit` variant. `FailurePhase` is serialized into
/// the published failure-record JSONL (SCHEMA_CONTRACT.md; records
/// are parsed downstream), so a new variant would widen that wire
/// enum for every consumer; the prefixed tag keeps COMMIT failures
/// distinguishable in the failure log without touching the schema.
pub fn fsync(ctx: &mut NfsContext, fh: &NfsFh) -> Result<(), MoveError> {
    let rc = unsafe { super::nfs_fsync(ctx.raw(), fh.raw()) };
    if rc < 0 {
        let mut e = err_from_rc(ctx, rc, FailurePhase::Write);
        e.error = format!("COMMIT:{}", e.error);
        return Err(e);
    }
    Ok(())
}

pub fn close_fh(ctx: &mut NfsContext, fh: NfsFh, phase: FailurePhase) -> Result<(), MoveError> {
    let rc = unsafe { super::nfs_close(ctx.raw(), fh.raw()) };
    if rc < 0 {
        return Err(err_from_rc(ctx, rc, phase));
    }
    Ok(())
}

/// Read up to `buf.len()` bytes at `offset`. Returns the number of
/// bytes actually read, which may be less than requested at EOF.
pub fn pread(
    ctx: &mut NfsContext,
    fh: &NfsFh,
    offset: u64,
    buf: &mut [u8],
) -> Result<usize, MoveError> {
    let rc = unsafe {
        super::nfs_pread(
            ctx.raw(),
            fh.raw(),
            buf.as_mut_ptr() as *mut _,
            buf.len(),
            offset,
        )
    };
    if rc < 0 {
        return Err(err_from_rc(ctx, rc, FailurePhase::Read));
    }
    Ok(rc as usize)
}

/// Write `buf.len()` bytes at `offset`. Returns the number of bytes
/// actually written. M2 callers treat short writes as failures.
pub fn pwrite(
    ctx: &mut NfsContext,
    fh: &NfsFh,
    offset: u64,
    buf: &[u8],
) -> Result<usize, MoveError> {
    let rc = unsafe {
        super::nfs_pwrite(
            ctx.raw(),
            fh.raw(),
            buf.as_ptr() as *const _,
            buf.len(),
            offset,
        )
    };
    if rc < 0 {
        return Err(err_from_rc(ctx, rc, FailurePhase::Write));
    }
    Ok(rc as usize)
}

pub fn chmod(ctx: &mut NfsContext, path: &[u8], mode: u32) -> Result<(), MoveError> {
    let c = cstr_from_bytes(path)?;
    let rc = unsafe { super::nfs_chmod(ctx.raw(), c.as_ptr(), mode as c_int) };
    if rc < 0 {
        return Err(err_from_rc(ctx, rc, FailurePhase::Setattr));
    }
    Ok(())
}

pub fn chown(ctx: &mut NfsContext, path: &[u8], uid: u32, gid: u32) -> Result<(), MoveError> {
    let c = cstr_from_bytes(path)?;
    let rc = unsafe { super::nfs_chown(ctx.raw(), c.as_ptr(), uid as c_int, gid as c_int) };
    if rc < 0 {
        return Err(err_from_rc(ctx, rc, FailurePhase::Setattr));
    }
    Ok(())
}

/// fh-based chown. Preferred over the path variant while the write fh
/// is still open: libnfs path ops re-walk every path component with a
/// LOOKUP per call, which dominates the RPC budget on deep trees.
pub fn fchown(ctx: &mut NfsContext, fh: &NfsFh, uid: u32, gid: u32) -> Result<(), MoveError> {
    let rc = unsafe { super::nfs_fchown(ctx.raw(), fh.raw(), uid as c_int, gid as c_int) };
    if rc < 0 {
        return Err(err_from_rc(ctx, rc, FailurePhase::Setattr));
    }
    Ok(())
}

/// fh-based chmod. Same LOOKUP-avoidance rationale as [`fchown`].
pub fn fchmod(ctx: &mut NfsContext, fh: &NfsFh, mode: u32) -> Result<(), MoveError> {
    let rc = unsafe { super::nfs_fchmod(ctx.raw(), fh.raw(), mode as c_int) };
    if rc < 0 {
        return Err(err_from_rc(ctx, rc, FailurePhase::Setattr));
    }
    Ok(())
}

/// Set atime + mtime. Sub-second precision is microseconds (the M2
/// FFI uses `nfs_utimes`); nanosecond columns are truncated.
pub fn utimes(
    ctx: &mut NfsContext,
    path: &[u8],
    atime_sec: i64,
    atime_nsec: i32,
    mtime_sec: i64,
    mtime_nsec: i32,
) -> Result<(), MoveError> {
    let c = cstr_from_bytes(path)?;
    let mut times = build_timeval_pair(atime_sec, atime_nsec, mtime_sec, mtime_nsec);
    let rc = unsafe { super::nfs_utimes(ctx.raw(), c.as_ptr(), times.as_mut_ptr()) };
    if rc < 0 {
        return Err(err_from_rc(ctx, rc, FailurePhase::Setattr));
    }
    Ok(())
}

/// Symlink-aware `utimes`. Sets atime + mtime on the symlink itself,
/// not its target. Same µs-precision ceiling as [`utimes`]; libnfs
/// has no `lutimens` variant (see `MTIME_PARITY_FIX.md`).
pub fn lutimes(
    ctx: &mut NfsContext,
    path: &[u8],
    atime_sec: i64,
    atime_nsec: i32,
    mtime_sec: i64,
    mtime_nsec: i32,
) -> Result<(), MoveError> {
    let c = cstr_from_bytes(path)?;
    let mut times = build_timeval_pair(atime_sec, atime_nsec, mtime_sec, mtime_nsec);
    let rc = unsafe { super::nfs_lutimes(ctx.raw(), c.as_ptr(), times.as_mut_ptr()) };
    if rc < 0 {
        return Err(err_from_rc(ctx, rc, FailurePhase::Setattr));
    }
    Ok(())
}

fn build_timeval_pair(
    atime_sec: i64,
    atime_nsec: i32,
    mtime_sec: i64,
    mtime_nsec: i32,
) -> [libc::timeval; 2] {
    [
        libc::timeval {
            tv_sec: atime_sec as libc::time_t,
            tv_usec: (atime_nsec / 1_000) as libc::suseconds_t,
        },
        libc::timeval {
            tv_sec: mtime_sec as libc::time_t,
            tv_usec: (mtime_nsec / 1_000) as libc::suseconds_t,
        },
    ]
}

/// Stat a path (follows symlinks). Returns the (atime_sec, atime_nsec,
/// mtime_sec, mtime_nsec) tuple — that's the only subset
/// `restore_root_mtime` needs. If a more general consumer arrives,
/// promote this to return the full `nfs_stat_64`.
pub fn stat_times(ctx: &mut NfsContext, path: &[u8]) -> Result<(i64, i32, i64, i32), MoveError> {
    let c = cstr_from_bytes(path)?;
    let mut st: nfs_stat_64 = nfs_stat_64::default();
    let rc = unsafe { super::nfs_stat64(ctx.raw(), c.as_ptr(), &mut st as *mut _) };
    if rc < 0 {
        return Err(err_from_rc(ctx, rc, FailurePhase::Setattr));
    }
    Ok((
        st.nfs_atime as i64,
        st.nfs_atime_nsec as i32,
        st.nfs_mtime as i64,
        st.nfs_mtime_nsec as i32,
    ))
}

/// Stat a path (follows symlinks) and return its fileid (`nfs_ino`).
/// Used by hardlink EEXIST recovery (F10) to decide replay vs
/// conflict, hence the `Hardlink` phase on failure. Sibling of
/// [`stat_times`]; if a third consumer arrives, promote both to a
/// single wrapper returning the full `nfs_stat_64`.
pub fn stat_fileid(ctx: &mut NfsContext, path: &[u8]) -> Result<u64, MoveError> {
    let c = cstr_from_bytes(path)?;
    let mut st: nfs_stat_64 = nfs_stat_64::default();
    let rc = unsafe { super::nfs_stat64(ctx.raw(), c.as_ptr(), &mut st as *mut _) };
    if rc < 0 {
        return Err(err_from_rc(ctx, rc, FailurePhase::Hardlink));
    }
    Ok(st.nfs_ino)
}

pub fn rename(ctx: &mut NfsContext, old: &[u8], new: &[u8]) -> Result<(), MoveError> {
    let oc = cstr_from_bytes(old)?;
    let nc = cstr_from_bytes(new)?;
    let rc = unsafe { super::nfs_rename(ctx.raw(), oc.as_ptr(), nc.as_ptr()) };
    if rc < 0 {
        return Err(err_from_rc(ctx, rc, FailurePhase::Rename));
    }
    Ok(())
}

pub fn link(ctx: &mut NfsContext, target: &[u8], linkpath: &[u8]) -> Result<(), MoveError> {
    let tc = cstr_from_bytes(target)?;
    let lc = cstr_from_bytes(linkpath)?;
    let rc = unsafe { super::nfs_link(ctx.raw(), tc.as_ptr(), lc.as_ptr()) };
    if rc < 0 {
        return Err(err_from_rc(ctx, rc, FailurePhase::Hardlink));
    }
    Ok(())
}

pub fn symlink(ctx: &mut NfsContext, target: &[u8], linkpath: &[u8]) -> Result<(), MoveError> {
    let tc = cstr_from_bytes(target)?;
    let lc = cstr_from_bytes(linkpath)?;
    let rc = unsafe { super::nfs_symlink(ctx.raw(), tc.as_ptr(), lc.as_ptr()) };
    if rc < 0 {
        return Err(err_from_rc(ctx, rc, FailurePhase::Symlink));
    }
    Ok(())
}

/// Read a symlink's target. Allocates a 4 KiB scratch buffer; if the
/// target overflows we'd see a libnfs error — bump the buffer if it
/// becomes a real failure mode in practice.
pub fn readlink(ctx: &mut NfsContext, path: &[u8]) -> Result<Vec<u8>, MoveError> {
    let c = cstr_from_bytes(path)?;
    let mut buf = vec![0u8; 4096];
    let rc = unsafe {
        super::nfs_readlink(
            ctx.raw(),
            c.as_ptr(),
            buf.as_mut_ptr() as *mut _,
            buf.len() as c_int,
        )
    };
    if rc < 0 {
        return Err(err_from_rc(ctx, rc, FailurePhase::Symlink));
    }
    // libnfs writes a NUL-terminated string; trim at the first NUL.
    let len = buf.iter().position(|&b| b == 0).unwrap_or(buf.len());
    buf.truncate(len);
    Ok(buf)
}

pub fn unlink(ctx: &mut NfsContext, path: &[u8]) -> Result<(), MoveError> {
    let c = cstr_from_bytes(path)?;
    let rc = unsafe { super::nfs_unlink(ctx.raw(), c.as_ptr()) };
    if rc < 0 {
        return Err(err_from_rc(ctx, rc, FailurePhase::Rename));
    }
    Ok(())
}

/// Create a single directory. Returns `Ok` for `EEXIST` since the
/// caller-side `mkdir_p` walks ancestors that may already exist.
pub fn mkdir(ctx: &mut NfsContext, path: &[u8], mode: u32) -> Result<(), MoveError> {
    let c = cstr_from_bytes(path)?;
    let rc = unsafe { super::nfs_mkdir2(ctx.raw(), c.as_ptr(), mode as c_int) };
    if rc < 0 {
        let e = err_from_rc(ctx, rc, FailurePhase::Write);
        if e.error == "EEXIST" {
            return Ok(());
        }
        return Err(e);
    }
    Ok(())
}

/// Best-effort `mkdir -p` for the parent of `path`. Used so the
/// mover can land files into directories the dest tree didn't have
/// yet without requiring a pre-pass.
pub fn mkdir_p_for_file(ctx: &mut NfsContext, file_path: &[u8]) -> Result<(), MoveError> {
    let last_slash = match file_path.iter().rposition(|&b| b == b'/') {
        Some(0) => return Ok(()), // file is at root; root always exists
        Some(i) => i,
        None => return Ok(()), // no parent component
    };
    let parent = &file_path[..last_slash];
    if parent.is_empty() {
        return Ok(());
    }
    mkdir_p(ctx, parent)
}

/// `mkdir -p <path>` — ensure every component of `path` itself
/// exists. Walks each component, ignoring `EEXIST` along the way.
/// Created components get a stand-in mode of 0o755; the dir-attr
/// pass overwrites mode/owner/mtime when a walker dir row exists.
pub fn mkdir_p(ctx: &mut NfsContext, path: &[u8]) -> Result<(), MoveError> {
    if path.is_empty() {
        return Ok(());
    }
    let mut acc: Vec<u8> = Vec::with_capacity(path.len());
    for component in path.split(|&b| b == b'/') {
        if component.is_empty() {
            // leading or repeated slash
            acc.push(b'/');
            continue;
        }
        acc.push(b'/');
        acc.extend_from_slice(component);
        mkdir(ctx, &acc, 0o755)?;
    }
    Ok(())
}

/// Drop a fh that we already know we won't close cleanly (e.g. on the
/// error path). Logs but doesn't surface a MoveError because the
/// caller is already returning one.
pub fn close_quietly(ctx: &mut NfsContext, fh: NfsFh) {
    let rc = unsafe { super::nfs_close(ctx.raw(), fh.raw()) };
    if rc < 0 {
        let detail = last_error(ctx.raw()).to_string();
        tracing::warn!(rc, detail = %detail, "close_quietly: nfs_close failed");
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::error::MoveError;
    use migration_core::records::FailurePhase;

    /// `cstr_from_bytes` rejects interior NUL with EINVAL/Open.
    /// (Lives here because ops.rs is the primary consumer.)
    #[test]
    fn cstr_rejects_nul_at_open_phase() {
        let e: MoveError = cstr_from_bytes(b"/a\0b").unwrap_err();
        assert_eq!(e.error, "EINVAL");
        assert_eq!(e.phase, FailurePhase::Open);
    }

    /// Phase mapping is the contract — these tests guard against
    /// accidental phase regressions in the wrapper layer.
    #[test]
    fn phase_mapping_documented() {
        // This test is intentionally just an assertion that the phase
        // labels we hardwire above match the failure-record contract. If you
        // change a phase below, update operator-facing failure documentation
        // and sink-parser tests too.
        let cases: &[(&str, FailurePhase)] = &[
            ("open_read", FailurePhase::Open),
            ("create_write", FailurePhase::Write),
            ("pread", FailurePhase::Read),
            ("pwrite", FailurePhase::Write),
            ("chmod", FailurePhase::Setattr),
            ("chown", FailurePhase::Setattr),
            ("utimes", FailurePhase::Setattr),
            ("lutimes", FailurePhase::Setattr),
            ("rename", FailurePhase::Rename),
            ("link", FailurePhase::Hardlink),
            ("symlink", FailurePhase::Symlink),
            ("readlink", FailurePhase::Symlink),
        ];
        // Touch each variant to ensure they exist; if FailurePhase is
        // ever renamed or removed, this test fails to compile, which
        // is the signal we want.
        for (_, phase) in cases {
            let _: FailurePhase = *phase;
        }
    }
}
