//! Read-only namespace operations used by independent verification.
//!
//! Directory enumeration is built on the raw NFSv3 READDIRPLUS surface that
//! is already pinned and shipped with Vamoose. Each call is cookie-resumable
//! and bounded, while copied attributes avoid a separate GETATTR for the
//! common case.

use super::{last_error, nfs_context, nfs_stat_64, raw, NfsContext};
use crate::paths::cstr_from_bytes;
use std::os::raw::{c_char, c_int};

extern "C" {
    fn nfs_lstat64(nfs: *mut nfs_context, path: *const c_char, st: *mut nfs_stat_64) -> c_int;
}

#[derive(Debug, Clone, thiserror::Error)]
#[error("libnfs {operation} failed for {path}: {detail}")]
pub struct ScanError {
    pub operation: &'static str,
    pub path: String,
    pub detail: String,
}

fn error(ctx: &mut NfsContext, operation: &'static str, path: &[u8], rc: c_int) -> ScanError {
    ScanError {
        operation,
        path: String::from_utf8_lossy(path).into_owned(),
        detail: format!("rc={rc}: {}", last_error(ctx.raw())),
    }
}

fn raw_error(operation: &'static str, path: &[u8], error: raw::RawError) -> ScanError {
    ScanError {
        operation,
        path: String::from_utf8_lossy(path).into_owned(),
        detail: format!("{}: {}", error.tag, error.detail),
    }
}

/// Symlink-aware metadata observation.
pub fn lstat(ctx: &mut NfsContext, path: &[u8]) -> Result<nfs_stat_64, ScanError> {
    let c = cstr_from_bytes(path).map_err(|e| ScanError {
        operation: "lstat",
        path: String::from_utf8_lossy(path).into_owned(),
        detail: e.to_string(),
    })?;
    let mut st = nfs_stat_64::default();
    let rc = unsafe { nfs_lstat64(ctx.raw(), c.as_ptr(), &mut st) };
    if rc < 0 {
        return Err(error(ctx, "lstat", path, rc));
    }
    Ok(st)
}

#[derive(Debug)]
pub struct DirEntry {
    pub name: Vec<u8>,
    pub fh: Option<raw::Fh>,
    pub attrs: Option<raw::NfsAttributes>,
}

#[derive(Debug)]
pub struct DirBatch {
    pub entries: Vec<DirEntry>,
    pub next_cookie: u64,
    pub cookie_verifier: [u8; 8],
    pub complete: bool,
}

/// Read at most one checkpoint batch of names and attributes. Individual NFS
/// reply pages remain capped at 8 KiB by `raw::readdirplus_page`; this method
/// may combine pages until `max_entries` useful names have been collected.
pub fn read_dir_batch(
    ctx: &mut NfsContext,
    path: &[u8],
    dir_fh: &[u8],
    cookie: u64,
    cookie_verifier: [u8; 8],
    max_entries: u32,
) -> Result<DirBatch, ScanError> {
    if max_entries == 0 {
        return Err(ScanError {
            operation: "readdirplus",
            path: String::from_utf8_lossy(path).into_owned(),
            detail: "max_entries must be greater than zero".to_string(),
        });
    }

    let mut entries = Vec::with_capacity(max_entries as usize);
    let mut next_cookie = cookie;
    let mut next_verifier = cookie_verifier;
    loop {
        let requested_cookie = next_cookie;
        let page = raw::readdirplus_page(ctx, dir_fh, requested_cookie, next_verifier)
            .map_err(|error| raw_error("readdirplus", path, error))?;
        let raw_len = page.entries.len();
        for (index, entry) in page.entries.into_iter().enumerate() {
            next_cookie = entry.cookie;
            next_verifier = page.cookie_verifier;
            if entry.name == b"." || entry.name == b".." {
                continue;
            }
            entries.push(DirEntry {
                name: entry.name,
                fh: entry.fh,
                attrs: entry.attrs,
            });
            if entries.len() == max_entries as usize {
                return Ok(DirBatch {
                    entries,
                    next_cookie,
                    cookie_verifier: next_verifier,
                    complete: page.eof && index + 1 == raw_len,
                });
            }
        }

        if page.eof {
            return Ok(DirBatch {
                entries,
                next_cookie,
                cookie_verifier: next_verifier,
                complete: true,
            });
        }
        let page_cookie = page.next_cookie.ok_or_else(|| ScanError {
            operation: "readdirplus",
            path: String::from_utf8_lossy(path).into_owned(),
            detail: "non-EOF page contained no continuation cookie".to_string(),
        })?;
        if page_cookie == requested_cookie {
            return Err(ScanError {
                operation: "readdirplus",
                path: String::from_utf8_lossy(path).into_owned(),
                detail: format!("server returned a non-advancing cookie {next_cookie}"),
            });
        }
        next_cookie = page_cookie;
        next_verifier = page.cookie_verifier;
    }
}
