//! libnfs FFI surface and a safe `NfsContext` newtype.
//!
//! IMPORTANT: parameter order in this file MUST match the linked
//! libnfs binary at runtime, NOT the system header at
//! `/usr/include/nfsc/libnfs.h`. Multiple libnfs versions can coexist
//! on the same host with different declarations for the same symbol;
//! verify with `objdump -T` of the linked `.so`. See
//! `docs/CORRECTNESS_RULES.md` "Cross-check C library FFI" and
//! `M2_NOTES.md` "M2/M3 verification incidents" for the failure
//! mode (silent zero-byte data loss).
//!
//! Smoke test (must run as root — VAST export's libnfs auth path
//! requires UID 0, same as the production worker under sudo):
//!   cargo build -p migration-mover --tests
//!   sudo -E target/debug/deps/libnfs_ffi_smoke-*  --ignored --nocapture
//!
//! **Vendored from `nfs-walker`** (<https://github.com/blakegolliher/nfs-walker>, MIT)
//! and extended for the mover's WRITE / SETATTR / RENAME / LINK /
//! SYMLINK / READLINK / UTIMES / MKDIR ops.
//!
//! Preserve the original MIT copyright header on any code copied
//! verbatim. Net-new code carries the workspace's MIT header.
//!
//! ## Why libnfs (user-space)
//!
//! See DESIGN.md "Mover behavior". The current data plane uses libnfs
//! directly, without a kernel NFS mount.
//!
//! ## Concurrency model
//!
//! libnfs contexts are **not** thread-safe. The sync mover lends one
//! source/destination pair per blocking task from `MultiPool`; the async mover
//! gives each context to one service task and issues work through its request
//! channel.

#![allow(non_camel_case_types, dead_code)]

use std::ffi::CStr;
use std::os::raw::{c_char, c_int, c_void};

pub mod asyncio;
pub mod ops;
pub mod pool;
pub mod raw;

pub use pool::{ContextPair, LibnfsContextPool, MultiPool, SimplePool};

// =============================================================================
// Opaque types — defined in libnfs C headers.
// =============================================================================

#[repr(C)]
pub struct nfs_context {
    _private: [u8; 0],
}

#[repr(C)]
pub struct nfsfh {
    _private: [u8; 0],
}

/// libnfs's stat shape. Layout matches `struct nfs_stat_64` in
/// `/usr/local/include/nfsc/libnfs.h`. Duplicated here (deliberately,
/// not re-exported from `asyncio/ffi.rs`) so the sync surface can
/// stand alone — `asyncio/` is gated by the async pre-merge runbook
/// and we don't want sync edits to drag it in.
#[repr(C)]
#[derive(Debug, Clone, Copy, Default)]
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

// =============================================================================
// Bindings the mover needs.
// =============================================================================

extern "C" {
    pub fn nfs_init_context() -> *mut nfs_context;
    pub fn nfs_destroy_context(nfs: *mut nfs_context);

    pub fn nfs_mount(nfs: *mut nfs_context, server: *const c_char, export: *const c_char) -> c_int;
    pub fn nfs_get_error(nfs: *mut nfs_context) -> *const c_char;

    /// Pin the NFS protocol version on a freshly-initialized context.
    /// Returns 0 on success, negative on invalid version. Must be
    /// called before `nfs_mount`. Project mandate is v3 (see
    /// `docs/CORRECTNESS_RULES.md` "NFSv3 is the protocol baseline");
    /// `mount_url` always passes 3.
    pub fn nfs_set_version(nfs: *mut nfs_context, version: c_int) -> c_int;

    pub fn nfs_set_uid(nfs: *mut nfs_context, uid: c_int);
    pub fn nfs_set_gid(nfs: *mut nfs_context, gid: c_int);

    // Read / write
    pub fn nfs_open(
        nfs: *mut nfs_context,
        path: *const c_char,
        flags: c_int,
        fh: *mut *mut nfsfh,
    ) -> c_int;
    pub fn nfs_close(nfs: *mut nfs_context, fh: *mut nfsfh) -> c_int;
    pub fn nfs_pread(
        nfs: *mut nfs_context,
        fh: *mut nfsfh,
        buf: *mut c_void,
        count: usize,
        offset: u64,
    ) -> c_int;
    pub fn nfs_pwrite(
        nfs: *mut nfs_context,
        fh: *mut nfsfh,
        buf: *const c_void,
        count: usize,
        offset: u64,
    ) -> c_int;
    /// Open-or-create with both flags and mode. We use `nfs_open2`
    /// rather than `nfs_create` because some libnfs builds (including
    /// the one we link against in dev) export only `nfs_creat` (which
    /// drops the `flags` argument). `nfs_open2` is the supported
    /// way to combine `O_WRONLY|O_CREAT|O_TRUNC` with an explicit mode.
    pub fn nfs_open2(
        nfs: *mut nfs_context,
        path: *const c_char,
        flags: c_int,
        mode: c_int,
        fh: *mut *mut nfsfh,
    ) -> c_int;
    pub fn nfs_unlink(nfs: *mut nfs_context, path: *const c_char) -> c_int;
    pub fn nfs_rename(
        nfs: *mut nfs_context,
        oldpath: *const c_char,
        newpath: *const c_char,
    ) -> c_int;
    pub fn nfs_mkdir2(nfs: *mut nfs_context, path: *const c_char, mode: c_int) -> c_int;

    // Links
    pub fn nfs_link(nfs: *mut nfs_context, oldpath: *const c_char, newpath: *const c_char)
        -> c_int;
    pub fn nfs_symlink(
        nfs: *mut nfs_context,
        target: *const c_char,
        linkpath: *const c_char,
    ) -> c_int;
    pub fn nfs_readlink(
        nfs: *mut nfs_context,
        path: *const c_char,
        buf: *mut c_char,
        bufsize: c_int,
    ) -> c_int;

    // Attributes
    pub fn nfs_chmod(nfs: *mut nfs_context, path: *const c_char, mode: c_int) -> c_int;
    pub fn nfs_chown(nfs: *mut nfs_context, path: *const c_char, uid: c_int, gid: c_int) -> c_int;
    pub fn nfs_fchmod(nfs: *mut nfs_context, nfsfh: *mut nfsfh, mode: c_int) -> c_int;
    pub fn nfs_fchown(nfs: *mut nfs_context, nfsfh: *mut nfsfh, uid: c_int, gid: c_int) -> c_int;
    /// `times` points to an array of two `struct timeval` —
    /// `[atime, mtime]`. Sub-second precision is microseconds; the
    /// nanosecond columns in the index are truncated and the precision
    /// loss is documented in `M2_NOTES.md`.
    pub fn nfs_utimes(
        nfs: *mut nfs_context,
        path: *const c_char,
        times: *mut libc::timeval,
    ) -> c_int;
    /// Symlink-aware utimes — sets atime + mtime on the link itself
    /// rather than its target. Same `[atime, mtime]` timeval layout as
    /// `nfs_utimes`; same µs-precision ceiling. libnfs 1.16 has no
    /// ns-precision (`lutimens`) variant — neither this build nor
    /// upstream master, see `docs/work-items/MTIME_PARITY_FIX.md`.
    pub fn nfs_lutimes(
        nfs: *mut nfs_context,
        path: *const c_char,
        times: *mut libc::timeval,
    ) -> c_int;
    /// Sync stat — fills in `nfs_stat_64` for `path`. Currently used
    /// by the end-of-run root-dir mtime restore (see
    /// `Mover::restore_root_mtime` / orchestrator slice 3) which
    /// source-stats the migration root once at shutdown.
    pub fn nfs_stat64(nfs: *mut nfs_context, path: *const c_char, st: *mut nfs_stat_64) -> c_int;
}

// =============================================================================
// PROTECTED_FFI_BATCH additions (F12/F09). Both signatures verified
// 2026-07-30 as byte-identical between the pinned source tree
// (~/projects/libnfs, tag libnfs-6.0.2-148-gdc7e6f8), the installed
// header (/usr/local/include/nfsc/libnfs.h), and exported by the
// linked /usr/local/lib/libnfs.so.16.0.2:
//
//   grep -n "nfs_fsync(\|nfs_set_timeout(" /usr/local/include/nfsc/libnfs.h \
//       ~/projects/libnfs/include/nfsc/libnfs.h | grep -v async
//   nm -D /usr/local/lib/libnfs.so.16.0.2 | grep -wE "nfs_fsync|nfs_set_timeout"
//
// Kept in their own block so the long-verified block above stays
// textually untouched.
// =============================================================================

extern "C" {
    /// F12: per-RPC timeout in milliseconds for every subsequent RPC
    /// on this context (`lib/libnfs.c:nfs_set_timeout` — stores the
    /// value in both `nfs_context_internal` and the rpc context, so
    /// the MOUNT dance is bounded too when called pre-mount). The
    /// pinned tree's built-in default is 60_000 ms
    /// (`lib/init.c: rpc->timeout = 60 * 1000`); timed-out RPCs fire
    /// their callback with `-EINTR` / `"Command timed out"`.
    pub fn nfs_set_timeout(nfs: *mut nfs_context, milliseconds: c_int);

    /// F09: sync whole-file NFS COMMIT for an open fh. Drives
    /// `nfs_fsync_async` → COMMIT3 with `offset = 0, count = 0`
    /// (`lib/nfs_v3.c:nfs3_fsync_async`) and waits for the reply.
    /// Returns 0 on success, negative `-errno` on failure — same
    /// convention as the rest of the sync surface. The write path
    /// needs it because the linked libnfs issues WRITEs UNSTABLE
    /// unless the fh was opened `O_SYNC`
    /// (`lib/nfs_v3.c:nfs3_fill_WRITE3args`).
    pub fn nfs_fsync(nfs: *mut nfs_context, nfsfh: *mut nfsfh) -> c_int;
}

/// F12 default per-RPC timeout, in milliseconds. Matches the pinned
/// libnfs's implicit default (`lib/init.c`), now explicit at every
/// context-creation point and configurable via
/// `[mover] rpc_timeout_ms`. `0` means "leave the library default
/// untouched" — see [`effective_rpc_timeout`].
pub const DEFAULT_RPC_TIMEOUT_MS: u32 = 60_000;

/// Pure seam for the F12 timeout policy: what value, if any, should
/// be passed to `nfs_set_timeout` for a configured `rpc_timeout_ms`.
///
/// - `0` → `None`: do not call `nfs_set_timeout` at all (the
///   documented "leave the libnfs built-in default untouched" value).
/// - anything else → `Some(ms)`, clamped to `c_int::MAX` because the
///   FFI takes a C `int` and libnfs treats a negative timeout as
///   "no timeout" — a silent wrap would disable the bound the
///   operator asked for.
pub fn effective_rpc_timeout(rpc_timeout_ms: u32) -> Option<c_int> {
    match rpc_timeout_ms {
        0 => None,
        ms => Some(ms.min(c_int::MAX as u32) as c_int),
    }
}

/// Apply the configured per-RPC timeout to a freshly created context.
/// Must run before `nfs_mount` / `nfs_mount_async` so the mount's own
/// RPCs are bounded as well. No-op when `rpc_timeout_ms == 0`.
///
/// Deliberately not `unsafe fn` (same rationale as [`last_error`]):
/// every caller passes a context pointer it owns, and the body
/// null-checks before handing it to the FFI.
#[allow(clippy::not_unsafe_ptr_arg_deref)]
pub(crate) fn apply_rpc_timeout(ctx: *mut nfs_context, rpc_timeout_ms: u32) {
    if ctx.is_null() {
        return;
    }
    if let Some(ms) = effective_rpc_timeout(rpc_timeout_ms) {
        unsafe { nfs_set_timeout(ctx, ms) };
    }
}

/// Last error string from a context, as a borrowed `&str`.
///
/// Deliberately not `unsafe fn`: every caller passes a context pointer
/// it owns (or just null-checked), and the body null-checks both the
/// context and the returned string before dereferencing.
#[allow(clippy::not_unsafe_ptr_arg_deref)]
pub fn last_error<'a>(ctx: *mut nfs_context) -> &'a str {
    if ctx.is_null() {
        return "<null context>";
    }
    unsafe {
        let p = nfs_get_error(ctx);
        if p.is_null() {
            "<no error>"
        } else {
            CStr::from_ptr(p).to_str().unwrap_or("<non-utf8>")
        }
    }
}

// =============================================================================
// NfsContext — owned, Send, single-threaded use enforced by &mut.
// =============================================================================

/// An owned libnfs context. `Send` because libnfs contexts can be moved
/// between threads as long as no two threads use one *concurrently*.
/// Single-threaded use is enforced by callers taking `&mut NfsContext`.
pub struct NfsContext {
    raw: *mut nfs_context,
    /// Which server this context is mounted against — tags every raw
    /// RPC's latency sample. Defaults to `Src`; the pool sets it.
    side: migration_core::latency::Side,
}

unsafe impl Send for NfsContext {}

impl NfsContext {
    /// Build and mount a new context against `nfs://server/export`.
    /// Pins NFSv3 unconditionally (`docs/CORRECTNESS_RULES.md`
    /// "NFSv3 is the protocol baseline") — without this, the libnfs build at
    /// `/usr/local/lib/libnfs.so.16` negotiates v4 by default on a
    /// bare URL and crashes in `nfs4_mount_1_cb` against VAST.
    /// Failure to init, set version, or mount returns an error
    /// containing the libnfs error string.
    ///
    /// `rpc_timeout_ms` (F12) is applied immediately after the
    /// context is created, before the mount, so every RPC — the mount
    /// dance included — is bounded. `0` leaves the libnfs built-in
    /// default untouched. The parameter is non-optional by design:
    /// no creation point may forget the decision.
    pub fn mount_url(url: &str, rpc_timeout_ms: u32) -> anyhow::Result<Self> {
        let (server, export) = parse_nfs_url(url)?;
        let raw = unsafe { nfs_init_context() };
        if raw.is_null() {
            anyhow::bail!("nfs_init_context returned null for {url}");
        }
        let me = Self {
            raw,
            side: migration_core::latency::Side::Src,
        };
        // F12: bound every RPC on this context (including the mount).
        apply_rpc_timeout(me.raw, rpc_timeout_ms);
        let rc = unsafe { nfs_set_version(me.raw, 3) };
        if rc < 0 {
            let err = last_error(me.raw).to_string();
            anyhow::bail!("nfs_set_version(3) for {url} failed (rc={rc}): {err}");
        }
        let server_c = std::ffi::CString::new(server.clone())
            .map_err(|_| anyhow::anyhow!("server has interior NUL: {server}"))?;
        let export_c = std::ffi::CString::new(export.clone())
            .map_err(|_| anyhow::anyhow!("export has interior NUL: {export}"))?;
        let rc = unsafe { nfs_mount(me.raw, server_c.as_ptr(), export_c.as_ptr()) };
        if rc < 0 {
            let err = last_error(me.raw).to_string();
            anyhow::bail!("nfs_mount {url} failed (rc={rc}): {err}");
        }
        Ok(me)
    }

    /// Raw pointer for FFI calls. Caller must not call concurrently
    /// from another thread.
    #[inline]
    /// Tag this context as the source or destination side for latency
    /// accounting.
    pub fn set_side(&mut self, side: migration_core::latency::Side) {
        self.side = side;
    }

    pub fn side(&self) -> migration_core::latency::Side {
        self.side
    }

    pub fn raw(&mut self) -> *mut nfs_context {
        self.raw
    }
}

impl Drop for NfsContext {
    fn drop(&mut self) {
        if !self.raw.is_null() {
            unsafe { nfs_destroy_context(self.raw) };
            self.raw = std::ptr::null_mut();
        }
    }
}

/// Parse an `nfs://server/export[/...]` URL into (server, export).
/// The export keeps its leading slash so it can be passed to
/// `nfs_mount` directly.
pub fn parse_nfs_url(url: &str) -> anyhow::Result<(String, String)> {
    let rest = url
        .strip_prefix("nfs://")
        .ok_or_else(|| anyhow::anyhow!("nfs URL must start with nfs://: {url}"))?;
    let slash = rest
        .find('/')
        .ok_or_else(|| anyhow::anyhow!("nfs URL must include export path: {url}"))?;
    let server = rest[..slash].to_string();
    let export = rest[slash..].to_string();
    if server.is_empty() {
        anyhow::bail!("nfs URL has empty server: {url}");
    }
    if export == "/" || export.is_empty() {
        // libnfs accepts "/" for some servers but most VAST exports
        // look like "/exportname". Allow but warn.
        tracing::warn!(
            url,
            "nfs URL export path is '/'; this may not be what you meant"
        );
    }
    Ok((server, export))
}

// =============================================================================
// Errno → name mapping. Used by ops::* to populate MoveError.error with
// stable strings the failure log + retry tooling can match on.
// =============================================================================

/// Convert a positive errno to its conventional name. Falls back to
/// `errno=<n>` for codes the mover hasn't seen before, so the failure
/// log is never lossy.
pub fn errno_name(err: i32) -> String {
    let s = match err {
        libc::EPERM => "EPERM",
        libc::ENOENT => "ENOENT",
        // libnfs cancels RPCs that exceed the F12 per-RPC timeout
        // with -EINTR ("Command timed out"), so timeout failures in
        // the published JSONL carry this name, not `errno=4`.
        libc::EINTR => "EINTR",
        libc::EIO => "EIO",
        libc::EBADF => "EBADF",
        libc::EACCES => "EACCES",
        libc::EEXIST => "EEXIST",
        libc::EXDEV => "EXDEV",
        libc::ENOTDIR => "ENOTDIR",
        libc::EISDIR => "EISDIR",
        libc::EINVAL => "EINVAL",
        libc::ENFILE => "ENFILE",
        libc::EMFILE => "EMFILE",
        libc::EFBIG => "EFBIG",
        libc::ENOSPC => "ENOSPC",
        libc::EROFS => "EROFS",
        libc::EMLINK => "EMLINK",
        libc::EPIPE => "EPIPE",
        libc::ENAMETOOLONG => "ENAMETOOLONG",
        libc::ENOSYS => "ENOSYS",
        libc::ENOTEMPTY => "ENOTEMPTY",
        libc::ELOOP => "ELOOP",
        libc::ESTALE => "ESTALE",
        libc::EDQUOT => "EDQUOT",
        _ => return format!("errno={err}"),
    };
    s.to_string()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_nfs_url_ok() {
        let (s, e) = parse_nfs_url("nfs://10.0.0.1/export/data").unwrap();
        assert_eq!(s, "10.0.0.1");
        assert_eq!(e, "/export/data");
    }

    #[test]
    fn parse_nfs_url_rejects_non_nfs_scheme() {
        assert!(parse_nfs_url("https://example/").is_err());
    }

    #[test]
    fn parse_nfs_url_rejects_empty_server() {
        assert!(parse_nfs_url("nfs:///export").is_err());
    }

    #[test]
    fn parse_nfs_url_rejects_no_export() {
        assert!(parse_nfs_url("nfs://server").is_err());
    }

    // ---- F12: per-RPC timeout seam ---------------------------------

    /// `0` means "leave the libnfs default untouched": the wrapper
    /// must NOT call `nfs_set_timeout` at all. Everything else maps
    /// to `Some(ms)` for the FFI call.
    #[test]
    fn effective_rpc_timeout_zero_skips_the_call() {
        assert_eq!(effective_rpc_timeout(0), None);
    }

    #[test]
    fn effective_rpc_timeout_default_is_60000() {
        assert_eq!(effective_rpc_timeout(DEFAULT_RPC_TIMEOUT_MS), Some(60_000));
        assert_eq!(DEFAULT_RPC_TIMEOUT_MS, 60_000);
    }

    /// `nfs_set_timeout` takes a C `int`; a u32 config value above
    /// `c_int::MAX` must clamp rather than wrap negative (libnfs
    /// treats timeout < 0 as "no timeout", which would silently
    /// disable the bound the operator asked for).
    #[test]
    fn effective_rpc_timeout_clamps_to_c_int_max() {
        assert_eq!(
            effective_rpc_timeout(u32::MAX),
            Some(std::os::raw::c_int::MAX)
        );
    }

    #[test]
    fn errno_name_known() {
        assert_eq!(errno_name(libc::ENOSPC), "ENOSPC");
        assert_eq!(errno_name(libc::EPERM), "EPERM");
    }

    #[test]
    fn errno_name_unknown() {
        assert_eq!(errno_name(9999), "errno=9999");
    }

    /// BETA_POLISH_BATCH Item 3: F12 timeout failures surface errno 4
    /// (libnfs cancels timed-out RPCs with -EINTR). Without an EINTR
    /// arm the published failures JSONL records the lossy numeric
    /// fallback `errno=4` — and beta freezes that wire format.
    #[test]
    fn errno_4_names_eintr() {
        assert_eq!(libc::EINTR, 4, "EINTR is errno 4 on Linux");
        assert_eq!(errno_name(libc::EINTR), "EINTR");
    }
}
