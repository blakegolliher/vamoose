//! Async libnfs surface.
//!
//! Each `AsyncNfsContext` owns one `nfs_context` and one tokio
//! service task. The service task is the only entity that may call
//! libnfs functions against the context (libnfs contexts are not
//! thread-safe). Callers issue requests via an mpsc channel; results
//! arrive on per-request oneshots. See
//! `docs/work-items/LIBNFS_ASYNC_FORK.md` for the design rationale
//! and `docs/work-items/LIBNFS_ASYNC_FORK_AUDIT.md` for the symbol
//! mapping that lets us bind without C-side patches.
//!
//! ## Concurrency model
//!
//! - One `nfs_context` per `AsyncNfsContext`.
//! - One service task per context. Owns the context exclusively.
//! - Many concurrent in-flight RPCs per context; libnfs muxes them
//!   over its single TCP connection.
//! - Callbacks fire on the service-task thread inside `nfs_service`;
//!   their only job is to send a result on the matching oneshot.
//!
//! ## Cancellation
//!
//! Dropping a returned future does **not** cancel the underlying
//! RPC — libnfs has no NFSv3 cancel surface. The oneshot result is
//! dropped on completion. Callers that need true cancellation must
//! use a higher-level token + check it themselves.
//!
//! ## What this surface deliberately does NOT do
//!
//! - It does not replace the sync FFI. Existing pass-0 paths continue
//!   to use `crate::libnfs::ops`. The async surface is additive.
//! - It does not assemble a bucketed pool. That's
//!   `docs/work-items/MULTI_PASS_MOVER.md`.

pub mod callbacks;
pub mod driver;
pub mod error;
pub mod ffi;
pub mod request;

pub use error::NfsError;
pub use request::NfsStat64;

use crate::libnfs::{last_error, nfs_init_context, nfs_set_version, nfsfh, parse_nfs_url};
use crate::paths::cstr_from_bytes;
use request::{RawFh, Request};
use std::ffi::CString;
use std::os::raw::c_int;
use std::sync::{Arc, Weak};
use tokio::sync::{mpsc, oneshot, Mutex};

/// Bound on the in-flight request channel between caller tasks and
/// the service task. This is a back-pressure knob, not a correctness
/// knob — the multi-pass mover's per-file pipeline caps in-flight
/// well below this. Default sized for 1024 outstanding requests; bump
/// if you see callers blocking on `req_tx.send().await`.
const REQUEST_CHANNEL_CAP: usize = 1024;

/// Open flags. Wraps the libc `O_*` set. Includes `O_SYNC` (which
/// libnfs translates to per-write FILE_SYNC on the resulting fh —
/// see `nfs_v3.c:nfs3_open_async_cb`); use `RDWR_SYNC` / `WRONLY_SYNC`
/// when you want every write to be stable (no separate COMMIT).
#[derive(Debug, Clone, Copy)]
pub struct Flags(c_int);

impl Flags {
    pub const fn rdonly() -> Self {
        Self(libc::O_RDONLY)
    }
    pub const fn wronly() -> Self {
        Self(libc::O_WRONLY)
    }
    pub const fn rdwr() -> Self {
        Self(libc::O_RDWR)
    }
    pub fn wronly_sync() -> Self {
        Self(libc::O_WRONLY | libc::O_SYNC)
    }
    pub fn raw(self) -> c_int {
        self.0
    }
    pub fn with_create(self) -> Self {
        Self(self.0 | libc::O_CREAT | libc::O_TRUNC)
    }
}

/// Mount-time tunables. Maps onto whatever combination of `nfs_set_*`
/// calls and URL options the linked libnfs supports — see
/// `LIBNFS_ASYNC_FORK_AUDIT.md` for the per-knob mapping.
#[derive(Debug, Clone)]
pub struct MountOpts {
    /// Per-RPC READ size in bytes. Mapped to `nfs_set_readmax`.
    pub rsize: u32,
    /// Per-RPC WRITE size in bytes. Mapped to `nfs_set_writemax`.
    pub wsize: u32,
    /// Number of TCP connections per context. **Linked libnfs (v6)
    /// does not support this**; values > 1 are rejected at mount
    /// time with `NfsError::UnsupportedOpt`. The field exists for
    /// forward compatibility with upstream-supplied nconnect.
    pub nconnect: u32,
    /// NFS protocol version. Hard-pinned to 3 per
    /// `docs/CORRECTNESS_RULES.md` "NFSv3 is the protocol baseline".
    /// Setting any other value returns `UnsupportedOpt`.
    pub version: u8,
    /// F12: per-RPC timeout in milliseconds, applied via
    /// `nfs_set_timeout` immediately after the context is created —
    /// before the mount, so the mount dance is bounded too. `0`
    /// leaves the libnfs built-in default untouched (the call is
    /// skipped; see `crate::libnfs::effective_rpc_timeout`). Timed
    /// -out RPCs complete with `-EINTR` / `"Command timed out"`.
    pub rpc_timeout_ms: u32,
}

impl Default for MountOpts {
    fn default() -> Self {
        Self {
            rsize: 1024 * 1024,
            wsize: 1024 * 1024,
            nconnect: 1,
            version: 3,
            rpc_timeout_ms: crate::libnfs::DEFAULT_RPC_TIMEOUT_MS,
        }
    }
}

impl MountOpts {
    fn validate(&self) -> Result<(), NfsError> {
        if self.version != 3 {
            return Err(NfsError::UnsupportedOpt(format!(
                "version={} (project pins NFSv3; see CORRECTNESS_RULES.md)",
                self.version
            )));
        }
        if self.nconnect > 1 {
            return Err(NfsError::UnsupportedOpt(format!(
                "nconnect={} not supported by linked libnfs; see \
                 LIBNFS_ASYNC_FORK_AUDIT.md (#1)",
                self.nconnect
            )));
        }
        if self.rsize == 0 {
            return Err(NfsError::UnsupportedOpt("rsize=0".into()));
        }
        if self.wsize == 0 {
            return Err(NfsError::UnsupportedOpt("wsize=0".into()));
        }
        Ok(())
    }
}

/// Async-borrowed file handle. Closes via `ctx.close(fh).await` —
/// dropping without closing leaks the libnfs fh because close
/// requires a roundtrip and Drop can't await. The integration tests
/// document the leak behavior.
pub struct AsyncNfsFh {
    raw: *mut nfsfh,
    /// Set to true once `close()` has accepted the handle, so Drop
    /// can warn loudly about leaks rather than silently leaking.
    consumed: bool,
    /// Weak ref to the context that produced this fh. Drop uses it to
    /// suppress the leak warning when the context is already gone —
    /// `nfs_destroy_context` cleans up associated state, so the
    /// "leak" doesn't survive context teardown and the warning would
    /// just be noise.
    ctx: Weak<Inner>,
}

// Safe to move across threads — the raw pointer is used only by the
// service task (we send it back through the request mpsc).
unsafe impl Send for AsyncNfsFh {}
unsafe impl Sync for AsyncNfsFh {}

impl AsyncNfsFh {
    fn new(raw: *mut nfsfh, ctx: Weak<Inner>) -> Self {
        Self {
            raw,
            consumed: false,
            ctx,
        }
    }
    fn raw(&self) -> *mut nfsfh {
        self.raw
    }
}

impl Drop for AsyncNfsFh {
    fn drop(&mut self) {
        if self.consumed || self.raw.is_null() {
            return;
        }
        if self.ctx.upgrade().is_none() {
            // Context is already torn down; nfs_destroy_context has
            // cleaned up any libnfs-side state for this fh.
            return;
        }
        // Context is still alive, the caller forgot to call `close()`,
        // and we can't issue an async RPC from Drop. Surface a warning
        // and accept the libnfs-side leak.
        tracing::warn!(
            fh = ?self.raw,
            "AsyncNfsFh dropped without close(); libnfs handle leaked. \
             Call AsyncNfsContext::close(fh).await before drop."
        );
    }
}

/// Public handle to an async-driven libnfs context. Cloneable; clones
/// share the same service task. Drop the last clone (and any in-flight
/// futures) to trigger graceful shutdown.
#[derive(Clone, Debug)]
pub struct AsyncNfsContext {
    inner: Arc<Inner>,
}

impl std::fmt::Debug for Inner {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AsyncNfsContext::Inner")
            .field("req_tx_capacity", &self.req_tx.capacity())
            .finish()
    }
}

struct Inner {
    req_tx: mpsc::Sender<Request>,
    // Held so the service task can be awaited on shutdown by callers
    // that care (currently just tests). The mutex is uncontended
    // outside of that one path.
    join: Mutex<Option<tokio::task::JoinHandle<Result<(), NfsError>>>>,
}

impl Drop for Inner {
    fn drop(&mut self) {
        // Best-effort: tell the service task to wind down. The
        // mpsc::Sender is dropped as part of the Inner drop, which
        // also signals shutdown to the receiver.
        let _ = self.req_tx.try_send(Request::Shutdown);
    }
}

impl AsyncNfsContext {
    /// Mount a new async context against `nfs://server/export`.
    ///
    /// Steps (each one failure-routed back to the caller):
    /// 1. Parse the URL into server/export bytes.
    /// 2. Validate `MountOpts` (version, nconnect, rsize/wsize).
    /// 3. `nfs_init_context`, then `nfs_set_timeout(rpc_timeout_ms)`
    ///    (F12; skipped when 0) so the mount itself is bounded.
    /// 4. `nfs_set_version(3)`.
    /// 5. `nfs_set_readmax(rsize)`, `nfs_set_writemax(wsize)`.
    /// 6. `nfs_get_fd` — capture the socket fd to drive readiness.
    /// 7. Spawn the service task.
    /// 8. Send `Request::Mount` to it and await completion.
    ///
    /// On any failure between step 3 and step 7, we destroy the
    /// context to avoid leaks before returning.
    pub async fn mount(url: &str, opts: MountOpts) -> Result<Self, NfsError> {
        opts.validate()?;
        let (server, export) =
            parse_nfs_url(url).map_err(|e| NfsError::InvalidPath(e.to_string()))?;
        let server_c = CString::new(server.clone())
            .map_err(|_| NfsError::InvalidPath(format!("server has interior NUL: {server}")))?;
        let export_c = CString::new(export.clone())
            .map_err(|_| NfsError::InvalidPath(format!("export has interior NUL: {export}")))?;

        let raw = unsafe { nfs_init_context() };
        if raw.is_null() {
            return Err(NfsError::Init(format!(
                "nfs_init_context returned NULL for {url}"
            )));
        }

        // F12: bound every RPC on this context (the mount included).
        // Applied immediately after creation; skipped when the
        // configured value is 0 ("leave the library default").
        crate::libnfs::apply_rpc_timeout(raw, opts.rpc_timeout_ms);

        // Helper: destroy the context if we bail out before the
        // service task takes ownership.
        let cleanup = |raw: *mut crate::libnfs::nfs_context| {
            if !raw.is_null() {
                unsafe { crate::libnfs::nfs_destroy_context(raw) };
            }
        };

        let rc = unsafe { nfs_set_version(raw, opts.version as c_int) };
        if rc < 0 {
            let detail = last_error(raw).to_string();
            cleanup(raw);
            return Err(NfsError::Init(format!(
                "nfs_set_version({}) failed (rc={rc}): {detail}",
                opts.version
            )));
        }

        // Apply tunables BEFORE mount per libnfs's documented order.
        unsafe { ffi::nfs_set_readmax(raw, opts.rsize as usize) };
        unsafe { ffi::nfs_set_writemax(raw, opts.wsize as usize) };

        // Grab the fd *after* version is set; libnfs lazily creates
        // the transport when needed. v3 path: fd is available now.
        let fd = unsafe { ffi::nfs_get_fd(raw) };
        if fd < 0 {
            // libnfs may not allocate the socket until mount is
            // initiated. Issue a 0-byte tickle by calling mount which
            // creates the connection. We can't easily refactor this
            // step out without restructuring; just plumb the fd
            // through after the service task starts.
            // For libnfs v6 / v5, `nfs_get_fd` returns -1 until a
            // socket exists. We tolerate that and let the service
            // task fetch the fd after the mount fires the first RPC.
            tracing::debug!(
                "nfs_get_fd returned {} pre-mount; will resolve in service task",
                fd
            );
        }

        let (req_tx, req_rx) = mpsc::channel::<Request>(REQUEST_CHANNEL_CAP);
        let owned = driver::OwnedContext(raw);
        let join = tokio::spawn(driver::run_with_lazy_fd(owned, req_rx));

        let inner = Arc::new(Inner {
            req_tx: req_tx.clone(),
            join: Mutex::new(Some(join)),
        });
        let me = AsyncNfsContext {
            inner: inner.clone(),
        };

        // Issue the mount via the service task. The task will pick
        // up the libnfs fd as part of starting the loop; we rely on
        // the AsyncFd re-registration logic in `driver::run_with_lazy_fd`.
        let (tx, rx) = oneshot::channel();
        if req_tx
            .send(Request::Mount {
                server: server_c,
                export: export_c,
                tx,
            })
            .await
            .is_err()
        {
            return Err(NfsError::Closed);
        }
        match rx.await {
            Ok(Ok(())) => Ok(me),
            Ok(Err(e)) => Err(e),
            Err(_) => Err(NfsError::Closed),
        }
    }

    /// Wait for the service task to exit. Useful in tests and at
    /// program shutdown when graceful drain matters.
    pub async fn shutdown(self) -> Result<(), NfsError> {
        let _ = self.inner.req_tx.send(Request::Shutdown).await;
        // Take the join handle out so we can await it.
        let mut guard = self.inner.join.lock().await;
        if let Some(j) = guard.take() {
            match j.await {
                Ok(r) => r,
                Err(_) => Err(NfsError::Closed),
            }
        } else {
            Ok(())
        }
    }

    async fn send_unit<F>(&self, build: F) -> Result<(), NfsError>
    where
        F: FnOnce(oneshot::Sender<Result<(), NfsError>>) -> Request,
    {
        let (tx, rx) = oneshot::channel();
        self.inner
            .req_tx
            .send(build(tx))
            .await
            .map_err(|_| NfsError::Closed)?;
        rx.await.map_err(|_| NfsError::Closed)?
    }

    pub async fn open(&self, path: &[u8], flags: Flags) -> Result<AsyncNfsFh, NfsError> {
        let path = path_to_cstring(path)?;
        let (tx, rx) = oneshot::channel();
        self.inner
            .req_tx
            .send(Request::Open {
                path,
                flags: flags.raw(),
                tx,
            })
            .await
            .map_err(|_| NfsError::Closed)?;
        let raw = rx.await.map_err(|_| NfsError::Closed)??;
        Ok(AsyncNfsFh::new(raw.0, Arc::downgrade(&self.inner)))
    }

    /// Open-or-create a file with explicit flags+mode. Used for the
    /// destination `.partial` (`O_WRONLY | O_CREAT | O_TRUNC`).
    pub async fn create(
        &self,
        path: &[u8],
        flags: Flags,
        mode: u32,
    ) -> Result<AsyncNfsFh, NfsError> {
        let path = path_to_cstring(path)?;
        let (tx, rx) = oneshot::channel();
        self.inner
            .req_tx
            .send(Request::Open2 {
                path,
                flags: flags.raw(),
                mode: mode as c_int,
                tx,
            })
            .await
            .map_err(|_| NfsError::Closed)?;
        let raw = rx.await.map_err(|_| NfsError::Closed)??;
        Ok(AsyncNfsFh::new(raw.0, Arc::downgrade(&self.inner)))
    }

    pub async fn close(&self, mut fh: AsyncNfsFh) -> Result<(), NfsError> {
        fh.consumed = true;
        let raw = RawFh(fh.raw());
        self.send_unit(|tx| Request::Close { fh: raw, tx }).await
    }

    pub async fn pread(
        &self,
        fh: &AsyncNfsFh,
        offset: u64,
        len: usize,
    ) -> Result<Vec<u8>, NfsError> {
        let (tx, rx) = oneshot::channel();
        self.inner
            .req_tx
            .send(Request::Pread {
                fh: RawFh(fh.raw()),
                offset,
                len,
                tx,
            })
            .await
            .map_err(|_| NfsError::Closed)?;
        rx.await.map_err(|_| NfsError::Closed)?
    }

    /// Asynchronous write. Stability is whatever the fh was opened
    /// with — open with `Flags::wronly_sync()` (or any flag that
    /// includes `O_SYNC`) to get FILE_SYNC; otherwise the write is
    /// UNSTABLE and you need a follow-up `fsync` to commit.
    pub async fn pwrite(
        &self,
        fh: &AsyncNfsFh,
        offset: u64,
        buf: Vec<u8>,
    ) -> Result<usize, NfsError> {
        let (tx, rx) = oneshot::channel();
        self.inner
            .req_tx
            .send(Request::Pwrite {
                fh: RawFh(fh.raw()),
                offset,
                buf,
                tx,
            })
            .await
            .map_err(|_| NfsError::Closed)?;
        rx.await.map_err(|_| NfsError::Closed)?
    }

    /// NFS COMMIT / fsync. Whole-file in this libnfs (no per-range
    /// support; see `LIBNFS_ASYNC_FORK_AUDIT.md` #2).
    pub async fn fsync(&self, fh: &AsyncNfsFh) -> Result<(), NfsError> {
        self.send_unit(|tx| Request::Fsync {
            fh: RawFh(fh.raw()),
            tx,
        })
        .await
    }

    pub async fn stat(&self, path: &[u8]) -> Result<NfsStat64, NfsError> {
        let path = path_to_cstring(path)?;
        let (tx, rx) = oneshot::channel();
        self.inner
            .req_tx
            .send(Request::Stat64 { path, tx })
            .await
            .map_err(|_| NfsError::Closed)?;
        rx.await.map_err(|_| NfsError::Closed)?
    }

    pub async fn fstat(&self, fh: &AsyncNfsFh) -> Result<NfsStat64, NfsError> {
        let (tx, rx) = oneshot::channel();
        self.inner
            .req_tx
            .send(Request::Fstat64 {
                fh: RawFh(fh.raw()),
                tx,
            })
            .await
            .map_err(|_| NfsError::Closed)?;
        rx.await.map_err(|_| NfsError::Closed)?
    }

    pub async fn unlink(&self, path: &[u8]) -> Result<(), NfsError> {
        let path = path_to_cstring(path)?;
        self.send_unit(|tx| Request::Unlink { path, tx }).await
    }

    pub async fn rename(&self, oldpath: &[u8], newpath: &[u8]) -> Result<(), NfsError> {
        let oldpath = path_to_cstring(oldpath)?;
        let newpath = path_to_cstring(newpath)?;
        self.send_unit(|tx| Request::Rename {
            oldpath,
            newpath,
            tx,
        })
        .await
    }

    /// Set atime + mtime via `nfs_utimes_async`. Microsecond
    /// precision — nanosecond inputs are truncated (M2 documented
    /// limitation).
    pub async fn utimes(
        &self,
        path: &[u8],
        atime_sec: i64,
        atime_nsec: i32,
        mtime_sec: i64,
        mtime_nsec: i32,
    ) -> Result<(), NfsError> {
        let path = path_to_cstring(path)?;
        let times = build_timeval_pair(atime_sec, atime_nsec, mtime_sec, mtime_nsec);
        self.send_unit(|tx| Request::Utimes { path, times, tx })
            .await
    }

    /// Symlink-aware `utimes` — sets atime + mtime on the link itself
    /// rather than its target. µs-precision (libnfs has no
    /// ns-precision variant, see
    /// `docs/work-items/MTIME_PARITY_FIX.md`).
    pub async fn lutimes(
        &self,
        path: &[u8],
        atime_sec: i64,
        atime_nsec: i32,
        mtime_sec: i64,
        mtime_nsec: i32,
    ) -> Result<(), NfsError> {
        let path = path_to_cstring(path)?;
        let times = build_timeval_pair(atime_sec, atime_nsec, mtime_sec, mtime_nsec);
        self.send_unit(|tx| Request::Lutimes { path, times, tx })
            .await
    }

    pub async fn chmod(&self, path: &[u8], mode: u32) -> Result<(), NfsError> {
        let path = path_to_cstring(path)?;
        self.send_unit(|tx| Request::Chmod {
            path,
            mode: mode as c_int,
            tx,
        })
        .await
    }

    pub async fn chown(&self, path: &[u8], uid: u32, gid: u32) -> Result<(), NfsError> {
        let path = path_to_cstring(path)?;
        self.send_unit(|tx| Request::Chown {
            path,
            uid: uid as c_int,
            gid: gid as c_int,
            tx,
        })
        .await
    }

    pub async fn symlink(&self, target: &[u8], linkname: &[u8]) -> Result<(), NfsError> {
        let target = path_to_cstring(target)?;
        let linkname = path_to_cstring(linkname)?;
        self.send_unit(|tx| Request::Symlink {
            target,
            linkname,
            tx,
        })
        .await
    }

    pub async fn link(&self, oldpath: &[u8], newpath: &[u8]) -> Result<(), NfsError> {
        let oldpath = path_to_cstring(oldpath)?;
        let newpath = path_to_cstring(newpath)?;
        self.send_unit(|tx| Request::Link {
            oldpath,
            newpath,
            tx,
        })
        .await
    }

    /// Create a single directory. `mode` is the initial mode (subject
    /// to umask on the server side).
    pub async fn mkdir(&self, path: &[u8], mode: u32) -> Result<(), NfsError> {
        let path = path_to_cstring(path)?;
        self.send_unit(|tx| Request::Mkdir2 {
            path,
            mode: mode as c_int,
            tx,
        })
        .await
    }

    pub async fn readlink(&self, path: &[u8]) -> Result<Vec<u8>, NfsError> {
        let path = path_to_cstring(path)?;
        let (tx, rx) = oneshot::channel();
        self.inner
            .req_tx
            .send(Request::Readlink { path, tx })
            .await
            .map_err(|_| NfsError::Closed)?;
        rx.await.map_err(|_| NfsError::Closed)?
    }

    /// Snapshot of in-flight RPC count on this context. Advisory only
    /// — for tests and observability, not load-bearing for any
    /// correctness gate.
    pub async fn queue_length(&self) -> Result<usize, NfsError> {
        let (tx, rx) = oneshot::channel();
        self.inner
            .req_tx
            .send(Request::QueueLen { tx })
            .await
            .map_err(|_| NfsError::Closed)?;
        rx.await.map_err(|_| NfsError::Closed)
    }
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

fn path_to_cstring(path: &[u8]) -> Result<CString, NfsError> {
    // Reuse the same path-validation that the sync ops layer uses,
    // then unwrap the CString. cstr_from_bytes returns a MoveError;
    // we translate to NfsError::InvalidPath.
    let c = cstr_from_bytes(path).map_err(|e| {
        NfsError::InvalidPath(format!(
            "path={:?}: {}",
            String::from_utf8_lossy(path),
            e.error
        ))
    })?;
    Ok(c)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn mount_opts_rejects_v4() {
        let err = MountOpts {
            version: 4,
            ..MountOpts::default()
        }
        .validate()
        .unwrap_err();
        assert!(matches!(err, NfsError::UnsupportedOpt(_)));
        assert!(format!("{err}").contains("version=4"));
    }

    #[test]
    fn mount_opts_rejects_nconnect_gt_one() {
        let err = MountOpts {
            nconnect: 2,
            ..MountOpts::default()
        }
        .validate()
        .unwrap_err();
        assert!(matches!(err, NfsError::UnsupportedOpt(_)));
        assert!(format!("{err}").contains("LIBNFS_ASYNC_FORK_AUDIT"));
    }

    #[test]
    fn mount_opts_accepts_nconnect_one_and_zero() {
        // nconnect = 1 is the default and must pass.
        MountOpts {
            nconnect: 1,
            ..MountOpts::default()
        }
        .validate()
        .expect("nconnect=1 ok");
        // nconnect = 0 also passes (treated as "default"); the gate
        // is specifically ">1" because that's the libnfs-unsupported
        // multi-connection regime.
        MountOpts {
            nconnect: 0,
            ..MountOpts::default()
        }
        .validate()
        .expect("nconnect=0 ok");
    }

    #[test]
    fn mount_opts_rejects_zero_rsize_wsize() {
        let e = MountOpts {
            rsize: 0,
            ..MountOpts::default()
        }
        .validate()
        .unwrap_err();
        assert!(format!("{e}").contains("rsize"));
        let e = MountOpts {
            wsize: 0,
            ..MountOpts::default()
        }
        .validate()
        .unwrap_err();
        assert!(format!("{e}").contains("wsize"));
    }

    /// F12: every async context gets an explicit per-RPC timeout at
    /// creation; the default MountOpts must carry 60_000 ms so no
    /// mount site can accidentally fall back to "whatever the library
    /// does".
    #[test]
    fn mount_opts_default_rpc_timeout_is_60000() {
        assert_eq!(
            MountOpts::default().rpc_timeout_ms,
            crate::libnfs::DEFAULT_RPC_TIMEOUT_MS
        );
        assert_eq!(MountOpts::default().rpc_timeout_ms, 60_000);
    }

    /// F12: `rpc_timeout_ms: 0` ("leave library default") passes
    /// validation — it is a documented value, not a config error.
    #[test]
    fn mount_opts_accepts_zero_rpc_timeout() {
        MountOpts {
            rpc_timeout_ms: 0,
            ..MountOpts::default()
        }
        .validate()
        .expect("rpc_timeout_ms=0 must validate (means: skip set_timeout)");
    }

    #[test]
    fn flags_helpers_set_expected_bits() {
        assert_eq!(Flags::rdonly().raw() & libc::O_WRONLY, 0);
        assert!(Flags::wronly().raw() & libc::O_WRONLY != 0);
        assert!(Flags::wronly_sync().raw() & libc::O_SYNC != 0);
        let cf = Flags::wronly().with_create().raw();
        assert!(cf & libc::O_CREAT != 0);
        assert!(cf & libc::O_TRUNC != 0);
    }
}
