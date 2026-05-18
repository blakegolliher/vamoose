//! Service-task loop.
//!
//! Exactly one of these runs per `AsyncNfsContext`. It owns the
//! `*mut nfs_context` exclusively for the lifetime of the context;
//! no other task or thread may call libnfs against the same context.
//! See `docs/CORRECTNESS_RULES.md` "Service task owns the context".
//!
//! Responsibilities:
//! 1. Drain the request mpsc, issuing each call via the appropriate
//!    `*_async` entry point. The boxed `Pending<T>` carries the
//!    oneshot sender that the matching callback will fire.
//! 2. Watch the libnfs socket fd via `tokio::io::unix::AsyncFd`. When
//!    it's readable (or writable, depending on `nfs_which_events`),
//!    call `nfs_service(ctx, revents)` which drains the wire and
//!    invokes any pending callbacks.
//! 3. Tick `nfs_service(ctx, 0)` periodically so timeout handling has
//!    a chance to fire. libnfs's header explicitly asks for this.
//! 4. On shutdown, stop accepting requests and drain remaining
//!    callbacks until `nfs_queue_length(ctx) == 0`, then destroy the
//!    context.

use super::callbacks::{
    err_from_issue, fh_cb, pread_cb, pwrite_cb, readlink_cb, stat_cb, unit_cb, PendingBytes,
    PendingFh, PendingPread, PendingStat, PendingUnit, PendingWrite,
};
use super::error::NfsError;
use super::ffi;
use super::request::Request;
use crate::libnfs::{nfs_context, nfs_destroy_context};
use std::os::fd::RawFd;
use std::os::raw::c_int;
use std::os::unix::io::AsRawFd;
use std::time::Duration;
use tokio::io::unix::AsyncFd;
use tokio::io::Interest;
use tokio::sync::mpsc;

/// Borrowed socket fd. libnfs owns the underlying fd; this wrapper
/// just exists so `AsyncFd` can be built without taking ownership /
/// closing on drop.
struct BorrowedSocket(RawFd);

impl AsRawFd for BorrowedSocket {
    fn as_raw_fd(&self) -> RawFd {
        self.0
    }
}

/// Cadence at which the service task ticks `nfs_service` for both
/// timeout processing AND as a level-triggered backstop for mio's
/// edge-triggered epoll (see the tick arm in `run`).
///
/// Tuning: the perf smoke against var204 measured ~110 MB/s at 10 ms
/// tick vs ~213 MB/s for the sync path (also single-context). The
/// per-RPC cost lined up with one tick miss per response. At 1 ms
/// the async surface meets or beats sync. The cost is one
/// non-blocking `libc::poll` per tick — cheap enough that even a
/// dozen idle async contexts contribute negligible CPU.
const SERVICE_TICK: Duration = Duration::from_millis(1);

/// Wraps the raw context for the duration of the service task so we
/// can rely on Drop to call `nfs_destroy_context` even on panic.
/// Owning the context: only the service task ever sees this — and
/// the service task ever runs on at most one thread at a time, so
/// the `Send` impl is sound (the context moves into the task on
/// `tokio::spawn` and never escapes).
pub(crate) struct OwnedContext(pub *mut nfs_context);

unsafe impl Send for OwnedContext {}

impl Drop for OwnedContext {
    fn drop(&mut self) {
        if !self.0.is_null() {
            unsafe { nfs_destroy_context(self.0) };
            self.0 = std::ptr::null_mut();
        }
    }
}

/// Map libnfs's `nfs_which_events` POLL mask to `tokio::io::Interest`.
fn poll_to_interest(mask: c_int) -> Interest {
    let pin = (mask & libc::POLLIN as c_int) != 0;
    let pout = (mask & libc::POLLOUT as c_int) != 0;
    match (pin, pout) {
        (true, true) => Interest::READABLE | Interest::WRITABLE,
        (true, false) => Interest::READABLE,
        (false, true) => Interest::WRITABLE,
        // libnfs sometimes returns 0 when it has nothing to do; treat
        // as readable so we still pick up unexpected wire activity.
        (false, false) => Interest::READABLE,
    }
}

/// Service-task entry point used by `run_with_lazy_fd` once a real
/// libnfs fd is in hand. Takes an `OwnedContext` (Send) so callers
/// can spawn this on a multi-thread runtime.
async fn run(
    owned: OwnedContext,
    fd: RawFd,
    mut rx: mpsc::Receiver<Request>,
) -> Result<(), NfsError> {
    // Pull the raw pointer fresh at each sync region rather than
    // keeping it in a local: a `*mut nfs_context` local is `!Send`
    // and would poison this future's `Send`-ness across awaits.
    // Using `owned.0` inline restricts the !Send pointer to
    // synchronous sub-scopes that finish before the next `.await`.
    let async_fd = AsyncFd::new(BorrowedSocket(fd))
        .map_err(|e| NfsError::Init(format!("AsyncFd::new(libnfs fd={fd}): {e}")))?;

    let mut tick = tokio::time::interval(SERVICE_TICK);
    tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    let mut shutting_down = false;

    loop {
        // Refresh which events libnfs currently wants. Calling this
        // every iteration is cheap and is the only correct way to
        // track POLLIN vs POLLOUT — the mask flips as the libnfs
        // send queue empties.
        let interest = poll_to_interest(unsafe { ffi::nfs_which_events(owned.0) });

        tokio::select! {
            // Bias the request branch so issuance doesn't starve
            // behind a chatty wire — pread floods would otherwise
            // hold the loop on the ready arm.
            biased;

            // Incoming request from a caller task.
            req = rx.recv(), if !shutting_down => {
                match req {
                    None => {
                        // All senders dropped; treat as shutdown.
                        shutting_down = true;
                    }
                    Some(Request::Shutdown) => {
                        shutting_down = true;
                    }
                    Some(req) => issue(owned.0, req),
                }
            }

            // Socket is ready in the direction libnfs asked for.
            ready = async_fd.ready(interest) => {
                let mut guard = match ready {
                    Ok(g) => g,
                    Err(e) => {
                        return Err(NfsError::Init(format!("AsyncFd::ready: {e}")));
                    }
                };
                // mio uses edge-triggered epoll. Once we
                // `clear_ready`, we won't be re-notified for the
                // same direction until a *new* state transition. But
                // libnfs callbacks fired from inside nfs_service can
                // enqueue more RPCs that need the same direction
                // (POLLOUT) we already have readiness for. If we
                // return to await without sending them, the fd
                // never transitions (kernel buffer state didn't
                // change) and we deadlock until the 100ms tick.
                //
                // Drain pattern: call nfs_service with the events
                // we have, then keep calling while libnfs still
                // wants any direction we have readiness for. Bound
                // the iteration count to prevent runaway spinning
                // on a pathological wake; libnfs's per-wake fanout
                // is typically 1–3 callbacks, so 8 is generous.
                let revents: c_int = match (
                    guard.ready().is_readable(),
                    guard.ready().is_writable(),
                ) {
                    (true, true) => (libc::POLLIN | libc::POLLOUT) as c_int,
                    (true, false) => libc::POLLIN as c_int,
                    (false, true) => libc::POLLOUT as c_int,
                    (false, false) => 0,
                };
                let mut rc = 0;
                for _ in 0..8 {
                    rc = unsafe { ffi::nfs_service(owned.0, revents) };
                    if rc < 0 {
                        break;
                    }
                    let wants = unsafe { ffi::nfs_which_events(owned.0) };
                    let still_useful = (revents
                        & wants
                        & (libc::POLLIN | libc::POLLOUT) as c_int)
                        != 0;
                    let q = unsafe { ffi::nfs_queue_length(owned.0) };
                    if !still_useful || q == 0 {
                        break;
                    }
                }
                guard.clear_ready();
                if rc < 0 {
                    tracing::error!(rc, "nfs_service returned error; tearing down");
                    return Err(NfsError::Protocol(format!("nfs_service rc={rc}")));
                }
            }

            // Periodic timeout-handling tick.
            //
            // ALSO: serve as a level-triggered backstop for mio's
            // edge-triggered epoll. libnfs's callbacks can enqueue
            // RPCs that need POLLOUT after we cleared ready; with
            // ET semantics, there's no new edge to wake us. The
            // sync `wait_for_nfs_reply` in `libnfs-sync.c` uses
            // `poll(pfd, 1, poll_timeout)` which is level-triggered
            // — it always sees the current state. We mirror that
            // here with a non-blocking `libc::poll` on each tick.
            _ = tick.tick() => {
                let wants = unsafe { ffi::nfs_which_events(owned.0) };
                let mut pfd = libc::pollfd {
                    fd,
                    events: wants as i16,
                    revents: 0,
                };
                let p = unsafe { libc::poll(&mut pfd, 1, 0) };
                let revents = if p > 0 { pfd.revents as c_int } else { 0 };
                let rc = unsafe { ffi::nfs_service(owned.0, revents) };
                if rc < 0 {
                    tracing::error!(rc, "nfs_service tick returned error; tearing down");
                    return Err(NfsError::Protocol(format!("nfs_service tick rc={rc}")));
                }
            }
        }

        if shutting_down {
            // Drain any in-flight callbacks before destroying.
            // libnfs's `nfs_queue_length` is the authoritative count.
            let q = unsafe { ffi::nfs_queue_length(owned.0) };
            if q <= 0 {
                break;
            }
            // One more service spin to let the queue clear.
            let interest = poll_to_interest(unsafe { ffi::nfs_which_events(owned.0) });
            match tokio::time::timeout(SERVICE_TICK, async_fd.ready(interest)).await {
                Ok(Ok(mut guard)) => {
                    let revents: c_int =
                        match (guard.ready().is_readable(), guard.ready().is_writable()) {
                            (true, true) => (libc::POLLIN | libc::POLLOUT) as c_int,
                            (true, false) => libc::POLLIN as c_int,
                            (false, true) => libc::POLLOUT as c_int,
                            (false, false) => 0,
                        };
                    let _ = unsafe { ffi::nfs_service(owned.0, revents) };
                    guard.clear_ready();
                }
                _ => {
                    // Timed out or fd errored — tick anyway.
                    let _ = unsafe { ffi::nfs_service(owned.0, 0) };
                }
            }
        }
    }

    // owned drops here → nfs_destroy_context.
    Ok(())
}

/// Production entry point. Receives a freshly-initialized context
/// (post-`nfs_set_version`, pre-mount) and the request channel.
/// Walks the lazy-fd dance: libnfs does not allocate a socket until
/// the first call that needs one (typically `nfs_mount_async`). So
/// we tick `nfs_service(ctx, 0)` and drain requests in a short-spin
/// phase until `nfs_get_fd >= 0`, then graduate to the AsyncFd-driven
/// loop in `run`.
pub(crate) async fn run_with_lazy_fd(
    owned: OwnedContext,
    mut rx: mpsc::Receiver<Request>,
) -> Result<(), NfsError> {
    // Phase 1: socket not yet allocated. Process whatever requests
    // arrive (mount creates the socket), tick libnfs for connect
    // progress, until nfs_get_fd reports a real fd.
    let fd: RawFd = loop {
        let cur = unsafe { ffi::nfs_get_fd(owned.0) };
        if cur >= 0 {
            break cur;
        }
        tokio::select! {
            biased;
            req = rx.recv() => {
                match req {
                    None | Some(Request::Shutdown) => {
                        return Ok(());
                    }
                    Some(req) => issue(owned.0, req),
                }
            }
            _ = tokio::time::sleep(Duration::from_millis(5)) => {
                let _ = unsafe { ffi::nfs_service(owned.0, 0) };
            }
        }
    };

    // Phase 2: real fd is up. Run the standard event loop.
    run(owned, fd, rx).await
}

/// Issue one request against the live context. Always packages a
/// `Pending<T>` cookie that the matching callback unboxes.
fn issue(ctx: *mut nfs_context, req: Request) {
    match req {
        Request::Mount { server, export, tx } => {
            let pd = Box::into_raw(Box::new(PendingUnit { tx }));
            let rc = unsafe {
                ffi::nfs_mount_async(ctx, server.as_ptr(), export.as_ptr(), unit_cb, pd as *mut _)
            };
            handle_issue_failure_unit(ctx, rc, pd);
            // server, export drop on function return.
            let _ = (server, export);
        }
        Request::Open { path, flags, tx } => {
            let pd = Box::into_raw(Box::new(PendingFh { tx }));
            let rc = unsafe { ffi::nfs_open_async(ctx, path.as_ptr(), flags, fh_cb, pd as *mut _) };
            handle_issue_failure_fh(ctx, rc, pd);
            let _ = path;
        }
        Request::Open2 {
            path,
            flags,
            mode,
            tx,
        } => {
            let pd = Box::into_raw(Box::new(PendingFh { tx }));
            let rc = unsafe {
                ffi::nfs_open2_async(ctx, path.as_ptr(), flags, mode, fh_cb, pd as *mut _)
            };
            handle_issue_failure_fh(ctx, rc, pd);
            let _ = path;
        }
        Request::Close { fh, tx } => {
            let pd = Box::into_raw(Box::new(PendingUnit { tx }));
            let rc = unsafe { ffi::nfs_close_async(ctx, fh.0, unit_cb, pd as *mut _) };
            handle_issue_failure_unit(ctx, rc, pd);
        }
        Request::Pread {
            fh,
            offset,
            len,
            tx,
        } => {
            // libnfs delivers READ bytes by writing directly into the
            // caller-provided buffer (see lib/nfs_v3.c:nfs3_pread_cb
            // — the user callback fires with `data == NULL`; the
            // bytes live in the read buffer we passed at issue time).
            // PendingPread owns the Vec for the lifetime of the RPC
            // and hands it back at completion.
            let mut buf = vec![0u8; len];
            let buf_ptr = buf.as_mut_ptr() as *mut _;
            let pd = Box::into_raw(Box::new(PendingPread { tx, buf })) as *mut PendingPread;
            let rc = unsafe {
                ffi::nfs_pread_async(ctx, fh.0, buf_ptr, len, offset, pread_cb, pd as *mut _)
            };
            if rc < 0 {
                let p: Box<PendingPread> = unsafe { Box::from_raw(pd) };
                let _ = p.tx.send(Err(err_from_issue(ctx, rc)));
            }
        }
        Request::Pwrite {
            fh,
            offset,
            buf,
            tx,
        } => {
            let len = buf.len();
            let pd = Box::into_raw(Box::new(PendingWrite { tx, _buf: buf })) as *mut PendingWrite;
            let buf_ptr = unsafe { (*pd)._buf.as_ptr() } as *const _;
            let rc = unsafe {
                ffi::nfs_pwrite_async(ctx, fh.0, buf_ptr, len, offset, pwrite_cb, pd as *mut _)
            };
            if rc < 0 {
                // Issue failed → callback won't fire → unbox here.
                let p: Box<PendingWrite> = unsafe { Box::from_raw(pd) };
                let _ = p.tx.send(Err(err_from_issue(ctx, rc)));
            }
        }
        Request::Fsync { fh, tx } => {
            let pd = Box::into_raw(Box::new(PendingUnit { tx }));
            let rc = unsafe { ffi::nfs_fsync_async(ctx, fh.0, unit_cb, pd as *mut _) };
            handle_issue_failure_unit(ctx, rc, pd);
        }
        Request::Stat64 { path, tx } => {
            let pd = Box::into_raw(Box::new(PendingStat { tx }));
            let rc = unsafe { ffi::nfs_stat64_async(ctx, path.as_ptr(), stat_cb, pd as *mut _) };
            handle_issue_failure_stat(ctx, rc, pd);
            let _ = path;
        }
        Request::Fstat64 { fh, tx } => {
            let pd = Box::into_raw(Box::new(PendingStat { tx }));
            let rc = unsafe { ffi::nfs_fstat64_async(ctx, fh.0, stat_cb, pd as *mut _) };
            handle_issue_failure_stat(ctx, rc, pd);
        }
        Request::Unlink { path, tx } => {
            let pd = Box::into_raw(Box::new(PendingUnit { tx }));
            let rc = unsafe { ffi::nfs_unlink_async(ctx, path.as_ptr(), unit_cb, pd as *mut _) };
            handle_issue_failure_unit(ctx, rc, pd);
            let _ = path;
        }
        Request::Rename {
            oldpath,
            newpath,
            tx,
        } => {
            let pd = Box::into_raw(Box::new(PendingUnit { tx }));
            let rc = unsafe {
                ffi::nfs_rename_async(
                    ctx,
                    oldpath.as_ptr(),
                    newpath.as_ptr(),
                    unit_cb,
                    pd as *mut _,
                )
            };
            handle_issue_failure_unit(ctx, rc, pd);
            let _ = (oldpath, newpath);
        }
        Request::Utimes {
            path,
            mut times,
            tx,
        } => {
            let pd = Box::into_raw(Box::new(PendingUnit { tx }));
            let rc = unsafe {
                ffi::nfs_utimes_async(
                    ctx,
                    path.as_ptr(),
                    times.as_mut_ptr(),
                    unit_cb,
                    pd as *mut _,
                )
            };
            handle_issue_failure_unit(ctx, rc, pd);
            let _ = (path, times);
        }
        Request::Chmod { path, mode, tx } => {
            let pd = Box::into_raw(Box::new(PendingUnit { tx }));
            let rc =
                unsafe { ffi::nfs_chmod_async(ctx, path.as_ptr(), mode, unit_cb, pd as *mut _) };
            handle_issue_failure_unit(ctx, rc, pd);
            let _ = path;
        }
        Request::Chown { path, uid, gid, tx } => {
            let pd = Box::into_raw(Box::new(PendingUnit { tx }));
            let rc = unsafe {
                ffi::nfs_chown_async(ctx, path.as_ptr(), uid, gid, unit_cb, pd as *mut _)
            };
            handle_issue_failure_unit(ctx, rc, pd);
            let _ = path;
        }
        Request::Symlink {
            target,
            linkname,
            tx,
        } => {
            let pd = Box::into_raw(Box::new(PendingUnit { tx }));
            let rc = unsafe {
                ffi::nfs_symlink_async(
                    ctx,
                    target.as_ptr(),
                    linkname.as_ptr(),
                    unit_cb,
                    pd as *mut _,
                )
            };
            handle_issue_failure_unit(ctx, rc, pd);
            let _ = (target, linkname);
        }
        Request::Link {
            oldpath,
            newpath,
            tx,
        } => {
            let pd = Box::into_raw(Box::new(PendingUnit { tx }));
            let rc = unsafe {
                ffi::nfs_link_async(
                    ctx,
                    oldpath.as_ptr(),
                    newpath.as_ptr(),
                    unit_cb,
                    pd as *mut _,
                )
            };
            handle_issue_failure_unit(ctx, rc, pd);
            let _ = (oldpath, newpath);
        }
        Request::Mkdir2 { path, mode, tx } => {
            let pd = Box::into_raw(Box::new(PendingUnit { tx }));
            let rc =
                unsafe { ffi::nfs_mkdir2_async(ctx, path.as_ptr(), mode, unit_cb, pd as *mut _) };
            handle_issue_failure_unit(ctx, rc, pd);
            let _ = path;
        }
        Request::Readlink { path, tx } => {
            let pd = Box::into_raw(Box::new(PendingBytes { tx }));
            let rc =
                unsafe { ffi::nfs_readlink_async(ctx, path.as_ptr(), readlink_cb, pd as *mut _) };
            handle_issue_failure_bytes(ctx, rc, pd);
            let _ = path;
        }
        Request::QueueLen { tx } => {
            let q = unsafe { ffi::nfs_queue_length(ctx) };
            let _ = tx.send(q.max(0) as usize);
        }
        Request::Shutdown => {
            // Handled by the loop body (caller already set the flag).
        }
    }
}

fn handle_issue_failure_unit(ctx: *mut nfs_context, rc: c_int, pd: *mut PendingUnit) {
    if rc < 0 {
        let p: Box<PendingUnit> = unsafe { Box::from_raw(pd) };
        let _ = p.tx.send(Err(err_from_issue(ctx, rc)));
    }
}

fn handle_issue_failure_fh(ctx: *mut nfs_context, rc: c_int, pd: *mut PendingFh) {
    if rc < 0 {
        let p: Box<PendingFh> = unsafe { Box::from_raw(pd) };
        let _ = p.tx.send(Err(err_from_issue(ctx, rc)));
    }
}

fn handle_issue_failure_bytes(ctx: *mut nfs_context, rc: c_int, pd: *mut PendingBytes) {
    if rc < 0 {
        let p: Box<PendingBytes> = unsafe { Box::from_raw(pd) };
        let _ = p.tx.send(Err(err_from_issue(ctx, rc)));
    }
}

fn handle_issue_failure_stat(ctx: *mut nfs_context, rc: c_int, pd: *mut PendingStat) {
    if rc < 0 {
        let p: Box<PendingStat> = unsafe { Box::from_raw(pd) };
        let _ = p.tx.send(Err(err_from_issue(ctx, rc)));
    }
}
