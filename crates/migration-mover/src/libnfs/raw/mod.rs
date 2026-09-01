//! Raw NFSv3 operations over cached filehandles.
//!
//! The high-level libnfs API is path-based: every call re-walks the
//! full path with one LOOKUP per component. On deep trees that is the
//! entire RPC budget — the 2026-08-22 600M-file run measured LOOKUP at
//! 88% of all wire RPCs (~60–80 per tiny file; see
//! `bigrun/FINDINGS.md`). This module talks NFSv3 directly through
//! libnfs's raw `rpc_nfs3_*_task` interface so the mover can:
//!
//! - resolve a directory's filehandle once and reuse it for every
//!   child (NFSv3 filehandles are stateless — no open/close RPCs);
//! - CREATE with mode/uid/gid in the initial `sattr3` (no separate
//!   chown/chmod);
//! - WRITE with `FILE_SYNC` stability for single-chunk files (no
//!   separate COMMIT);
//! - set atime+mtime in one SETATTR against the file handle;
//! - RENAME by (dir_fh, name) pairs.
//!
//! After one amortized READDIRPLUS per source directory, a typical
//! tiny file costs READ + CREATE + WRITE + SETATTR + RENAME = 5 RPCs
//! (4 with direct commit). Missing/omitted/stale prefetched handles
//! retain the original per-name LOOKUP fallback.
//!
//! ## Execution model
//!
//! Every function here is synchronous and runs on the caller's thread
//! (the mover's `spawn_blocking` bodies): issue one `*_task`, then
//! pump the context's poll/service loop until the callback fires.
//! That is exactly how libnfs's own sync API is built. One op in
//! flight per context at a time, by construction — concurrency comes
//! from the `MultiPool` context count, same as the sync mover.
//!
//! ## Safety
//!
//! The bindings in [`bindings`] are bindgen-generated from the same
//! libnfs 16.2.0 headers the workspace links against, with layout
//! assertions compiled in. Arg structs are fully marshalled into the
//! RPC PDU before `*_task` returns, so borrowed buffers only need to
//! outlive the call itself; result structs are only dereferenced
//! inside the callback, which libnfs invokes before freeing them.

#[allow(
    non_camel_case_types,
    non_snake_case,
    non_upper_case_globals,
    dead_code,
    clippy::all
)]
pub mod bindings;

use std::ffi::{CStr, CString};
use std::os::raw::{c_char, c_int, c_void};

use bindings as b;

use super::NfsContext;

/// RPC_STATUS_* from libnfs-raw.h (defines, not enums — kept local).
const RPC_STATUS_SUCCESS: c_int = 0;
const RPC_STATUS_TIMEOUT: c_int = 3;

/// Hard wall for a single op's pump loop, on top of libnfs's own
/// per-RPC timeout (which fires first in any healthy configuration).
const PUMP_DEADLINE_SECS: u64 = 120;

/// A server filehandle. Plain bytes; valid for the life of the export.
pub type Fh = Vec<u8>;

/// One name returned by READDIRPLUS. `fh` is absent when the server
/// omitted the optional `name_handle`; callers must LOOKUP that name.
#[derive(Debug)]
pub struct ReaddirplusEntry {
    pub name: Vec<u8>,
    pub fh: Option<Fh>,
}

/// Result of a bounded, fully paged READDIRPLUS scan.
#[derive(Debug)]
pub enum ReaddirplusResult {
    Complete(Vec<ReaddirplusEntry>),
    /// The directory has more entries than the caller's cap. Partial
    /// results are deliberately discarded so memory remains bounded.
    TooMany,
}

struct ReaddirplusPage {
    entries: Vec<ReaddirplusEntry>,
    last_cookie: Option<u64>,
    cookieverf: b::cookieverf3,
    eof: bool,
}

/// Error from one raw op: the NFS3 status name (errno-style, e.g.
/// "ENOENT", "EEXIST") or a transport-level description.
#[derive(Debug)]
pub struct RawError {
    /// errno-style tag; "EIO" for transport errors.
    pub tag: &'static str,
    pub detail: String,
}

impl RawError {
    fn nfs(status: u32, op: &str) -> Self {
        RawError {
            tag: nfsstat_tag(status),
            detail: format!("{op}: NFS3 status {status} ({})", nfsstat_tag(status)),
        }
    }
    fn transport(op: &str, status: c_int, msg: String) -> Self {
        RawError {
            tag: if status == RPC_STATUS_TIMEOUT {
                "ETIMEDOUT"
            } else {
                "EIO"
            },
            detail: format!("{op}: rpc status {status}: {msg}"),
        }
    }
}

/// Map nfsstat3 to the errno-style tags the rest of the mover uses.
fn nfsstat_tag(status: u32) -> &'static str {
    match status {
        1 => "EPERM",
        2 => "ENOENT",
        5 => "EIO",
        6 => "ENXIO",
        13 => "EACCES",
        17 => "EEXIST",
        18 => "EXDEV",
        19 => "ENODEV",
        20 => "ENOTDIR",
        21 => "EISDIR",
        22 => "EINVAL",
        27 => "EFBIG",
        28 => "ENOSPC",
        30 => "EROFS",
        31 => "EMLINK",
        63 => "ENAMETOOLONG",
        66 => "ENOTEMPTY",
        69 => "EDQUOT",
        70 => "ESTALE",
        71 => "EREMOTE",
        10001 => "EBADHANDLE",
        10002 => "ENOTSYNC",
        10003 => "EBADCOOKIE",
        10004 => "ENOTSUPP",
        10005 => "ETOOSMALL",
        10006 => "ESERVERFAULT",
        10007 => "EBADTYPE",
        10008 => "EJUKEBOX",
        _ => "EIO",
    }
}

/// What a completed op handed back from its callback.
enum Out {
    Fh(Fh),
    Readdirplus(ReaddirplusPage),
    Read { count: u32, eof: bool },
    Write { count: u32 },
    Unit,
}

/// Per-op completion slot. Filled by the callback, which always runs
/// on this thread inside `rpc_service` — no synchronization needed.
struct Slot {
    /// Completion flag. `Cell` + raw-pointer access in `pump` on
    /// purpose: the callback mutates the slot through the raw pointer
    /// libnfs hands back while `pump` re-reads it in a loop. A plain
    /// `&Slot` borrow in `pump` would let the compiler assume the
    /// field can never change and hoist the load out of the loop
    /// (observed doing exactly that at opt-level 1 — the loop spun
    /// forever while the reply sat consumed). Everything is
    /// single-threaded (the callback runs inside `rpc_service` on the
    /// pumping thread); only the aliasing model needs the escape
    /// hatch, not synchronization.
    done: std::cell::Cell<bool>,
    rpc_status: c_int,
    rpc_err: String,
    /// NFS3 status when the RPC itself succeeded.
    nfs_status: u32,
    out: Option<Out>,
}

impl Slot {
    fn new() -> Self {
        Slot {
            done: std::cell::Cell::new(false),
            rpc_status: RPC_STATUS_SUCCESS,
            rpc_err: String::new(),
            nfs_status: 0,
            out: None,
        }
    }

    /// Record transport-level completion; returns None when the RPC
    /// failed and the res pointer must not be touched.
    unsafe fn begin(
        &mut self,
        rpc: *mut b::rpc_context,
        status: c_int,
        data: *mut c_void,
    ) -> Option<*mut c_void> {
        self.done.set(true);
        self.rpc_status = status;
        if status != RPC_STATUS_SUCCESS {
            self.rpc_err = if data.is_null() {
                let e = b::rpc_get_error(rpc);
                if e.is_null() {
                    String::new()
                } else {
                    std::ffi::CStr::from_ptr(e).to_string_lossy().into_owned()
                }
            } else {
                std::ffi::CStr::from_ptr(data as *const c_char)
                    .to_string_lossy()
                    .into_owned()
            };
            return None;
        }
        Some(data)
    }

    fn finish(self, op: &str) -> Result<Out, RawError> {
        if self.rpc_status != RPC_STATUS_SUCCESS {
            return Err(RawError::transport(op, self.rpc_status, self.rpc_err));
        }
        if self.nfs_status != 0 {
            return Err(RawError::nfs(self.nfs_status, op));
        }
        self.out
            .ok_or_else(|| RawError::transport(op, RPC_STATUS_SUCCESS, "no payload".into()))
    }
}

fn fh3(fh: &[u8]) -> b::nfs_fh3 {
    b::nfs_fh3 {
        data: b::nfs_fh3__bindgen_ty_1 {
            data_len: fh.len() as u32,
            data_val: fh.as_ptr() as *mut c_char,
        },
    }
}

fn copy_fh3(fh: &b::nfs_fh3) -> Fh {
    unsafe {
        std::slice::from_raw_parts(fh.data.data_val as *const u8, fh.data.data_len as usize)
            .to_vec()
    }
}

fn rpc_of(nfs: &mut NfsContext) -> *mut b::rpc_context {
    unsafe { b::nfs_get_rpc_context(nfs.raw() as *mut c_void as *mut b::nfs_context) }
}

/// Pump the context's event loop until `slot.done`. Mirrors
/// libnfs-sync.c's wait loop: poll the rpc fd for the events libnfs
/// asks for, then service. Calling `rpc_service` at least every 100ms
/// also drives libnfs's own RPC-timeout scanning.
fn pump(nfs: &mut NfsContext, slot: *const Slot, op: &str) -> Result<(), RawError> {
    let rpc = rpc_of(nfs);
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(PUMP_DEADLINE_SECS);
    // Raw pointer read each iteration — see `Slot::done` for why this
    // must not be a `&Slot` borrow held across `rpc_service`.
    while !unsafe { (*slot).done.get() } {
        if std::time::Instant::now() > deadline {
            return Err(RawError::transport(
                op,
                RPC_STATUS_TIMEOUT,
                format!("pump deadline ({PUMP_DEADLINE_SECS}s) exceeded"),
            ));
        }
        let mut pfd = libc::pollfd {
            fd: unsafe { b::rpc_get_fd(rpc) },
            events: unsafe { b::rpc_which_events(rpc) } as i16,
            revents: 0,
        };
        let rc = unsafe { libc::poll(&mut pfd, 1, 100) };
        if rc < 0 {
            let e = std::io::Error::last_os_error();
            if e.kind() == std::io::ErrorKind::Interrupted {
                continue;
            }
            return Err(RawError::transport(op, 1, format!("poll: {e}")));
        }
        let rc = unsafe { b::rpc_service(rpc, pfd.revents as c_int) };
        if rc < 0 {
            let e = unsafe { b::rpc_get_error(rpc) };
            let msg = if e.is_null() {
                "rpc_service failed".to_string()
            } else {
                unsafe { std::ffi::CStr::from_ptr(e).to_string_lossy().into_owned() }
            };
            return Err(RawError::transport(op, 1, msg));
        }
    }
    Ok(())
}

fn cstring(name: &[u8], op: &str) -> Result<CString, RawError> {
    CString::new(name).map_err(|_| RawError {
        tag: "EINVAL",
        detail: format!("{op}: name contains NUL"),
    })
}

/// Copy of the mount's root filehandle. No RPC — libnfs caches it at
/// mount time.
pub fn root_fh(nfs: &mut NfsContext) -> Result<Fh, RawError> {
    let fh = unsafe { b::nfs_get_rootfh(nfs.raw() as *mut c_void as *mut b::nfs_context) };
    if fh.is_null() {
        return Err(RawError {
            tag: "EIO",
            detail: "nfs_get_rootfh returned NULL (not mounted?)".into(),
        });
    }
    unsafe {
        let len = (*fh).len as usize;
        Ok(std::slice::from_raw_parts((*fh).val as *const u8, len).to_vec())
    }
}

macro_rules! issue {
    ($nfs:expr, $slot:expr, $op:literal, $call:expr) => {{
        // Wall time from issue to reply (or failure) is the RPC's
        // latency sample, tagged with the side this context serves.
        let started = std::time::Instant::now();
        let pdu = unsafe { $call };
        if pdu.is_null() {
            return Err(RawError::transport($op, 1, "task queue failed".into()));
        }
        let pumped = pump($nfs, $slot as *const Slot, $op);
        migration_core::latency::global().record_nfs(
            $nfs.side(),
            migration_core::latency::NfsOp::from_tag($op),
            started.elapsed(),
        );
        pumped?;
    }};
}

unsafe extern "C" fn cb_lookup(
    rpc: *mut b::rpc_context,
    status: c_int,
    data: *mut c_void,
    pd: *mut c_void,
) {
    let slot = &mut *(pd as *mut Slot);
    if let Some(data) = slot.begin(rpc, status, data) {
        let res = &*(data as *const b::LOOKUP3res);
        slot.nfs_status = res.status;
        if res.status == b::NFS3_OK {
            slot.out = Some(Out::Fh(copy_fh3(&res.LOOKUP3res_u.resok.object)));
        }
    }
}

/// LOOKUP `name` in `dir_fh`; returns the child's filehandle.
pub fn lookup(nfs: &mut NfsContext, dir_fh: &[u8], name: &[u8]) -> Result<Fh, RawError> {
    let cname = cstring(name, "LOOKUP")?;
    let mut slot = Slot::new();
    let mut args = b::LOOKUP3args {
        what: b::diropargs3 {
            dir: fh3(dir_fh),
            name: cname.as_ptr() as *mut c_char,
        },
    };
    issue!(nfs, &slot, "LOOKUP", {
        b::rpc_nfs3_lookup_task(
            rpc_of(nfs),
            Some(cb_lookup),
            &mut args,
            &mut slot as *mut Slot as *mut c_void,
        )
    });
    match slot.finish("LOOKUP")? {
        Out::Fh(fh) => Ok(fh),
        _ => unreachable!("LOOKUP slot holds Fh"),
    }
}

unsafe extern "C" fn cb_readdirplus(
    rpc: *mut b::rpc_context,
    status: c_int,
    data: *mut c_void,
    pd: *mut c_void,
) {
    let slot = &mut *(pd as *mut Slot);
    if let Some(data) = slot.begin(rpc, status, data) {
        let res = &*(data as *const b::READDIRPLUS3res);
        slot.nfs_status = res.status;
        if res.status == b::NFS3_OK {
            let ok = &res.READDIRPLUS3res_u.resok;
            let mut entries = Vec::new();
            let mut last_cookie = None;
            let mut cur = ok.reply.entries;
            while !cur.is_null() {
                let entry = &*cur;
                last_cookie = Some(entry.cookie);
                if !entry.name.is_null() {
                    let name = CStr::from_ptr(entry.name).to_bytes().to_vec();
                    let handle = &entry.name_handle;
                    let fh = if handle.handle_follows != 0 {
                        let fh = &handle.post_op_fh3_u.handle;
                        if fh.data.data_len > 0 && !fh.data.data_val.is_null() {
                            Some(copy_fh3(fh))
                        } else {
                            None
                        }
                    } else {
                        None
                    };
                    entries.push(ReaddirplusEntry { name, fh });
                }
                cur = entry.nextentry;
            }
            slot.out = Some(Out::Readdirplus(ReaddirplusPage {
                entries,
                last_cookie,
                cookieverf: ok.cookieverf,
                eof: ok.reply.eof != 0,
            }));
        }
    }
}

/// Page through `dir_fh` with READDIRPLUS, copying child names and
/// optional filehandles out of each callback before libnfs frees the
/// decoded reply.
///
/// `maxcount` (the total-reply byte budget) must keep every reply
/// inside a single RPC record-marking fragment: VAST streams READDIRPLUS
/// replies larger than ~14 KiB as multi-fragment records (observed on
/// VAST 5.x, 14,476-byte fragments), and the pinned libnfs cannot
/// reassemble those ("Fragment support not yet working") — it drops the
/// connection, auto-reconnects, and retransmits the same request, which
/// the server answers identically: an infinite reconnect storm that
/// wedges the context until the pump deadline. 8 KiB plus RPC overhead
/// stays safely under the fragment threshold; a large directory costs a
/// few dozen extra round trips, amortized over one prefetch per dir.
///
/// At most `entry_cap` entries are retained. If the server indicates
/// more entries exist, all partial results are dropped and `TooMany`
/// tells the caller to use per-name LOOKUP instead.
pub fn readdirplus(
    nfs: &mut NfsContext,
    dir_fh: &[u8],
    entry_cap: usize,
) -> Result<ReaddirplusResult, RawError> {
    const DIRCOUNT: u32 = 8 * 1024;
    const MAXCOUNT: u32 = 8 * 1024;

    let mut cookie = 0;
    let mut cookieverf: b::cookieverf3 = [0; 8];
    let mut all = Vec::new();
    loop {
        let mut slot = Slot::new();
        let mut args = b::READDIRPLUS3args {
            dir: fh3(dir_fh),
            cookie,
            cookieverf,
            dircount: DIRCOUNT,
            maxcount: MAXCOUNT,
        };
        issue!(nfs, &slot, "READDIRPLUS", {
            b::rpc_nfs3_readdirplus_task(
                rpc_of(nfs),
                Some(cb_readdirplus),
                &mut args,
                &mut slot as *mut Slot as *mut c_void,
            )
        });
        let page = match slot.finish("READDIRPLUS")? {
            Out::Readdirplus(page) => page,
            _ => unreachable!("READDIRPLUS slot holds Readdirplus"),
        };

        let total = all.len().saturating_add(page.entries.len());
        if total > entry_cap || (total == entry_cap && !page.eof) {
            return Ok(ReaddirplusResult::TooMany);
        }
        all.extend(page.entries);
        if page.eof {
            return Ok(ReaddirplusResult::Complete(all));
        }

        let next_cookie = page.last_cookie.ok_or_else(|| {
            RawError::transport(
                "READDIRPLUS",
                RPC_STATUS_SUCCESS,
                "non-EOF page contained no entries".into(),
            )
        })?;
        if next_cookie == cookie {
            return Err(RawError::transport(
                "READDIRPLUS",
                RPC_STATUS_SUCCESS,
                "server returned a non-advancing cookie".into(),
            ));
        }
        cookie = next_cookie;
        cookieverf = page.cookieverf;
    }
}

unsafe extern "C" fn cb_create(
    rpc: *mut b::rpc_context,
    status: c_int,
    data: *mut c_void,
    pd: *mut c_void,
) {
    let slot = &mut *(pd as *mut Slot);
    if let Some(data) = slot.begin(rpc, status, data) {
        let res = &*(data as *const b::CREATE3res);
        slot.nfs_status = res.status;
        if res.status == b::NFS3_OK {
            let obj = &res.CREATE3res_u.resok.obj;
            if obj.handle_follows != 0 {
                slot.out = Some(Out::Fh(copy_fh3(&obj.post_op_fh3_u.handle)));
            } else {
                // Server chose not to return the fh; caller LOOKUPs.
                slot.out = Some(Out::Fh(Vec::new()));
            }
        }
    }
}

/// Attributes to stamp at CREATE/MKDIR/SETATTR time. All optional;
/// unset fields are DONT_CHANGE on the wire.
#[derive(Default, Clone, Copy)]
pub struct RawSattr {
    pub mode: Option<u32>,
    pub uid: Option<u32>,
    pub gid: Option<u32>,
    /// Truncate/extend to this size. `Some(0)` at CREATE(UNCHECKED)
    /// guarantees a stale file under the same name loses its bytes.
    pub size: Option<u64>,
    /// (secs, nsecs) — NFSv3 carries u32 seconds.
    pub atime: Option<(i64, u32)>,
    pub mtime: Option<(i64, u32)>,
}

fn sattr3(a: &RawSattr) -> b::sattr3 {
    // Zeroed = every set_it 0 (DONT_CHANGE / don't set).
    let mut s: b::sattr3 = unsafe { std::mem::zeroed() };
    if let Some(m) = a.mode {
        s.mode.set_it = 1;
        s.mode.set_mode3_u.mode = m;
    }
    if let Some(u) = a.uid {
        s.uid.set_it = 1;
        s.uid.set_uid3_u.uid = u;
    }
    if let Some(g) = a.gid {
        s.gid.set_it = 1;
        s.gid.set_gid3_u.gid = g;
    }
    if let Some(sz) = a.size {
        s.size.set_it = 1;
        s.size.set_size3_u.size = sz;
    }
    if let Some((sec, nsec)) = a.atime {
        s.atime.set_it = b::SET_TO_CLIENT_TIME;
        s.atime.set_atime_u.atime = b::nfstime3 {
            seconds: sec.clamp(0, u32::MAX as i64) as u32,
            nseconds: nsec,
        };
    }
    if let Some((sec, nsec)) = a.mtime {
        s.mtime.set_it = b::SET_TO_CLIENT_TIME;
        s.mtime.set_mtime_u.mtime = b::nfstime3 {
            seconds: sec.clamp(0, u32::MAX as i64) as u32,
            nseconds: nsec,
        };
    }
    s
}

/// CREATE (UNCHECKED) `name` in `dir_fh` with `attrs` stamped at
/// creation. Returns the new file's fh (falls back to a LOOKUP if the
/// server omitted it from the reply).
pub fn create(
    nfs: &mut NfsContext,
    dir_fh: &[u8],
    name: &[u8],
    attrs: &RawSattr,
) -> Result<Fh, RawError> {
    let cname = cstring(name, "CREATE")?;
    let mut slot = Slot::new();
    let mut args: b::CREATE3args = unsafe { std::mem::zeroed() };
    args.where_ = b::diropargs3 {
        dir: fh3(dir_fh),
        name: cname.as_ptr() as *mut c_char,
    };
    args.how.mode = b::UNCHECKED;
    args.how.createhow3_u.obj_attributes = sattr3(attrs);
    issue!(nfs, &slot, "CREATE", {
        b::rpc_nfs3_create_task(
            rpc_of(nfs),
            Some(cb_create),
            &mut args,
            &mut slot as *mut Slot as *mut c_void,
        )
    });
    match slot.finish("CREATE")? {
        Out::Fh(fh) if !fh.is_empty() => Ok(fh),
        Out::Fh(_) => lookup(nfs, dir_fh, name),
        _ => unreachable!("CREATE slot holds Fh"),
    }
}

unsafe extern "C" fn cb_mkdir(
    rpc: *mut b::rpc_context,
    status: c_int,
    data: *mut c_void,
    pd: *mut c_void,
) {
    let slot = &mut *(pd as *mut Slot);
    if let Some(data) = slot.begin(rpc, status, data) {
        let res = &*(data as *const b::MKDIR3res);
        slot.nfs_status = res.status;
        if res.status == b::NFS3_OK {
            let obj = &res.MKDIR3res_u.resok.obj;
            if obj.handle_follows != 0 {
                slot.out = Some(Out::Fh(copy_fh3(&obj.post_op_fh3_u.handle)));
            } else {
                slot.out = Some(Out::Fh(Vec::new()));
            }
        }
    }
}

/// MKDIR `name` in `dir_fh`. Returns the new dir's fh (LOOKUP fallback
/// as for CREATE). EEXIST surfaces as `RawError { tag: "EEXIST" }` —
/// callers racing on shared ancestors LOOKUP on that.
pub fn mkdir(nfs: &mut NfsContext, dir_fh: &[u8], name: &[u8], mode: u32) -> Result<Fh, RawError> {
    let cname = cstring(name, "MKDIR")?;
    let mut slot = Slot::new();
    let mut args: b::MKDIR3args = unsafe { std::mem::zeroed() };
    args.where_ = b::diropargs3 {
        dir: fh3(dir_fh),
        name: cname.as_ptr() as *mut c_char,
    };
    args.attributes = sattr3(&RawSattr {
        mode: Some(mode),
        ..Default::default()
    });
    issue!(nfs, &slot, "MKDIR", {
        b::rpc_nfs3_mkdir_task(
            rpc_of(nfs),
            Some(cb_mkdir),
            &mut args,
            &mut slot as *mut Slot as *mut c_void,
        )
    });
    match slot.finish("MKDIR")? {
        Out::Fh(fh) if !fh.is_empty() => Ok(fh),
        Out::Fh(_) => lookup(nfs, dir_fh, name),
        _ => unreachable!("MKDIR slot holds Fh"),
    }
}

unsafe extern "C" fn cb_read(
    rpc: *mut b::rpc_context,
    status: c_int,
    data: *mut c_void,
    pd: *mut c_void,
) {
    let slot = &mut *(pd as *mut Slot);
    if let Some(data) = slot.begin(rpc, status, data) {
        let res = &*(data as *const b::READ3res);
        slot.nfs_status = res.status;
        if res.status == b::NFS3_OK {
            let ok = &res.READ3res_u.resok;
            // Data was copied into the caller's buffer by libnfs's
            // readv path; only count/eof come from the reply struct.
            slot.out = Some(Out::Read {
                count: ok.count,
                eof: ok.eof != 0,
            });
        }
    }
}

/// READ up to `count` bytes at `offset`. Returns (data, eof).
pub fn read(
    nfs: &mut NfsContext,
    fh: &[u8],
    offset: u64,
    count: u32,
) -> Result<(Vec<u8>, bool), RawError> {
    let mut slot = Slot::new();
    // libnfs copies reply data into this buffer (rpc_nfs3_read_task
    // wraps it in an iovec); a NULL buffer segfaults inside libnfs.
    let mut buf: Vec<u8> = vec![0; count as usize];
    let mut args: b::READ3args = unsafe { std::mem::zeroed() };
    args.file = fh3(fh);
    args.offset = offset;
    args.count = count;
    issue!(nfs, &slot, "READ", {
        b::rpc_nfs3_read_task(
            rpc_of(nfs),
            Some(cb_read),
            buf.as_mut_ptr() as *mut c_void,
            count as usize,
            &mut args,
            &mut slot as *mut Slot as *mut c_void,
        )
    });
    match slot.finish("READ")? {
        Out::Read { count: n, eof } => {
            buf.truncate((n as usize).min(buf.len()));
            Ok((buf, eof))
        }
        _ => unreachable!("READ slot holds Read"),
    }
}

unsafe extern "C" fn cb_write(
    rpc: *mut b::rpc_context,
    status: c_int,
    data: *mut c_void,
    pd: *mut c_void,
) {
    let slot = &mut *(pd as *mut Slot);
    if let Some(data) = slot.begin(rpc, status, data) {
        let res = &*(data as *const b::WRITE3res);
        slot.nfs_status = res.status;
        if res.status == b::NFS3_OK {
            slot.out = Some(Out::Write {
                count: res.WRITE3res_u.resok.count,
            });
        }
    }
}

/// WRITE `data` at `offset`. `stable = true` → FILE_SYNC (bytes are
/// durable when the reply arrives; no COMMIT needed), else UNSTABLE
/// (caller must COMMIT before publishing). Returns bytes accepted.
pub fn write(
    nfs: &mut NfsContext,
    fh: &[u8],
    offset: u64,
    data: &[u8],
    stable: bool,
) -> Result<u32, RawError> {
    let mut slot = Slot::new();
    let mut args: b::WRITE3args = unsafe { std::mem::zeroed() };
    args.file = fh3(fh);
    args.offset = offset;
    args.count = data.len() as u32;
    args.stable = if stable { b::FILE_SYNC } else { b::UNSTABLE };
    args.data.data_len = data.len() as u32;
    args.data.data_val = data.as_ptr() as *mut c_char;
    issue!(nfs, &slot, "WRITE", {
        b::rpc_nfs3_write_task(
            rpc_of(nfs),
            Some(cb_write),
            &mut args,
            &mut slot as *mut Slot as *mut c_void,
        )
    });
    match slot.finish("WRITE")? {
        Out::Write { count } => Ok(count),
        _ => unreachable!("WRITE slot holds Write"),
    }
}

unsafe extern "C" fn cb_unit_commit(
    rpc: *mut b::rpc_context,
    status: c_int,
    data: *mut c_void,
    pd: *mut c_void,
) {
    let slot = &mut *(pd as *mut Slot);
    if let Some(data) = slot.begin(rpc, status, data) {
        let res = &*(data as *const b::COMMIT3res);
        slot.nfs_status = res.status;
        if res.status == b::NFS3_OK {
            slot.out = Some(Out::Unit);
        }
    }
}

/// COMMIT the whole file (offset 0, count 0 = everything).
pub fn commit(nfs: &mut NfsContext, fh: &[u8]) -> Result<(), RawError> {
    let mut slot = Slot::new();
    let mut args: b::COMMIT3args = unsafe { std::mem::zeroed() };
    args.file = fh3(fh);
    issue!(nfs, &slot, "COMMIT", {
        b::rpc_nfs3_commit_task(
            rpc_of(nfs),
            Some(cb_unit_commit),
            &mut args,
            &mut slot as *mut Slot as *mut c_void,
        )
    });
    slot.finish("COMMIT").map(|_| ())
}

unsafe extern "C" fn cb_unit_setattr(
    rpc: *mut b::rpc_context,
    status: c_int,
    data: *mut c_void,
    pd: *mut c_void,
) {
    let slot = &mut *(pd as *mut Slot);
    if let Some(data) = slot.begin(rpc, status, data) {
        let res = &*(data as *const b::SETATTR3res);
        slot.nfs_status = res.status;
        if res.status == b::NFS3_OK {
            slot.out = Some(Out::Unit);
        }
    }
}

/// One SETATTR carrying any combination of mode/uid/gid/atime/mtime.
pub fn setattr(nfs: &mut NfsContext, fh: &[u8], attrs: &RawSattr) -> Result<(), RawError> {
    let mut slot = Slot::new();
    let mut args: b::SETATTR3args = unsafe { std::mem::zeroed() };
    args.object = fh3(fh);
    args.new_attributes = sattr3(attrs);
    // guard.check = 0 (no ctime guard) via zeroed.
    issue!(nfs, &slot, "SETATTR", {
        b::rpc_nfs3_setattr_task(
            rpc_of(nfs),
            Some(cb_unit_setattr),
            &mut args,
            &mut slot as *mut Slot as *mut c_void,
        )
    });
    slot.finish("SETATTR").map(|_| ())
}

unsafe extern "C" fn cb_unit_rename(
    rpc: *mut b::rpc_context,
    status: c_int,
    data: *mut c_void,
    pd: *mut c_void,
) {
    let slot = &mut *(pd as *mut Slot);
    if let Some(data) = slot.begin(rpc, status, data) {
        let res = &*(data as *const b::RENAME3res);
        slot.nfs_status = res.status;
        if res.status == b::NFS3_OK {
            slot.out = Some(Out::Unit);
        }
    }
}

/// RENAME `from_name` → `to_name`, both within `dir_fh`.
pub fn rename(
    nfs: &mut NfsContext,
    dir_fh: &[u8],
    from_name: &[u8],
    to_name: &[u8],
) -> Result<(), RawError> {
    let cfrom = cstring(from_name, "RENAME")?;
    let cto = cstring(to_name, "RENAME")?;
    let mut slot = Slot::new();
    let mut args = b::RENAME3args {
        from: b::diropargs3 {
            dir: fh3(dir_fh),
            name: cfrom.as_ptr() as *mut c_char,
        },
        to: b::diropargs3 {
            dir: fh3(dir_fh),
            name: cto.as_ptr() as *mut c_char,
        },
    };
    issue!(nfs, &slot, "RENAME", {
        b::rpc_nfs3_rename_task(
            rpc_of(nfs),
            Some(cb_unit_rename),
            &mut args,
            &mut slot as *mut Slot as *mut c_void,
        )
    });
    slot.finish("RENAME").map(|_| ())
}
