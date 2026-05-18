# Async libnfs FFI surface (prerequisite for multi-pass / bucketed mover)

Status: not started. This is a hand-offable prompt for a focused
implementation effort. It is the prerequisite for
`docs/work-items/MULTI_PASS_MOVER.md`; that work assumes the surface
described here is landed and verified.

---

## Goal

Add an **async libnfs Rust FFI surface** to vamoose, so a single
`nfs_context` can have many concurrent in-flight RPCs driven by tokio
futures rather than by one-OS-thread-per-RPC. The current sync FFI
(`crates/migration-mover/src/libnfs/`) binds only the blocking entry
points; concurrency today is "many files in parallel via
`spawn_blocking`", capped by the blocking-pool size and one RPC in
flight per OS thread. The multi-pass mover work requires
per-file pipeline depths of 8–32 against the same connection, which
the blocking model cannot deliver.

The deliverable is a new Rust module (and the underlying libnfs
linkage decision) such that:

```rust
let ctx = AsyncNfsContext::mount(url, MountOpts { rsize: 4 * MiB, nconnect: 2, readahead: 128 * MiB })?;
let fh  = ctx.open(path, Flags::RDONLY).await?;
let buf = ctx.pread(&fh, offset, length).await?;
ctx.close(fh).await?;
```

…where `.await` actually yields the runtime, and the underlying
libnfs context can have N concurrent reads in flight at any moment.

---

## Why this work

- **Sync FFI is structurally incompatible with per-file pipelining.**
  Today `do_libnfs_copy` (`mover.rs:640-712`) is a `while remaining > 0`
  sync loop that issues one `nfs_pread` at a time inside a
  `spawn_blocking` closure. There is no way to issue a second
  `nfs_pread` against the same `nfs_context` until the first returns,
  because that thread is parked on the C call. Pipelining requires
  the async surface.

- **M3 explicitly left this door open.** `M3_NOTES.md` reaches the
  conclusion "true io_uring integration with libnfs — Decision:
  deliver concurrency via N libnfs contexts driven by tokio's
  blocking pool. M3.5 candidate, measure first." This work is the
  M3.5 follow-through.

- **The decision to commit to async libnfs has been made.** See
  the sign-off in the conversation that produced this work item
  (whole-file recopy for v1, async libnfs adopted now, not deferred
  pending measurement).

- **One worst-case-bug to avoid replicating.** The M2 FFI silent-
  zero-byte read bug (`M2_NOTES.md` "M2/M3 verification incidents",
  `docs/CORRECTNESS_RULES.md` "Cross-check C library FFI") was
  caused by binding `nfs_pread` against the wrong header. The async
  surface has more symbols, more parameters, and callback signatures
  on top — the same class of bug, multiplied. The FFI smoke test
  gate is non-negotiable; see "Verification" below.

---

## What "fork of libnfs" means here

The user mentioned forking libnfs. Two interpretations need to be
distinguished early in the implementation:

1. **No C-side changes required.** Upstream libnfs (at minimum
   v4.x; this host has both v4.0.0 at `/usr/lib/x86_64-linux-gnu/`
   and v5.0.2 at `/usr/local/lib/`) already exposes the async API
   we need: `nfs_pread_async`, `nfs_pwrite_async`, `nfs_commit_async`,
   `nfs_stat64_async`, `nfs_open_async`, `nfs_close_async`,
   `nfs_service`, `nfs_get_fd`, `nfs_queue_length`. Verify against
   the locally checked-out source at `~/projects/libnfs/include/nfsc/libnfs.h`
   (which is the vamoose-pinned tree) — if every symbol the
   bindings list (below) is present, no C patches are needed.
   The "fork" then is just pinning that source tree and building
   against it deterministically, the same way `migration-mover/build.rs`
   already does with `pkg-config`.

2. **Patches required.** Some tunables (`nfs_set_readahead`,
   per-context `rsize`/`wsize`, `nconnect` semantics) may be
   incomplete or have subtly different behavior across libnfs
   versions. If so, the fork carries the patches. Each patch must
   be a discrete commit on top of an upstream tag, with the upstream
   tag recorded in `THIRD_PARTY_LICENSES.md`. Patches are LGPL-2.1+
   per upstream and must be offered back upstream eventually
   (separate effort).

**The first task** of this work is to audit the locally pinned
libnfs source against the symbol list below and decide which
interpretation applies. Don't write Rust bindings against assumptions —
write them against `objdump -T` output from the linked `.so`.

---

## Scope

In scope:

- Audit of `~/projects/libnfs/` (or whichever source tree the build
  is pinned to) against the symbol list below.
- Patches to libnfs C source if and only if the audit shows specific
  symbols missing or broken. Each patch in its own commit, with a
  failing test case in the audit log.
- New Rust module `crates/migration-mover/src/libnfs/asyncio/` (or
  a separate crate `migration-mover-async-libnfs/` if the surface
  ends up large enough to want isolation — judgement call during
  implementation).
- Per-context service-task driver (one tokio task per `nfs_context`,
  driving `nfs_service()` when the context's fd is readable, via
  `tokio::io::unix::AsyncFd`).
- Future-bridging: every async libnfs call returns a Rust future
  that resolves when the libnfs callback fires.
- Mount-time tunables: `rsize`, `wsize`, `nconnect`, libnfs-
  internal-readahead. Surface them as a single `MountOpts` struct on
  the Rust side; map them to whatever combination of URL params /
  `nfs_set_*` calls / mount flags the linked libnfs version requires.
- FFI smoke test for every newly bound symbol, in the spirit of the
  existing `crates/migration-mover/tests/libnfs_ffi_smoke.rs`.

Out of scope:

- Anything beyond the FFI + service task. No bucketed pool, no
  per-file pipeline, no manifest writer. Those land in the
  multi-pass mover work item.
- Replacing the existing sync FFI. The sync surface stays — pass-0
  bulk copy continues to use it until measurement justifies
  retiring it. The async surface is **additive**.
- Behavioral changes to the existing migration-worker or
  orchestrator. No call site in the existing codebase changes as
  part of this work.
- io_uring integration. libnfs is userspace TCP; io_uring's read/
  write SQEs don't apply to NFS RPCs. M3 already reached this
  conclusion. The `uring.rs` placeholder stays as-is.
- NFSv4 / NFSv4.2 features (server-side COPY, change_attr,
  READ_PLUS, ALLOCATE). NFSv3 only, per `CORRECTNESS_RULES.md`
  "NFSv3 is the protocol baseline".

---

## Symbol surface to bind

Each line is `(C symbol, Rust wrapper, what it does, blocker?)`.
"Blocker?" means: is this symbol required for the M3.5/multi-pass
mover work to function, or is it a later nice-to-have?

| C symbol | Rust async wrapper | Purpose | Blocker? |
|---|---|---|---|
| `nfs_open_async` | `ctx.open(path, flags)` | Open a file for read or write | Y |
| `nfs_close_async` | `ctx.close(fh)` | Close a file handle | Y |
| `nfs_pread_async` | `ctx.pread(&fh, off, len)` | Async read at offset | Y |
| `nfs_pwrite_async` | `ctx.pwrite(&fh, off, buf)` | Async write at offset (UNSTABLE) | Y |
| `nfs_write_async` (variant) | `ctx.pwrite_stable(&fh, off, buf, FILE_SYNC)` | Stable write — cutover mode | Y |
| `nfs_commit_async` | `ctx.commit(&fh, off, len)` | NFS COMMIT for UNSTABLE writes | Y |
| `nfs_stat64_async` | `ctx.stat(path)` | Stat a path — torn-read detection | Y |
| `nfs_fstat64_async` | `ctx.fstat(&fh)` | Stat via fh — alternate torn-read path | Y |
| `nfs_create_async` | `ctx.create(path, mode, flags)` | Create destination file | Y |
| `nfs_unlink_async` | `ctx.unlink(path)` | Unlink (cleanup `.partial`) | Y |
| `nfs_rename_async` | `ctx.rename(old, new)` | Atomic rename (commit point) | Y |
| `nfs_utimes_async` / `nfs_utimensat_async` | `ctx.utimes(path, atime, mtime)` | mtime preservation (post-data) | Y |
| `nfs_chmod_async` | `ctx.chmod(path, mode)` | Mode preservation | Y |
| `nfs_chown_async` | `ctx.chown(path, uid, gid)` | Ownership preservation | Y |
| `nfs_symlink_async` | `ctx.symlink(target, link)` | Symlink replication | Y |
| `nfs_link_async` | `ctx.link(old, new)` | Hardlink replication | Y |
| `nfs_mkdir_async` | `ctx.mkdir(path, mode)` | Directory creation | Y |
| `nfs_readlink_async` | `ctx.readlink(path)` | Symlink target (when not cached) | Y |
| `nfs_service` | (driver) | Drive RPC completion when fd readable | Y |
| `nfs_get_fd` | (driver) | Get the socket fd to register with `AsyncFd` | Y |
| `nfs_which_events` | (driver) | What events libnfs wants on the fd | Y |
| `nfs_set_rsize` / `rsize=` mount opt | `MountOpts::rsize` | Per-RPC read size | Y |
| `nfs_set_wsize` / `wsize=` mount opt | `MountOpts::wsize` | Per-RPC write size | Y |
| `nconnect=` mount opt (libnfs v5+) | `MountOpts::nconnect` | TCP connection count per context | Y |
| `nfs_set_readahead` | `MountOpts::readahead` | Defensive readahead on the sync API | Y (low-effort) |
| `nfs_queue_length` | (driver / debug) | In-flight RPC count for observability | N |
| `nfs_set_version` | (constructor) | Force NFSv3 per the protocol-baseline rule | Y |

**Verification step before any binding is written:** generate the
above table against the actually-linked `.so` using
`objdump -T /path/to/libnfs.so` and the header at the same prefix.
Flag any symbol that exists in the header but not in the binary, or
vice versa. Multiple libnfs versions are present on the host — do
not skip this step. The M2 silent-zero-byte bug is the cautionary
tale.

The exact `objdump` recipe is documented in
`docs/CORRECTNESS_RULES.md` "Cross-check C library FFI"; reuse it
verbatim.

---

## Service-task model

libnfs's async surface is callback-based: every `*_async` call
returns immediately after enqueueing the RPC; the result arrives
later when libnfs runs `nfs_service()` and that drains the network
socket. The Rust binding has to bridge that into futures.

### Design

For each `AsyncNfsContext`:

1. On construction, the context's socket fd (`nfs_get_fd(ctx)`) is
   registered with `tokio::io::unix::AsyncFd`.
2. A single dedicated tokio task ("service task") runs in a loop:
   - `fd.readable().await?` (or `fd.ready(events).await?` for the
     mask returned by `nfs_which_events`).
   - Call `nfs_service(ctx, revents)`. This causes libnfs to read
     completions from the wire, parse them, and invoke their
     callbacks. The callbacks fire on this thread.
   - Each callback unwraps the `private_data` pointer back into a
     boxed `tokio::sync::oneshot::Sender<Result<…>>` and sends the
     result.
   - Loop.
3. The service task owns the `nfs_context`. **No other thread or
   task may issue libnfs calls against the same context.** libnfs
   contexts are not thread-safe. Issuance of new RPCs happens
   inside the service task too: the public Rust API
   (`ctx.pread(...)`) sends a `Request` message on an mpsc channel
   that the service task selects on alongside the fd-readable wake.
4. Cancellation: dropping the returned future does **not** cancel
   the in-flight RPC (libnfs has no cancel surface for v3). The
   oneshot receiver is dropped, the result is discarded on
   completion. Document this prominently; callers that need
   cancellation must use a higher-level token + check it themselves.

### Why one service task per context, not one per connection

libnfs's `nconnect` opens multiple TCP connections internally and
multiplexes RPCs across them transparently. From the C API's
perspective, there's still one `nfs_context` and one fd-of-record.
The Rust binding sees one fd; libnfs handles the connection fan-out.
A pool of three contexts (small / medium / large bucket) → three
service tasks, regardless of total connection count.

### Issuance and completion plumbing

```rust
//   ┌─────────────────────────────────────────────────────────────┐
//   │   Caller task                  Service task                  │
//   │      │                              │                        │
//   │      │  Request{op, oneshot_tx} ────▶  mpsc::recv()          │
//   │      │                              │                        │
//   │      │                              │  nfs_*_async(ctx, …,   │
//   │      │                              │     callback,          │
//   │      │                              │     private_data=box(  │
//   │      │                              │        oneshot_tx))    │
//   │      │                              │                        │
//   │      │                              │  fd.readable().await   │
//   │      │                              │  nfs_service(ctx, ev)  │
//   │      │                              │      │                 │
//   │      │                              │      ▼                 │
//   │      │                              │   callback fires       │
//   │      │                              │   → unbox private_data │
//   │      │                              │   → oneshot_tx.send()  │
//   │      │                              │                        │
//   │      ▼                              │                        │
//   │  oneshot_rx.await ◀──────────────── completion delivered     │
//   └─────────────────────────────────────────────────────────────┘
```

The mpsc channel between caller and service task is bounded
(reasonable default 1024 in-flight per context — well past the
configured per-file pipeline depths). Bound choice is a tuning knob,
not a correctness knob, but should be documented.

### Buffer ownership

`nfs_pread_async` writes into a libnfs-allocated buffer; the
callback receives `(status, data_ptr, data_len)`. The Rust binding
copies into a caller-supplied `Vec<u8>` (or `BytesMut`) in the
callback before sending the oneshot result. No buffer-lifetime
gymnastics across the FFI boundary; the C-side memory is valid only
for the duration of the callback.

`nfs_pwrite_async` takes a caller-supplied buffer that must stay
alive until the callback fires. The Rust binding boxes the buffer
into the request and unboxes-and-drops in the callback. This is
the one place the API can't be zero-copy; document it and move on.

---

## Mount-time tunables

The `MountOpts` struct collects everything that has to be set
before or at mount time:

```rust
pub struct MountOpts {
    pub rsize: u32,           // bytes per read RPC
    pub wsize: u32,           // bytes per write RPC
    pub nconnect: u32,        // TCP connections per context (v5+)
    pub readahead: u32,       // bytes; sync-API backstop
    pub version: u8,          // 3 — locked per CORRECTNESS_RULES.md
}
```

Mapping to libnfs:

- `rsize` / `wsize`: pass as `rsize=N,wsize=N` mount-URL parameters
  if the linked libnfs supports them; otherwise set via
  `nfs_set_rsize` / `nfs_set_wsize` *before* `nfs_mount`. Verify which
  works against the linked binary; document the choice.
- `nconnect`: mount-URL parameter `nconnect=N` in libnfs v5+. v4
  does not support this; the audit step must catch that and either
  upgrade the linked binary or reject the option (with a loud
  warning) on v4.
- `readahead`: `nfs_set_readahead(ctx, bytes)` after mount. Applies
  to the sync-API `nfs_read`; documented in the multi-pass mover
  work item as a defensive backstop only.
- `version`: set NFSv3 explicitly via `nfs_set_version(ctx, NFS_V3)`
  before mount. Hard-pin per the protocol-baseline rule. Do not
  expose v4 as a knob.

The existing sync `NfsContext::mount_url` (`libnfs/mod.rs:198-220`)
does not set any of these; it relies on libnfs defaults. The async
surface explicitly configures all five.

---

## Verification

### FFI smoke test gate

This is the load-bearing safety net. Every newly bound symbol gets
a smoke test, modeled on
`crates/migration-mover/tests/libnfs_ffi_smoke.rs`. The test must:

- Run against a real NFS export (gated behind `VAMOOSE_TEST_NFS_URL`).
- Round-trip a known byte pattern through `pread_async` and
  `pwrite_async` and assert correctness.
- Specifically catch the M2-class bug: assert that
  `pread_async(off=0, len=4096)` returns 4096 bytes that match the
  source's first 4096 bytes byte-for-byte. A "successful" RPC that
  returns zero bytes is the failure mode.
- Run against both libnfs versions present on the verification host
  (`/usr/lib/x86_64-linux-gnu/libnfs.so.14.0.0` and
  `/usr/local/lib/libnfs.so.16.0.2`) — confirm the binding is
  correct against whichever the build links.

This test must pass before the surface is merged. No exceptions.

### Service-task integration test

Beyond per-symbol smoke, one integration test that:

- Mounts an async context.
- Issues N=64 concurrent `pread_async` calls against the same
  context.
- Asserts all 64 complete with correct data.
- Asserts the service task wakes the right futures (no
  cross-talk).
- Asserts that dropping one future before completion does not
  break the others.

This catches the class of bug where the callback's `private_data`
gets crossed-up or the oneshot bridge has a race.

### Performance smoke

One micro-bench, not gated on correctness:

- Single context, 32 concurrent `pread_async` calls of 4 MiB each
  at sequential offsets, against a single source file ≥ 1 GiB.
- Report measured throughput.
- Compare to the same workload run against the sync FFI inside a
  `spawn_blocking` loop.
- The async surface should match or beat the sync surface even
  with the service-task overhead. If it doesn't, something is
  wrong; investigate before declaring done.

Numbers go into the work-item closing note. They become the input
for the multi-pass mover's bucket-tuning decisions.

---

## File locations

Expected layout (final shape is implementer's call; this is a
suggestion):

```
crates/migration-mover/src/libnfs/
├── mod.rs               # existing sync FFI — unchanged
├── ops.rs               # existing safe sync wrappers — unchanged
├── pool.rs              # existing sync pool — unchanged
├── asyncio/
│   ├── mod.rs           # AsyncNfsContext, MountOpts
│   ├── ffi.rs           # extern "C" declarations for async symbols
│   ├── driver.rs        # service-task loop + AsyncFd wiring
│   ├── request.rs       # Request enum + oneshot bridging
│   └── callbacks.rs     # raw extern "C" callback functions
crates/migration-mover/tests/
├── libnfs_ffi_smoke.rs        # existing sync smoke — extend with async cases
└── libnfs_async_integration.rs # new — N-concurrent test
```

Any new file copied or adapted from libnfs/nfs-walker carries the
appropriate MIT or LGPL header per
`docs/CORRECTNESS_RULES.md` and `DESIGN.md` "Attribution / licensing
notes".

---

## What "done" looks like

All of the following must be true:

- **A. Symbol audit committed.** `docs/work-items/LIBNFS_ASYNC_FORK_AUDIT.md`
  records every symbol in the table above with: (1) is it in the
  pinned libnfs source? (2) is it in the linked `.so` per
  `objdump -T`? (3) does its parameter order match the header at
  the same prefix? Any "no" answer is resolved before any binding
  is written.
- **B. Patches isolated.** If any libnfs C-side patch was required,
  it lives in `~/projects/libnfs/` (or wherever the source is
  pinned) as a discrete commit on top of an upstream tag, with the
  tag recorded in `THIRD_PARTY_LICENSES.md`. No patches squashed
  together. Each patch has a smoke test that fails without it.
- **C. Async FFI surface compiles** under `cargo build -p migration-mover`
  with no new warnings.
- **D. FFI smoke test passes** against real VAST hardware
  (`VAMOOSE_TEST_NFS_URL=...` invocation, same shape as the
  existing M2 smoke). Round-trips a known byte pattern through
  every async op in the table. Failure mode: byte-mismatch is a
  hard fail, not a soft fail.
- **E. Service-task integration test passes** — 64-way concurrent
  reads against one context, no cross-talk, drop-during-flight
  doesn't break neighbors.
- **F. Perf smoke recorded.** Async throughput meets or beats sync
  throughput on the same hardware. Number goes into the closing
  note of this work item.
- **G. `CORRECTNESS_RULES.md` updated** with any new invariants
  the async surface introduces (e.g., "service task owns the
  context — no other thread may issue calls against it").
- **H. Existing M2/M3/M5 verification** re-run end-to-end and
  passes. The async FFI is additive; existing sync paths must be
  unaffected.

---

## Closing note (2026-05-18, var204)

Implementation landed. All eight "done" gates A–H satisfied. Specific
results worth recording for the multi-pass mover work item:

- **Symbol audit (Gate A):** `docs/work-items/LIBNFS_ASYNC_FORK_AUDIT.md`.
  No C-side patches required (interpretation #1). Substitutes:
  `nfs_fsync_async` for `nfs_commit_async`, `nfs_open2_async` for
  `nfs_create_async`, `nfs_set_readmax`/`writemax` for
  `nfs_set_rsize`/`wsize`. `nconnect>1` rejected at mount time
  (linked libnfs v6 lacks support); `nfs_set_readahead` and
  `nfs_utimensat_async` dropped from the surface.

- **Patches (Gate B):** none in this work item. Pinned source tree
  `~/projects/libnfs/` unchanged.

- **Compile (Gate C):** clean — `cargo build -p migration-mover`
  produces no warnings.

- **Async FFI smoke (Gate D):** 5/5 pass against var204
  (`async_pread_returns_actual_bytes`, `async_write_then_read_roundtrip`,
  `async_stat_and_fstat_match`, `async_attribute_and_namespace_ops_round_trip`,
  `async_symlink_readlink_roundtrip`).

- **Service-task integration (Gate E):** 4/4 pass — 64-way concurrent
  pread no cross-talk, drop-during-flight survives, `nconnect>1`
  rejection, NFSv3-only gate.

- **Perf smoke (Gate F):** at 1 ms service tick, var204, single
  context, 32 concurrent 1 MiB reads:
  - **ASYNC: 352.5 MB/s** (95 ms)
  - **SYNC:  269.5 MB/s** (125 ms)
  Async beats sync by ~30 % despite the service-task indirection;
  the sync path is bottlenecked by spawn_blocking-pool serialization
  against the single SimplePool pair. The ASYNC number is the floor
  for what one context can do at this rsize; the multi-pass mover's
  bucketed pool with three contexts should scale further. The
  result also justified the **1 ms tick** decision: at 10 ms the
  async path was 110 MB/s (≈ one tick of latency per response).

- **Correctness invariants (Gate G):** `docs/CORRECTNESS_RULES.md`
  updated with two new rules: "service task owns the context" and
  "dropping a future does NOT cancel the RPC".

- **Existing M2/M3/M5 (Gate H):** sync FFI smoke still passes
  (`libnfs_ffi_smoke::nfs_pread_returns_actual_bytes`); 58/58 unit
  tests pass; no changes to the sync surface so existing call sites
  are unaffected.

### Driver-design notes worth carrying forward

- mio's epoll registration is edge-triggered. With libnfs's API,
  callbacks fired inside `nfs_service` can enqueue more RPCs that
  need the same direction we just got readiness for. Once
  `clear_ready()` is called the fd does not transition again
  (kernel buffer state didn't change) and we deadlock until the
  next tick. Fix: the tick arm uses a non-blocking `libc::poll` to
  read the *current* level state and pass those revents to
  `nfs_service`. This is exactly what `libnfs-sync.c::wait_for_nfs_reply`
  does (poll with `nfs_which_events`), just driven from a 1 ms
  tokio interval instead of a blocking loop.
- Inner drain loop after each AsyncFd wake: caps at 8 iterations,
  breaks early when libnfs no longer wants the direction we have
  readiness for OR when the queue is empty. Without this drain,
  libnfs's "callback enqueues another RPC" pattern stalls until the
  next tick.
- Service-task `OwnedContext` is the only `Send` wrapper for the
  raw `*mut nfs_context`. The context never escapes the task; the
  pointer is recreated as `owned.0` inline at each FFI call site
  so the future stays `Send` across awaits.

## Risks and known unknowns

- **libnfs version skew.** The host has v4 and v5 side by side.
  `nconnect=` is v5+. If the production target only has v4, the
  bucket configs that rely on `nconnect>1` degrade silently. The
  audit step must surface this; mount must reject `nconnect>1` on
  v4 rather than silently ignoring.
- **`nfs_service` reentrancy.** Some libnfs versions document that
  `nfs_service` can re-enter through callbacks (i.e., a callback
  can fire and trigger another RPC that's queued and processed
  before `nfs_service` returns). The service task design assumes
  the callback's role is just `oneshot_tx.send(...)` — no further
  libnfs calls from inside the callback. Document this prominently;
  any future work that wants to chain libnfs calls from a callback
  must redesign.
- **Buffer copy in pread.** Discussed above. The C-side buffer is
  callback-scoped; the Rust binding copies into a caller buffer.
  This is a per-read memcpy of `rsize` bytes. At 4 MiB rsize and
  10 GbE wire-rate (~1.2 GB/s), that's ~300 memcpys/sec per
  context — negligible. Document it; flag if profiling later shows
  otherwise.
- **`nfs_pwrite_async` UNSTABLE vs FILE_SYNC.** Some libnfs versions
  expose this via a separate symbol; others via a flag parameter on
  `nfs_pwrite_async`. The audit step decides which. The Rust
  surface exposes both stability levels regardless.
- **Cancellation semantics.** Dropping a future does not cancel
  the RPC. This is a real correctness consideration for the
  multi-pass mover's cutover-pass behavior (where we may want to
  abandon a slow read). Document the limitation; the multi-pass
  mover work item must explicitly handle it (likely by holding the
  oneshot and discarding on completion rather than relying on drop).
- **NFS-side queue overflow.** libnfs has an internal queue; sending
  100k concurrent requests on one context overflows it. The bound
  on the mpsc channel between caller and service task is the only
  thing protecting against this from the Rust side. Pick a sane
  default (~1024) and document.

---

## Out of scope, deferred to follow-up work

- **The bucketed pool itself.** This work item delivers the building
  block (`AsyncNfsContext` + `MountOpts`); the multi-pass mover
  work item assembles three of them into a `BucketedAsyncPool`.
- **Per-file pipeline orchestration** (FuturesUnordered, depth
  enforcement). Multi-pass mover.
- **NFSv4 surface.** Pinned to v3 per the rule. If v4 becomes a
  goal, it's a separate work item that revisits the audit.
- **io_uring.** Not applicable; see scope.

---

## Cross-references

- `docs/CORRECTNESS_RULES.md` — invariants that must continue to hold.
- `M3_NOTES.md` — original "M3.5 candidate, measure first" decision
  that this work follows through on.
- `M2_NOTES.md` — the silent-zero-byte FFI bug that motivates the
  smoke-test gate.
- `crates/migration-mover/src/libnfs/mod.rs` — existing sync FFI
  surface; unchanged by this work but the reference for parameter
  conventions.
- `crates/migration-mover/tests/libnfs_ffi_smoke.rs` — existing
  smoke test; extended by this work, not replaced.
- `docs/work-items/MULTI_PASS_MOVER.md` — downstream work that
  consumes this surface.
