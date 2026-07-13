# Design: protected-FFI data-plane batch — durability (F09), close-while-inflight (F11), I/O deadlines (F12)

Status: DRAFT — decision document, nothing here is implemented.
Ledger: F09, F11, F12 in `docs/REVIEW_LEDGER.md`.
Why one doc: all three land in or against the protected FFI layer
(`crates/migration-mover/src/libnfs/`), where every change must be
verified against BOTH the pinned source tree
(`~/projects/libnfs`, tag `libnfs-6.0.2-148-gdc7e6f8`) and the
actually-linked binary (`/usr/local/lib/libnfs.so.16.0.2`,
pkg-config 16.2.0). The audit (`LIBNFS_ASYNC_FORK_AUDIT.md` item #4)
already flags header drift between the two — every new binding gets
checked against both, and gets an `#[ignore]`d smoke case in an
EXISTING test binary.

Decision points for the project owner are marked **D1–D5**.

## Context: which path is production

The DEFAULT data plane is the **sync** path
(`Mover::do_libnfs_copy`, `mover.rs:546-604`, via blocking
`ops::pread`/`ops::pwrite` on `spawn_blocking` threads). The async
pipelined path is opt-in (`use_bucketed_pool`, default false,
`config.rs:106-109`) and even when enabled only takes regular-file
rows; sync handles everything else. So F09 and F12 are findings
about the path every deployment runs today.

---

## F09 — sync copy path never issues NFS COMMIT

### Problem

Sync write order is: WRITE loop (UNSTABLE) → close both fhs →
attrs → fence check → rename (`mover.rs:546-604`). No stability
barrier exists between the last WRITE and the rename: the sync FFI
surface (`libnfs/mod.rs:94-195`) binds no `nfs_fsync` and no COMMIT
of any kind. On an NFSv3 server that buffers unstable writes, a
server crash after our rename-and-ack can lose acknowledged bytes —
the row is recorded moved, the file is torn. The async path already
got this right: `pipelined_copy` drains writes then issues a
whole-file COMMIT via `nfs_fsync_async` (`pipelined_copy.rs:245-258`),
and `file_mover.rs:22-28` states "bytes are durable" — for that path
only.

On VAST this is mitigated (writes ack from NVRAM), which is why the
fleet hasn't noticed. Nothing in DESIGN.md actually documents that
assumption — the only place "NVRAM" appears in the repo is the
ledger row itself.

(NFSv3 metadata ops — CREATE, RENAME, the hardlink LINK — are
synchronous at the protocol level; the gap is file DATA only.)

### D1 — mechanism

**Option A (recommended): bind sync `nfs_fsync`, COMMIT once per
file.** One new extern in `libnfs/mod.rs` (verified against pinned
tree + linked .so), one safe wrapper in `ops.rs`, one call site in
`do_libnfs_copy` after `stream_copy` and before `close_fh`. Mirrors
the async path's proven semantics (whole-file COMMIT; per-range
COMMIT doesn't exist in this libnfs — audit line 71). Cost: one RPC
per regular file — negligible against a full copy, and on VAST the
COMMIT returns immediately. A COMMIT failure fails the row through
the normal `MoveError` path (new `FailurePhase::Commit` or reuse
Write — implementer's call, but it must be diagnosable).

**Option B: open the sync dst with O_SYNC** (per-write FILE_SYNC —
`asyncio/mod.rs:59-62` documents libnfs's translation). Zero new FFI
bindings, smallest possible diff. But it serializes stability into
every WRITE: on a generic (non-NVRAM) destination this is the
slowest possible mode, and F09 exists precisely for generic
destinations. Wrong cost model for the case it fixes.

**Option C: document VAST-only durability, change no code.** Add the
NVRAM-ack assumption to DESIGN.md and MANUAL_VERIFY.md, declare
generic NFSv3 destinations unsupported for the sync path. Honest and
free, but it converts a fixable gap into a permanent product
limitation, and the async path already proves we care about generic
correctness.

**Recommendation: A**, plus the one paragraph of C's documentation
regardless (DESIGN.md should state the durability model explicitly:
UNSTABLE writes + whole-file COMMIT before rename, both paths).

Empty-file rows (CREATE + rename, no data) and hardlink/symlink/dir
rows carry no unstable data and need no COMMIT.

### Verification

CI: none possible (needs a real server). Hardware: extend the
existing `#[ignore]` sync smoke (`libnfs_ffi_smoke.rs` /
`file_mover_smoke.rs` — existing binaries only) with a
write→fsync→read-back case; the byte-perfect assertions in
`pipelined_copy_smoke.rs` stay the async vehicle. True
crash-durability (kill the server mid-run) is a rig exercise —
note it in MANUAL_VERIFY.md rather than pretending a test proves it.

---

## F11 — error path closes fhs with pread/pwrite RPCs still in flight

### Problem

`AsyncBucketedFileMover::copy_regular` (`file_mover.rs:245-252`):
when `pipelined_copy` errors out early, its `reads_inflight` /
`writes_inflight` sets are dropped — and dropping a future does NOT
cancel the RPC (`asyncio/mod.rs:19-26`; libnfs has no NFSv3 cancel).
The very next lines close both fhs. libnfs frees the `nfsfh` on
close while pending READ/WRITE RPCs on the same fh may still
complete against it — a use-after-free inside the C library. The
Rust-side buffers are safe (each pending op owns its buffer until
its callback fires, `callbacks.rs:53-64`); the hazard is purely
libnfs-internal fh state.

No drain mechanism exists: the only outstanding-op accounting is
libnfs's `nfs_queue_length`, consulted only at service-task shutdown
(`driver.rs:262-267`), never before a `close`. The nearest existing
test (`dropping_futures_does_not_break_neighbors`,
`libnfs_async_integration.rs:129`) closes AFTER awaiting survivors,
so nothing today exercises close-while-genuinely-inflight.

### D2 — mechanism

**Option A (recommended): drain-before-close in `pipelined_copy`'s
error path.** On any early error, before returning: stop issuing new
ops and await everything still in `reads_inflight`/`writes_inflight`,
discarding results. The fhs are then quiescent when `copy_regular`
closes them. Pure Rust-layer change, no FFI surface touched, robust
against libnfs internals changing. Cost: error-return latency is
bounded by the slowest outstanding RPC — today that's libnfs's ~60s
internal timeout worst-case, and it becomes the F12 deadline once
that lands (the two findings compose: F12 makes F11's drain bounded).

**Option B: verify against the linked .so and do nothing.** Audit
`libnfs.so.16.0.2` / the pinned tree to establish whether close with
outstanding same-fh RPCs is actually safe (e.g. internal
refcounting). Even if today's answer is "safe", the conclusion is
version-fragile — the audit already documents drift between pinned
source and installed binary — and it must be re-established at every
libnfs bump. Acceptable only as a stopgap.

**Recommendation: A.** Do the B audit once anyway during
implementation (it tells us how urgent backporting is), but the
drain ships regardless. The fork doc (`LIBNFS_ASYNC_FORK.md:550-555`)
already told us drop-doesn't-cancel must be handled explicitly —
this is that item coming due.

### Verification

Hardware: new `#[ignore]` case in `libnfs_async_integration.rs`
(existing binary): issue reads, inject a failure so the copy errors
with ops genuinely inflight, assert the drain leaves the context
usable (subsequent open/read works) and nothing crashes. CI: if the
drain is written as a distinct code path with a seam (e.g. a
`drain_inflight()` helper), its ordering logic gets a unit test; the
UAF itself is only demonstrable on hardware.

---

## F12 — no I/O deadline anywhere on the data plane

### Problem

No timeout API is bound (`nfs_set_timeout` absent from both FFI
surfaces; `MountOpts` has no timeout field). Sync path: a stalled
server wedges the `spawn_blocking` thread inside `nfs_pread`/`pwrite`
forever — the shard never completes, while the worker's heartbeat
task (separate tokio task) keeps the claim looking healthy
indefinitely. This defeats the reclaim design: progress-as-
reclaim-input never fires because the worker IS alive, just wedged.
Async path: the oneshots simply never resolve, subject only to
libnfs's ~60s internal RPC timeout (`LIBNFS_ASYNC_FORK.md:601-632`)
— which is at least bounded, but implicit and unconfigured.

### D3 — mechanism

**Recommended: bind `nfs_set_timeout` (one extern, both FFI
surfaces share it), call it at every context-creation point** — sync
`NfsContext::mount_url`, async `AsyncNfsContext::mount`, bucketed
`mount_pair` — plumbed from a new `[mover] rpc_timeout_ms` in
worker.toml (`config.rs` `[mover]` block, next to `nfs_connections`),
threaded through `MoverConfig::from_options` and `MountOpts`.
libnfs then fails the RPC with `RPC_STATUS_TIMEOUT`, which surfaces
as an ordinary error through the existing error paths on both the
sync and async surfaces — no Rust-side watchdog machinery.

Rust-side alternatives don't work for the default path: you cannot
cancel a blocking FFI call from tokio — a `tokio::time::timeout`
around `spawn_blocking` abandons the thread (leaks it wedged and the
fh with it) rather than freeing it.

Sub-decisions inside D3:
- **Default value.** Suggest 60_000 ms — matches libnfs's implicit
  behavior, makes it explicit and configurable; `0` = library
  default/disabled. A tighter default (e.g. 30s) risks failing slow-
  but-progressing large-file servers; the knob exists for operators
  who know their fleet.
- **Classification.** Timeout errors must classify as retryable
  WorkerLocal-style failures (transient path), not shard corruption
  — a wedged server is an infra event. The classifier bridge from
  the F-series error-classification work is the right seam.

### D4 — timeout granularity caveat to accept

`nfs_set_timeout` is per-RPC, per-context. It bounds each READ/WRITE
RPC, not the whole file copy; a server drip-feeding one RPC per 59s
still crawls. That is acceptable: the finding is "wedges FOREVER
while heartbeats look healthy," and per-RPC bounds fix exactly that.
A whole-row deadline on top would be Rust-side and cheap to add
later if drip-feed servers show up in practice; out of scope now.

### Verification

Hardware: `#[ignore]` case in `libnfs_async_integration.rs` against
an unreachable/blackholed address asserting the op fails within
~timeout rather than hanging (the existing fd-swap test at `:235`
already demonstrates the 60s hang shape to assert against). Config
plumbing: normal CI unit tests (parse → MoverConfig → MountOpts).

---

## D5 — packaging and sequencing

**Recommended: one work item, one session, in this order: F12 →
F11 → F09.** Rationale: F12's deadline is what makes F11's drain
bounded; F09's fsync is the only one that adds a *data-path* RPC and
is cleanest to land on a tree where errors are already bounded. All
three touch the protected layer, so one session holding the full FFI
context beats three sessions re-deriving the verification rules.
Fences for that work item: new externs verified against pinned tree
AND linked .so with the drift check written into the work-item doc;
no changes to existing extern signatures; no new top-level test
files; claim.rs untouched (confirmed: the FFI layer has no reference
to claim logic today).

Total new FFI surface if all recommendations are taken: **two
externs** (`nfs_fsync`, `nfs_set_timeout`) — the minimum any
code-fixing resolution of F09/F12 can have. F11 adds none.
