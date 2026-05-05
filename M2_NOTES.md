# M2 — Notes from the milestone

Information M3 should have when it picks up the data path.

## Library versions

- libnfs: **fill in via `pkg-config --modversion libnfs` after building**
  on the verification host.
- Built and statically observed `nfs_utimes(struct timeval *)`
  (microsecond precision). Nanosecond columns from the index are
  truncated. If a future libnfs build exposes `nfs_utimens`/
  `nfs_futimens`, switch to that and update the FFI block in
  `crates/migration-mover/src/libnfs/mod.rs`.

## Strategy coverage in M2

| Strategy | Status |
|---|---|
| `LibnfsIoUring` (default) | implemented as plain libnfs READ→WRITE, no io_uring (M3) |
| `Empty` | implemented |
| `Symlink` | implemented (cached target wins, falls back to readlink) |
| `HardlinkExisting` | implemented (within-shard map, populated post-rename with final path) |
| `ServerSideCopy` | stubbed → ENOSYS (M4) |
| `KernelCopyFileRange` | stubbed → ENOSYS |
| `Skip` | implemented (no-op for non-data entries) |

## Decisions worth knowing

1. **`.partial` lives next to final** (R2). `partial_path` constructs
   `<dirname>/.<basename>.<host>.<pid>.partial`. Cross-directory
   `nfs_rename` is not atomic on NFS.

2. **Mode at create time is `0o600`**, real mode applied via `chmod`
   before rename. Avoids a window where the in-flight `.partial` is
   readable by its eventual owner.

3. **Attribute order on commit (R4)**: `chmod` → `chown` → `utimes` →
   `rename`. utimes is last so a successful copy is always reflected
   in the dest mtime, regardless of side effects from chmod/chown.

4. **No fence checks inside a copy (R3)**. The shard processor checks
   the fence between rows; once a row enters the mover it runs to
   commit (rename) or per-file failure. This is the invariant that
   prevents partial-but-renamed files on dest.

5. **mkdir-on-demand on dest**. The mover calls `mkdir_p_for_file`
   before each create — directories are mode 0755, owner whatever the
   worker is running as. If/when the walker emits dir rows with the
   real mode/owner, route those through a separate dir-apply pass.

6. **Hardlink map populated post-rename** (R5) and only when
   `nlink > 1` — saves memory on the common case.

7. **CAP_CHOWN check at startup** rather than per-file — failing at
   startup with a clear message beats burning a shard producing
   one EPERM per file. `require_chown_capability=false` opts in to
   degraded mode (chown EPERM logged, not failed).

## Verification result

(Fill in after the manual run from `crates/migration-mover/MANUAL_VERIFY.md`.)

- Date:
- Operator:
- Source / dest endpoints:
- Tree shape (file count, bytes):
- Worker wall-clock:
- `manual-verify.sh` exit status:
- Per-file failures observed:
- libnfs quirks:

## What M3 needs from this code

- The mover's per-file path is `do_libnfs_copy` → swap the streaming
  buffer for `FixedBufferPool` lease + io_uring SQE/CQE pairs. The
  `stream_copy` helper is the right swap-point — it owns the loop
  shape and nothing else cares about its internals.
- The pool surface is `LibnfsContextPool::acquire()`. M3 should
  replace `SimplePool` with an N-pair pool and implement the same
  trait. Mover code does not change.
- The shard processor dispatches one row at a time. M3 makes
  `run_batch` issue rows concurrently up to the size-class in-flight
  budget (`InflightProfile`); the per-row mover entry points stay the
  same.

## Post-M2 bug fix: dest path construction + overlap guards

End-to-end verification on real VAST surfaced a data-loss bug. The
test data was synthetic (recreated in seconds from `filecreater.sh`)
and recovery cost was zero, but the failure mechanism applies to any
real source data, so the fix shipped immediately as a five-fix bundle.
Documented here so the loss-of-trust signal is preserved.

### The failure

The first verification manifest was misconfigured: `source.url ==
dest.url` and `source.root="/"` was a path-prefix of
`dest.root="/dst-test"`. Two latent code bugs combined into a
truncate-source data loss:

1. The mover used `row.path` directly for libnfs ops, ignoring
   `dest.root` from the manifest.
2. `.partial` files therefore landed in the source's parent dir.
3. `nfs_create` on `.partial` opened with `O_TRUNC`, which truncated
   the *source* file (because dest dir == source dir).
4. The streaming loop then read 0 bytes and renamed the empty
   `.partial` over the source.

Six source files were zeroed before the operator stopped the run.
Two hardlink rows escaped because `nfs_link` returned EEXIST before
trashing anything.

### The five fixes that landed

1. **`join_root` helper** — `migration_mover::join_root` byte-aware
   concatenates `endpoint.root` and `row.path`. Every libnfs op uses
   it; raw `row.path` access is forbidden in `do_*` strategies.
2. **Per-file self-target check** — `Mover::check_self_target` runs
   before any `nfs_create`. Refuses with `SELF_TARGET` when source
   and dest URLs match and either path or `.partial` parent dir
   collide.
3. **Startup overlap guard** — `migration_core::overlap::check` runs
   before any libnfs mount or shard claim. Refuses to start when
   `source.url == dest.url` and roots overlap. Returns
   `Error::SourceDestOverlap` with a multi-line operator message.
4. **NFSv3 protocol baseline** — `strategy::pick` no longer returns
   `Strategy::ServerSideCopy` (variant retained for forward
   compatibility). `[copy].server_side_copy` defaults to `"off"`.
   Per-attribute `chmod`/`chown`/`utimes` is the only attr-apply
   path. M4 (NFSv4.2 server-side COPY) is deferred indefinitely;
   it's not on the roadmap.
5. **Symlink mode preservation degrades on NFSv3** —
   `do_symlink` writes a `SYMLINK_MODE_NFSV3` downgrade record and
   counts the row as success when source mode != `0o0777`, instead
   of failing. NFSv4-style strict preservation returns when an
   NFSv4 protocol path is added.

### What this means for M4

M4 (NFSv4.2 server-side COPY fast path) is **deferred / repurposed**.
The strategy variant still exists in the enum so it can be revived;
the strategy selector and the `same_server_v42` plumbing are now
dormant. Production environments are overwhelmingly NFSv3, and
several existing design assumptions implicitly required NFSv4
features that don't exist on NFSv3 — fix 4 closes that gap.

### Verification result

- Verification finding: data-loss bug discovered during the first
  end-to-end run; mechanism above.
- Test data: synthetic, regenerated from `filecreater.sh`. Recovery
  cost: zero.
- Date: (fill in after re-run with the post-fix manifest)
- Operator: (fill in)
- Source / dest endpoints: src=`vamoose-source`, dst=`vamoose-dest`
  (new dest export — distinct from source, no root overlap).
- Tree shape (file count, bytes):
- Worker wall-clock:
- `manual-verify.sh` exit status:
- Per-file failures observed:
- Symlink downgrades observed: `SYMLINK_MODE_NFSV3` records for each
  symlink in the verification tree (link-rel.bin, link-abs.bin),
  expected on NFSv3.
- libnfs quirks:

## M2/M3 verification incidents

Three real bugs surfaced during manual verification on real VAST
hardware. Each was undetectable by unit tests; each was caught only
because the verification gate refused to declare a milestone done
without an end-to-end run. The "verify against real hardware" rule
in `docs/CORRECTNESS_RULES.md` is load-bearing — these incidents are the receipts.

### 1. Walker schema mismatch

**Symptom.** Worker rejected freshly-walked parquet shards with
schema validation errors during the M2 verification setup.

**Cause.** `nfs-walker` emitted a pre-canonical schema (older column
naming/typing) while the mover read the canonical schema defined in
`SCHEMA_CONTRACT.md`. The two had drifted because no end-to-end
run had cross-checked them since the contract finalized.

**Fix.** Resolved by `mig-walker-rewrite` (a one-pass shim that
upgrades pre-canonical shards to canonical) plus a pending
`nfs-walker` PR to emit canonical directly. No data was at risk —
the shards were rejected at load time, before any libnfs op.

### 2. Dest path overlap → source truncation

**Symptom.** First end-to-end M2 run zeroed six source files and
escaped two hardlink rows on EEXIST before the operator stopped
the worker.

**Cause.** Two latent bugs combined: (a) the mover used `row.path`
directly for libnfs ops, ignoring `dest.root` from the manifest,
and (b) the manifest had `source.url == dest.url` with `source.root="/"`
a path-prefix of `dest.root="/dst-test"`. `.partial` files therefore
landed in the source's parent dir; `nfs_create` opened with
`O_TRUNC` and zeroed the source file before the streaming loop ran.

**Fix.** `BUGFIX_PLAN.md` shipped a five-fix bundle (see "Post-M2
bug fix" above): `join_root` helper, per-file self-target check,
startup overlap guard (`migration_core::overlap::check`), NFSv3
protocol baseline, and symlink mode degradation.

**Recovery.** Test data was synthetic, regenerated from
`filecreater.sh` in seconds. Recovery cost: zero. The bug class
applies to any real source data, so the fix shipped immediately.

### 3. libnfs FFI signature mismatch → silent zero-byte writes

**Symptom.** Re-run M2 verification after the path-overlap fix.
Worker reported every file copied successfully ("CONTENT MATCHES"
log line). `manual-verify.sh` then failed: every regular file on
the dest was zero bytes. Both source preservation and atomic
rename worked correctly — the operation just moved zero bytes
successfully.

**Cause.** The FFI declarations for `nfs_pread` / `nfs_pwrite` in
`crates/migration-mover/src/libnfs/mod.rs` had parameter order that
matched the libnfs 5.x header at `/usr/local/include` but not the
libnfs 4.x `.so` actually linked at runtime. The wrong-shaped call
returned 0 bytes for every read; the streaming loop's `if n == 0
{ break }` clause then exited cleanly and reported success.

**Fix.** Three layers, landing across the baseline commit and PR2
("PR 2: libnfs FFI hardening"):

1. **The FFI fix itself** (in baseline). Parameter order corrected
   to match the linked binary.
2. **Operator-visible early-EOF surface** (PR 2, change 2):
   `stream_copy` now logs a structured warning on premature EOF,
   and `do_libnfs_copy` writes an `EARLY_EOF` downgrade record
   when `written < row.size`. Had this been in place during the
   incident, all 8 non-empty regular files would have produced
   `EARLY_EOF` records and the bug would have been instantly
   visible — without any test, against any real hardware.
3. **Regression smoke test** (PR 2, change 1):
   `crates/migration-mover/tests/libnfs_ffi_smoke.rs` is an
   `#[ignore]`-tagged integration test that mounts a real NFS
   export and exercises `pread` against a known-good file. It
   would have caught the original bug in 30 seconds.

**Recovery.** Same as incident 2 — synthetic test data, zero
recovery cost.

**Class of bug.** The FFI's parameter mismatch was undetectable
by:
- Unit tests (Rust mocks don't go through the FFI).
- Log inspection (the buggy code and a successful copy log
  identically).
- `cargo build` (FFI signatures are not type-checked against the
  linked `.so`).

It required either (a) running against real libnfs and a real
file, or (b) wiring a tracing surface that exposes the
contract-relevant invariant (bytes written vs indexed size). PR 2
adds both.

### Why these three matter together

Every incident here was found by manual verification on real VAST,
not by the test suite. The test suite is doing what test suites
do — guarding against regressions in pure-Rust logic. Real bugs in
the system live at the boundaries: schema interop with another
codebase, manifest configuration interacting with mover internals,
FFI declarations against a binary the build system never sees.

The verification gate exists precisely to catch this class of bug.
Building M3 on top of an unverified M2 would have compounded the
attribution problem — was the silent-write bug an M3 concurrency
bug or an M2 FFI bug? Sequential verification kept the answer
local.

## Milestone status

- **M2 — VERIFIED.** Three incidents above resolved; manual
  verification recipe in `crates/migration-mover/MANUAL_VERIFY.md`
  passed end-to-end against real VAST on the baseline commit
  (see `git log` for the baseline tag). Per
  `docs/CORRECTNESS_RULES.md` verification-gate rules, this is the
  formal completion record.
- **M3 — VERIFIED.** Concurrent dispatch, multi-context pool, and
  blocking-pool offload exercised by the same M2 verification tree
  with `nfs_connections > 1`. See `M3_NOTES.md` for what M3 ships
  vs. defers.
