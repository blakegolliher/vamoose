# PR 1: libnfs FFI signature fix for nfs_pread / nfs_pwrite

**Severity:** critical correctness. Silent zero-byte writes on every
non-empty file copy. Worker reports `files_ok` and `bytes_done` as
if the copy succeeded.

**Scope:** narrow. FFI declaration changes and call-site updates only.
Hardening (regression test, stream_copy tightening, correctness-rules
addition, M2_NOTES.md) is in a follow-up PR.

---

## Background

During M2/M3 manual verification, the worker ran cleanly end-to-end
(no errors logged, all metadata preserved, hardlinks linked,
symlinks created) but every regular file at the destination ended
up zero bytes. Source data intact.

Root cause: the FFI declarations for `nfs_pread` and `nfs_pwrite`
in `migration-mover/src/libnfs/mod.rs` use parameter order
`(nfs, fh, offset, count, buf)`. The actually-linked library at
`/usr/local/lib/libnfs.so.16` (built from `~/projects/libnfs/`)
exports these functions with parameter order
`(nfs, fh, buf, count, offset)`.

The system header `/usr/include/nfsc/libnfs.h` declares
`(offset, count, buf)`. The locally-built header at
`/usr/local/include/nfsc/libnfs.h` declares `(buf, count, offset)`.
The FFI was written against the wrong header.

When the misordered arguments were passed:
- `count` happened to land in the right slot by accident.
- `buf` (a pointer) was interpreted as `offset` (a 47-bit number,
  past EOF on every file).
- `offset` (small u64) was interpreted as `buf` (a near-null pointer).

libnfs received "read `count` bytes at offset `<huge-pointer-value>`"
which is past EOF, returned 0 cleanly, no error. The Rust wrapper
saw `rc=0`, treated as EOF, broke out of the read loop. The atomic
rename then committed an empty `.partial` file as the final
destination.

The bug was invisible to unit tests because pure-Rust mocks of
`pread`/`pwrite` never go through the FFI.

---

## Required changes

### `crates/migration-mover/src/libnfs/mod.rs`

**Before:**

```rust
pub fn nfs_pread(
    nfs: *mut nfs_context,
    fh: *mut nfsfh,
    offset: u64,
    count: usize,
    buf: *mut c_void,
) -> c_int;
pub fn nfs_pwrite(
    nfs: *mut nfs_context,
    fh: *mut nfsfh,
    offset: u64,
    count: usize,
    buf: *const c_void,
) -> c_int;
```

**After:**

```rust
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
```

This matches the `(buf, count, offset)` order in the linked library
at `/usr/local/lib/libnfs.so.16`, confirmed via:

```bash
grep -A 1 'EXTERN int nfs_pread\b' /usr/local/include/nfsc/libnfs.h
# EXTERN int nfs_pread(struct nfs_context *nfs, struct nfsfh *nfsfh,
#                      void *buf, size_t count, uint64_t offset);
```

### `crates/migration-mover/src/libnfs/ops.rs`

Update the call sites in `pread()` and `pwrite()` to match the
new argument order:

**Before:**

```rust
super::nfs_pread(
    ctx.raw(),
    fh.raw(),
    offset,
    buf.len(),
    buf.as_mut_ptr() as *mut _,
)
```

**After:**

```rust
super::nfs_pread(
    ctx.raw(),
    fh.raw(),
    buf.as_mut_ptr() as *mut _,
    buf.len(),
    offset,
)
```

Same shape for `pwrite`.

---

## Verification

This fix has been verified end-to-end against the real VAST cluster:

```text
2026-05-04T04:28:43.030988Z manifest loaded shards=2 total_rows=18
2026-05-04T04:28:43.229195Z libnfs pool mounted
2026-05-04T04:28:43.817879Z all shards terminal; worker exiting

CONTENT MATCHES — M2/M3 VERIFIED
```

All 17 source files (10 regular files including a 100 MiB binary,
5 directories, 2 symlinks, 1 hardlink trio sharing an inode) were
correctly copied to a separate dest export. SHA-256 over all
regular files at source and destination match exactly. Source tree
unchanged.

Run time: 547ms for 18 rows + 100 MiB of data. The same workload
under the buggy code reported "files_ok=18, bytes_done=105MB" but
all dest files were zero-length.

---

## Done criteria

- `cargo build --workspace` clean.
- `cargo test --workspace` — all 95 tests pass (no regression
  expected; the bug class isn't covered by unit tests).
- A manual verification run against real libnfs reproduces the
  "CONTENT MATCHES" outcome with the same source tree.
- Commit message references this PR's analysis and the follow-up
  PR that adds the regression test.

## Out of scope (in PR 2)

- libnfs FFI smoke test that runs against real libnfs.
- `stream_copy` tightening to surface premature EOF.
- `docs/CORRECTNESS_RULES.md` rule about cross-checking FFI against `objdump -T`.
- `M2_NOTES.md` write-up of the verification incident.

These are not in this PR because they involve new code paths and
test infrastructure that could themselves introduce bugs. The FFI
fix here is a minimal, well-validated change. PR 2 is the
"lessons learned, codified" follow-up.
