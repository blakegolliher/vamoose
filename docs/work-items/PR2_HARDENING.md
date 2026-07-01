# PR 2: libnfs FFI hardening + verification narrative

Follow-up to PR 1 (the urgent FFI fix, already applied in the
baseline commit). This PR adds the things that would have caught
the bug, surfaces it in operator output, and codifies the lessons
into the doc surface.

**Builds on:** the FFI fix for `nfs_pread` / `nfs_pwrite` is
already in the baseline. This PR adds nothing to the FFI itself.

---

## Background

The baseline commit fixed a silent data-loss bug caused by a
libnfs FFI parameter order mismatch. The bug was undetectable by
unit tests because pure-Rust mocks don't go through the FFI. It
was undetectable by log inspection because both the buggy code
and a successful copy report the same thing in the worker log.

This PR adds:

1. A regression test that runs against real libnfs and would have
   caught this immediately.
2. A `stream_copy` change that would have surfaced the bug visibly
   in operator output even before tests ran.
3. A correctness rule in `docs/CORRECTNESS_RULES.md` to prevent the
   same class of bug from recurring when wrapping any C library FFI.
4. A `M2_NOTES.md` write-up of the verification incident, joining
   the previous two bugs surfaced by manual verification.

---

## Five changes, one PR

### Change 1: libnfs FFI smoke test (the regression test)

Add a new integration test crate `crates/migration-mover/tests/libnfs_ffi_smoke.rs`
that links libnfs at runtime and exercises the full read/write
round-trip:

```rust
//! Smoke test for libnfs FFI signatures. This test runs against
//! a real libnfs instance and a real NFS export; it cannot be
//! mocked. Marked `#[ignore]` so it doesn't run in CI without an
//! environment, but MUST be run before any change to libnfs FFI
//! declarations or wrapper code.
//!
//! Run manually with:
//!   cargo test -p migration-mover --test libnfs_ffi_smoke -- \
//!       --ignored --nocapture
//!
//! Required environment:
//!   VAMOOSE_TEST_NFS_URL=nfs://server/export
//!   VAMOOSE_TEST_NFS_PATH=/path/to/known-good-file
//!   VAMOOSE_TEST_NFS_EXPECTED_SIZE=<bytes>

use migration_mover::libnfs::ops;
use migration_mover::libnfs::pool::{LibnfsContextPool, SimplePool};
use std::env;

#[tokio::test]
#[ignore]
async fn nfs_pread_returns_actual_bytes() {
    let url = env::var("VAMOOSE_TEST_NFS_URL")
        .expect("VAMOOSE_TEST_NFS_URL not set");
    let path = env::var("VAMOOSE_TEST_NFS_PATH")
        .expect("VAMOOSE_TEST_NFS_PATH not set");
    let expected_size: u64 = env::var("VAMOOSE_TEST_NFS_EXPECTED_SIZE")
        .expect("VAMOOSE_TEST_NFS_EXPECTED_SIZE not set")
        .parse()
        .expect("VAMOOSE_TEST_NFS_EXPECTED_SIZE not a number");

    assert!(expected_size > 0, "test requires a non-empty file");

    // Mount source and dest both pointing at the same export — we
    // only exercise reads, so dest is unused.
    let pool = SimplePool::new(&url, &url, 1).await
        .expect("pool creation");
    let mut pair = pool.acquire().await.expect("acquire pair");

    let path_bytes = path.as_bytes();
    let fh = ops::open_read(pair.src(), path_bytes)
        .expect("nfs_open_read failed against known-good file");

    // Read the first 1 KiB. For any non-empty file this must
    // return more than zero bytes.
    let mut buf = vec![0u8; 1024];
    let n = ops::pread(pair.src(), &fh, 0, &mut buf)
        .expect("nfs_pread returned an error");

    ops::close_quietly(pair.src(), fh);

    assert!(
        n > 0,
        "pread returned 0 bytes from a {} byte file; FFI signature \
         likely mismatches the linked library",
        expected_size,
    );
    assert!(
        n as u64 <= expected_size,
        "pread returned more bytes ({}) than the file contains ({})",
        n, expected_size,
    );
}
```

The test asserts the only thing that's actually contract-relevant:
`pread` returns non-zero on a non-empty file. It does NOT check
content correctness — that's covered by full M2/M3 manual
verification — but it would have caught today's bug in 30 seconds.

Document the test in the crate-level docs and in M2_NOTES.md so
future engineers know it exists and how to run it.

### Change 2: tighten `stream_copy` to surface premature EOF

Current behavior in `migration-mover/src/mover.rs`:

```rust
while remaining > 0 {
    let want = remaining.min(STREAM_BUF_SIZE as u64) as usize;
    let n = ops::pread(pair.src(), src_fh, off, &mut buf[..want])?;
    if n == 0 {
        break;  // silent success
    }
    // ... pwrite loop ...
    off += n as u64;
    remaining -= n as u64;
}
Ok(off)
```

The `if n == 0 { break }` path returns `Ok(off)` even when
`remaining > 0`. This is the silent-success vector that hid the FFI
bug. Per contract decision #11 ("size is advisory by default"),
mismatch should NOT fail the row, but it MUST be surfaced.

**New behavior:** when `n == 0` and `remaining > 0`, log a warning,
emit a downgrade record `EARLY_EOF`, then return `Ok(off)`. The
file is committed (matching the contract's "size is advisory"
intent), but the operator can see that the actual size differs
from the indexed size:

```rust
while remaining > 0 {
    let want = remaining.min(STREAM_BUF_SIZE as u64) as usize;
    let n = ops::pread(pair.src(), src_fh, off, &mut buf[..want])?;
    if n == 0 {
        // EOF before remaining == 0. Per contract, size is
        // advisory; do not fail. But surface the discrepancy so
        // the operator can investigate.
        tracing::warn!(
            row_id,
            indexed_size = size,
            actual_size = off,
            short = remaining,
            "pread returned 0 with remaining bytes; treating as EOF \
             (contract: size is advisory)",
        );
        // Caller (do_libnfs_copy) writes the EARLY_EOF downgrade
        // record after stream_copy returns.
        break;
    }
    // ... pwrite loop ...
    off += n as u64;
    remaining -= n as u64;
}
Ok(off)
```

In `do_libnfs_copy`, after `stream_copy` returns, compare `written`
to `row.size` and write a downgrade record if they differ:

```rust
let written = result?;
// ... close fhs ...
if written < row.size {
    self.downgrade_sink.record(DowngradeRecord {
        row_id: row.row_id,
        shard: self.shard_name.clone(),
        path_b64: base64::encode(&row.path),
        downgrade: "EARLY_EOF".into(),
        ts: UtcTime::now(),
    }).await;
}
```

Add `DowngradeKind::EarlyEof` to `migration-core::records`.

If we'd had this in place during today's verification, all 8
non-empty regular files would have produced `EARLY_EOF` downgrade
records and the bug would have been instantly visible. That's the
test that the test environment couldn't have written, but the code
itself can document.

### Change 3: docs/CORRECTNESS_RULES.md addition

Add to the correctness rules section:

```markdown
- **Cross-check C library FFI against the linked binary, not just
  the system header.** When wrapping a C library, parameter order
  and types must match the actually-linked `.so`, not whatever
  header is installed at `/usr/include`. Multiple library versions
  may coexist on the same host. Verify with `ldd <binary>` to find
  the linked .so, then `objdump -T <so> | grep <symbol>` to
  identify it. The header that ships with that .so is the
  authoritative declaration. Specifically, on this host, libnfs
  4.x at `/usr/include` and libnfs 5.x at `/usr/local/include`
  declare `nfs_pread` with reversed parameter order; the FFI was
  written against the wrong header and produced silent data loss
  in M2 verification (see M2_NOTES.md).

  Future libnfs FFI changes must be tested against
  `tests/libnfs_ffi_smoke.rs` before merging.
```

### Change 4: M2_NOTES.md write-up

Add a section "M2/M3 verification incidents" documenting the three
real bugs that manual verification surfaced:

1. **Walker schema mismatch (resolved by `mig-walker-rewrite`).**
   Walker emits a pre-canonical schema; mover reads canonical.
   Solved by a shim, walker PR pending.

2. **Dest path overlap data-loss (resolved by `BUGFIX_PLAN.md`).**
   The mover ignored `dest.root` and used `row.path` directly. With
   `source.url == dest.url` and overlapping roots, the worker
   wrote `.partial` files into the source tree, truncating source
   files. Recovery: synthetic data, recreated. Fix: three-layer
   defense (path joining, per-file self-target check, startup
   overlap guard).

3. **libnfs FFI signature mismatch (resolved in baseline; this PR
   adds the regression test and operator-visible early-EOF
   surface).** Silent zero-byte writes on every file. Both source
   preservation and atomic rename worked correctly — the
   operation just moved zero bytes successfully. Discovery
   required real-hardware verification.

Frame it explicitly: each of these was undetectable by unit tests.
Each was caught only because the verification gate refused to
declare a milestone done without an end-to-end run on real
hardware. The "verify against real hardware" rule in
`docs/CORRECTNESS_RULES.md` is load-bearing.

### Change 5: a note in `mover.rs` about the FFI requirement

Single-line comment near the top of `libnfs/mod.rs`:

```rust
//! libnfs FFI bindings.
//!
//! IMPORTANT: parameter order in this file MUST match the linked
//! libnfs binary at runtime, NOT the system header at
//! `/usr/include/nfsc/libnfs.h`. Verify with `objdump -T` of the
//! linked .so. See `docs/CORRECTNESS_RULES.md` "Cross-check C
//! library FFI" and `M2_NOTES.md` "FFI signature mismatch" for the
//! failure mode.
//!
//! Smoke test:
//!   cargo test -p migration-mover --test libnfs_ffi_smoke -- \
//!       --ignored --nocapture
```

---

## Tests

- The new integration test `libnfs_ffi_smoke.rs` (marked `#[ignore]`).
- Unit test for the new `EARLY_EOF` downgrade record serialization
  (in `migration-core::records`, mirrors existing
  `SymlinkModeNfsV3` test).
- Unit test for `stream_copy` short-read behavior using a mock
  `pread` that returns 0 prematurely. Asserts:
  - The function returns `Ok(off)` not an error.
  - `off < size` (the actual short count).
  - This is a unit test against the mock, not the real FFI.

---

## Done criteria

- `cargo build --workspace` clean.
- `cargo test --workspace` passes (existing 95 + new unit tests).
- The integration test runs successfully when invoked with the
  documented environment variables against the verification VAST.
- A re-run of the full M2/M3 manual verification still produces
  "CONTENT MATCHES" with no `EARLY_EOF` records (because the FFI
  is correct).
- Doc surface updated:
  - `docs/CORRECTNESS_RULES.md` correctness rules section
  - `M2_NOTES.md` verification incidents section
  - Header comment in `libnfs/mod.rs`
- M2 and M3 are formally marked complete in M2_NOTES.md with
  verification evidence cited.

---

## Out of scope

- Auto-generated bindings via `bindgen`. Tempting, but doesn't
  solve the multi-libnfs-version problem on its own — bindgen
  reads the system header by default. The fix would be to
  configure `bindgen` to use `/usr/local/include/nfsc/`, which is
  worth doing eventually but isn't this PR.

- Vendoring libnfs into the workspace and statically linking. Real
  long-term answer for FFI determinism, but a substantial
  build-system change separate from this incident response.

- Detecting EARLY_EOF as a hard failure when
  `[copy].require_unchanged_size = true`. The flag exists per
  contract decision #11; wiring it to also catch EARLY_EOF (in
  addition to the existing `SIZE_CHANGED` check) is a small
  follow-up but not part of this PR.

---

## Validation steps after merge

1. `cargo test --workspace` (existing + new unit tests pass).
2. Set `VAMOOSE_TEST_NFS_URL`, `_PATH`, `_EXPECTED_SIZE` env vars.
3. Run `cargo test --test libnfs_ffi_smoke -- --ignored
   --nocapture`. Confirm pread returns expected bytes.
4. Re-run M2/M3 manual verification. Confirm "CONTENT MATCHES"
   with no `EARLY_EOF` records.
5. Mark M2 and M3 complete in M2_NOTES.md.
