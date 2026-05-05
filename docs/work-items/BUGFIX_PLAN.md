# Bug fix: dest path construction + source/dest overlap guards + NFSv3 baseline

Critical correctness fix. During M2/M3 manual verification, a
misconfigured manifest combined with two latent code bugs caused
the worker to truncate source files. This work item closes that
class of bug **and** locks in NFSv3 as the protocol baseline.

---

## Background

During the first end-to-end verification run against real VAST,
the worker executed against a manifest whose `dest.url` matched
`source.url` and whose `dest.root="/dst-test"` was a sub-prefix of
`source.root="/"`. The mover then:

1. Used `row.path` directly as the destination path for libnfs
   operations, ignoring `dest.root` from the manifest.
2. Created `.partial` files in the source's parent directory.
3. The `nfs_create` for `.partial` truncated the *source* file
   (because dest dir == source dir).
4. For regular files: the create-as-partial succeeded, the read
   loop pulled bytes from the (now-truncated) source, wrote 0
   bytes, then renamed the empty `.partial` over the source.

Result: 6 source files were zeroed. The 2 hardlinks that escaped
were saved by `nfs_link` returning EEXIST before doing damage.

The data was synthetic (recreated in seconds from `filecreater.sh`),
but the failure mechanism applies to any real source data. **This
class of bug must be impossible to trigger by misconfiguration.**

Separately, this work item also locks in NFSv3 as the assumed
protocol baseline. The verification environment is NFSv3, the
deployment environment is overwhelmingly NFSv3, and several
existing design assumptions implicitly required NFSv4 features
that don't exist on NFSv3.

---

## Five fixes, one PR

### Fix 1 (correctness): join `endpoint.root` + `row.path`

The mover constructs source and destination paths from the manifest's
`source` and `dest` blocks. Currently `row.path` is used directly.
Per `SCHEMA_CONTRACT.md`, paths in the parquet are relative to the
export root with leading slash, and `endpoint.root` documents the
prefix to prepend.

**Required change:** every libnfs operation against the source uses
`source.root + row.path` (path-join, leading-slash-aware). Every
libnfs operation against the destination uses `dest.root + row.path`.
The libnfs mount target remains `endpoint.url` only — never
concatenated with `row.path`.

Path-join must be byte-aware (paths are `Vec<u8>` per the contract).
Both inputs may or may not have leading slashes; the joined path
must have exactly one leading slash and no double slashes.

```rust
// In migration-mover or migration-core, depending on where path
// construction lives today:
fn join_root(root: &[u8], path: &[u8]) -> Vec<u8> {
    let root = root.strip_suffix(b"/").unwrap_or(root);
    let path = if path.starts_with(b"/") { path } else {
        return [b"/", path].concat();
    };
    if root.is_empty() || root == b"/" {
        path.to_vec()
    } else {
        let mut out = Vec::with_capacity(root.len() + path.len());
        if !root.starts_with(b"/") { out.push(b'/'); }
        out.extend_from_slice(root);
        out.extend_from_slice(path);
        out
    }
}
```

Unit tests:
- `join_root("/", "/foo/bar")` → `"/foo/bar"`
- `join_root("", "/foo/bar")` → `"/foo/bar"`
- `join_root("/dst", "/foo/bar")` → `"/dst/foo/bar"`
- `join_root("/dst/", "/foo/bar")` → `"/dst/foo/bar"`
- `join_root("/dst", "/")` → `"/dst/"`
- Non-UTF-8 bytes preserved through the join.
- `join_root("dst", "/foo")` → `"/dst/foo"` (defensive leading slash on root).

### Fix 2 (defense in depth): per-file self-target check

Before opening any destination file for write, the mover must verify
that the destination is provably distinct from the source for that
specific file.

The check is conservative — refuse to write if any of the following
are true at the per-file level:

- The computed dest path equals the computed source path AND
  source endpoint URL == dest endpoint URL.
- The computed `.partial` path's parent directory equals the source
  file's parent directory AND source endpoint URL == dest endpoint
  URL.

When the check fires, fail the row with `FailurePhase::Open` and
error tag `SELF_TARGET`. Do not call `nfs_create`. Do not log a
downgrade — this is a real failure that must be surfaced.

This is belt-and-suspenders against fix 3 (the startup guard).

### Fix 3 (defense in depth): startup guard for overlapping source and dest

Before mounting libnfs or claiming any shard, the orchestrator
checks the manifest for source/dest overlap and refuses to start
if any of these are true:

- `source.url == dest.url` AND `source.root == dest.root`.
- `source.url == dest.url` AND `source.root` is a path-prefix of
  `dest.root` (or vice versa).

"Path-prefix" means: split both roots by `/`, compare component-wise
up to the shorter length. `/` is a path-prefix of every path.
`/foo` is a path-prefix of `/foo/bar` but not of `/foobar`.

Implementation note: do the byte-level comparison after normalizing
trailing slashes off both roots.

When the check fires, exit with a clear message identifying both
endpoints, the offending overlap, and the fix:

```text
Error: source and destination overlap; refusing to start

  source.url:  nfs://main.selab-var204.selab.vastdata.com/bgolliher/vamoose-source
  source.root: /
  dest.url:    nfs://main.selab-var204.selab.vastdata.com/bgolliher/vamoose-source
  dest.root:   /dst-test

  Problem: source.url == dest.url, and source.root="/" is a
  path-prefix of dest.root="/dst-test". Any file written to dest
  could overlap with source.

  Fix: use a separate dest export, or move the source root so
  neither root is a prefix of the other.
```

Use `Error::SourceDestOverlap` (new variant). Prefer the typed
variant for testability.

### Fix 4 (NFSv3 baseline): lock in NFSv3 as the supported protocol

The migration system targets NFSv3 as the baseline protocol. NFSv4
features are optional optimizations and must never be required for
correct operation.

Specific changes:

**Strategy::ServerSideCopy is removed from selection.** The variant
stays in the `Strategy` enum (we may want it back if NFSv4.2 becomes
common in target environments), but `strategy::pick` never returns
it. Add a comment in `strategy.rs`:

```rust
// Strategy::ServerSideCopy uses NFSv4.2 COPY op. Not selected because
// the system targets NFSv3 as the protocol baseline. Keep the variant
// for future NFSv4.2 support; do not remove from the enum.
```

The `same_server_v42` field of `StrategyContext` becomes effectively
unused. Leave it in the struct (don't break the public API) but
document its current state.

**Per-attribute application sequence is the only path.** NFSv4
batched SETATTR was an optimization noted in the M2 plan; it
becomes dead code under NFSv3. The mover already does the
sequence (`chmod` → `chown` → `utimes`); confirm that the NFSv4
batched path either doesn't exist or is unreachable, and add a
comment in `attrs.rs` noting the rationale.

**The `[mover].server_side_copy` config field stays** but its
default becomes `"off"` (was `"auto"`). With strategy selection
no longer picking it, the field is informational. Leave it for
forward compatibility.

### Fix 5 (NFSv3 baseline): symlink mode preservation degrades on NFSv3

NFSv3's `chmod` follows symlinks; there is no way to chmod a
symlink itself in NFSv3. The current contract decision (#12 in
SCHEMA_CONTRACT.md) said "always preserve, fail file on error,"
which against NFSv3 would fail every symlink with mode != 0o0777.

**New behavior on NFSv3:** when a symlink's source mode differs
from the conventional default (`0o0777`), the mover writes a
downgrade record (consistent with the null-attribute pattern from
contract decision #2) and counts the file as success. The symlink
is created at the destination with whatever default mode the
server assigns.

Specific implementation:

```rust
async fn do_symlink(&self, row: &RowView) -> Result<(), MoveError> {
    // ... existing readlink + nfs_symlink path ...

    if self.cfg.policy.preserve_mode && row.mode & 0o7777 != 0o0777 {
        // NFSv3 has no lchmod-equivalent. Log a downgrade and continue.
        self.downgrade_sink.record(DowngradeRecord {
            row_id: row.row_id,
            shard: self.shard_name.clone(),
            path_b64: base64::encode(&row.path),
            downgrade: "SYMLINK_MODE_NFSV3".into(),
            ts: UtcTime::now(),
        }).await;
    }
    Ok(())
}
```

The `SYMLINK_MODE_NFSV3` downgrade tag is distinct from
`NULL_MTIME` and similar — it identifies "symlink mode bits not
preserved due to NFSv3 protocol limitation, not source attribute
absence."

Future NFSv4 path: if NFSv4 strategy support is added later, the
downgrade-on-symlink-mode goes away because NFSv4 SETATTR-on-fh
can target the symlink directly. The fix is then conditional on
detected protocol version.

### `SCHEMA_CONTRACT.md` update

Update section "Symlink mode preservation" in
`SCHEMA_CONTRACT.md` to reflect the NFSv3 reality:

```markdown
## Symlink mode preservation

When `preserve_mode = true`, the mover preserves symlink mode bits
where the destination protocol allows it.

**On NFSv4 destinations:** the mover issues SETATTR on the symlink
file handle. Errors fail the row with `FailurePhase::Setattr`.

**On NFSv3 destinations:** the mover cannot preserve symlink mode
because NFSv3 has no lchmod-equivalent (`nfs_chmod` follows
symlinks). When source mode differs from the conventional default
(`0o0777`), the mover writes a `SYMLINK_MODE_NFSV3` downgrade
record and counts the file as success. The symlink at the
destination has the default mode assigned by the server.

Operators on NFSv3 destinations who require symlink mode fidelity
must use a different protocol or accept the downgrade.
```

Also update the change log to note `contract_version = 1` already
reflects this (no version bump required — the new degradation
behavior is a more permissive version of what the contract allowed).

---

## Tests

### Unit tests

`migration-core` or `migration-mover` (wherever `join_root` lands):
- `join_root` cases above (~7 tests).

`migration-mover::mover` self-target check:
- `self_target_check_blocks_same_path`
- `self_target_check_blocks_same_parent_dir`
- `self_target_check_allows_different_url`
- `self_target_check_allows_disjoint_dirs`

`migration-worker::orchestrator` startup guard:
- `overlap_guard_rejects_identical_endpoints`
- `overlap_guard_rejects_root_prefix_in_either_direction`
- `overlap_guard_allows_different_urls`
- `overlap_guard_allows_disjoint_roots`
- `overlap_guard_handles_trailing_slashes`

`migration-mover::strategy`:
- `strategy_pick_never_returns_server_side_copy`: synthesize various
  inputs including a hypothetical `same_server_v42=true` case;
  assert `Strategy::ServerSideCopy` never appears in output.

`migration-mover` symlink path:
- `symlink_mode_default_no_downgrade`: source mode `0o0777` →
  no downgrade record.
- `symlink_mode_nondefault_emits_downgrade`: source mode `0o0644`
  → downgrade record with tag `SYMLINK_MODE_NFSV3`.
- `symlink_mode_disabled_no_downgrade`: `preserve_mode=false` →
  no downgrade regardless of source mode.

### Integration test

A dry-run integration test that:
1. Builds a synthetic manifest in memory with overlapping source/dest.
2. Calls the orchestrator's startup path far enough to hit the guard.
3. Asserts `Error::SourceDestOverlap`.
4. Asserts no S3 calls and no libnfs calls were attempted.

This is the regression test for the data-loss bug. **It must exist.**

### Manual verification (after the fix)

1. The original test manifest (overlapping source/dest) is rejected
   before any libnfs op runs. Operator validates the error message
   matches the spec. Documented in `M2_NOTES.md`.
2. A new manifest pointing src=`vamoose-source`, dst=`vamoose-dest`
   (the new dest export) runs cleanly. Operator validates with
   `manual-verify.sh`.
3. The dest tree contains symlinks (`link-rel.bin`, `link-abs.bin`)
   with mode `0o0777` (NFSv3 default), and `downgrades/host-*.jsonl`
   contains `SYMLINK_MODE_NFSV3` records for each.

---

## Worker config and example updates

### `examples/worker.toml`

```toml
[mover]
strategy_default = "libnfs_io_uring"
src_url          = "nfs://src-server/source-export"
dst_url          = "nfs://dst-server/dest-export"
# Source and destination MUST be distinct exports OR distinct roots
# on the same export. The worker refuses to start if they overlap.

[copy]
# ... existing fields ...
server_side_copy = "off"   # NFSv3 baseline; NFSv4.2 COPY not used
```

### `migration-worker/src/config.rs`

No new fields required. The startup guard reads existing
`manifest.source.url`, `manifest.source.root`, `manifest.dest.url`,
`manifest.dest.root`.

Update `default_ssc()` from `"auto"` to `"off"`.

### `examples/build_manifest.py`

If checked in, update its template manifest so source and dest
point at obviously distinct exports. Default `server_side_copy`
to `"off"`.

---

## Done criteria

- `cargo build --workspace` clean.
- All existing tests still pass (62 prior + new tests above).
- New unit tests for `join_root`, self-target check, overlap guard,
  strategy selection (no v4.2), and symlink-mode downgrade all pass.
- Integration test for the startup guard passes.
- Running the worker against a manifest with `source.url == dest.url`
  and overlapping roots produces the spec'd error and exits before
  any libnfs or S3 data-plane operation.
- Running the worker against a non-overlapping manifest with
  symlinks succeeds, dest has the symlinks at default mode, and
  `downgrades/` contains `SYMLINK_MODE_NFSV3` records.
- `examples/worker.toml` and `SCHEMA_CONTRACT.md` updated.
- `M2_NOTES.md` updated to capture:
  - The data-loss bug discovered during verification (mechanism + impact).
  - The five fixes that landed.
  - The NFSv3-baseline decision and what it means for M4 (M4 is
    deferred / repurposed; NFSv4.2 server-side COPY is not on the
    roadmap).
  - The verification finding that drove this work item.
  - That the test data was synthetic and the recovery cost was zero.

---

## Out of scope (do not implement)

- `run_prefix` for multi-run-per-bucket layout. Real gap, separate
  follow-up.
- Any NFSv4.2-specific code paths. NFSv3 is the baseline; any
  protocol-version detection or v4-specific optimizations are a
  separate future work item.
- Resume-after-restart of partially-completed shards.
- Cleanup of `.partial` files left by previous failed runs (worker
  should not delete files in dest at startup; that's
  `mig-aggr clean-partials`).

---

## Correctness-rules additions

Add these correctness rules to `docs/CORRECTNESS_RULES.md` so future
work doesn't unwittingly create regressions:

- **`endpoint.root` is part of the path.** Mover operations on
  source use `source.root + row.path`. Mover operations on dest
  use `dest.root + row.path`. The libnfs mount target is just
  `endpoint.url`, never `endpoint.url + row.path`.

- **Source and dest paths must be provably distinct before any
  write.** A startup check verifies endpoints don't overlap; a
  per-file check verifies the specific paths don't collide. Both
  checks must exist; either alone is insufficient.

- **`.partial` is in the same directory as the final destination.**
  This is a correctness requirement for atomic rename, but combined
  with overlapping source/dest it becomes a data-loss vector. The
  per-file self-target check protects against this.

- **NFSv3 is the protocol baseline.** Code paths that require
  NFSv4 features (server-side COPY, batched SETATTR,
  SETATTR-on-symlink) must not be on the default path. NFSv4
  optimizations may exist but must be optional.
