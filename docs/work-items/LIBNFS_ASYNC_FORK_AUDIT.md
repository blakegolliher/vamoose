# libnfs async FFI symbol audit

Companion to `docs/work-items/LIBNFS_ASYNC_FORK.md`. Resolves the
"two interpretations" choice in that spec by inspecting the linked
libnfs `.so`, the header that ships with it, and the pinned source
tree at `~/projects/libnfs/`.

## Methodology

1. Build `migration-mover` and inspect the test binary's `ldd` output
   to identify which `libnfs.so` the existing FFI actually links.
2. For every symbol in the spec's symbol table, check:
   - presence in the linked `.so` via `objdump -T`
   - declaration in the header at the same prefix
   - declaration in the pinned source's header (drift check)
   - parameter order against both header copies
3. For URL mount options (`rsize=`, `wsize=`, `nconnect=`), grep the
   pinned source's URL parser (`lib/libnfs.c:nfs_parse_url`) for the
   recognized argument names.
4. For mount-time tunables not in the URL, grep for the corresponding
   `nfs_set_*` symbol.

## Environment

| Path | Identity |
|---|---|
| Linked `.so` | `/usr/local/lib/libnfs.so.16.0.2` (SONAME `libnfs.so.16`) |
| pkg-config version | `16.2.0` (upstream libnfs ≈ 6.0.2) |
| Header that ships with linked `.so` | `/usr/local/include/nfsc/libnfs.h` |
| Alternate `.so` on host | `/usr/lib/x86_64-linux-gnu/libnfs.so.14.0.0` (upstream ≈ 5.0.x; not linked) |
| Pinned source tree | `~/projects/libnfs/` at `libnfs-6.0.2-148-gdc7e6f8` |
| Authoritative header for FFI declarations | `/usr/local/include/nfsc/libnfs.h` (header that ships with the linked `.so`, per `docs/CORRECTNESS_RULES.md`) |

Identification commands:

```
$ ldd target/debug/deps/libnfs_ffi_smoke-* | grep -i nfs
        libnfs.so.16 => /usr/local/lib/libnfs.so.16 (0x...)
$ readlink -f /usr/local/lib/libnfs.so.16
/usr/local/lib/libnfs.so.16.0.2
$ pkg-config --modversion libnfs
16.2.0
$ (cd ~/projects/libnfs && git describe --tags)
libnfs-6.0.2-148-gdc7e6f8
```

The installed `/usr/local/include/nfsc/libnfs.h` differs from the
pinned `~/projects/libnfs/include/nfsc/libnfs.h`: the installed
header has additional declarations the pinned tree lacks (e.g.
`nfs_opendir_at_cookie_async`). This means **`/usr/local/`'s install
was made from a tree slightly newer than `~/projects/libnfs/`'s
current HEAD**. Action item below covers re-pinning, but for the
async FFI all the symbols we need exist in both header revisions, so
this drift does not block the binding work.

## Symbol-by-symbol audit

Source for the "linked `.so`" column: `objdump -T
/usr/local/lib/libnfs.so.16.0.2 | grep <symbol>`. Source for the
"linked header" column: `/usr/local/include/nfsc/libnfs.h`. Source
for the "pinned header" column: `~/projects/libnfs/include/nfsc/libnfs.h`.

| Spec symbol | In linked `.so`? | Linked-header signature | Pinned-header parity | Decision |
|---|---|---|---|---|
| `nfs_open_async` | yes | `(ctx, path, flags, cb, pd)` | identical | bind as-is |
| `nfs_open2_async` | yes | `(ctx, path, flags, mode, cb, pd)` | identical | bind as-is; use for `ctx.create()` (see below) |
| `nfs_close_async` | yes | `(ctx, fh, cb, pd)` | identical | bind as-is |
| `nfs_pread_async` | yes | `(ctx, fh, buf, count, offset, cb, pd)` | identical | bind as-is |
| `nfs_pwrite_async` | yes | `(ctx, fh, buf, count, offset, cb, pd)` | identical | bind as-is |
| `nfs_write_async` | yes | `(ctx, fh, buf, count, cb, pd)` | identical | bind, but use append-only / sequential variant; pread/pwrite is the preferred surface |
| `nfs_commit_async` | **NO** | — | — | **Rename**: libnfs has no `nfs_commit_async`. The equivalent is `nfs_fsync_async(ctx, fh, cb, pd)` which is **whole-file** COMMIT. Per-range COMMIT requires the raw RPC interface (`rpc_nfs3_commit_task`, `COMMIT3args { offset, count }`). Decision: surface as `ctx.fsync(&fh)` only; per-range COMMIT is out of scope until the multi-pass mover's cutover pass actually needs it. No C-side patch needed. |
| `nfs_stat64_async` | yes | `(ctx, path, cb, pd)` | identical | bind as-is |
| `nfs_fstat64_async` | yes | `(ctx, fh, cb, pd)` | identical | bind as-is |
| `nfs_create_async` | **NO** | — | — | **Substitute**: libnfs exposes `nfs_creat_async(ctx, path, mode, cb, pd)` (no flags) and `nfs_open2_async(ctx, path, flags, mode, cb, pd)`. Decision: implement `ctx.create(path, flags, mode)` on top of `nfs_open2_async` (same choice the sync side made — see `crates/migration-mover/src/libnfs/mod.rs:110-116`). No C-side patch needed. |
| `nfs_unlink_async` | yes | `(ctx, path, cb, pd)` | identical | bind as-is |
| `nfs_rename_async` | yes | `(ctx, oldpath, newpath, cb, pd)` | identical | bind as-is |
| `nfs_utimes_async` | yes | `(ctx, path, struct timeval *times, cb, pd)` | identical | bind as-is. Note: `struct timeval` is microsecond-precision; nanosecond truncation is a known and documented limitation (`M2_NOTES.md`). |
| `nfs_utimensat_async` | **NO** | — | — | **Drop**: not in this libnfs. Microsecond precision via `nfs_utimes_async` is sufficient for current mover behavior. Recorded as a future gap if the mover ever wants nanosecond mtime fidelity. No patch in this work item. |
| `nfs_chmod_async` | yes | `(ctx, path, mode, cb, pd)` | identical | bind as-is |
| `nfs_chown_async` | yes | `(ctx, path, uid, gid, cb, pd)` | identical | bind as-is |
| `nfs_symlink_async` | yes | `(ctx, target, linkname, cb, pd)` | identical | bind as-is |
| `nfs_link_async` | yes | `(ctx, oldpath, newpath, cb, pd)` | identical | bind as-is |
| `nfs_mkdir_async` | yes | `(ctx, path, cb, pd)` | identical | also bind `nfs_mkdir2_async(ctx, path, mode, cb, pd)` (the sync mover already uses `nfs_mkdir2` for the mode argument) |
| `nfs_mkdir2_async` | yes | `(ctx, path, mode, cb, pd)` | identical | bind as-is — this is the entry point for `ctx.mkdir(path, mode)` |
| `nfs_readlink_async` | yes | `(ctx, path, cb, pd)` | identical | bind as-is. Callback delivers a `char *` target; copy into Rust buffer before completing. |
| `nfs_service` | yes | `(ctx, revents) -> int` | identical | drive from service task |
| `nfs_get_fd` | yes | `(ctx) -> int` | identical | register with `tokio::io::unix::AsyncFd` |
| `nfs_which_events` | yes | `(ctx) -> int` | identical | poll mask for `AsyncFd::ready` |
| `nfs_queue_length` | yes | `(ctx) -> int` | identical | bind for observability / shutdown wait |
| `nfs_set_version` | yes | `(ctx, version) -> int` | identical | call before mount; pass `NFS_V3` literal value `3` (matches existing sync code at `libnfs/mod.rs:205`) |
| `nfs_set_readmax` | yes | `(ctx, size_t readmax) -> void` | identical | this is libnfs's renamed `nfs_set_rsize`. Spec's `MountOpts::rsize` maps here. |
| `nfs_set_writemax` | yes | `(ctx, size_t writemax) -> void` | identical | this is libnfs's renamed `nfs_set_wsize`. Spec's `MountOpts::wsize` maps here. |
| `nfs_set_rsize` (spec name) | **NO** | — | — | Symbol was removed in libnfs 6.x. Replacement is `nfs_set_readmax` above. Use URL param `rsize=N` (which libnfs translates to `nfs_set_readmax` — see `~/projects/libnfs/lib/libnfs.c:314`). No patch needed; the audit's role is to record the rename. |
| `nfs_set_wsize` (spec name) | **NO** | — | — | Same as above: removed; use `nfs_set_writemax` or URL `wsize=N`. |
| `nfs_set_readahead` | **NO in linked v6**; YES in v4 (`libnfs.so.14.0.0`) | — | — | **Removed upstream between v4 and v6.** The spec marks this as "Y (low-effort) — sync-API backstop", but it has no effect on async reads and is not on the async hot path. Decision: drop `readahead` from `MountOpts`. If the multi-pass mover later wants a sync-API readahead backstop on libnfs ≥ 6, that's a separate libnfs C-side patch (commit on top of the pinned tag; restore `nfs_set_readahead`). Documented gap, not blocking. |
| `nconnect=` URL option | **NO** in pinned source | — | — | Pinned `~/projects/libnfs/lib/libnfs.c:nfs_parse_url_args` recognizes only `version, nfsport, mountport, rsize, wsize, readdir-buffer, xprtsec, sec, if` — `nconnect` is **not** in the list. Unknown URL args return `-1` ("Unknown url argument"), so passing `nconnect=N` on this libnfs would hard-fail at mount. Decision: keep `MountOpts::nconnect` for forward compatibility, but **reject `nconnect > 1` at `AsyncNfsContext::mount()` time** with a typed error that points at this audit. If/when libnfs gains `nconnect` upstream, this gate is the single place that has to relax. |

## Decision summary

**Interpretation #1 in the spec applies: no C-side patches are
required for this work item.** Every symbol the multi-pass mover
needs is either present in the linked `.so` under its current name,
or has a documented substitute path:

- `nfs_commit_async` → `nfs_fsync_async` (whole-file). Per-range COMMIT
  is deferred until needed.
- `nfs_create_async` → `nfs_open2_async(O_WRONLY|O_CREAT|O_TRUNC, mode)`.
- `nfs_set_rsize` / `nfs_set_wsize` → `nfs_set_readmax` /
  `nfs_set_writemax`, or equivalently `rsize=`/`wsize=` URL params.
- `nfs_utimensat_async` → drop; `nfs_utimes_async` is the surface.
- `nfs_set_readahead` → drop; removed upstream between v4 and v6;
  multi-pass mover does not need it.
- `nconnect=` → reject at mount time on this libnfs; surface the
  option in `MountOpts` for forward compatibility.

The "fork" therefore reduces to **pinning** the existing source tree,
not patching it. No new commit on `~/projects/libnfs/` is required as
part of this work item.

## Items recorded for future follow-up (not blocking)

These are gaps the audit found that the current work item explicitly
does **not** fix. The async FFI ships without them; the downstream
multi-pass mover work item must accept the same constraints.

1. **`nconnect>1` requires libnfs upstream support.** When upstream
   adds `nconnect=`, lift the `mount()` guard. Alternatively, a fork
   patch on `~/projects/libnfs/` could implement it (multiple TCP
   connections per `nfs_context`). Not in this work item's scope.
2. **Per-range NFS COMMIT requires the raw RPC surface.** If the
   multi-pass mover's cutover pass needs `COMMIT(offset, len)` per
   the spec, switch to `rpc_nfs3_commit_task` from `libnfs-raw.h`.
   For v1 whole-file recopy, `nfs_fsync_async` is sufficient.
3. **Sync-API readahead backstop is gone in libnfs ≥ 6.** If a
   defensive readahead is needed against the sync path, restore
   `nfs_set_readahead` as a patch commit on the pinned tree.
4. **Re-pin `~/projects/libnfs/` to the exact rev that built
   `/usr/local/lib/libnfs.so.16.0.2`.** The header drift noted above
   is benign for the async FFI but indicates the source pin and the
   installed binary are not bit-identical. Fold this into a separate
   ops-hygiene task; the async FFI does not depend on it.

## Cross-check: parameter-order spot-check against linked `.so`

Per `docs/CORRECTNESS_RULES.md` "Cross-check C library FFI", parameter
order is what causes silent data loss (see M2 zero-byte bug). For the
async surface this is harder to spot-check than for sync (we cannot
just look at decompiled assembly), so the FFI smoke tests under
"Verification" in `LIBNFS_ASYNC_FORK.md` carry the load. The
smoke test must:

- Round-trip a known byte pattern through `nfs_pwrite_async` +
  `nfs_fsync_async` + `nfs_pread_async`.
- Assert byte equality, not just "RPC returned success".
- Specifically assert `pread_async(off=0, len=4096)` returns 4096
  bytes that match the source's first 4096 bytes byte-for-byte.

These tests run against `VAMOOSE_TEST_NFS_URL` on var204 and are the
gate that prevents an M2-class regression in the async surface.
