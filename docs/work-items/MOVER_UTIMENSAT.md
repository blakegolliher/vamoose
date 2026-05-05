# Mover should use nfs_utimensat for nanosecond fidelity

## State today

After today's walker schema migration, the canonical parquet schema
carries full nanosecond precision in `mtime_sec`/`mtime_nsec` (and
atime equivalents). The shim plumbs those values through verbatim
when walker emits them.

But the destination NFS file ends up microsecond-truncated. The
trace:

- Source: `2024-01-15 12:34:56.789123456`
- Walker parquet `mtime_sec=1705322096, mtime_nsec=789123456` ✓
- Canonical parquet `mtime_sec=1705322096, mtime_nsec=789123456` ✓
- Destination stat: `12:34:56.789123000` ← lost bottom 3 digits

The lossy step is the mover's call to libnfs's `nfs_utimes`. That
function takes `struct timeval` (`tv_sec` + `tv_usec`) — microsecond
resolution. Even though we hand it `(sec, nsec)`, the `nsec/1000` µsec
truncation happens before the SETATTR goes on the wire.

## What to do

Switch the mover to `nfs_utimensat` (or the libnfs equivalent that
takes nanosecond-resolution `struct timespec`). NFSv3 SETATTR can
carry full nanoseconds in `nfstime3.nseconds`; libnfs's higher-level
APIs may or may not expose it depending on version.

## Investigation steps

1. Check libnfs (5.x at `/usr/local/`) for nanosecond-aware setattr APIs:
nm -D /usr/local/lib/libnfs.so.16 | grep -i 'utimens|setattr'
   Expected candidates: `nfs_utimensat`, `nfs_utimensat_async`,
   `nfs_setattr_async` with explicit `struct nfs_setattr` argument.

2. Check the mover's current call site:
grep -rn 'nfs_utimes|nfs_utimens' crates/migration-mover/src/

3. If the high-level `nfs_utimes` is the only API, drop to the lower-
   level NFSv3 SETATTR RPC: `rpc_nfs3_setattr_async`. Build the
   `nfs_setattrargs3` directly with `set_atime` and `set_mtime` of
   variant `SET_TO_CLIENT_TIME`, encoding `nfstime3 { seconds, nseconds }`.

## Verification

Same recipe as today's M3 metadata-fidelity verification:
sudo touch -d '2024-01-15 12:34:56.789123456' /mnt/.../tiny.txt
walk + shim + worker + drop_caches + stat

Expected destination after the fix:
Modify: 2024-01-15 12:34:56.789123456 +0000   ← all 9 digits

## Out of scope

- ctime preservation. POSIX/NFS doesn't allow setting ctime
  separately; it tracks server-side modifications. Not addressable
  by any client-side API.
- nsec for symlinks under NFSv3. Already a downgrade
  (SYMLINK_TIME_NFSV3); v3 lacks SETATTR-on-symlink, so even
  microseconds aren't preserved for symlinks.

## Related work

- `docs/work-items/AGGREGATOR_IMPLEMENTATION.md` — separate, can
  proceed in parallel.
- M5 multi-host fence test — unrelated.
