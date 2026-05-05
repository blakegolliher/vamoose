# Parquet Index Schema Contract

**Version:** 1
**Status:** Authoritative
**Vendored in:** `nfs-walker/SCHEMA_CONTRACT.md`, `migration/SCHEMA_CONTRACT.md`

This document defines the parquet index schema that `nfs-walker` produces
and the migration system (`mig-worker` and friends) consumes. It is the
single source of truth. When walker and mover disagree, this document is
right and one of them is wrong.

The two repos vendor identical copies of this file. CI in either repo
should fail if its copy diverges from the other.

---

## Versioning

Two version numbers travel through the system:

- **`format_version`** — versions the parquet schema (column names,
  types, additive vs. breaking changes).
- **`contract_version`** — versions this document (operational rules,
  null semantics, path encoding, fsid grouping, etc.).

They usually move together but don't have to: a doc clarification can
bump `contract_version` without touching `format_version`.

This document describes **`format_version = 1`, `contract_version = 1`**.

Breaking schema changes (renames, type changes, removed columns)
require bumping `format_version` and updating both repos in lockstep.
Additive schema changes (new optional columns) do not require a version
bump. Both walker and mover must continue to operate against parquet
files that omit additive-but-not-yet-emitted columns.

Versions live in two places:

1. **Parquet KV file metadata** (footer key-value pairs). Walker writes
   them; mover validates at shard open. Specific keys defined below.
2. **`manifest.json`** (`format_version` field). Manifest-level checks
   happen first; shard-level checks catch drift.

---

## Migration strategy: additive

Walker emits **both** legacy columns (for the analytics dashboard and
existing DataFusion queries) and canonical columns (for the migration
mover). Readers pick the columns they need:

- **Walker dashboard / analytics consumers** read legacy columns:
  `permissions`, `file_type_mime`, `mtime_us`, `path_legacy`,
  `parent_path`, `filename`, `extension`. Their code is updated in the
  walker PR that introduces this contract — see "Renames in the walker
  PR" below.
- **Migration mover** reads canonical columns: `mode`, `file_type`,
  `mtime_sec` + `mtime_nsec`, `path`. Sees no legacy columns.
- **New consumers** read canonical columns only. Legacy columns are
  not for new code.

Storage cost is a few redundant integer/string columns per row.
Acceptable at billion-row scale.

### Renames in the walker PR

Two columns are renamed to resolve same-name collisions between legacy
and canonical:

| Old name | New name | Reason |
|---|---|---|
| `path` (Utf8) | `path_legacy` (Utf8) | Canonical `path` (Binary) takes the simple name. |
| `file_type` (Utf8) | `file_type_mime` (Utf8) | Canonical `file_type` (UInt8) takes the simple name. |

Dashboard queries referencing the old names update in the same PR.

---

## Required canonical columns

These columns must always be present and non-null where marked. The
mover refuses to read shards missing any of these.

### Identity and content

| Column | Arrow Type | Nullable | Definition |
|---|---|---|---|
| `row_id` | `UInt64` | No | Globally unique row identifier within a single run. **Materialized at write time.** Format: `(shard_idx << 40) \| row_in_shard`. Shard index uses the high 24 bits, row offset the low 40. **Not stable across runs** of the same scan — re-walking the same source produces fresh `row_id` values. Cross-run identity uses `path` or `(fsid, inode)`. **Never derived at read time** — predicate pushdown can reorder rows. |
| `path` | `Binary` | No | Path within the export, **relative to the export root, with leading slash**. Example: `b"/m2-verify/large.bin"` for a file at `/bgolliher/vamoose-source/m2-verify/large.bin` in an export rooted at `/bgolliher/vamoose-source`. Encoded as raw POSIX bytes, not UTF-8. May contain non-UTF-8 sequences. The kernel has no charset opinion on filenames; readers must not impose one. The export root is documented in `manifest.source.root`. |
| `size` | `UInt64` | No | File logical size in bytes. **Advisory** — see "Size semantics" below. `0` for empty files, dirs, special files, symlinks (link target text length is not used). |
| `mode` | `UInt32` | No | POSIX mode bits including type bits (`S_IFMT`). E.g. `0o100644` for a regular file with rw-r--r--. Includes the type bits — readers may derive `file_type` from `mode & S_IFMT` if needed but must not rely on it; use `file_type` instead. |
| `file_type` | `UInt8` | No | `FileTypeTag` enum value (1-7). **`Unknown = 0` MUST NOT appear in parquet** — see "FileTypeTag values". |

### POSIX attributes

| Column | Arrow Type | Nullable | Definition |
|---|---|---|---|
| `mtime_sec` | `Int64` | Yes | mtime seconds since Unix epoch. Null only if walker could not stat. Mover behavior on null: see "Null attribute semantics". |
| `mtime_nsec` | `Int32` | Yes | mtime nanoseconds, in `[0, 1_000_000_000)`. Non-negative even when `mtime_sec` is negative (matches `struct timespec`). For sources with second-precision filesystems, `0` (not null). Null only paired with null `mtime_sec`. |
| `atime_sec` | `Int64` | Yes | atime seconds since Unix epoch. May be null. |
| `atime_nsec` | `Int32` | Yes | atime nanoseconds. May be null. |
| `uid` | `UInt32` | Yes | Owner user ID. Null if walker could not determine. |
| `gid` | `UInt32` | Yes | Owner group ID. Null if walker could not determine. |
| `nlink` | `UInt32` | Yes | Hard link count. Files with `nlink > 1` participate in hardlink groups (see `inode` and `fsid`). |
| `inode` | `UInt64` | Yes | Source filesystem inode number. Used **with `fsid`** to identify hardlink groups. Inode numbers are only unique within a single filesystem; `(fsid, inode)` is the canonical hardlink key. Null only if walker could not determine. |
| `fsid` | `UInt64` | Yes | Source filesystem identifier from the file handle's post-op attributes. Combined with `inode` to disambiguate hardlinks across underlying filesystems within an export. Null when walker can't determine; mover falls back to grouping by `inode` alone with a one-time WARN at shard open. |

---

## Optional canonical columns

May be absent. The mover handles their absence gracefully.

| Column | Arrow Type | Nullable | Definition |
|---|---|---|---|
| `symlink_target` | `Binary` | Yes | For symlinks, the link target as raw bytes. Allows the mover to skip a `READLINK` round-trip. Walker emits when present; mover falls back to `nfs_readlink` when absent. |
| `xattr_blob` | `Binary` | Yes | Serialized extended attributes. Format defined in "xattr_blob format". **Reserved for future walker support** — currently always null. |

---

## Legacy columns (additive, retained for compatibility)

These pre-date the canonical schema. Walker continues to emit them for
the analytics dashboard. **New consumers must not read these.**

| Column | Arrow Type | Definition |
|---|---|---|
| `path_legacy` | `Utf8` | Absolute path including export root prefix. Lossy for non-UTF-8 names. The pre-contract `path` column, renamed. |
| `filename` | `Utf8` | Basename only, UTF-8. Lossy. |
| `extension` | `Utf8` | File extension. |
| `file_type_mime` | `Utf8` | MIME type from `infer` crate, or descriptive string ("directory", "symlink") for non-regular entries. The pre-contract `file_type` column, renamed. |
| `permissions` | `UInt16` | Mode bits without type. Sub-canonical width. |
| `mtime_us` | `Int64` | mtime in microseconds since Unix epoch. **Derived** from `mtime_sec * 1_000_000 + mtime_nsec / 1000`. Walker computes; not source-of-truth. |
| `atime_us` | `Int64` | atime in microseconds. Derived. |
| `ctime_us` | `Int64` | ctime in microseconds. **No canonical equivalent — see footnote.** |
| `allocated_blocks` | `UInt64` | Disk blocks allocated. Used by dashboard's allocation-waste page; not used by mover in M-series. |
| `depth` | `UInt16` | Directory depth from scan root. |
| `parent_path` | `Utf8` | UTF-8 parent directory. |
| `scan_id` | `Utf8` | Walker scan UUID. |
| `scan_timestamp_us` | `Int64` | Walker start time. |
| `checksum` | `Utf8` (nullable) | gxhash, when walker `-c` flag is used. |

> **ctime footnote.** ctime is the inode change time — updated whenever
> any attribute changes. POSIX provides no syscall to set ctime
> explicitly, only to read it. The migration system therefore cannot
> preserve ctime; dest files have ctime equal to migration time
> regardless of source ctime. `ctime_us` is retained as a legacy
> column for the dashboard but has no canonical equivalent.

---

## FileTypeTag values

```rust
#[repr(u8)]
pub enum FileTypeTag {
    Unknown  = 0,   // RESERVED, MUST NOT appear in parquet
    Regular  = 1,
    Dir      = 2,
    Symlink  = 3,
    Fifo     = 4,
    Socket   = 5,
    BlockDev = 6,
    CharDev  = 7,
}
```

Walker computes from `mode & S_IFMT`:

| Mode bits | Tag |
|---|---|
| `S_IFREG` | `Regular` |
| `S_IFDIR` | `Dir` |
| `S_IFLNK` | `Symlink` |
| `S_IFIFO` | `Fifo` |
| `S_IFSOCK` | `Socket` |
| `S_IFBLK` | `BlockDev` |
| `S_IFCHR` | `CharDev` |
| anything else | walker SKIPS the entry |

**Walker MUST emit a concrete value (1-7) for every row in parquet.**
`Unknown = 0` exists in the enum as an in-memory placeholder during
construction but MUST NOT be written to parquet. If walker encounters a
mode it cannot classify, it logs a WARN, increments a per-scan
counter, skips the entry, and surfaces the count in its scan summary.

The mover treats `Unknown = 0` in parquet as shard corruption:
`Error::ShardCorrupt` with the offending row identified.

---

## row_id materialization rule

Walker assigns `row_id` at write time as `(shard_index << 40) |
row_in_shard`. `shard_index` is the zero-based index of the parquet
file within the run (`part-r00-00000.parquet` is shard 0,
`part-r01-00000.parquet` is shard 1). `row_in_shard` is the zero-based
row offset.

The high bits (≥ bit 40) identify the shard. The low 40 bits are
per-shard offset. Up to 16M shards × ~1T rows/shard.

**Readers MUST read `row_id` from the column.** They MUST NOT derive
it from `(parquet_file_index, current_row_index)` — predicate
pushdown, column pruning, and row-group filtering can all reorder
rows between disk and reader.

`row_id` is **not stable across runs**. Two runs of the same scan
produce different `row_id` values for the same logical files because
walker entry order is server-dependent. Consumers needing cross-run
identity use `path` or `(fsid, inode)`.

---

## Path encoding

`path` is **raw POSIX bytes, relative to the export root, with leading
slash**. Examples for an export rooted at `/bgolliher/vamoose-source`:

| Source absolute path | `path` column |
|---|---|
| `/bgolliher/vamoose-source/m2-verify/large.bin` | `b"/m2-verify/large.bin"` |
| `/bgolliher/vamoose-source/data/é.txt` | `b"/data/\xc3\xa9.txt"` |
| `/bgolliher/vamoose-source/weird-\xff-name.bin` | `b"/weird-\xff-name.bin"` |

The export root is `b"/"` itself. No trailing slash. Subdirectories
use `b"/"`. Path is absolute within the export but contains no part
of the export-root prefix.

When serialized into JSON (failure records, downgrade records, etc.),
paths are base64-encoded under the field `path_b64`. Never `path` as a
string in JSON — that loses non-UTF-8 paths.

The mover constructs source and dest URLs as
`<endpoint.url><path>` — endpoint URL ends without trailing slash,
path begins with leading slash, concatenation produces the full
target.

---

## Null attribute semantics

When walker emits null for an attribute, the mover applies the
following rules:

| Null column | Mover behavior |
|---|---|
| `mtime_sec` / `mtime_nsec` | Skip `utimes` for mtime. Apply `atime` if present. Write a downgrade record. |
| `atime_sec` / `atime_nsec` | Skip atime. Apply mtime if present. Downgrade record only if user requested atime preservation. |
| `uid` / `gid` | Skip `chown`. Write a downgrade record. |
| `mode` | Cannot occur — `mode` is non-null per contract. Mover treats as shard corruption if it does. |
| `inode` | Treat as not part of any hardlink group. File is copied whole. |
| `fsid` | Fall back to grouping by `inode` alone, with one-time WARN at shard open. |
| `nlink` | Treat as `1` (assume not hardlinked). |

A **downgrade record** is a JSON line written to `downgrades/host-<id>.jsonl`
(parallel to `failures/`):

```json
{
  "row_id": 17592186049321,
  "shard":  "part-r01-00000.parquet",
  "path_b64": "...",
  "downgrade": "NULL_MTIME",
  "ts": "2026-05-02T10:55:33Z"
}
```

The file copy still succeeds and counts as `files_ok`. Downgrades are
discoverable post-run without polluting the failure rate metric.

---

## Size semantics

`size` in the index is the file's logical size at scan time. **By
default, the mover treats this as advisory:** it reads from the source
until EOF and writes exactly that many bytes to the destination,
regardless of whether the live size matches `size`. Files that grew
between walk and copy are migrated whole; files that shrank are
migrated to their new shorter size.

When `[copy].require_unchanged_size = true`, the mover verifies the
copied byte count against `size` after each file and fails the row
(`FailurePhase::Open`, error tag `SIZE_CHANGED`) if they differ. The
verification happens after the copy because checking before would cost
a stat round-trip on every file. Default is `false`.

`size` is also used for batch budgeting and pre-flight capacity
planning, where exactness doesn't matter.

---

## Sparse files

The migration system does **not** preserve sparseness in M-series
milestones. A 100 GB file with 0 allocated blocks at the source
becomes a 100 GB dense file at the destination, occupying 100 GB of
disk. Content is preserved byte-for-byte; capacity is not.

M3 introduces `SEEK_HOLE` / `SEEK_DATA`-driven sparse-aware copy via
libnfs's `nfs_lseek`. Until then, operators migrating sparse-heavy
trees should expect destination capacity inflation and plan
accordingly.

`size` is logical, `allocated_blocks` is physical; the gap between
them indicates sparseness for pre-flight inspection.

---

## Snapshot directories

Walker, by default, **excludes** directories named:

- `.snapshot`
- `.zfs/snapshot`
- `~snapshot`

These are well-known snapshot mount-point conventions across NFS
servers (VAST, NetApp, ZFS). Indexing them produces N× data movement
for N retained snapshots, which is almost never what users want.

`--include-snapshots` overrides the default and indexes everything.

Operators wanting to migrate a specific snapshot should walk the
snapshot path directly as the export root:
`nfs://server/export/.snapshot/2026-05-01/`. This treats the snapshot
as live data and produces a clean point-in-time migration.

---

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

---

## xattr_blob format

**Reserved for future walker support.** Always null in `format_version = 1`.

When walker xattr capture is implemented, the format is:

```text
repeat:
  u32 name_len      (big-endian)
  <name_len> bytes  (xattr name, raw bytes)
  u32 value_len     (big-endian)
  <value_len> bytes (xattr value, raw bytes)
```

No outer length, no terminator — the parquet column's byte length is
the record length. An empty blob (zero length) means "walker checked,
no xattrs found." Distinct from null which means "walker did not check."

When this lands, the mover's `attrs::parse_xattr_blob` activates
automatically. No mover code changes required at that time.

---

## Parquet file metadata (KV footer)

Walker writes the following key-value pairs into every parquet file's
footer metadata. Mover validates at `ShardReader::open`.

| Key | Value | Purpose |
|---|---|---|
| `migration.format_version` | `"1"` | Schema version. Mismatch → `Error::SchemaVersionMismatch`. |
| `migration.contract_version` | `"1"` | This document's version. Mismatch → WARN, continue. |
| `migration.shard_index` | `"0"`, `"1"`, ... | Zero-based shard index. Used to verify `row_id` high bits match. |
| `migration.walker_version` | e.g. `"0.2.0"` | Walker semver. Informational. |
| `migration.row_count` | e.g. `"1234"` | Redundant with parquet metadata; sanity check. |

Future keys may be added; mover ignores keys it doesn't recognize.

---

## Operational rules

1. **Walker emits the entire required canonical column set in every
   shard.** Optional columns may be omitted entirely or emitted as
   all-null.

2. **Mover validates required columns at shard open.** Missing
   required column is `Error::MissingColumn` and the shard is
   rejected.

3. **Empty shards are valid.** A parquet file with zero rows but a
   complete schema is acceptable. Manifest builders may include them
   with `rows: 0` or skip them. Walker's parallel-export sometimes
   produces these.

4. **Schema-drift detection.** Both repos pin to this contract version
   in `Cargo.toml` metadata. Parquet KV `migration.format_version`
   mismatches cause shard rejection at runtime with a clear error
   pointing to this document.

5. **Cross-version reads.** A `format_version = 1` mover MUST refuse
   shards with any other `format_version` value. Forward and backward
   compatibility require an explicit version bump and migration logic.

---

## Change log

| Version | Change |
|---|---|
| 1 | Initial contract. |

### Decisions baked into v1 (with rationale for future readers)

- **Canonical names own simple identifiers.** `file_type` (UInt8) and
  `path` (Binary) win over the renamed legacy `file_type_mime` and
  `path_legacy`. Reasoning: legacy is on a deprecation glide path; new
  code is cheaper than legacy code to write carefully.
- **`(fsid, inode)` for hardlink grouping**, not `inode` alone.
  Multi-filesystem exports exist; silent mis-grouping is unacceptable.
- **`row_id` not stable across runs.** Cheap. Stable IDs are
  hypothetical use cases; making them stable at billion-file scale is
  expensive.
- **Paths relative to export root, leading slash.** Migrating between
  exports is the tool's purpose; relative paths reroot trivially.
- **Parquet KV metadata for versions.** Self-describing shards;
  catches manifest/shard drift; the standard parquet pattern.
- **`Unknown = 0` forbidden in parquet.** Walker classifies every row
  or skips it; pushing uncertainty downstream is the wrong default.
- **ctime acknowledged in legacy table only**, not promoted.
- **Sparse files become dense in M-series**, fixed in M3.
- **Snapshot dirs excluded by default**, opt-in via flag.
- **`size` advisory by default**, strict via opt-in. User intent is
  "copy what's there now," not "enforce what walker saw."
- **Symlink mode preservation degrades on NFSv3.** NFSv3 has no
  lchmod equivalent — `nfs_chmod` follows symlinks. Forcing strict
  preservation would fail every symlink with mode != `0o0777` against
  an NFSv3 destination, which is the deployment baseline. The mover
  records a `SYMLINK_MODE_NFSV3` downgrade and continues. NFSv4
  destinations can still preserve strictly via SETATTR-on-symlink.
  `contract_version = 1` already permits this — the new behavior is
  a more permissive instance of the existing downgrade pattern, no
  version bump required.
