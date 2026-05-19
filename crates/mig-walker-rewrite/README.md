# mig-walker-rewrite

A throwaway pre-flight shim that converts an `nfs-walker` parquet
output directory to the canonical migration schema defined in
`migration/SCHEMA_CONTRACT.md`. It exists so M2/M3 manual verification
can proceed against real VAST hardware today, before walker is updated
to emit the canonical schema natively.

This crate will be **removed** when walker emits canonical schema
natively. Do not build long-lived workflows on top of it.

## Usage

```text
# Walker output root (auto-descends into scans/<scan_id>/):
mig-walker-rewrite \
    --input  /path/to/walker-output/ \
    --output /path/to/canonical-shards/ \
    --source-root /bgolliher/vamoose-source

# Or point directly at the scan subdirectory:
mig-walker-rewrite \
    --input  /path/to/walker-output/scans/<scan_id>/ \
    --output /path/to/canonical-shards/ \
    --source-root /bgolliher/vamoose-source
```

`--input` accepts either form. If an output root contains more than
one `scans/<scan_id>/` subdirectory, the shim refuses to guess and
requires `--input` pointed at a specific scan.

`--source-root` is the export root that the walker scanned. Walker
emits absolute paths (e.g. `/bgolliher/vamoose-source/m2-verify/file.bin`);
the canonical schema requires paths relative to the export root with a
leading slash (`/m2-verify/file.bin`). The shim strips this prefix.
If a walker path does not begin with `--source-root`, the shim refuses
the shard rather than emit a corrupted path.

Output filenames mirror input filenames. Shard indices for `row_id`
materialization are assigned in lexicographic order of input
filenames; running the shim twice on the same input produces
byte-identical canonical `row_id` values.

After running the shim, operators run `aws s3 cp --recursive` on the
output directory themselves. This tool deliberately does not handle
S3, manifest generation, or filtering — it is purely a schema
translator.

## Limitations (do not fix; wait for native walker support)

The translation `permissions` (`UInt16`, no type bits) →
`mode` (`UInt32`, with `S_IFMT` type bits) requires synthesizing the
type bits from walker's MIME-string `file_type` column. The mapping is:

| Walker `file_type` | Synthesized `S_IFMT` |
|---|---|
| `"directory"` | `S_IFDIR` |
| `"symlink"` | `S_IFLNK` |
| anything else | `S_IFREG` |

This means **the shim cannot produce `Fifo`, `Socket`, `BlockDev`, or
`CharDev` `FileTypeTag` values**. Trees containing those file types
will be misclassified as `Regular`. The mover will then attempt to
read them as data files and fail with libnfs errors at copy time —
visible per-file failures, not silent corruption.

For M2/M3 manual verification with the curated test tree (regular
files, directories, symlinks only), this is fine. For migrating
production data, **operators must wait for native walker support**.

In addition, the shim emits `fsid`, `symlink_target`, and `xattr_blob`
as null on every row, because walker does not capture them. The mover
handles each gracefully: `fsid = null` issues a one-time WARN and
falls back to grouping hardlinks by `inode` alone; `symlink_target =
null` triggers a `READLINK` round-trip during the symlink copy;
`xattr_blob = null` is the contract default and means xattrs are not
preserved.

## Sunset

This crate will be removed when `nfs-walker` emits canonical schema
natively. Track the deletion in the walker canonical-schema PR.
