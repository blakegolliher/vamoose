# mig-walker-rewrite — pre-flight schema shim

A throwaway tool that converts `nfs-walker`'s current parquet output to
the canonical schema defined in `SCHEMA_CONTRACT.md`. Unblocks M2/M3
manual verification today without requiring walker changes.

**This tool will be deleted** when walker emits canonical schema
natively. Optimize for clarity over flexibility — this is bridge code.

---

## Why this exists

`nfs-walker` today emits a parquet schema that pre-dates the migration
system's canonical schema:

- `path` is `Utf8`, absolute including export root → canonical wants
  `Binary`, relative with leading slash.
- `permissions` is `UInt16` without type bits → canonical wants `mode`
  `UInt32` including `S_IFMT` type bits.
- `file_type` is `Utf8` MIME → canonical wants `UInt8` `FileTypeTag`.
- `mtime_us` is `Int64` microseconds → canonical wants `mtime_sec`
  `Int64` + `mtime_nsec` `Int32`.
- No `row_id` column at all.
- No `fsid`, `symlink_target`, `xattr_blob`.
- No parquet KV metadata identifying schema version.

Walker will emit canonical natively in a follow-up PR. Until then,
this shim translates one to the other so M2/M3 verification can
proceed.

---

## Scope (locked)

In scope:
- Read walker parquet shards from a local directory.
- Write canonical parquet shards to a local directory.
- One CLI invocation processes all input shards in a single run.
- Required canonical columns populated; optional canonical columns
  null where walker can't produce them.
- Legacy columns preserved as-is for additive compatibility.
- Parquet KV metadata written per `SCHEMA_CONTRACT.md`.

Out of scope (do not implement):
- S3 upload. Operators run `aws s3 cp` themselves.
- `manifest.json` generation. That's `mig-manifest-build`, a separate
  tool.
- Filtering, exclusion patterns, snapshot-dir handling. Walker's job.
- Streaming over network. Local files only.
- Performance optimization. Run-time at billion-row scale doesn't
  matter for verification.

---

## Known limitations (document, do not fix)

The translation `permissions` (UInt16, no type bits) →
`mode` (UInt32, with type bits) requires synthesizing the type bits
from the walker's MIME-string `file_type` column. The mapping is:

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
files, dirs, symlinks only), this is fine. For migrating production
data, **operators must wait for native walker support**. The shim's
README must call this out.

---

## Where it lives

Add a new crate to the migration workspace:

```
migration/
└── crates/
    └── mig-walker-rewrite/
        ├── Cargo.toml
        ├── README.md         # MUST document the limitations above
        └── src/
            └── main.rs
```

Add to workspace `Cargo.toml` `members`. Binary name: `mig-walker-rewrite`.

**Not in walker's repo.** Reasons: depends on `migration-core::schema`
for canonical column constants and `FileTypeTag`. Deletion when walker
ships canonical doesn't touch walker. Cleaner separation.

---

## CLI

```text
mig-walker-rewrite [OPTIONS] --input <DIR> --output <DIR> --source-root <PATH>

OPTIONS:
  -i, --input <DIR>          Directory containing walker parquet shards
                             (e.g. .../scans/<scan_id>/)
  -o, --output <DIR>         Directory to write canonical parquet shards
                             (created if absent; refuses to clobber non-empty)
      --source-root <PATH>   Export root prefix to strip from absolute paths.
                             Example: /bgolliher/vamoose-source
      --walker-version <S>   Walker version string for parquet KV metadata.
                             Default: "shim-via-unknown"
  -v, --verbose              Trace per-row translation
  -h, --help
```

Naming convention for output shards: identical to input. The shim sees
`part-r00-00000.parquet`, writes `part-r00-00000.parquet`. Shard
indices for `row_id` are assigned in lexicographic order of input
filenames; same input → same output every time.

---

## Implementation order

### Step 1 — Crate scaffold

```
crates/mig-walker-rewrite/Cargo.toml
```

Minimal deps. Reuse workspace versions:

```toml
[package]
name        = "mig-walker-rewrite"
version     = { workspace = true }
edition     = { workspace = true }
license     = { workspace = true }

[[bin]]
name = "mig-walker-rewrite"
path = "src/main.rs"

[dependencies]
migration-core = { workspace = true }
arrow          = { workspace = true }
parquet        = { workspace = true }
clap           = { workspace = true }
anyhow         = { workspace = true }
tracing        = { workspace = true }
tracing-subscriber = { workspace = true }
```

Add `crates/mig-walker-rewrite` to workspace members.

### Step 2 — Reader: walker parquet → in-memory rows

Use `parquet::arrow::ParquetRecordBatchReader`. For each input file:

1. Open with `ParquetRecordBatchReaderBuilder`.
2. Read all `RecordBatch`es. (At verification scale this is fine; at
   billion-row scale it isn't, but verification is the use case.)
3. Verify the schema has the columns the shim consumes. Required from
   walker: `path` Utf8, `permissions` UInt16, `file_type` Utf8,
   `mtime_us` Int64, `inode` UInt64, `nlink` UInt32, `uid` UInt32,
   `gid` UInt32, `size` UInt64.
   Optional: `atime_us`, `parent_path`, `filename`, `extension`,
   `ctime_us`, `allocated_blocks`, `depth`, `scan_id`,
   `scan_timestamp_us`, `checksum`.

Missing required walker column → fail with a clear error pointing at
the file. Not a panic, a typed error.

### Step 3 — Translator: walker row → canonical row

For each row, compute canonical values:

```rust
fn translate_row(walker: &WalkerRow, shard_idx: u32, row_in_shard: u64,
                 source_root: &[u8]) -> Result<CanonicalRow> {
    // row_id: materialized
    let row_id = (shard_idx as u64) << 40 | (row_in_shard & ((1 << 40) - 1));

    // path: strip source_root, ensure leading slash, encode as bytes
    let path_bytes = walker.path.as_bytes();
    let stripped = path_bytes.strip_prefix(source_root)
        .ok_or_else(|| anyhow!("path does not start with --source-root: {:?}", walker.path))?;
    let path = if stripped.is_empty() {
        b"/".to_vec()
    } else if !stripped.starts_with(b"/") {
        let mut v = vec![b'/']; v.extend_from_slice(stripped); v
    } else {
        stripped.to_vec()
    };

    // file_type tag from MIME string
    let tag = match walker.file_type.as_str() {
        "directory" => FileTypeTag::Dir,
        "symlink"   => FileTypeTag::Symlink,
        _           => FileTypeTag::Regular,  // see "Known limitations"
    };

    // mode: combine permissions with synthesized S_IFMT
    let s_ifmt = match tag {
        FileTypeTag::Dir     => libc::S_IFDIR,
        FileTypeTag::Symlink => libc::S_IFLNK,
        _                    => libc::S_IFREG,
    } as u32;
    let mode = (walker.permissions as u32) | s_ifmt;

    // mtime split
    let (mtime_sec, mtime_nsec) = split_us(walker.mtime_us);
    let (atime_sec, atime_nsec) = walker.atime_us.map(split_us)
        .map(|(s, ns)| (Some(s), Some(ns))).unwrap_or((None, None));

    Ok(CanonicalRow {
        row_id,
        path,
        size: walker.size,
        mode,
        file_type: tag as u8,
        mtime_sec: Some(mtime_sec),
        mtime_nsec: Some(mtime_nsec),
        atime_sec, atime_nsec,
        uid: Some(walker.uid),
        gid: Some(walker.gid),
        nlink: Some(walker.nlink),
        inode: Some(walker.inode),
        fsid: None,             // walker doesn't capture; mover handles null with WARN
        symlink_target: None,    // walker doesn't capture; mover falls back to readlink
        xattr_blob: None,        // reserved
    })
}

fn split_us(us: i64) -> (i64, i32) {
    // Microseconds → (seconds, nanoseconds), handling negatives correctly.
    // For us = -1500: sec = -1, nsec = 998_500_000 (NOT -1500*1000)
    let sec = us.div_euclid(1_000_000);
    let rem = us.rem_euclid(1_000_000) as i32;
    (sec, rem * 1000)
}
```

Note `div_euclid`/`rem_euclid` not `/` and `%` — the latter handle
negative numerators wrong, producing negative `nsec` values which the
contract forbids.

### Step 4 — Writer: canonical row → parquet

Use `parquet::arrow::ArrowWriter`. Build a single `RecordBatch` per
input shard (verification-scale, not production-scale). Schema is the
canonical schema **plus** the legacy columns from input (additive).

The legacy columns pass through unchanged: copy each Arrow array from
the input batch to the output batch. Renames per `SCHEMA_CONTRACT.md`:

- input `path` Utf8 → output `path_legacy` Utf8 (copy bytes, rename column)
- input `file_type` Utf8 → output `file_type_mime` Utf8

Write parquet KV metadata via `WriterPropertiesBuilder::set_key_value_metadata`:

```rust
let kv = vec![
    KeyValue::new("migration.format_version".into(), Some("1".into())),
    KeyValue::new("migration.contract_version".into(), Some("1".into())),
    KeyValue::new("migration.shard_index".into(), Some(shard_idx.to_string())),
    KeyValue::new("migration.walker_version".into(), Some(walker_version.into())),
    KeyValue::new("migration.row_count".into(), Some(rows.to_string())),
];
let props = WriterProperties::builder()
    .set_key_value_metadata(Some(kv))
    .build();
```

### Step 5 — Driver

```rust
fn main() -> Result<()> {
    let args = Cli::parse();

    // Discover input shards in lexicographic order.
    let mut inputs: Vec<PathBuf> = std::fs::read_dir(&args.input)?
        .filter_map(|e| e.ok())
        .map(|e| e.path())
        .filter(|p| p.extension() == Some("parquet".as_ref()))
        .collect();
    inputs.sort();

    if inputs.is_empty() {
        anyhow::bail!("no parquet files found in {}", args.input.display());
    }

    std::fs::create_dir_all(&args.output)?;
    if std::fs::read_dir(&args.output)?.next().is_some() {
        anyhow::bail!("output directory not empty: {}", args.output.display());
    }

    let source_root = args.source_root.as_bytes();

    for (shard_idx, input) in inputs.iter().enumerate() {
        let output = args.output.join(input.file_name().unwrap());
        let rows = rewrite_shard(input, &output, shard_idx as u32,
                                 source_root, &args.walker_version)?;
        tracing::info!(input = %input.display(), output = %output.display(),
                       shard_idx, rows, "shard rewritten");
    }

    Ok(())
}
```

### Step 6 — README

`crates/mig-walker-rewrite/README.md` MUST contain:

1. One-paragraph summary: what it does, that it's a temporary shim.
2. Usage example with the actual command line and explanation of
   `--source-root`.
3. **Limitations section** (verbatim from "Known limitations" above).
4. **Sunset note**: "This crate will be removed when nfs-walker emits
   canonical schema natively. Track <issue link / TBD>."

---

## Tests

Unit tests:

- `split_us` round-trips through `(sec, nsec)` and back, including
  negative values: `(-1500, _)` → `(-1, 998_500_000)`, not `(-1, -1_500_000)`.
- `translate_row` produces correct `mode` for `directory`, `symlink`,
  and a real MIME like `"application/pdf"`.
- `translate_row` correctly strips `--source-root` from `path`,
  including the case where `path == source_root` (yields `b"/"`).
- `translate_row` errors when `--source-root` doesn't prefix `path`.
- `row_id` materialization matches `migration_core::schema::make_row_id`.
- File-type translation table covers all known walker MIME strings
  (test against the values walker actually emits).

Integration test:
- Build a small in-memory walker-shape `RecordBatch`, run the writer,
  open the result with `migration_core::shard::ShardReader::open`, and
  iterate. Verify required columns present and values match. **This
  test is the single most important deliverable** — it's the contract
  conformance check for the shim.

---

## Done criteria

- `cargo build --workspace` clean.
- `cargo test --workspace` passes (M3's 51 + shim's tests).
- Shim runs successfully against the parquet you produced earlier in
  the verification thread (`/home/vastdata/projects/vamoose/m2.parquet/`).
- Output parquet validates with `ShardReader::open` without any
  `Error::MissingColumn` or `Error::ShardCorrupt`.
- Output parquet's KV metadata contains all five `migration.*` keys.
- `crates/mig-walker-rewrite/README.md` is checked in with the
  limitations section.

When all those are true, the shim is done and M2/M3 manual
verification can proceed.

---

## What this does NOT unblock

The shim makes the M2/M3 mover code path testable against real VAST.
It does not:

- Validate the walker PR (separate work, separate verification).
- Validate production-scale operation (the test tree is 12 files).
- Cover non-regular non-symlink non-dir file types (limitation
  documented above).
- Replace `manifest.json` generation (separate tool).

After the shim ships and verification passes, the next decision point
is whether to write `mig-manifest-build` next or to start the walker
canonical-schema PR. Either is reasonable; current preference TBD by
user.
