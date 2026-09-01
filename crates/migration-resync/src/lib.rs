//! Scan-vs-scan change classifier for converging delta passes.
//!
//! Diffs two canonical parquet indexes — the previous pass's scan
//! (baseline) and a fresh rescan (current) — and classifies every
//! path per `docs/work-items/MONGOOSE_RESYNC.md`:
//!
//! | Current | Baseline | Tuple | Classification |
//! |---------|----------|-------|----------------|
//! | yes     | no       | —     | NEW            |
//! | yes     | yes      | match | UNCHANGED (or DIRTY if pending) |
//! | yes     | yes      | diff  | DIRTY          |
//! | no      | yes      | —     | DELETED (recorded, never propagated) |
//!
//! The change tuple is `(file_type, size, mtime, ctime)` — inherited
//! from `MULTI_PASS_MOVER.md`: ctime is the backstop (content writes,
//! chmod/chown, link-count changes, and `utimes()` games all bump it,
//! and users cannot set it). Whole-row recopy on dirty; no block diff.
//!
//! ## Mechanics
//!
//! Neither index is globally sorted by path (walker emission is
//! DFS-ish and sharded by path hash), so the join is a
//! **hash-partitioned merge**: one streaming pass over each side
//! routes fixed-format records into B bucket files by path hash;
//! then per bucket, the baseline side loads into a hash map and the
//! current side streams against it (leftovers are DELETED). Peak
//! memory is ~|baseline|/B; I/O is one sequential read of both
//! indexes plus ~1x of both in temporary bucket files (deleted as
//! each bucket completes).
//!
//! ## Outputs (under the caller's `out_dir`)
//!
//! - `keep/shard-NNNNN.rows` — per-source-shard lists of `row_id`s
//!   (u64 LE) to copy (NEW + DIRTY). Shard index comes from the
//!   `row_id` high bits ([`migration_core::schema::split_row_id`]),
//!   so the delta emitter can filter each canonical shard by its own
//!   list.
//! - `deleted.jsonl` — one `{path_b64, ts}` record per baseline path
//!   absent from the current scan.
//! - The returned [`ClassifyCounts`]; the caller checkpoints them.
//!
//! This crate is deliberately independent of mongoose so the fleet
//! pass driver (`MULTI_PASS_MOVER.md` phases 3+) can reuse it.

use anyhow::{Context, Result};
use arrow::array::{Array, BinaryArray, Int32Array, Int64Array, UInt64Array, UInt8Array};
use base64::Engine;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet::arrow::ProjectionMask;
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use std::fs::File;
use std::io::{BufReader, BufWriter, Read, Write};
use std::path::{Path, PathBuf};

/// Per-class row counts from one classification run. `keep_rows` =
/// `new + dirty_tuple + dirty_pending` = rows the delta pass copies.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ClassifyCounts {
    pub baseline_rows: u64,
    pub current_rows: u64,
    pub new: u64,
    pub dirty_tuple: u64,
    /// Tuple matched but the path is in the pending set (failed or
    /// torn in an earlier pass) — forced into the delta.
    pub dirty_pending: u64,
    pub unchanged: u64,
    pub deleted: u64,
    pub keep_rows: u64,
}

/// The change tuple for one row. `None` time components mean the
/// scan didn't carry them; two `None`s compare equal (no evidence of
/// change), consistent with the walker's nullable time columns.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct ChangeTuple {
    file_type: u8,
    size: u64,
    mtime: Option<(i64, i32)>,
    ctime: Option<(i64, i32)>,
}

/// One partitioned record: a path, its tuple, and (current side
/// only) the row_id to copy if the row classifies NEW/DIRTY.
struct Rec {
    path: Vec<u8>,
    tuple: ChangeTuple,
    row_id: u64,
}

/// Classify `current` against `baseline`. Both are lists of canonical
/// parquet shard files. `pending` paths are forced DIRTY on a tuple
/// match. Outputs land under `out_dir` (created; `keep/` and
/// `deleted.jsonl` are wiped first so a re-run never mixes passes).
pub fn classify(
    baseline_shards: &[PathBuf],
    current_shards: &[PathBuf],
    pending: &HashSet<Vec<u8>>,
    out_dir: &Path,
    buckets: usize,
) -> Result<ClassifyCounts> {
    let buckets = buckets.max(1);
    let tmp_dir = out_dir.join("tmp");
    let keep_dir = out_dir.join("keep");
    for dir in [&tmp_dir, &keep_dir] {
        if dir.exists() {
            std::fs::remove_dir_all(dir).with_context(|| format!("clearing {}", dir.display()))?;
        }
        std::fs::create_dir_all(dir).with_context(|| format!("creating {}", dir.display()))?;
    }
    let deleted_path = out_dir.join("deleted.jsonl");

    // ---- Pass 1: partition both sides by path hash -----------------
    let mut counts = ClassifyCounts {
        baseline_rows: partition_side(baseline_shards, &tmp_dir, "base", buckets)?,
        current_rows: partition_side(current_shards, &tmp_dir, "cur", buckets)?,
        ..ClassifyCounts::default()
    };

    // ---- Pass 2: join per bucket -----------------------------------
    let mut keep = KeepWriters::new(&keep_dir);
    let mut deleted = BufWriter::new(
        File::create(&deleted_path)
            .with_context(|| format!("creating {}", deleted_path.display()))?,
    );
    for b in 0..buckets {
        let base_path = tmp_dir.join(format!("base-{b:04}.bin"));
        let cur_path = tmp_dir.join(format!("cur-{b:04}.bin"));

        let mut map: HashMap<Vec<u8>, ChangeTuple> = HashMap::new();
        for rec in RecReader::open(&base_path)? {
            let rec = rec?;
            map.insert(rec.path, rec.tuple);
        }
        for rec in RecReader::open(&cur_path)? {
            let rec = rec?;
            match map.remove(&rec.path) {
                None => {
                    counts.new += 1;
                    keep.push(rec.row_id)?;
                }
                Some(base_tuple) if base_tuple == rec.tuple => {
                    if pending.contains(&rec.path) {
                        counts.dirty_pending += 1;
                        keep.push(rec.row_id)?;
                    } else {
                        counts.unchanged += 1;
                    }
                }
                Some(_) => {
                    counts.dirty_tuple += 1;
                    keep.push(rec.row_id)?;
                }
            }
        }
        // Baseline leftovers: gone from the source. Recorded only —
        // deletion is never propagated (MULTI_PASS_MOVER.md).
        let ts = chrono::Utc::now().format("%Y-%m-%dT%H:%M:%SZ").to_string();
        for (path, _tuple) in map.drain() {
            counts.deleted += 1;
            let line = serde_json::json!({
                "path_b64": base64::engine::general_purpose::STANDARD.encode(&path),
                "ts": ts,
            });
            serde_json::to_writer(&mut deleted, &line)?;
            deleted.write_all(b"\n")?;
        }
        // Buckets are independent; free the disk as we go.
        let _ = std::fs::remove_file(&base_path);
        let _ = std::fs::remove_file(&cur_path);
    }
    deleted.flush()?;
    keep.finish()?;
    let _ = std::fs::remove_dir_all(&tmp_dir);

    counts.keep_rows = counts.new + counts.dirty_tuple + counts.dirty_pending;
    tracing::info!(
        baseline = counts.baseline_rows,
        current = counts.current_rows,
        new = counts.new,
        dirty = counts.dirty_tuple,
        pending = counts.dirty_pending,
        unchanged = counts.unchanged,
        deleted = counts.deleted,
        "classification complete",
    );
    Ok(counts)
}

/// Name of the keep file for one source shard, under `keep/`.
pub fn keep_file_name(shard_index: u32) -> String {
    format!("shard-{shard_index:05}.rows")
}

/// Read one keep file back: the sorted `row_id`s to retain from that
/// shard. Missing file = empty (no rows kept from that shard).
pub fn read_keep_file(path: &Path) -> Result<Vec<u64>> {
    let mut bytes = Vec::new();
    match File::open(path) {
        Ok(mut f) => {
            f.read_to_end(&mut bytes)
                .with_context(|| format!("reading {}", path.display()))?;
        }
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(Vec::new()),
        Err(e) => return Err(e).with_context(|| format!("opening {}", path.display())),
    }
    anyhow::ensure!(
        bytes.len() % 8 == 0,
        "keep file {} is torn ({} bytes)",
        path.display(),
        bytes.len()
    );
    let mut ids: Vec<u64> = bytes
        .as_chunks::<8>()
        .0
        .iter()
        .map(|c| u64::from_le_bytes(*c))
        .collect();
    ids.sort_unstable();
    Ok(ids)
}

// ---------------------------------------------------------------------
// Partitioning
// ---------------------------------------------------------------------

/// Stream every shard of one side into its bucket files. Returns the
/// row count seen.
fn partition_side(shards: &[PathBuf], tmp_dir: &Path, tag: &str, buckets: usize) -> Result<u64> {
    let mut writers: Vec<BufWriter<File>> = (0..buckets)
        .map(|b| {
            let p = tmp_dir.join(format!("{tag}-{b:04}.bin"));
            Ok(BufWriter::new(
                File::create(&p).with_context(|| format!("creating {}", p.display()))?,
            ))
        })
        .collect::<Result<_>>()?;
    let mut rows = 0u64;
    for shard in shards {
        stream_shard(shard, |path, tuple, row_id| {
            rows += 1;
            let b = (fnv1a64(path) % buckets as u64) as usize;
            write_rec(&mut writers[b], path, tuple, row_id)
        })
        .with_context(|| format!("classifying {}", shard.display()))?;
    }
    for mut w in writers {
        w.flush()?;
    }
    Ok(rows)
}

/// FNV-1a over path bytes: stable across runs (unlike SipHash), so a
/// resumed classification repartitions identically.
fn fnv1a64(bytes: &[u8]) -> u64 {
    let mut h: u64 = 0xcbf2_9ce4_8422_2325;
    for &b in bytes {
        h ^= b as u64;
        h = h.wrapping_mul(0x100_0000_01b3);
    }
    h
}

// ---------------------------------------------------------------------
// Canonical parquet projection reader
// ---------------------------------------------------------------------

/// Stream one canonical shard with a narrow projection, invoking `f`
/// per row with (path bytes, tuple, row_id).
///
/// Required columns: `row_id`, `path`, `size`, `file_type`,
/// `mtime_sec`, `mtime_nsec`. ctime arrives via the rewrite's legacy
/// passthrough: `ctime_sec`/`ctime_nsec` preferred, `ctime_us`
/// fallback, absent tolerated (compares as "no evidence of change").
fn stream_shard(
    path: &Path,
    mut f: impl FnMut(&[u8], ChangeTuple, u64) -> Result<()>,
) -> Result<()> {
    use migration_core::schema as s;

    let file = File::open(path).with_context(|| format!("opening {}", path.display()))?;
    let builder = ParquetRecordBatchReaderBuilder::try_new(file)?;
    let pq = builder.parquet_schema();

    let mut leaf_of = HashMap::new();
    for (i, col) in pq.columns().iter().enumerate() {
        leaf_of.insert(col.name().to_string(), i);
    }
    let want_required = [s::COL_ROW_ID, s::COL_PATH, s::COL_SIZE, s::COL_FILE_TYPE];
    let want_optional = [
        s::COL_MTIME_SEC,
        s::COL_MTIME_NSEC,
        "ctime_sec",
        "ctime_nsec",
        "ctime_us",
    ];
    let mut leaves = Vec::new();
    for name in want_required {
        let idx = leaf_of
            .get(name)
            .copied()
            .ok_or_else(|| anyhow::anyhow!("{} lacks column {name}", path.display()))?;
        leaves.push(idx);
    }
    for name in want_optional {
        if let Some(idx) = leaf_of.get(name) {
            leaves.push(*idx);
        }
    }
    let mask = ProjectionMask::leaves(pq, leaves);
    let reader = builder
        .with_projection(mask)
        .with_batch_size(8192)
        .build()?;

    for batch in reader {
        let batch = batch?;
        let get = |name: &str| batch.column_by_name(name);
        let row_id = downcast::<UInt64Array>(&batch, s::COL_ROW_ID, path)?;
        let path_col = downcast::<BinaryArray>(&batch, s::COL_PATH, path)?;
        let size = downcast::<UInt64Array>(&batch, s::COL_SIZE, path)?;
        let file_type = downcast::<UInt8Array>(&batch, s::COL_FILE_TYPE, path)?;
        let mtime_sec = get(s::COL_MTIME_SEC).and_then(|c| c.as_any().downcast_ref::<Int64Array>());
        let mtime_nsec =
            get(s::COL_MTIME_NSEC).and_then(|c| c.as_any().downcast_ref::<Int32Array>());
        let ctime_sec = get("ctime_sec").and_then(|c| c.as_any().downcast_ref::<Int64Array>());
        let ctime_nsec = get("ctime_nsec").and_then(|c| c.as_any().downcast_ref::<Int32Array>());
        let ctime_us = get("ctime_us").and_then(|c| c.as_any().downcast_ref::<Int64Array>());

        for i in 0..batch.num_rows() {
            let mtime = match mtime_sec {
                Some(sec) if !sec.is_null(i) => Some((
                    sec.value(i),
                    mtime_nsec
                        .filter(|n| !n.is_null(i))
                        .map(|n| n.value(i))
                        .unwrap_or(0),
                )),
                _ => None,
            };
            let ctime = match (ctime_sec, ctime_nsec) {
                (Some(sec), Some(nsec)) if !sec.is_null(i) && !nsec.is_null(i) => {
                    Some((sec.value(i), nsec.value(i)))
                }
                _ => match ctime_us {
                    Some(us) if !us.is_null(i) => Some(split_us(us.value(i))),
                    _ => None,
                },
            };
            let tuple = ChangeTuple {
                file_type: file_type.value(i),
                size: size.value(i),
                mtime,
                ctime,
            };
            f(path_col.value(i), tuple, row_id.value(i))?;
        }
    }
    Ok(())
}

fn downcast<'a, T: 'static>(
    batch: &'a arrow::record_batch::RecordBatch,
    name: &str,
    shard: &Path,
) -> Result<&'a T> {
    batch
        .column_by_name(name)
        .and_then(|c| c.as_any().downcast_ref::<T>())
        .ok_or_else(|| anyhow::anyhow!("{} column {name} missing or mistyped", shard.display()))
}

/// Microseconds → (sec, nsec), euclidean so pre-epoch times split
/// consistently (mirrors mig-walker-rewrite's split).
fn split_us(us: i64) -> (i64, i32) {
    (
        us.div_euclid(1_000_000),
        (us.rem_euclid(1_000_000) * 1000) as i32,
    )
}

// ---------------------------------------------------------------------
// Partition record encoding (fixed little-endian layout)
// ---------------------------------------------------------------------

const FLAG_MTIME: u8 = 1 << 0;
const FLAG_CTIME: u8 = 1 << 1;

fn write_rec(w: &mut impl Write, path: &[u8], t: ChangeTuple, row_id: u64) -> Result<()> {
    let mut flags = 0u8;
    let (mt_s, mt_n) = t.mtime.map_or((0, 0), |v| {
        flags |= FLAG_MTIME;
        v
    });
    let (ct_s, ct_n) = t.ctime.map_or((0, 0), |v| {
        flags |= FLAG_CTIME;
        v
    });
    w.write_all(&(path.len() as u32).to_le_bytes())?;
    w.write_all(path)?;
    w.write_all(&[t.file_type, flags])?;
    w.write_all(&t.size.to_le_bytes())?;
    w.write_all(&mt_s.to_le_bytes())?;
    w.write_all(&mt_n.to_le_bytes())?;
    w.write_all(&ct_s.to_le_bytes())?;
    w.write_all(&ct_n.to_le_bytes())?;
    w.write_all(&row_id.to_le_bytes())?;
    Ok(())
}

/// Streaming reader over one bucket file.
struct RecReader {
    r: BufReader<File>,
    path: PathBuf,
}

impl RecReader {
    fn open(path: &Path) -> Result<Self> {
        Ok(Self {
            r: BufReader::new(
                File::open(path).with_context(|| format!("opening {}", path.display()))?,
            ),
            path: path.to_path_buf(),
        })
    }
}

impl Iterator for RecReader {
    type Item = Result<Rec>;

    fn next(&mut self) -> Option<Self::Item> {
        let mut len = [0u8; 4];
        match self.r.read_exact(&mut len) {
            Ok(()) => {}
            Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => return None,
            Err(e) => {
                return Some(Err(e).with_context(|| format!("reading {}", self.path.display())))
            }
        }
        Some(self.read_body(u32::from_le_bytes(len) as usize))
    }
}

impl RecReader {
    fn read_body(&mut self, path_len: usize) -> Result<Rec> {
        let mut path = vec![0u8; path_len];
        self.r.read_exact(&mut path)?;
        let mut fixed = [0u8; 2 + 8 + 8 + 4 + 8 + 4 + 8];
        self.r
            .read_exact(&mut fixed)
            .with_context(|| format!("torn record in {}", self.path.display()))?;
        let file_type = fixed[0];
        let flags = fixed[1];
        let size = u64::from_le_bytes(fixed[2..10].try_into().unwrap());
        let mt_s = i64::from_le_bytes(fixed[10..18].try_into().unwrap());
        let mt_n = i32::from_le_bytes(fixed[18..22].try_into().unwrap());
        let ct_s = i64::from_le_bytes(fixed[22..30].try_into().unwrap());
        let ct_n = i32::from_le_bytes(fixed[30..34].try_into().unwrap());
        let row_id = u64::from_le_bytes(fixed[34..42].try_into().unwrap());
        Ok(Rec {
            path,
            tuple: ChangeTuple {
                file_type,
                size,
                mtime: (flags & FLAG_MTIME != 0).then_some((mt_s, mt_n)),
                ctime: (flags & FLAG_CTIME != 0).then_some((ct_s, ct_n)),
            },
            row_id,
        })
    }
}

// ---------------------------------------------------------------------
// Keep-list writers (one file per source shard, appended lazily)
// ---------------------------------------------------------------------

struct KeepWriters {
    dir: PathBuf,
    writers: HashMap<u32, BufWriter<File>>,
}

impl KeepWriters {
    fn new(dir: &Path) -> Self {
        Self {
            dir: dir.to_path_buf(),
            writers: HashMap::new(),
        }
    }

    fn push(&mut self, row_id: u64) -> Result<()> {
        let (shard, _row) = migration_core::schema::split_row_id(row_id);
        let w = match self.writers.entry(shard) {
            std::collections::hash_map::Entry::Occupied(e) => e.into_mut(),
            std::collections::hash_map::Entry::Vacant(e) => {
                let p = self.dir.join(keep_file_name(shard));
                e.insert(BufWriter::new(
                    File::create(&p).with_context(|| format!("creating {}", p.display()))?,
                ))
            }
        };
        w.write_all(&row_id.to_le_bytes())?;
        Ok(())
    }

    fn finish(self) -> Result<()> {
        for (_, mut w) in self.writers {
            w.flush()?;
        }
        Ok(())
    }
}

// =====================================================================
// Tests
// =====================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{
        ArrayRef, BinaryBuilder, Int32Builder, Int64Builder, UInt64Builder, UInt8Builder,
    };
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;
    use migration_core::schema::make_row_id;
    use parquet::arrow::ArrowWriter;
    use std::sync::Arc;

    struct Row {
        path: &'static str,
        file_type: u8,
        size: u64,
        mtime: Option<(i64, i32)>,
        ctime: Option<(i64, i32)>,
    }

    fn row(path: &'static str, size: u64, mtime: i64, ctime: i64) -> Row {
        Row {
            path,
            file_type: 0,
            size,
            mtime: Some((mtime, 0)),
            ctime: Some((ctime, 0)),
        }
    }

    /// Write a minimal canonical-projection shard: the columns the
    /// classifier reads, with the given rows and shard index.
    fn write_shard(path: &Path, shard_idx: u32, rows: &[Row], with_ctime: bool) {
        let mut fields = vec![
            Field::new("row_id", DataType::UInt64, false),
            Field::new("path", DataType::Binary, false),
            Field::new("size", DataType::UInt64, false),
            Field::new("mtime_sec", DataType::Int64, true),
            Field::new("mtime_nsec", DataType::Int32, true),
            Field::new("file_type", DataType::UInt8, false),
        ];
        if with_ctime {
            fields.push(Field::new("ctime_sec", DataType::Int64, true));
            fields.push(Field::new("ctime_nsec", DataType::Int32, true));
        }
        let schema = Arc::new(Schema::new(fields));

        let mut row_id_b = UInt64Builder::new();
        let mut path_b = BinaryBuilder::new();
        let mut size_b = UInt64Builder::new();
        let mut mt_s = Int64Builder::new();
        let mut mt_n = Int32Builder::new();
        let mut ft_b = UInt8Builder::new();
        let mut ct_s = Int64Builder::new();
        let mut ct_n = Int32Builder::new();
        for (i, r) in rows.iter().enumerate() {
            row_id_b.append_value(make_row_id(shard_idx, i as u64));
            path_b.append_value(r.path.as_bytes());
            size_b.append_value(r.size);
            match r.mtime {
                Some((s, n)) => {
                    mt_s.append_value(s);
                    mt_n.append_value(n);
                }
                None => {
                    mt_s.append_null();
                    mt_n.append_null();
                }
            }
            ft_b.append_value(r.file_type);
            match r.ctime {
                Some((s, n)) => {
                    ct_s.append_value(s);
                    ct_n.append_value(n);
                }
                None => {
                    ct_s.append_null();
                    ct_n.append_null();
                }
            }
        }
        let mut arrays: Vec<ArrayRef> = vec![
            Arc::new(row_id_b.finish()),
            Arc::new(path_b.finish()),
            Arc::new(size_b.finish()),
            Arc::new(mt_s.finish()),
            Arc::new(mt_n.finish()),
            Arc::new(ft_b.finish()),
        ];
        if with_ctime {
            arrays.push(Arc::new(ct_s.finish()));
            arrays.push(Arc::new(ct_n.finish()));
        }
        let batch = RecordBatch::try_new(schema.clone(), arrays).unwrap();
        let mut w = ArrowWriter::try_new(File::create(path).unwrap(), schema, None).unwrap();
        w.write(&batch).unwrap();
        w.close().unwrap();
    }

    fn run(base: &[Row], cur: &[Row], pending: &[&str], dir: &Path) -> (ClassifyCounts, PathBuf) {
        let base_p = dir.join("base.parquet");
        let cur_p = dir.join("cur.parquet");
        write_shard(&base_p, 0, base, true);
        write_shard(&cur_p, 0, cur, true);
        let out = dir.join("out");
        let pending: HashSet<Vec<u8>> = pending.iter().map(|p| p.as_bytes().to_vec()).collect();
        let counts = classify(&[base_p], &[cur_p], &pending, &out, 4).unwrap();
        (counts, out)
    }

    #[test]
    fn full_matrix_new_dirty_unchanged_deleted() {
        let dir = tempfile::tempdir().unwrap();
        let base = vec![
            row("/a", 1, 10, 10), // unchanged
            row("/b", 1, 10, 10), // dirty: mtime moves
            row("/c", 1, 10, 10), // dirty: ctime alone moves (chmod)
            row("/d", 1, 10, 10), // deleted
            row("/e", 5, 10, 10), // dirty: size moves
        ];
        let cur = vec![
            row("/a", 1, 10, 10),
            row("/b", 1, 20, 20),
            row("/c", 1, 10, 30),
            row("/e", 6, 10, 10),
            row("/f", 1, 10, 10), // new
        ];
        let (c, out) = run(&base, &cur, &[], dir.path());
        assert_eq!(c.new, 1);
        assert_eq!(c.dirty_tuple, 3);
        assert_eq!(c.dirty_pending, 0);
        assert_eq!(c.unchanged, 1);
        assert_eq!(c.deleted, 1);
        assert_eq!(c.keep_rows, 4);
        assert_eq!(c.baseline_rows, 5);
        assert_eq!(c.current_rows, 5);

        // Deleted record is /d, base64-encoded.
        let deleted = std::fs::read_to_string(out.join("deleted.jsonl")).unwrap();
        let rec: serde_json::Value = serde_json::from_str(deleted.lines().next().unwrap()).unwrap();
        let path = base64::engine::general_purpose::STANDARD
            .decode(rec["path_b64"].as_str().unwrap())
            .unwrap();
        assert_eq!(path, b"/d");

        // Keep list holds the four row_ids from shard 0 of the CURRENT scan.
        let ids = read_keep_file(&out.join("keep").join(keep_file_name(0))).unwrap();
        assert_eq!(ids.len(), 4);
        for id in &ids {
            assert_eq!(migration_core::schema::split_row_id(*id).0, 0);
        }
        // Temp partitions are cleaned up.
        assert!(!out.join("tmp").exists());
    }

    #[test]
    fn empty_baseline_means_everything_is_new() {
        let dir = tempfile::tempdir().unwrap();
        let cur = vec![row("/a", 1, 1, 1), row("/b", 2, 2, 2)];
        let (c, _) = run(&[], &cur, &[], dir.path());
        assert_eq!(c.new, 2);
        assert_eq!(c.unchanged + c.dirty_tuple + c.deleted, 0);
        assert_eq!(c.keep_rows, 2);
    }

    #[test]
    fn identical_scans_are_fully_unchanged_and_keep_nothing() {
        let dir = tempfile::tempdir().unwrap();
        let rows = vec![row("/a", 1, 1, 1), row("/b", 2, 2, 2)];
        let (c, out) = run(&rows, &rows, &[], dir.path());
        assert_eq!(c.unchanged, 2);
        assert_eq!(c.keep_rows, 0);
        assert!(read_keep_file(&out.join("keep").join(keep_file_name(0)))
            .unwrap()
            .is_empty());
    }

    #[test]
    fn pending_forces_dirty_on_tuple_match() {
        let dir = tempfile::tempdir().unwrap();
        let rows = vec![row("/a", 1, 1, 1), row("/b", 2, 2, 2)];
        let (c, _) = run(&rows, &rows, &["/b"], dir.path());
        assert_eq!(c.unchanged, 1);
        assert_eq!(c.dirty_pending, 1);
        assert_eq!(c.keep_rows, 1);
    }

    #[test]
    fn file_type_flip_is_dirty_even_with_equal_times() {
        let dir = tempfile::tempdir().unwrap();
        let mut base = vec![row("/a", 1, 1, 1)];
        base[0].file_type = 0;
        let mut cur = vec![row("/a", 1, 1, 1)];
        cur[0].file_type = 1;
        let (c, _) = run(&base, &cur, &[], dir.path());
        assert_eq!(c.dirty_tuple, 1);
    }

    #[test]
    fn absent_ctime_columns_compare_as_no_evidence() {
        let dir = tempfile::tempdir().unwrap();
        let base_p = dir.path().join("base.parquet");
        let cur_p = dir.path().join("cur.parquet");
        // Neither side carries ctime columns at all: only mtime/size drive.
        write_shard(
            &base_p,
            0,
            &[row("/a", 1, 1, 99), row("/b", 1, 1, 99)],
            false,
        );
        write_shard(&cur_p, 0, &[row("/a", 1, 1, 5), row("/b", 1, 2, 5)], false);
        let out = dir.path().join("out");
        let c = classify(&[base_p], &[cur_p], &HashSet::new(), &out, 2).unwrap();
        assert_eq!(c.unchanged, 1, "/a: same mtime, ctime columns absent");
        assert_eq!(c.dirty_tuple, 1, "/b: mtime moved");
    }

    #[test]
    fn keep_lists_split_by_source_shard() {
        let dir = tempfile::tempdir().unwrap();
        let cur0 = dir.path().join("cur-0.parquet");
        let cur7 = dir.path().join("cur-7.parquet");
        write_shard(&cur0, 0, &[row("/a", 1, 1, 1)], true);
        write_shard(&cur7, 7, &[row("/b", 1, 1, 1)], true);
        let out = dir.path().join("out");
        let c = classify(&[], &[cur0, cur7], &HashSet::new(), &out, 4).unwrap();
        assert_eq!(c.new, 2);
        let k0 = read_keep_file(&out.join("keep").join(keep_file_name(0))).unwrap();
        let k7 = read_keep_file(&out.join("keep").join(keep_file_name(7))).unwrap();
        assert_eq!(k0, vec![make_row_id(0, 0)]);
        assert_eq!(k7, vec![make_row_id(7, 0)]);
    }

    #[test]
    fn many_buckets_and_rows_stay_consistent() {
        // 1000 rows across 16 buckets: half unchanged, a quarter
        // dirty, a quarter new, plus 250 deleted. Verifies the
        // partition/join produces exact global counts.
        let dir = tempfile::tempdir().unwrap();
        let paths: Vec<String> = (0..1000).map(|i| format!("/f/{i:04}")).collect();
        let leak: Vec<&'static str> = paths
            .iter()
            .map(|s| Box::leak(s.clone().into_boxed_str()) as &'static str)
            .collect();
        let mut base = Vec::new();
        let mut cur = Vec::new();
        for (i, p) in leak.iter().enumerate() {
            match i % 4 {
                0 | 1 => {
                    base.push(row(p, 1, 1, 1));
                    cur.push(row(p, 1, 1, 1));
                }
                2 => {
                    base.push(row(p, 1, 1, 1));
                    cur.push(row(p, 2, 2, 2));
                }
                _ => {
                    base.push(row(p, 1, 1, 1)); // deleted
                    cur.push(Row {
                        path: Box::leak(format!("{p}.new").into_boxed_str()),
                        file_type: 0,
                        size: 1,
                        mtime: Some((1, 0)),
                        ctime: Some((1, 0)),
                    });
                }
            }
        }
        let base_p = dir.path().join("base.parquet");
        let cur_p = dir.path().join("cur.parquet");
        write_shard(&base_p, 0, &base, true);
        write_shard(&cur_p, 0, &cur, true);
        let out = dir.path().join("out");
        let c = classify(&[base_p], &[cur_p], &HashSet::new(), &out, 16).unwrap();
        assert_eq!(c.unchanged, 500);
        assert_eq!(c.dirty_tuple, 250);
        assert_eq!(c.new, 250);
        assert_eq!(c.deleted, 250);
        assert_eq!(c.keep_rows, 500);
        let ids = read_keep_file(&out.join("keep").join(keep_file_name(0))).unwrap();
        assert_eq!(ids.len(), 500);
        assert!(ids.windows(2).all(|w| w[0] < w[1]), "sorted, no dups");
    }
}
