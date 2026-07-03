//! Parquet shard reader.
//!
//! Once a worker has claimed a shard and downloaded it to local
//! scratch (tmpfs), it opens the file through this module. The reader
//! is responsible for:
//!
//! - Opening the parquet file and reading its metadata.
//! - Validating the schema against [`crate::schema::REQUIRED_COLUMNS`].
//! - Validating the parquet KV footer per `SCHEMA_CONTRACT.md`
//!   ("Parquet file metadata"). `migration.format_version` mismatch
//!   is fatal; `migration.contract_version` mismatch is a one-shot
//!   WARN; `migration.shard_index` is cross-checked against the high
//!   bits of the first row's `row_id`.
//! - Iterating rows in `row_id` order.
//! - Surfacing the columns the mover needs as a typed `RowView`.
//!
//! Iteration is **synchronous and blocking** because parquet decode is
//! CPU work — the worker drives it from a `spawn_blocking` pool and
//! feeds the resulting rows into the async mover.

use crate::errors::{Error, Result};
use crate::schema::{self, FileTypeTag};
use arrow::array::{
    Array, BinaryArray, Int32Array, Int64Array, UInt32Array, UInt64Array, UInt8Array,
};
use arrow::record_batch::RecordBatch;
use parquet::arrow::arrow_reader::{ParquetRecordBatchReader, ParquetRecordBatchReaderBuilder};
use std::path::{Path, PathBuf};
use std::sync::Arc;

/// A row-shaped view over one parquet row, materialized into native
/// Rust types. `path`, `xattr_blob`, and `symlink_target` are owned
/// because we want to release the parquet row group as soon as we've
/// extracted what we need.
#[derive(Debug, Clone)]
pub struct RowView {
    pub row_id: u64,
    pub path: Vec<u8>,
    pub size: u64,
    pub mtime_sec: Option<i64>,
    pub mtime_nsec: Option<i32>,
    pub atime_sec: Option<i64>,
    pub atime_nsec: Option<i32>,
    pub mode: u32,
    pub uid: Option<u32>,
    pub gid: Option<u32>,
    pub nlink: Option<u32>,
    pub inode: Option<u64>,
    /// Source filesystem identifier. Combined with `inode` to identify
    /// hardlink groups; null implies "walker didn't capture" and the
    /// mover falls back to grouping by inode alone.
    pub fsid: Option<u64>,
    pub xattr_blob: Option<Vec<u8>>,
    pub symlink_target: Option<Vec<u8>>,
    pub file_type: FileTypeTag,
}

impl RowView {
    /// True if this row references actual file data the mover should
    /// copy. False for symlinks (handled separately), dirs, sockets,
    /// etc.
    pub fn is_data_file(&self) -> bool {
        self.file_type.has_data()
    }
}

/// Handle to a downloaded parquet shard. Cheap — only metadata is read
/// at open time; row decode happens in [`Self::into_rows`].
pub struct ShardReader {
    path: PathBuf,
    total_rows: u64,
    arrow_schema: Arc<arrow::datatypes::Schema>,
    /// `migration.shard_index` from the KV footer, parsed. None if the
    /// walker didn't stamp it (M1-era data).
    shard_index: Option<u32>,
}

impl ShardReader {
    /// Open a downloaded parquet shard.
    ///
    /// Validates that all `REQUIRED_COLUMNS` are present and inspects
    /// the parquet KV footer per the contract. Mismatches surface here
    /// rather than mid-iteration so a bad shard fails the worker
    /// immediately with a precise error.
    pub fn open(path: &Path) -> Result<Self> {
        let file = std::fs::File::open(path)?;
        let builder = ParquetRecordBatchReaderBuilder::try_new(file)?;
        let arrow_schema = builder.schema().clone();
        validate_schema(&arrow_schema)?;
        let file_metadata = builder.metadata().file_metadata();
        let total_rows = file_metadata.num_rows() as u64;
        let kv = collect_kv(file_metadata.key_value_metadata());

        // Format version: hard requirement (when stamped).
        match kv.get(schema::KV_FORMAT_VERSION) {
            Some(s) => {
                let actual: u32 = s.parse().map_err(|_| Error::SchemaVersionMismatch {
                    expected: schema::FORMAT_VERSION,
                    actual: 0,
                })?;
                if actual != schema::FORMAT_VERSION {
                    return Err(Error::SchemaVersionMismatch {
                        expected: schema::FORMAT_VERSION,
                        actual,
                    });
                }
            }
            None => {
                tracing::warn!(
                    path = %path.display(),
                    "shard missing KV {key}; accepting (likely pre-contract walker)",
                    key = schema::KV_FORMAT_VERSION,
                );
            }
        }

        // Contract version: WARN-only.
        if let Some(s) = kv.get(schema::KV_CONTRACT_VERSION) {
            if let Ok(actual) = s.parse::<u32>() {
                if actual != schema::CONTRACT_VERSION {
                    tracing::warn!(
                        path = %path.display(),
                        expected = schema::CONTRACT_VERSION,
                        actual,
                        "shard contract_version differs; check SCHEMA_CONTRACT.md change log",
                    );
                }
            }
        }

        // Optional informational stamps.
        if let Some(s) = kv.get(schema::KV_WALKER_VERSION) {
            tracing::debug!(path = %path.display(), walker_version = %s, "shard walker version");
        }
        if let Some(s) = kv.get(schema::KV_ROW_COUNT) {
            if let Ok(declared) = s.parse::<u64>() {
                if declared != total_rows {
                    tracing::warn!(
                        path = %path.display(),
                        declared,
                        actual = total_rows,
                        "shard KV row_count disagrees with parquet footer num_rows",
                    );
                }
            }
        }

        let shard_index = kv
            .get(schema::KV_SHARD_INDEX)
            .and_then(|s| s.parse::<u32>().ok());

        Ok(Self {
            path: path.to_owned(),
            total_rows,
            arrow_schema,
            shard_index,
        })
    }

    pub fn schema(&self) -> &Arc<arrow::datatypes::Schema> {
        &self.arrow_schema
    }

    /// Total row count read from the parquet footer.
    pub fn rows(&self) -> u64 {
        self.total_rows
    }

    /// Stamped `migration.shard_index` if the walker emitted it.
    pub fn shard_index(&self) -> Option<u32> {
        self.shard_index
    }

    /// Consume the reader and stream rows in `row_id` order. Reopens
    /// the file (cheap on tmpfs) so callers can inspect metadata before
    /// committing to a full scan. The first row produced is
    /// cross-checked against `shard_index` if both are available.
    pub fn into_rows(self) -> Result<RowIter> {
        let file = std::fs::File::open(&self.path)?;
        let inner = ParquetRecordBatchReaderBuilder::try_new(file)?.build()?;
        Ok(RowIter {
            inner,
            batch: None,
            cursor: 0,
            expected_shard_index: self.shard_index,
            verified_first: false,
        })
    }
}

/// Streaming row iterator. Each call to `next()` returns one decoded
/// `RowView` or terminates when the shard is exhausted. The first row
/// is cross-checked against the stamped shard_index.
pub struct RowIter {
    inner: ParquetRecordBatchReader,
    batch: Option<RecordBatch>,
    cursor: usize,
    expected_shard_index: Option<u32>,
    verified_first: bool,
}

impl Iterator for RowIter {
    type Item = Result<RowView>;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            if let Some(batch) = &self.batch {
                if self.cursor < batch.num_rows() {
                    let row = self.cursor;
                    self.cursor += 1;
                    let view = match extract_row(batch, row) {
                        Ok(v) => v,
                        Err(e) => return Some(Err(e)),
                    };
                    if !self.verified_first {
                        self.verified_first = true;
                        if let Some(expected) = self.expected_shard_index {
                            let actual = (view.row_id >> 40) as u32;
                            if expected != actual {
                                return Some(Err(Error::CorruptRow {
                                    row_id: view.row_id,
                                    reason: format!(
                                        "row_id high bits {actual} disagree with KV migration.shard_index {expected}"
                                    ),
                                }));
                            }
                        }
                    }
                    return Some(Ok(view));
                }
                self.batch = None;
                self.cursor = 0;
            }
            match self.inner.next() {
                Some(Ok(batch)) => {
                    self.batch = Some(batch);
                    self.cursor = 0;
                }
                Some(Err(e)) => return Some(Err(Error::Arrow(e))),
                None => return None,
            }
        }
    }
}

fn validate_schema(schema: &Arc<arrow::datatypes::Schema>) -> Result<()> {
    for &col in schema::REQUIRED_COLUMNS {
        if schema.column_with_name(col).is_none() {
            return Err(Error::MissingColumn(col));
        }
    }
    Ok(())
}

/// Fold the parquet KV vector into a name→value lookup. Skips entries
/// with no value (KV is `(String, Option<String>)` in the parquet
/// thrift schema).
fn collect_kv(
    kv: Option<&Vec<parquet::format::KeyValue>>,
) -> std::collections::HashMap<String, String> {
    let mut out = std::collections::HashMap::new();
    if let Some(v) = kv {
        for entry in v {
            if let Some(value) = &entry.value {
                out.insert(entry.key.clone(), value.clone());
            }
        }
    }
    out
}

// =============================================================================
// Per-row column extraction.
// =============================================================================

fn extract_row(batch: &RecordBatch, row: usize) -> Result<RowView> {
    let row_id = req_u64(batch, schema::COL_ROW_ID, row)?;
    let path = req_bin(batch, schema::COL_PATH, row)?;
    let size = req_u64(batch, schema::COL_SIZE, row)?;
    let mode = req_u32(batch, schema::COL_MODE, row)?;
    let file_type_u8 = req_u8(batch, schema::COL_FILE_TYPE, row)?;

    // Per SCHEMA_CONTRACT.md "FileTypeTag values": `Unknown = 0` MUST
    // NOT appear in parquet, and only 1..=7 are valid. Any other value
    // is shard corruption — surface immediately with the offending row
    // identified rather than silently treating it as Unknown.
    if file_type_u8 == 0 || file_type_u8 > 7 {
        return Err(Error::CorruptRow {
            row_id,
            reason: format!(
                "file_type={file_type_u8} not in canonical range 1..=7 (see SCHEMA_CONTRACT.md)",
            ),
        });
    }
    let file_type = FileTypeTag::from_u8(file_type_u8);

    Ok(RowView {
        row_id,
        path,
        size,
        mtime_sec: opt_i64(batch, schema::COL_MTIME_SEC, row)?,
        mtime_nsec: opt_i32(batch, schema::COL_MTIME_NSEC, row)?,
        atime_sec: opt_i64(batch, schema::COL_ATIME_SEC, row)?,
        atime_nsec: opt_i32(batch, schema::COL_ATIME_NSEC, row)?,
        mode,
        uid: opt_u32(batch, schema::COL_UID, row)?,
        gid: opt_u32(batch, schema::COL_GID, row)?,
        nlink: opt_u32(batch, schema::COL_NLINK, row)?,
        inode: opt_u64(batch, schema::COL_INODE, row)?,
        fsid: opt_u64(batch, schema::COL_FSID, row)?,
        xattr_blob: opt_bin(batch, schema::COL_XATTR_BLOB, row)?,
        symlink_target: opt_bin(batch, schema::COL_SYMLINK_TARGET, row)?,
        file_type,
    })
}

fn col<'a>(batch: &'a RecordBatch, name: &'static str) -> Option<&'a Arc<dyn Array>> {
    let (idx, _) = batch.schema().column_with_name(name)?;
    Some(batch.column(idx))
}

fn type_err(col: &'static str) -> Error {
    Error::Other(anyhow::anyhow!("column {col}: unexpected arrow type"))
}

fn req_u64(batch: &RecordBatch, name: &'static str, row: usize) -> Result<u64> {
    let arr = col(batch, name).ok_or(Error::MissingColumn(name))?;
    let arr = arr
        .as_any()
        .downcast_ref::<UInt64Array>()
        .ok_or_else(|| type_err(name))?;
    Ok(arr.value(row))
}

fn req_u32(batch: &RecordBatch, name: &'static str, row: usize) -> Result<u32> {
    let arr = col(batch, name).ok_or(Error::MissingColumn(name))?;
    let arr = arr
        .as_any()
        .downcast_ref::<UInt32Array>()
        .ok_or_else(|| type_err(name))?;
    Ok(arr.value(row))
}

fn req_u8(batch: &RecordBatch, name: &'static str, row: usize) -> Result<u8> {
    let arr = col(batch, name).ok_or(Error::MissingColumn(name))?;
    let arr = arr
        .as_any()
        .downcast_ref::<UInt8Array>()
        .ok_or_else(|| type_err(name))?;
    Ok(arr.value(row))
}

fn req_bin(batch: &RecordBatch, name: &'static str, row: usize) -> Result<Vec<u8>> {
    let arr = col(batch, name).ok_or(Error::MissingColumn(name))?;
    let arr = arr
        .as_any()
        .downcast_ref::<BinaryArray>()
        .ok_or_else(|| type_err(name))?;
    Ok(arr.value(row).to_vec())
}

fn opt_u32(batch: &RecordBatch, name: &'static str, row: usize) -> Result<Option<u32>> {
    let Some(arr) = col(batch, name) else {
        return Ok(None);
    };
    let arr = arr
        .as_any()
        .downcast_ref::<UInt32Array>()
        .ok_or_else(|| type_err(name))?;
    Ok(if arr.is_null(row) {
        None
    } else {
        Some(arr.value(row))
    })
}

fn opt_u64(batch: &RecordBatch, name: &'static str, row: usize) -> Result<Option<u64>> {
    let Some(arr) = col(batch, name) else {
        return Ok(None);
    };
    let arr = arr
        .as_any()
        .downcast_ref::<UInt64Array>()
        .ok_or_else(|| type_err(name))?;
    Ok(if arr.is_null(row) {
        None
    } else {
        Some(arr.value(row))
    })
}

fn opt_i32(batch: &RecordBatch, name: &'static str, row: usize) -> Result<Option<i32>> {
    let Some(arr) = col(batch, name) else {
        return Ok(None);
    };
    let arr = arr
        .as_any()
        .downcast_ref::<Int32Array>()
        .ok_or_else(|| type_err(name))?;
    Ok(if arr.is_null(row) {
        None
    } else {
        Some(arr.value(row))
    })
}

fn opt_i64(batch: &RecordBatch, name: &'static str, row: usize) -> Result<Option<i64>> {
    let Some(arr) = col(batch, name) else {
        return Ok(None);
    };
    let arr = arr
        .as_any()
        .downcast_ref::<Int64Array>()
        .ok_or_else(|| type_err(name))?;
    Ok(if arr.is_null(row) {
        None
    } else {
        Some(arr.value(row))
    })
}

fn opt_bin(batch: &RecordBatch, name: &'static str, row: usize) -> Result<Option<Vec<u8>>> {
    let Some(arr) = col(batch, name) else {
        return Ok(None);
    };
    let arr = arr
        .as_any()
        .downcast_ref::<BinaryArray>()
        .ok_or_else(|| type_err(name))?;
    Ok(if arr.is_null(row) {
        None
    } else {
        Some(arr.value(row).to_vec())
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{
        BinaryBuilder, Int32Builder, Int64Builder, UInt32Builder, UInt64Builder, UInt8Builder,
    };
    use arrow::record_batch::RecordBatch;
    use parquet::arrow::ArrowWriter;
    use parquet::file::properties::WriterProperties;
    use parquet::format::KeyValue;
    use std::sync::Arc;

    /// Builder for synthetic shards. Lets each test write the exact
    /// rows + KV footer it needs without re-doing 50 lines of plumbing.
    struct ShardWriter {
        schema: Arc<arrow::datatypes::Schema>,
        path: std::path::PathBuf,
        kv: Vec<KeyValue>,
    }

    impl ShardWriter {
        fn new(name: &str) -> Self {
            let dir = tempdir_local();
            Self {
                schema: schema::canonical_schema(),
                path: dir.join(name),
                kv: vec![
                    KeyValue {
                        key: schema::KV_FORMAT_VERSION.into(),
                        value: Some(schema::FORMAT_VERSION.to_string()),
                    },
                    KeyValue {
                        key: schema::KV_CONTRACT_VERSION.into(),
                        value: Some(schema::CONTRACT_VERSION.to_string()),
                    },
                    KeyValue {
                        key: schema::KV_SHARD_INDEX.into(),
                        value: Some("0".into()),
                    },
                ],
            }
        }

        fn with_kv(mut self, key: &str, value: Option<&str>) -> Self {
            // Replace if present, else append.
            if let Some(slot) = self.kv.iter_mut().find(|k| k.key == key) {
                slot.value = value.map(|s| s.to_string());
            } else {
                self.kv.push(KeyValue {
                    key: key.into(),
                    value: value.map(|s| s.to_string()),
                });
            }
            self
        }

        fn write(self, rows: &[TestRow]) -> std::path::PathBuf {
            let batch = build_batch(self.schema.clone(), rows);
            self.write_batch(batch)
        }

        /// Raw-batch write path for schema-drift tests: the parquet
        /// file takes the batch's own schema, which need not be
        /// canonical. KV footer handling is identical to `write`.
        fn write_batch(self, batch: RecordBatch) -> std::path::PathBuf {
            let props = WriterProperties::builder()
                .set_key_value_metadata(Some(self.kv))
                .build();
            let file = std::fs::File::create(&self.path).unwrap();
            let mut writer = ArrowWriter::try_new(file, batch.schema(), Some(props)).unwrap();
            writer.write(&batch).unwrap();
            writer.close().unwrap();
            self.path
        }
    }

    /// Canonical single-row batch with column `name` dropped — the
    /// "producer forgot a column" drift shape.
    fn batch_without_column(name: &str) -> RecordBatch {
        let batch = build_batch(schema::canonical_schema(), &[TestRow::ok(0)]);
        let (idx, _) = batch.schema().column_with_name(name).unwrap();
        let keep: Vec<usize> = (0..batch.num_columns()).filter(|&i| i != idx).collect();
        batch.project(&keep).unwrap()
    }

    /// Canonical single-row batch with column `name` re-typed to Utf8
    /// (same name, wrong arrow type) — the M2 incident-1 drift shape.
    fn batch_with_utf8_column(name: &str) -> RecordBatch {
        let batch = build_batch(schema::canonical_schema(), &[TestRow::ok(0)]);
        let (idx, _) = batch.schema().column_with_name(name).unwrap();
        let mut fields: Vec<arrow::datatypes::Field> = batch
            .schema()
            .fields()
            .iter()
            .map(|f| f.as_ref().clone())
            .collect();
        fields[idx] = arrow::datatypes::Field::new(name, arrow::datatypes::DataType::Utf8, false);
        let mut columns = batch.columns().to_vec();
        columns[idx] = Arc::new(arrow::array::StringArray::from(vec![
            "drifted";
            batch.num_rows()
        ]));
        RecordBatch::try_new(Arc::new(arrow::datatypes::Schema::new(fields)), columns).unwrap()
    }

    #[derive(Clone)]
    struct TestRow {
        row_id: u64,
        path: Vec<u8>,
        size: u64,
        mode: u32,
        file_type: u8,
        fsid: Option<u64>,
    }

    impl TestRow {
        fn ok(i: u64) -> Self {
            Self {
                row_id: schema::make_row_id(0, i),
                path: format!("/file-{i}").into_bytes(),
                size: 1024 * (i + 1),
                mode: 0o100644,
                file_type: FileTypeTag::Regular as u8,
                fsid: Some(7),
            }
        }
    }

    fn build_batch(schema: Arc<arrow::datatypes::Schema>, rows: &[TestRow]) -> RecordBatch {
        let mut row_id = UInt64Builder::new();
        let mut p = BinaryBuilder::new();
        let mut size = UInt64Builder::new();
        let mut mt_s = Int64Builder::new();
        let mut mt_n = Int32Builder::new();
        let mut at_s = Int64Builder::new();
        let mut at_n = Int32Builder::new();
        let mut mode = UInt32Builder::new();
        let mut uid = UInt32Builder::new();
        let mut gid = UInt32Builder::new();
        let mut nlink = UInt32Builder::new();
        let mut inode = UInt64Builder::new();
        let mut fsid = UInt64Builder::new();
        let mut xattr = BinaryBuilder::new();
        let mut symt = BinaryBuilder::new();
        let mut ft = UInt8Builder::new();

        for r in rows {
            row_id.append_value(r.row_id);
            p.append_value(&r.path);
            size.append_value(r.size);
            mt_s.append_null();
            mt_n.append_null();
            at_s.append_null();
            at_n.append_null();
            mode.append_value(r.mode);
            uid.append_null();
            gid.append_null();
            nlink.append_null();
            inode.append_null();
            match r.fsid {
                Some(f) => fsid.append_value(f),
                None => fsid.append_null(),
            }
            xattr.append_null();
            symt.append_null();
            ft.append_value(r.file_type);
        }

        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(row_id.finish()),
                Arc::new(p.finish()),
                Arc::new(size.finish()),
                Arc::new(mt_s.finish()),
                Arc::new(mt_n.finish()),
                Arc::new(at_s.finish()),
                Arc::new(at_n.finish()),
                Arc::new(mode.finish()),
                Arc::new(uid.finish()),
                Arc::new(gid.finish()),
                Arc::new(nlink.finish()),
                Arc::new(inode.finish()),
                Arc::new(fsid.finish()),
                Arc::new(xattr.finish()),
                Arc::new(symt.finish()),
                Arc::new(ft.finish()),
            ],
        )
        .unwrap()
    }

    #[test]
    fn open_and_iterate_synthetic_shard() {
        let path = ShardWriter::new("part-good.parquet").write(&[
            TestRow::ok(0),
            TestRow::ok(1),
            TestRow::ok(2),
        ]);
        let reader = ShardReader::open(&path).unwrap();
        assert_eq!(reader.rows(), 3);
        assert_eq!(reader.shard_index(), Some(0));

        let rows: Vec<_> = reader
            .into_rows()
            .unwrap()
            .collect::<Result<Vec<_>>>()
            .unwrap();
        assert_eq!(rows.len(), 3);
        assert_eq!(rows[0].path, b"/file-0");
        assert_eq!(rows[0].fsid, Some(7));
        assert_eq!(rows[1].size, 2048);
    }

    #[test]
    fn open_rejects_format_version_mismatch() {
        let path = ShardWriter::new("part-bad-fmt.parquet")
            .with_kv(schema::KV_FORMAT_VERSION, Some("99"))
            .write(&[TestRow::ok(0)]);
        match ShardReader::open(&path) {
            Err(Error::SchemaVersionMismatch { expected, actual }) => {
                assert_eq!(expected, schema::FORMAT_VERSION);
                assert_eq!(actual, 99);
            }
            Err(other) => panic!("expected SchemaVersionMismatch, got {other:?}"),
            Ok(_) => panic!("expected SchemaVersionMismatch, got Ok(reader)"),
        }
    }

    #[test]
    fn open_accepts_missing_kv_with_warn() {
        // Drop the format_version KV entirely. Reader should accept
        // (M1-era walker compatibility) and issue a WARN.
        let path = ShardWriter::new("part-no-kv.parquet")
            .with_kv(schema::KV_FORMAT_VERSION, None)
            .write(&[TestRow::ok(0)]);
        let reader = ShardReader::open(&path).expect("missing KV is WARN, not error");
        assert_eq!(reader.rows(), 1);
    }

    #[test]
    fn open_rejects_missing_required_column() {
        // The M2 incident-1 class, rejecting direction: a producer
        // whose schema lacks a required column must fail at OPEN, not
        // mid-iteration, with the offending column named.
        for &missing in schema::REQUIRED_COLUMNS {
            let path = ShardWriter::new(&format!("part-missing-{missing}.parquet"))
                .write_batch(batch_without_column(missing));
            match ShardReader::open(&path) {
                Err(Error::MissingColumn(c)) => {
                    assert_eq!(c, missing, "error should name the dropped column");
                }
                Err(other) => panic!("dropping `{missing}`: expected MissingColumn, got {other:?}"),
                Ok(_) => panic!("dropping `{missing}`: expected MissingColumn, got Ok(reader)"),
            }
        }
    }

    #[test]
    fn decode_rejects_mistyped_required_column() {
        // `size` present but Utf8 instead of UInt64. By design,
        // `ShardReader::open` validates column NAMES only
        // (`validate_schema`), so open must SUCCEED here; the type
        // mismatch surfaces on the first `into_rows()` item via
        // `type_err`. Pin the open-vs-decode split explicitly — the
        // late surfacing is exactly what bit in M2 and a future
        // open-time type check would be a (welcome) behavior change
        // this test forces to be made consciously.
        let path = ShardWriter::new("part-mistyped-size.parquet")
            .write_batch(batch_with_utf8_column(schema::COL_SIZE));
        let reader = ShardReader::open(&path)
            .expect("open is a name-only schema check by design; type drift passes open");
        let mut it = reader.into_rows().expect("builder re-open succeeds");
        match it.next() {
            Some(Err(Error::Other(e))) => {
                let msg = e.to_string();
                assert!(
                    msg.contains("unexpected arrow type") && msg.contains(schema::COL_SIZE),
                    "error should name the column and the type problem: {msg}",
                );
            }
            other => panic!("expected Err(Error::Other(..unexpected arrow type..)), got {other:?}"),
        }
    }

    #[test]
    fn drifted_errors_classify_fatal() {
        // Bridge test for the classification gap: the worker's
        // `classify_shard_error` (migration-worker/src/orchestrator.rs)
        // matches on the error VARIANT alone — `MissingColumn` and
        // `Other` map to Fatal (terminal Failed claim, operator
        // intervenes), while e.g. `SchemaVersionMismatch` and `Io` map
        // to WorkerLocal (release + peer retries). The worker-side
        // table test (worker_error_classification.rs) pins variant →
        // class with hand-built errors; this test pins that the errors
        // the reader ACTUALLY produces for schema drift are those
        // Fatal variants. If drift ever started surfacing as a
        // WorkerLocal variant instead, a drifted shard would cycle
        // through the whole fleet forever instead of failing loudly.
        let missing_err = match ShardReader::open(
            &ShardWriter::new("part-classify-missing.parquet")
                .write_batch(batch_without_column(schema::COL_SIZE)),
        ) {
            Err(e) => e,
            Ok(_) => panic!("missing column must not open"),
        };
        assert!(
            matches!(missing_err, Error::MissingColumn(_)),
            "missing-column drift must surface as MissingColumn (classifies Fatal), got {missing_err:?}",
        );

        let reader = ShardReader::open(
            &ShardWriter::new("part-classify-mistyped.parquet")
                .write_batch(batch_with_utf8_column(schema::COL_SIZE)),
        )
        .expect("type drift passes the name-only open check");
        let mistyped_err = match reader.into_rows().unwrap().next() {
            Some(Err(e)) => e,
            other => panic!("mistyped column must fail decode, got {other:?}"),
        };
        assert!(
            matches!(mistyped_err, Error::Other(_)),
            "type drift must surface as Other (classifies Fatal), got {mistyped_err:?}",
        );
    }

    #[test]
    fn iterator_rejects_unknown_file_type() {
        let mut row = TestRow::ok(0);
        row.file_type = 0; // Unknown — forbidden in parquet
        let path = ShardWriter::new("part-unknown.parquet").write(&[row]);
        let reader = ShardReader::open(&path).unwrap();
        let mut it = reader.into_rows().unwrap();
        match it.next() {
            Some(Err(Error::CorruptRow { row_id, reason })) => {
                assert_eq!(row_id, schema::make_row_id(0, 0));
                assert!(reason.contains("file_type=0"), "reason: {reason}");
            }
            other => panic!("expected CorruptRow, got {other:?}"),
        }
    }

    #[test]
    fn iterator_rejects_out_of_range_file_type() {
        let mut row = TestRow::ok(0);
        row.file_type = 9; // not in 1..=7
        let path = ShardWriter::new("part-bad-ft.parquet").write(&[row]);
        let reader = ShardReader::open(&path).unwrap();
        let mut it = reader.into_rows().unwrap();
        assert!(matches!(it.next(), Some(Err(Error::CorruptRow { .. }))));
    }

    #[test]
    fn iterator_verifies_row_id_against_shard_index_on_first_row() {
        // KV says shard_index=5 but rows carry shard_index=0 in their
        // high bits — should fail on the first row.
        let path = ShardWriter::new("part-misaligned.parquet")
            .with_kv(schema::KV_SHARD_INDEX, Some("5"))
            .write(&[TestRow::ok(0)]);
        let reader = ShardReader::open(&path).unwrap();
        let mut it = reader.into_rows().unwrap();
        match it.next() {
            Some(Err(Error::CorruptRow { reason, .. })) => {
                assert!(
                    reason.contains("shard_index 5") || reason.contains("disagree"),
                    "reason: {reason}",
                );
            }
            other => panic!("expected CorruptRow, got {other:?}"),
        }
    }

    fn tempdir_local() -> std::path::PathBuf {
        let base = std::env::temp_dir().join(format!(
            "migration-shard-test-{}-{}",
            std::process::id(),
            uuid_like(),
        ));
        std::fs::create_dir_all(&base).unwrap();
        base
    }

    fn uuid_like() -> String {
        let nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.subsec_nanos())
            .unwrap_or(0);
        format!("{nanos:08x}")
    }
}
