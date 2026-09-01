//! Delta shard emitter: filter a pass's canonical shards down to the
//! NEW+DIRTY rows the classifier kept, producing `delta/` shards plus
//! a delta manifest for the unchanged copy loop.
//!
//! Each delta shard is its source shard minus the unchanged rows:
//! same schema, same KV footer (`migration.shard_index` preserved, so
//! `ShardReader`'s row_id cross-check still holds; row count updated),
//! original `row_id`s, zstd like the rewrite. Written via `.partial` +
//! rename, so a torn emit never passes for a finished shard.

use crate::manifest::{LocalManifest, LocalShard, MANIFEST_FORMAT_VERSION};
use crate::util::{sha256_file, utc_now, write_json_atomic};
use crate::workdir::WorkDir;
use anyhow::{Context, Result};
use arrow::array::{Array, BooleanArray, UInt64Array};
use migration_core::schema::{COL_ROW_ID, KV_ROW_COUNT};
use migration_core::shard::ShardReader;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet::arrow::ArrowWriter;
use parquet::basic::{Compression, ZstdLevel};
use parquet::file::properties::WriterProperties;
use std::fs::File;
use std::path::Path;

/// Emit the delta for one pass: for every shard of `full` (the pass's
/// complete index) with a non-empty keep list under
/// `<pass>/classify/keep/`, write the filtered shard to
/// `<pass>/delta/` and record it. Returns `None` when nothing was
/// kept (the trees are in sync); otherwise writes
/// `delta-manifest.json` and returns it.
pub fn emit(pass_wd: &WorkDir, full: &LocalManifest) -> Result<Option<LocalManifest>> {
    let keep_dir = pass_wd.classify_dir().join("keep");
    let delta_dir = pass_wd.delta_dir();
    std::fs::create_dir_all(&delta_dir)
        .with_context(|| format!("creating {}", delta_dir.display()))?;

    let mut shards = Vec::new();
    for shard in &full.shards {
        let src = pass_wd.shard_path(&shard.path);
        // KV-stamped by the rewrite; ShardReader::open also validates
        // the schema so a bad shard fails here, not mid-copy.
        let shard_index = ShardReader::open(&src)?.shard_index().ok_or_else(|| {
            anyhow::anyhow!(
                "{} carries no migration.shard_index KV; cannot map its keep list",
                src.display()
            )
        })?;
        let keep = migration_resync::read_keep_file(
            &keep_dir.join(migration_resync::keep_file_name(shard_index)),
        )?;
        if keep.is_empty() {
            continue;
        }
        let out = delta_dir.join(shard.file_name());
        let rows = filter_shard(&src, &out, &keep)
            .with_context(|| format!("filtering {} into the delta", src.display()))?;
        anyhow::ensure!(
            rows == keep.len() as u64,
            "{}: keep list has {} rows but {} matched — classifier and shard disagree",
            src.display(),
            keep.len(),
            rows,
        );
        shards.push(LocalShard {
            path: format!("delta/{}", shard.file_name()),
            rows,
            bytes: std::fs::metadata(&out)?.len(),
            sha256: sha256_file(&out)?,
        });
    }

    if shards.is_empty() {
        return Ok(None);
    }
    let delta = LocalManifest {
        format_version: MANIFEST_FORMAT_VERSION,
        run_id: full.run_id.clone(),
        created_utc: utc_now(),
        source: full.source.clone(),
        dest: full.dest.clone(),
        options: full.options.clone(),
        total_rows: shards.iter().map(|s| s.rows).sum(),
        total_bytes: shards.iter().map(|s| s.bytes).sum(),
        shards,
    };
    write_json_atomic(&pass_wd.delta_manifest_json(), &delta)?;
    Ok(Some(delta))
}

/// Copy `src` to `out` keeping only rows whose `row_id` is in the
/// sorted `keep` list. Schema and KV footer are preserved verbatim
/// except `migration.row_count`, updated to the kept count. Returns
/// rows written.
fn filter_shard(src: &Path, out: &Path, keep: &[u64]) -> Result<u64> {
    let file = File::open(src).with_context(|| format!("opening {}", src.display()))?;
    let builder = ParquetRecordBatchReaderBuilder::try_new(file)?;
    let schema = builder.schema().clone();
    let mut kvs = builder
        .metadata()
        .file_metadata()
        .key_value_metadata()
        .cloned()
        .unwrap_or_default();
    for kv in &mut kvs {
        if kv.key == KV_ROW_COUNT {
            kv.value = Some(keep.len().to_string());
        }
    }
    let reader = builder.with_batch_size(8192).build()?;

    let props = WriterProperties::builder()
        .set_compression(Compression::ZSTD(
            ZstdLevel::try_new(3).expect("zstd level 3"),
        ))
        .set_key_value_metadata(Some(kvs))
        .build();
    let partial = out.with_extension("parquet.partial");
    let mut writer = ArrowWriter::try_new(
        File::create(&partial).with_context(|| format!("creating {}", partial.display()))?,
        schema,
        Some(props),
    )?;

    let mut written = 0u64;
    for batch in reader {
        let batch = batch?;
        let row_ids = batch
            .column_by_name(COL_ROW_ID)
            .and_then(|c| c.as_any().downcast_ref::<UInt64Array>())
            .ok_or_else(|| anyhow::anyhow!("{} lacks a u64 row_id column", src.display()))?;
        let mask: BooleanArray = (0..batch.num_rows())
            .map(|i| Some(keep.binary_search(&row_ids.value(i)).is_ok()))
            .collect();
        let kept = arrow::compute::filter_record_batch(&batch, &mask)?;
        if kept.num_rows() > 0 {
            written += kept.num_rows() as u64;
            writer.write(&kept)?;
        }
    }
    writer.close()?;
    File::open(&partial)?.sync_all()?;
    std::fs::rename(&partial, out)
        .with_context(|| format!("activating {} as {}", partial.display(), out.display()))?;
    Ok(written)
}
