//! Open a canonical-schema parquet shard with the production
//! `ShardReader` and walk every row. Used to confirm shim output is
//! actually consumable by the mover, not just superficially valid.
//!
//!     cargo run -p mig-walker-rewrite --example verify_shard -- <dir>

use anyhow::{Context, Result};
use migration_core::shard::ShardReader;
use std::path::PathBuf;

fn main() -> Result<()> {
    let dir = std::env::args()
        .nth(1)
        .map(PathBuf::from)
        .context("usage: verify_shard <dir>")?;

    let mut shards: Vec<PathBuf> = std::fs::read_dir(&dir)?
        .filter_map(|e| e.ok())
        .map(|e| e.path())
        .filter(|p| p.is_file() && p.extension().and_then(|s| s.to_str()) == Some("parquet"))
        .collect();
    shards.sort();

    if shards.is_empty() {
        anyhow::bail!("no parquet shards in {}", dir.display());
    }

    let mut total_rows = 0u64;
    for shard in &shards {
        let reader = ShardReader::open(shard)
            .with_context(|| format!("ShardReader::open {}", shard.display()))?;
        let stamped = reader.shard_index();
        let declared_rows = reader.rows();
        let mut iter_rows = 0u64;
        for row in reader.into_rows()? {
            let _row = row.with_context(|| format!("iterating {}", shard.display()))?;
            iter_rows += 1;
        }
        if iter_rows != declared_rows {
            anyhow::bail!(
                "{}: footer declared {declared_rows} rows, iterator yielded {iter_rows}",
                shard.display(),
            );
        }
        println!(
            "ok {}  rows={iter_rows}  shard_index={:?}",
            shard.file_name().unwrap().to_string_lossy(),
            stamped,
        );
        total_rows += iter_rows;
    }
    println!(
        "\nopened {} shard(s), {total_rows} row(s) total",
        shards.len()
    );
    Ok(())
}
