//! `vamoose status` — read manifest + claims + per-host progress
//! from S3 and render a one-shot or continuously-refreshed text
//! summary.
//!
//! All S3 reads go through the same `S3Client` the worker uses, so
//! authentication and TLS behavior match what the operator has
//! already verified via `vamoose doctor`.

use crate::config::Config;
use clap::Args as ClapArgs;
use migration_core::claim::ClaimStore;
use migration_core::layout;
use migration_core::records::{ClaimRecord, ClaimState, Manifest, ProgressRecord};
use migration_core::s3::S3Client;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

#[derive(ClapArgs)]
pub struct Args {
    /// Refresh continuously instead of returning after one snapshot.
    #[arg(short, long)]
    pub watch: bool,
    /// Refresh interval in seconds (with --watch).
    #[arg(long, default_value = "5")]
    pub interval: u64,
}

pub async fn run(args: Args, config_path: Option<PathBuf>) -> anyhow::Result<()> {
    let cfg = Config::load(config_path)?;
    let storage = cfg.storage();
    let s3 = Arc::new(
        S3Client::from_config(
            &storage.endpoint,
            &storage.region,
            &storage.bucket,
            storage.profile.as_deref(),
            storage.verify_tls,
        )
        .await?,
    );
    let store: Arc<dyn ClaimStore> = s3.clone();

    loop {
        let snap = collect_status(&store).await?;
        if args.watch {
            // Clear screen between renders so the snapshot doesn't
            // accumulate. Plain ANSI; harmless in non-tty.
            print!("\x1b[2J\x1b[H");
        }
        render(&snap, args.interval, args.watch);
        if !args.watch {
            break;
        }
        tokio::time::sleep(Duration::from_secs(args.interval)).await;
    }
    Ok(())
}

struct Snapshot {
    when: chrono::DateTime<chrono::Utc>,
    total_shards: usize,
    completed: usize,
    in_progress: usize,
    unclaimed: usize,
    workers: Vec<WorkerLine>,
}

struct WorkerLine {
    host_id: String,
    status: String,
    current_shard: Option<String>,
    rows_done: u64,
    rows_total: u64,
    throughput_mb_s: f64,
}

async fn collect_status(store: &Arc<dyn ClaimStore>) -> anyhow::Result<Snapshot> {
    // Manifest → total shard count + the set of shard filenames.
    let manifest_bytes = store
        .get(layout::MANIFEST_KEY)
        .await?
        .ok_or_else(|| anyhow::anyhow!("manifest.json not found in bucket"))?
        .0;
    let manifest: Manifest = serde_json::from_slice(&manifest_bytes)?;

    // Claims under shards/. Each key is shards/<shard>.parquet.claim.
    let mut completed = 0usize;
    let mut in_progress = 0usize;
    let claim_entries = store.list(layout::SHARDS_PREFIX).await?;
    for e in &claim_entries {
        if !e.key.ends_with(layout::CLAIM_SUFFIX) {
            continue;
        }
        let Some((body, _)) = store.get(&e.key).await? else {
            continue;
        };
        let Ok(rec) = serde_json::from_slice::<ClaimRecord>(&body) else {
            continue;
        };
        match rec.state {
            ClaimState::Completed => completed += 1,
            ClaimState::Active => in_progress += 1,
            ClaimState::Failed => {} // counted as terminal-not-completed
        }
    }
    let total_shards = manifest.shards.len();
    let unclaimed = total_shards.saturating_sub(completed + in_progress);

    // Per-host progress.
    let mut workers = Vec::new();
    let progress_entries = store.list(layout::PROGRESS_PREFIX).await?;
    for e in &progress_entries {
        if !e.key.ends_with(".json") {
            continue;
        }
        let Some((body, _)) = store.get(&e.key).await? else {
            continue;
        };
        let Ok(p) = serde_json::from_slice::<ProgressRecord>(&body) else {
            continue;
        };
        workers.push(WorkerLine {
            host_id: p.host,
            status: p.status,
            current_shard: p.current_shard,
            rows_done: p.shard_rows_done,
            rows_total: p.shard_rows_total,
            throughput_mb_s: p.throughput_mb_s_1m,
        });
    }
    workers.sort_by(|a, b| a.host_id.cmp(&b.host_id));

    Ok(Snapshot {
        when: chrono::Utc::now(),
        total_shards,
        completed,
        in_progress,
        unclaimed,
        workers,
    })
}

fn render(s: &Snapshot, interval: u64, watch: bool) {
    println!("vamoose status @ {}\n", s.when.format("%Y-%m-%dT%H:%M:%SZ"));
    println!(
        "Shards: {} total | {} completed | {} in-progress | {} unclaimed\n",
        s.total_shards, s.completed, s.in_progress, s.unclaimed,
    );
    if s.workers.is_empty() {
        println!(
            "Workers: (none — no progress objects in s3://.../{})",
            layout::PROGRESS_PREFIX
        );
    } else {
        println!("Workers:");
        for w in &s.workers {
            let shard = w.current_shard.as_deref().unwrap_or("-");
            let rows = if w.rows_total > 0 {
                format!("{}/{}", w.rows_done, w.rows_total)
            } else if w.rows_done > 0 {
                format!("{}", w.rows_done)
            } else {
                "-".to_string()
            };
            let tput = if w.throughput_mb_s > 0.0 {
                format!("{:.0} MB/s", w.throughput_mb_s)
            } else {
                "-".to_string()
            };
            println!(
                "  {:<12} {:<8} shard={:<20} rows={:<14} {}",
                w.host_id, w.status, shard, rows, tput,
            );
        }
    }
    if watch {
        println!("\nNext refresh in {interval}s (Ctrl-C to stop)");
    }
}
