//! `vamoose status` — read manifest + claims + per-host progress
//! from S3 and render a one-shot or continuously-refreshed summary.
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
use serde::Serialize;
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, Instant};

#[derive(ClapArgs)]
pub struct Args {
    /// Refresh continuously instead of returning after one snapshot.
    #[arg(short, long)]
    pub watch: bool,
    /// Refresh interval in seconds (with --watch).
    #[arg(long, default_value = "5")]
    pub interval: u64,
    /// Emit one machine-readable JSON snapshot.
    #[arg(long, conflicts_with = "watch")]
    pub json: bool,
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
    let mut previous: Option<(Instant, u64)> = None;

    loop {
        let sample_time = Instant::now();
        let mut snap = collect_status(&store).await?;
        if let Some((previous_time, previous_rows)) = previous {
            let elapsed = sample_time.duration_since(previous_time).as_secs_f64();
            if elapsed > 0.0 && snap.rows_progress >= previous_rows {
                let rate = (snap.rows_progress - previous_rows) as f64 / elapsed;
                snap.rows_per_second = Some(rate);
                if rate > 0.0 && snap.rows_progress < snap.total_rows && snap.failed == 0 {
                    snap.eta_seconds =
                        Some(((snap.total_rows - snap.rows_progress) as f64 / rate).ceil() as u64);
                }
            }
        }

        if args.json {
            println!("{}", serde_json::to_string_pretty(&snap)?);
        } else {
            if args.watch {
                // Clear screen between renders so the snapshot doesn't
                // accumulate. Plain ANSI; harmless in non-tty.
                print!("\x1b[2J\x1b[H");
            }
            render(&snap, args.interval, args.watch);
        }
        if !args.watch || snap.terminal {
            break;
        }
        previous = Some((sample_time, snap.rows_progress));
        tokio::time::sleep(Duration::from_secs(args.interval)).await;
    }
    Ok(())
}

#[derive(Debug, Serialize)]
struct Snapshot {
    when: chrono::DateTime<chrono::Utc>,
    total_shards: usize,
    total_rows: u64,
    completed: usize,
    failed: usize,
    in_progress: usize,
    unclaimed: usize,
    terminal: bool,
    rows_completed: u64,
    rows_failed: u64,
    rows_active: u64,
    rows_progress: u64,
    progress_percent: f64,
    aggregate_throughput_mb_s: f64,
    rows_per_second: Option<f64>,
    eta_seconds: Option<u64>,
    files_ok: u64,
    files_failed: u64,
    files_fenced: u64,
    workers: Vec<WorkerLine>,
}

#[derive(Debug, Serialize)]
struct WorkerLine {
    host_id: String,
    status: String,
    started_utc: chrono::DateTime<chrono::Utc>,
    heartbeat_utc: chrono::DateTime<chrono::Utc>,
    heartbeat_age_seconds: u64,
    stale_after_seconds: Option<u64>,
    stale: Option<bool>,
    current_shard: Option<String>,
    rows_done: u64,
    rows_total: u64,
    files_ok: u64,
    files_failed: u64,
    files_fenced: u64,
    throughput_mb_s: f64,
}

async fn collect_status(store: &Arc<dyn ClaimStore>) -> anyhow::Result<Snapshot> {
    let now = chrono::Utc::now();
    // Manifest is the authority for which claims belong to this run.
    let manifest_bytes = store
        .get(layout::MANIFEST_KEY)
        .await?
        .ok_or_else(|| anyhow::anyhow!("manifest.json not found in bucket"))?
        .0;
    let manifest: Manifest = serde_json::from_slice(&manifest_bytes)?;
    let shard_rows: HashMap<&str, u64> = manifest
        .shards
        .iter()
        .filter_map(|shard| {
            shard
                .key
                .strip_prefix(layout::INDEX_PREFIX)
                .map(|name| (name, shard.rows))
        })
        .collect();

    let mut states: HashMap<String, ClaimState> = HashMap::new();
    let claim_entries = store.list(layout::SHARDS_PREFIX).await?;
    for entry in &claim_entries {
        let Some(shard_name) = layout::shard_from_claim_key(&entry.key) else {
            continue;
        };
        if !shard_rows.contains_key(shard_name) {
            continue;
        }
        let Some((body, _)) = store.get(&entry.key).await? else {
            continue;
        };
        let Ok(record) = serde_json::from_slice::<ClaimRecord>(&body) else {
            continue;
        };
        states.insert(shard_name.to_string(), record.state);
    }

    let mut completed = 0usize;
    let mut failed = 0usize;
    let mut in_progress = 0usize;
    let mut rows_completed = 0u64;
    let mut rows_failed = 0u64;
    for (shard_name, state) in &states {
        let rows = shard_rows.get(shard_name.as_str()).copied().unwrap_or(0);
        match state {
            ClaimState::Completed => {
                completed += 1;
                rows_completed = rows_completed.saturating_add(rows);
            }
            ClaimState::Failed => {
                failed += 1;
                rows_failed = rows_failed.saturating_add(rows);
            }
            ClaimState::Active => in_progress += 1,
        }
    }
    let total_shards = manifest.shards.len();
    let unclaimed = total_shards.saturating_sub(completed + failed + in_progress);

    // Per-worker progress also supplies the active-shard partial row count.
    // Bound every value by the manifest row count and take the maximum if a
    // stale progress object happens to name the same shard as its replacement.
    let mut active_rows_by_shard: HashMap<String, u64> = HashMap::new();
    let mut workers = Vec::new();
    let progress_entries = store.list(layout::PROGRESS_PREFIX).await?;
    for entry in &progress_entries {
        if !entry.key.ends_with(".json") {
            continue;
        }
        let Some((body, _)) = store.get(&entry.key).await? else {
            continue;
        };
        let Ok(progress) = serde_json::from_slice::<ProgressRecord>(&body) else {
            continue;
        };
        let heartbeat_age_seconds = now
            .signed_duration_since(progress.heartbeat_utc.0)
            .num_seconds()
            .max(0) as u64;
        let stale_after_seconds =
            (progress.heartbeat_sec > 0).then(|| progress.heartbeat_sec.saturating_mul(2));
        let stale = stale_after_seconds.map(|threshold| heartbeat_age_seconds > threshold);

        if let Some(shard_name) = progress.current_shard.as_deref() {
            if states.get(shard_name) == Some(&ClaimState::Active) {
                if let Some(total) = shard_rows.get(shard_name) {
                    let done = progress.shard_rows_done.min(*total);
                    active_rows_by_shard
                        .entry(shard_name.to_string())
                        .and_modify(|current| *current = (*current).max(done))
                        .or_insert(done);
                }
            }
        }

        workers.push(WorkerLine {
            host_id: progress.host,
            status: progress.status,
            started_utc: progress.started_utc.0,
            heartbeat_utc: progress.heartbeat_utc.0,
            heartbeat_age_seconds,
            stale_after_seconds,
            stale,
            current_shard: progress.current_shard,
            rows_done: progress.shard_rows_done,
            rows_total: progress.shard_rows_total,
            files_ok: progress.files_ok,
            files_failed: progress.files_failed,
            files_fenced: progress.files_fenced,
            throughput_mb_s: progress.throughput_mb_s_1m,
        });
    }
    workers.sort_by(|a, b| a.host_id.cmp(&b.host_id));

    let rows_active = active_rows_by_shard.values().copied().sum::<u64>();
    let rows_progress = rows_completed.saturating_add(rows_active);
    let progress_percent = if manifest.total_rows == 0 {
        100.0
    } else {
        100.0 * rows_progress.min(manifest.total_rows) as f64 / manifest.total_rows as f64
    };
    let aggregate_throughput_mb_s = workers.iter().map(|worker| worker.throughput_mb_s).sum();
    let files_ok = workers.iter().map(|worker| worker.files_ok).sum();
    let files_failed = workers.iter().map(|worker| worker.files_failed).sum();
    let files_fenced = workers.iter().map(|worker| worker.files_fenced).sum();
    let process_rows_per_second = workers
        .iter()
        .filter_map(|worker| {
            let elapsed = now
                .signed_duration_since(worker.started_utc)
                .num_milliseconds();
            (elapsed > 0).then(|| worker.files_ok as f64 / (elapsed as f64 / 1000.0))
        })
        .sum::<f64>();
    let rows_per_second = (process_rows_per_second > 0.0).then_some(process_rows_per_second);
    let eta_seconds = rows_per_second.and_then(|rate| {
        (rows_progress < manifest.total_rows && failed == 0)
            .then(|| ((manifest.total_rows - rows_progress) as f64 / rate).ceil() as u64)
    });

    Ok(Snapshot {
        when: now,
        total_shards,
        total_rows: manifest.total_rows,
        completed,
        failed,
        in_progress,
        unclaimed,
        terminal: completed + failed == total_shards,
        rows_completed,
        rows_failed,
        rows_active,
        rows_progress,
        progress_percent,
        aggregate_throughput_mb_s,
        rows_per_second,
        eta_seconds,
        files_ok,
        files_failed,
        files_fenced,
        workers,
    })
}

fn render(snapshot: &Snapshot, interval: u64, watch: bool) {
    println!(
        "vamoose status @ {}\n",
        snapshot.when.format("%Y-%m-%dT%H:%M:%SZ")
    );
    println!(
        "Shards: {} total | {} completed | {} failed | {} in-progress | {} unclaimed",
        snapshot.total_shards,
        snapshot.completed,
        snapshot.failed,
        snapshot.in_progress,
        snapshot.unclaimed,
    );
    println!(
        "Rows:   {}/{} ({:.1}%) | active {} | failed-shard rows {}",
        snapshot.rows_progress,
        snapshot.total_rows,
        snapshot.progress_percent,
        snapshot.rows_active,
        snapshot.rows_failed,
    );
    let row_rate = snapshot
        .rows_per_second
        .map(|rate| format!("{rate:.0} rows/s"))
        .unwrap_or_else(|| "collecting".to_string());
    let eta = snapshot
        .eta_seconds
        .map(format_duration)
        .unwrap_or_else(|| "-".to_string());
    println!(
        "Rate:   {:.1} MB/s aggregate | {} | ETA {}",
        snapshot.aggregate_throughput_mb_s, row_rate, eta,
    );
    println!(
        "Files:  {} ok | {} failed | {} fenced\n",
        snapshot.files_ok, snapshot.files_failed, snapshot.files_fenced,
    );
    if snapshot.workers.is_empty() {
        println!(
            "Workers: (none — no progress objects under {})",
            layout::PROGRESS_PREFIX
        );
    } else {
        println!("Workers:");
        for worker in &snapshot.workers {
            let shard = worker.current_shard.as_deref().unwrap_or("-");
            let rows = if worker.rows_total > 0 {
                format!("{}/{}", worker.rows_done, worker.rows_total)
            } else if worker.rows_done > 0 {
                worker.rows_done.to_string()
            } else {
                "-".to_string()
            };
            let freshness = match worker.stale {
                Some(true) => format!("STALE {}s", worker.heartbeat_age_seconds),
                Some(false) => format!("age={}s", worker.heartbeat_age_seconds),
                None => format!("age={}s (?)", worker.heartbeat_age_seconds),
            };
            println!(
                "  {:<16} {:<9} {:<12} shard={:<20} rows={:<14} {:>7.1} MB/s fail={} fence={}",
                worker.host_id,
                worker.status,
                freshness,
                shard,
                rows,
                worker.throughput_mb_s,
                worker.files_failed,
                worker.files_fenced,
            );
        }
    }
    if snapshot.terminal {
        if snapshot.failed == 0 && snapshot.files_failed == 0 {
            println!("\nTerminal: all manifest shards completed");
        } else {
            println!("\nTerminal: migration ended with failures");
        }
    } else if watch {
        println!("\nNext refresh in {interval}s (Ctrl-C to stop)");
    }
}

fn format_duration(seconds: u64) -> String {
    let hours = seconds / 3600;
    let minutes = (seconds % 3600) / 60;
    let seconds = seconds % 60;
    if hours > 0 {
        format!("{hours}h {minutes:02}m")
    } else if minutes > 0 {
        format!("{minutes}m {seconds:02}s")
    } else {
        format!("{seconds}s")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn duration_rendering_is_compact() {
        assert_eq!(format_duration(9), "9s");
        assert_eq!(format_duration(69), "1m 09s");
        assert_eq!(format_duration(3_661), "1h 01m");
    }

    #[test]
    fn snapshot_json_exposes_terminal_health_fields() {
        let snapshot = Snapshot {
            when: chrono::Utc::now(),
            total_shards: 2,
            total_rows: 20,
            completed: 1,
            failed: 1,
            in_progress: 0,
            unclaimed: 0,
            terminal: true,
            rows_completed: 10,
            rows_failed: 10,
            rows_active: 0,
            rows_progress: 10,
            progress_percent: 50.0,
            aggregate_throughput_mb_s: 0.0,
            rows_per_second: None,
            eta_seconds: None,
            files_ok: 10,
            files_failed: 1,
            files_fenced: 2,
            workers: Vec::new(),
        };
        let json = serde_json::to_value(snapshot).unwrap();
        assert_eq!(json["failed"], 1);
        assert_eq!(json["files_fenced"], 2);
        assert_eq!(json["terminal"], true);
        assert!(json.get("eta_seconds").is_some());
    }
}
