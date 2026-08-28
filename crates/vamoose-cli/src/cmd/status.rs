//! `vamoose status` — read manifest + claims + per-host progress
//! from S3 and render a one-shot or continuously-refreshed summary.
//!
//! All S3 reads go through the same `S3Client` the worker uses, so
//! authentication and TLS behavior match what the operator has
//! already verified via `vamoose doctor`.

use crate::config::Config;
use clap::Args as ClapArgs;
use migration_coord::schema::{PrepareIndex, PreparePhase, PrepareProgress, PrepareScan};
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
        .await?
        .with_prefix(&storage.prefix),
    );
    let store: Arc<dyn ClaimStore> = s3.clone();
    let mut previous: Option<(Instant, u64)> = None;

    loop {
        let sample_time = Instant::now();
        // Before the manifest exists there is no run to report on;
        // show what `vamoose prepare` says about itself instead.
        if store.get(layout::MANIFEST_KEY).await?.is_none() {
            let prepare = store
                .get(layout::PREPARE_PROGRESS_KEY)
                .await?
                .map(|(body, _)| serde_json::from_slice::<PrepareProgress>(&body))
                .transpose()?;
            if args.json {
                println!(
                    "{}",
                    serde_json::to_string_pretty(&serde_json::json!({
                        "when": chrono::Utc::now(),
                        "manifest": false,
                        "prepare": prepare,
                    }))?
                );
            } else {
                if args.watch {
                    print!("\x1b[2J\x1b[H");
                }
                println!("{}", render_prepare(prepare.as_ref(), chrono::Utc::now()));
            }
            if !args.watch
                || prepare
                    .as_ref()
                    .is_some_and(|p| p.phase == PreparePhase::Failed)
            {
                break;
            }
            tokio::time::sleep(Duration::from_secs(args.interval)).await;
            continue;
        }
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
    /// The worker wrote a terminal status (`exiting` / `fenced`) on
    /// its way out: the process is gone on purpose, its row is
    /// history, and a stale heartbeat is expected rather than a
    /// problem. See [`is_terminal_status`].
    exited: bool,
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
        let exited = is_terminal_status(&progress.status);
        // An exited worker's heartbeat is old by definition; only a
        // worker that should still be writing can be stale.
        let stale = if exited {
            Some(false)
        } else {
            stale_after_seconds.map(|threshold| heartbeat_age_seconds > threshold)
        };

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
            exited,
            current_shard: progress.current_shard,
            rows_done: progress.shard_rows_done,
            rows_total: progress.shard_rows_total,
            files_ok: progress.files_ok,
            files_failed: progress.files_failed,
            files_fenced: progress.files_fenced,
            throughput_mb_s: progress.throughput_mb_s_1m,
        });
    }
    // Live workers first, exited ones (old instances of restarted
    // hosts, finished workers) at the bottom.
    workers.sort_by(|a, b| a.exited.cmp(&b.exited).then(a.host_id.cmp(&b.host_id)));

    let rows_active = active_rows_by_shard.values().copied().sum::<u64>();
    let rows_progress = rows_completed.saturating_add(rows_active);
    let progress_percent = if manifest.total_rows == 0 {
        100.0
    } else {
        100.0 * rows_progress.min(manifest.total_rows) as f64 / manifest.total_rows as f64
    };
    let aggregate_throughput_mb_s = workers
        .iter()
        .filter(|worker| !worker.exited)
        .map(|worker| worker.throughput_mb_s)
        .sum();
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

/// The pre-manifest report: where `prepare` is, or that nothing has
/// started.
fn render_prepare(prepare: Option<&PrepareProgress>, now: chrono::DateTime<chrono::Utc>) -> String {
    let stamp = now.format("%Y-%m-%dT%H:%M:%SZ");
    let Some(p) = prepare else {
        return format!(
            "vamoose status @ {stamp}\n\nNo run yet: the bucket has no manifest.json and no prepare \
             in progress.\nStart one with `sudo vamoose prepare` on any host."
        );
    };
    let age = (now - p.updated_utc).num_seconds().max(0);
    let since_start = (now - p.started_utc).num_seconds().max(0);
    let stale = if age > 60 && !matches!(p.phase, PreparePhase::Done | PreparePhase::Failed) {
        format!(
            " — NO UPDATE FOR {} (prepare may have died; re-run it to resume)",
            hms(age as u64)
        )
    } else {
        String::new()
    };
    let mut out = format!(
        "vamoose status @ {stamp}\n\nPreparing {} on {} (pid {}) — {} elapsed, updated {}s ago{stale}\n  {}  ->  {}\n\n",
        p.run_id,
        p.host,
        p.pid,
        hms(since_start as u64),
        age,
        p.source,
        p.dest
    );
    for line in prepare_steps(p) {
        out.push_str("  ");
        out.push_str(&line);
        out.push('\n');
    }
    if let Some(msg) = &p.message {
        out.push_str(&format!("\n  {msg}\n"));
    }
    if p.phase == PreparePhase::Done {
        out.push_str(
            "\nmanifest.json is published; workers are claiming. Run again for the run's status.\n",
        );
    }
    out
}

/// One line per stage, marked done / running / pending, shared by
/// `status` and the TUI's wording.
fn prepare_steps(p: &PrepareProgress) -> Vec<String> {
    let mark = |stage: PreparePhase| -> &'static str {
        let order = |ph: PreparePhase| match ph {
            PreparePhase::Scan => 0,
            PreparePhase::Index => 1,
            PreparePhase::Publish => 2,
            PreparePhase::Done => 3,
            PreparePhase::Failed => 3,
        };
        match (order(stage), order(p.phase)) {
            (s, c) if s < c => "[done]",
            (s, c) if s == c && p.phase == PreparePhase::Failed => "[FAILED]",
            (s, c) if s == c => "[ .. ]",
            _ => "[    ]",
        }
    };
    let PrepareScan {
        files,
        dirs,
        errors,
        rate_per_sec,
        elapsed_secs,
        ..
    } = p.scan;
    let PrepareIndex {
        shards_total,
        shards_rewritten,
        shards_uploaded,
        rows_uploaded,
        bytes_uploaded,
    } = p.index;
    let total = shards_total.map_or("?".to_string(), |n| n.to_string());
    vec![
        format!(
            "{} 1. scan     {} files, {} dirs, {} errors, {} ({}/s)",
            mark(PreparePhase::Scan),
            group(files),
            group(dirs),
            errors,
            hms(elapsed_secs),
            group(rate_per_sec)
        ),
        format!(
            "{} 2. index    {shards_rewritten}/{total} shards rewritten, {shards_uploaded} uploaded ({} rows, {:.1} GB)",
            mark(PreparePhase::Index),
            group(rows_uploaded),
            bytes_uploaded as f64 / 1e9
        ),
        format!(
            "{} 3. publish  manifest.json (workers start claiming the moment it lands)",
            mark(PreparePhase::Publish)
        ),
    ]
}

fn hms(secs: u64) -> String {
    let (h, m, s) = (secs / 3600, secs % 3600 / 60, secs % 60);
    if h > 0 {
        format!("{h}h{m:02}m")
    } else if m > 0 {
        format!("{m}m{s:02}s")
    } else {
        format!("{s}s")
    }
}

/// `1234567` → `1,234,567`.
fn group(n: u64) -> String {
    let digits = n.to_string();
    let mut out = String::with_capacity(digits.len() + digits.len() / 3);
    for (i, c) in digits.chars().enumerate() {
        if i > 0 && (digits.len() - i).is_multiple_of(3) {
            out.push(',');
        }
        out.push(c);
    }
    out
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
        let exited = snapshot.workers.iter().filter(|w| w.exited).count();
        println!(
            "Workers: {} live | {} exited",
            snapshot.workers.len() - exited,
            exited
        );
        for worker in &snapshot.workers {
            let shard = worker.current_shard.as_deref().unwrap_or("-");
            let rows = if worker.rows_total > 0 {
                format!("{}/{}", worker.rows_done, worker.rows_total)
            } else if worker.rows_done > 0 {
                worker.rows_done.to_string()
            } else {
                "-".to_string()
            };
            let freshness = freshness_label(worker);
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

/// Progress statuses a worker writes on its way out (see
/// `migration_worker::heartbeat`): `exiting` on an orderly exit,
/// `fenced` after a self-fence. Anything else means the worker was
/// still running when it last wrote, so a stale heartbeat is real.
fn is_terminal_status(status: &str) -> bool {
    matches!(status, "exiting" | "exited" | "fenced")
}

/// The freshness column: how long ago the worker last wrote, and
/// whether that is a problem. An exited worker's age is shown as
/// history ("exited 12m ago"), never as STALE — the old instance of a
/// restarted host would otherwise read as trouble for the rest of the
/// run (rig smoke 2026-08-25, HANDOFF.md finding #10).
fn freshness_label(worker: &WorkerLine) -> String {
    let age = worker.heartbeat_age_seconds;
    if worker.exited {
        let what = if worker.status == "fenced" {
            "fenced"
        } else {
            "exited"
        };
        return format!("{what} {} ago", format_duration(age));
    }
    match worker.stale {
        Some(true) => format!("STALE {age}s"),
        Some(false) => format!("age={age}s"),
        None => format!("age={age}s (?)"),
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

    fn worker(status: &str, age: u64, stale: Option<bool>) -> WorkerLine {
        let now = chrono::Utc::now();
        WorkerLine {
            host_id: format!("host-{status}"),
            status: status.to_string(),
            started_utc: now,
            heartbeat_utc: now,
            heartbeat_age_seconds: age,
            stale_after_seconds: Some(60),
            stale,
            exited: is_terminal_status(status),
            current_shard: None,
            rows_done: 0,
            rows_total: 0,
            files_ok: 0,
            files_failed: 0,
            files_fenced: 0,
            throughput_mb_s: 0.0,
        }
    }

    /// Only the statuses the worker writes on its way out are
    #[test]
    fn prepare_report_marks_stages_and_flags_a_stale_object() {
        let t0 = chrono::DateTime::parse_from_rfc3339("2026-08-26T00:02:17Z")
            .unwrap()
            .with_timezone(&chrono::Utc);
        let p = PrepareProgress {
            schema_version: 1,
            run_id: "run-20260826T000217Z".into(),
            host: "k8s-se-3".into(),
            pid: 1,
            source: "nfs://s/source".into(),
            dest: "nfs://d/destination/v3".into(),
            phase: PreparePhase::Index,
            started_utc: t0,
            updated_utc: t0 + chrono::Duration::seconds(3000),
            scan: PrepareScan {
                files: 603_266_804,
                dirs: 4_208_101,
                errors: 0,
                rate_per_sec: 213_000,
                elapsed_secs: 2829,
                complete: true,
            },
            index: PrepareIndex {
                shards_total: Some(320),
                shards_rewritten: 129,
                shards_uploaded: 128,
                rows_uploaded: 246_528_665,
                bytes_uploaded: 12_400_000_000,
            },
            message: None,
        };
        let text = render_prepare(Some(&p), t0 + chrono::Duration::seconds(3005));
        assert!(text.contains("[done] 1. scan     603,266,804 files, 4,208,101 dirs, 0 errors, 47m09s (213,000/s)"), "{text}");
        assert!(text.contains("[ .. ] 2. index    129/320 shards rewritten, 128 uploaded (246,528,665 rows, 12.4 GB)"), "{text}");
        assert!(text.contains("[    ] 3. publish"), "{text}");
        assert!(text.contains("updated 5s ago"), "{text}");
        assert!(!text.contains("NO UPDATE"), "{text}");
        // Nothing written for two minutes: say so.
        let stale = render_prepare(Some(&p), t0 + chrono::Duration::seconds(3120));
        assert!(stale.contains("NO UPDATE FOR 2m00s"), "{stale}");
        // No object at all.
        let none = render_prepare(None, t0);
        assert!(none.contains("No run yet"), "{none}");
        assert_eq!(group(0), "0");
        assert_eq!(group(999), "999");
        assert_eq!(group(1_000), "1,000");
        assert_eq!(hms(3661), "1h01m");
    }

    /// terminal; a worker that was `active` or `degraded` when it
    /// last wrote may really be gone and must still read as STALE.
    #[test]
    fn terminal_statuses_are_the_exit_ones() {
        for status in ["exiting", "exited", "fenced"] {
            assert!(is_terminal_status(status), "{status}");
        }
        for status in [
            "starting",
            "active",
            "degraded:throughput_low:probe-pending",
            "",
        ] {
            assert!(!is_terminal_status(status), "{status}");
        }
    }

    /// An exited worker is history, not STALE, however old its
    /// heartbeat; a live worker keeps the STALE / age rendering.
    #[test]
    fn exited_workers_are_not_rendered_stale() {
        assert_eq!(
            freshness_label(&worker("exiting", 900, Some(false))),
            "exited 15m 00s ago"
        );
        assert_eq!(
            freshness_label(&worker("fenced", 61, Some(false))),
            "fenced 1m 01s ago"
        );
        assert_eq!(
            freshness_label(&worker("active", 61, Some(true))),
            "STALE 61s"
        );
        assert_eq!(freshness_label(&worker("active", 5, Some(false))), "age=5s");
        assert_eq!(freshness_label(&worker("active", 5, None)), "age=5s (?)");
    }

    #[test]
    fn worker_json_exposes_exited_flag() {
        let json = serde_json::to_value(worker("exiting", 120, Some(false))).unwrap();
        assert_eq!(json["exited"], true);
        assert_eq!(json["stale"], false);
        let json = serde_json::to_value(worker("active", 120, Some(true))).unwrap();
        assert_eq!(json["exited"], false);
        assert_eq!(json["stale"], true);
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
