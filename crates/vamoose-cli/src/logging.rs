//! Vamoose worker logging.
//!
//! Two layers always run:
//!   * stderr (`tracing_subscriber::fmt`) — preserves the dev/harness
//!     workflow where operators read logs as the worker prints them.
//!   * rotating file — active file is plain text so `tail -F` and
//!     `grep` work; archives are gzipped by `file_rotate` on rotation.
//!
//! When `[logging].s3_upload = true`, a background tokio task polls
//! the log directory for `.gz` archives and uploads them to
//! `s3://{bucket}/{s3_prefix}/{hostname}-{pid}/{startup_ts}/...`. The
//! task owns its own cancellation token (decoupled from the
//! orchestrator fence) so non-worker subcommands can use the same
//! logging path.
//!
//! Shutdown ordering: drop the non-blocking writer guard (drains the
//! event channel into the file) → flush the file → cancel the
//! uploader → wait with a deadline. If anything stalls, the deadline
//! makes sure the binary exits anyway.

use anyhow::{Context, Result};
use file_rotate::{compression::Compression, suffix::AppendCount, ContentLimit, FileRotate};
use std::collections::HashSet;
use std::fs;
use std::io::{self, Read, Write};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;
use tracing_appender::non_blocking::{NonBlocking, WorkerGuard};
use tracing_subscriber::fmt::MakeWriter;
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;
use tracing_subscriber::EnvFilter;

use crate::config::{Logging, S3};

type SharedRotator = Arc<Mutex<FileRotate<AppendCount>>>;

/// Handle returned by [`init`]; hold it for the lifetime of the
/// process and call [`LoggingHandle::shutdown`] before exit so the
/// last batched events flush and the final archive uploads.
pub struct LoggingHandle {
    guard: Option<WorkerGuard>,
    writer: SharedRotator,
    uploader: Option<UploaderTask>,
}

struct UploaderTask {
    cancel: CancellationToken,
    handle: JoinHandle<()>,
}

/// Initialize tracing. Installs the global subscriber, opens the
/// rotating log file, and (optionally) spawns the S3 uploader task.
///
/// `s3_cfg` and `bucket` are only consulted when `logging.s3_upload`
/// is true; otherwise they're ignored.
pub fn init(
    filter: EnvFilter,
    logging: &Logging,
    s3_cfg: &S3,
    bucket: &str,
) -> Result<LoggingHandle> {
    // Parent dir must exist (or we must be able to create it). Hard
    // fail with a clear message — silent fallback to stderr-only would
    // hide a misconfigured operator setup.
    if let Some(parent) = logging.path.parent() {
        if !parent.as_os_str().is_empty() {
            fs::create_dir_all(parent)
                .with_context(|| format!("creating log directory {}", parent.display()))?;
        }
    }

    let max_bytes = parse_size(&logging.max_bytes)
        .with_context(|| format!("invalid [logging].max_bytes: {:?}", logging.max_bytes))?;

    let rotator = FileRotate::new(
        &logging.path,
        AppendCount::new(logging.max_archives),
        ContentLimit::Bytes(max_bytes as usize),
        // Compress every archive (0 plaintext archives kept).
        Compression::OnRotate(0),
        // Default file permissions.
        None,
    );
    let writer: SharedRotator = Arc::new(Mutex::new(rotator));

    // `non_blocking` runs writes on a dedicated thread so log emission
    // can't stall the worker on disk I/O. Lossy by default — if the
    // channel fills (slow disk, full disk), events are dropped rather
    // than blocking the publisher.
    let (nb, guard) = tracing_appender::non_blocking(SharedWriter(writer.clone()));

    install_subscriber(filter, nb)?;

    let uploader = if logging.s3_upload {
        Some(spawn_uploader(logging, s3_cfg, bucket)?)
    } else {
        None
    };

    Ok(LoggingHandle {
        guard: Some(guard),
        writer,
        uploader,
    })
}

impl LoggingHandle {
    /// Flush pending events, finalize the active log, and drain the
    /// uploader task. The whole sequence is bounded by `deadline`.
    pub async fn shutdown(mut self, deadline: Duration) {
        // 1. Drop the non-blocking guard: drains the channel into the
        //    file synchronously.
        self.guard.take();

        // 2. Flush the file so any buffered bytes hit disk before the
        //    uploader's final pass.
        if let Ok(mut w) = self.writer.lock() {
            let _ = w.flush();
        }

        // 3. Drain uploader. It runs one final pass when cancelled
        //    (picks up any new archives and uploads the active tail
        //    as `final.log.gz`).
        if let Some(task) = self.uploader.take() {
            task.cancel.cancel();
            let _ = tokio::time::timeout(deadline, task.handle).await;
        }
    }
}

impl Drop for LoggingHandle {
    fn drop(&mut self) {
        // Best-effort flush for the abnormal-exit path. We can't await
        // the uploader here; that's what shutdown() is for.
        self.guard.take();
        if let Ok(mut w) = self.writer.lock() {
            let _ = w.flush();
        }
        if let Some(task) = self.uploader.take() {
            task.cancel.cancel();
            task.handle.abort();
        }
    }
}

/// Wrapper that implements `Write` over `Arc<Mutex<FileRotate>>` so
/// the rotator can be both owned by `non_blocking` (which consumes
/// its writer) and retained by us for shutdown flush.
#[derive(Clone)]
struct SharedWriter(SharedRotator);

impl Write for SharedWriter {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        match self.0.lock() {
            Ok(mut w) => w.write(buf),
            Err(_) => Err(io::Error::other("log writer poisoned")),
        }
    }
    fn flush(&mut self) -> io::Result<()> {
        match self.0.lock() {
            Ok(mut w) => w.flush(),
            Err(_) => Err(io::Error::other("log writer poisoned")),
        }
    }
}

/// `MakeWriter` is what `fmt::Layer::with_writer` expects. We give it
/// the `NonBlocking` writer, which is `Clone + Write + 'static`.
struct NbMaker(NonBlocking);
impl<'a> MakeWriter<'a> for NbMaker {
    type Writer = NonBlocking;
    fn make_writer(&'a self) -> Self::Writer {
        self.0.clone()
    }
}

fn install_subscriber(filter: EnvFilter, file_writer: NonBlocking) -> Result<()> {
    let stderr_layer = tracing_subscriber::fmt::layer().with_writer(std::io::stderr);
    let file_layer = tracing_subscriber::fmt::layer()
        .with_ansi(false)
        .with_writer(NbMaker(file_writer));

    tracing_subscriber::registry()
        .with(filter)
        .with(stderr_layer)
        .with(file_layer)
        .try_init()
        .map_err(|e| anyhow::anyhow!("install tracing subscriber: {e}"))?;
    Ok(())
}

// ---------- uploader ---------------------------------------------------

fn spawn_uploader(logging: &Logging, s3_cfg: &S3, bucket: &str) -> Result<UploaderTask> {
    let cancel = CancellationToken::new();
    let cancel_child = cancel.clone();

    let log_dir: PathBuf = logging
        .path
        .parent()
        .filter(|p| !p.as_os_str().is_empty())
        .map(Path::to_path_buf)
        .unwrap_or_else(|| PathBuf::from("."));
    let active_path = logging.path.clone();
    let active_filename = logging
        .path
        .file_name()
        .map(|s| s.to_string_lossy().into_owned())
        .unwrap_or_else(|| "vamoose.log".to_string());

    let prefix_root = build_prefix(&logging.s3_prefix);

    // Endpoint/profile/region snapshot for the uploader's own S3
    // client. Building a second client here (the orchestrator builds
    // its own at orchestrator::run) is intentional: the uploader runs
    // for every subcommand, not just `worker`.
    let endpoint = s3_cfg.endpoint.clone();
    let region = s3_cfg.region.clone();
    let profile = s3_cfg.profile.clone();
    let verify_tls = !s3_cfg.no_verify_ssl.unwrap_or(false);
    let bucket = bucket.to_string();
    let poll = Duration::from_secs(logging.poll_secs.max(1));

    let handle = tokio::spawn(async move {
        let s3 = match migration_core::s3::S3Client::from_config(
            &endpoint,
            &region,
            &bucket,
            profile.as_deref(),
            verify_tls,
        )
        .await
        {
            Ok(c) => c,
            Err(e) => {
                let _ = writeln!(
                    io::stderr(),
                    "vamoose log uploader disabled: S3 init failed: {e}"
                );
                // Wait for cancellation so shutdown still joins quickly.
                cancel_child.cancelled().await;
                return;
            }
        };

        let mut seen: HashSet<PathBuf> = HashSet::new();

        loop {
            scan_and_upload(&log_dir, &active_filename, &prefix_root, &s3, &mut seen).await;
            tokio::select! {
                _ = tokio::time::sleep(poll) => {}
                _ = cancel_child.cancelled() => break,
            }
        }

        // Final pass on shutdown: pick up any archives produced
        // between the last poll and cancellation, then upload the
        // active tail as `final.log.gz` so the run's last bytes land
        // in S3 even though they were never rotated.
        scan_and_upload(&log_dir, &active_filename, &prefix_root, &s3, &mut seen).await;
        upload_final_active(&active_path, &prefix_root, &s3).await;
    });

    Ok(UploaderTask { cancel, handle })
}

async fn scan_and_upload(
    log_dir: &Path,
    active_filename: &str,
    prefix_root: &str,
    s3: &migration_core::s3::S3Client,
    seen: &mut HashSet<PathBuf>,
) {
    let entries = match fs::read_dir(log_dir) {
        Ok(e) => e,
        Err(_) => return,
    };
    let mut archives: Vec<PathBuf> = entries
        .filter_map(|e| e.ok())
        .map(|e| e.path())
        .filter(|p| is_archive_for(p, active_filename))
        .filter(|p| !seen.contains(p))
        .collect();
    // Upload in name order so older sequence numbers go first.
    archives.sort();

    for path in archives {
        match fs::read(&path) {
            Ok(body) => {
                let key = format!(
                    "{prefix_root}/{}",
                    path.file_name()
                        .map(|s| s.to_string_lossy().into_owned())
                        .unwrap_or_else(|| "unknown.log.gz".to_string()),
                );
                match s3.put(&key, body).await {
                    Ok(_) => {
                        seen.insert(path);
                    }
                    Err(e) => {
                        // Leave the file on disk; we'll retry on the
                        // next poll. file-rotate's max_archives cap
                        // bounds disk usage during outages.
                        let _ = writeln!(
                            io::stderr(),
                            "vamoose log uploader: upload of {} failed: {e}",
                            path.display()
                        );
                    }
                }
            }
            Err(e) => {
                let _ = writeln!(
                    io::stderr(),
                    "vamoose log uploader: read {} failed: {e}",
                    path.display()
                );
            }
        }
    }
}

async fn upload_final_active(
    active_path: &Path,
    prefix_root: &str,
    s3: &migration_core::s3::S3Client,
) {
    // Read the active (uncompressed) log and gzip it in memory.
    let plain = match fs::File::open(active_path) {
        Ok(mut f) => {
            let mut buf = Vec::new();
            if f.read_to_end(&mut buf).is_err() || buf.is_empty() {
                return;
            }
            buf
        }
        Err(_) => return,
    };
    // file-rotate uses miniz_oxide internally; reuse it transitively
    // via flate2 would mean a new workspace dep. Keep it simple: gzip
    // manually with `miniz_oxide::deflate` is awkward, so the final
    // upload ships plaintext under `.log` instead of `.log.gz`. It's
    // a small file (whatever didn't trigger the last rotation, bounded
    // by max_bytes) and gunzip is unnecessary to read it.
    let key = format!("{prefix_root}/final.log");
    if let Err(e) = s3.put(&key, plain).await {
        let _ = writeln!(
            io::stderr(),
            "vamoose log uploader: final upload failed: {e}"
        );
    }
}

fn is_archive_for(path: &Path, active_filename: &str) -> bool {
    let name = match path.file_name().and_then(|s| s.to_str()) {
        Some(n) => n,
        None => return false,
    };
    // file-rotate AppendCount names archives as <basename>.<n>(.gz),
    // e.g. "vamoose.log.1.gz". Match anything that starts with the
    // active filename and ends in .gz.
    name != active_filename && name.starts_with(active_filename) && name.ends_with(".gz")
}

fn build_prefix(configured: &str) -> String {
    let host = hostname::get()
        .ok()
        .and_then(|s| s.into_string().ok())
        .unwrap_or_else(|| "unknown-host".to_string());
    let pid = std::process::id();
    let ts = chrono::Utc::now().format("%Y-%m-%dT%H-%M-%SZ");
    format!("{}/{host}-{pid}/{ts}", configured.trim_matches('/'),)
}

/// Tiny size-string parser matching the one in
/// `migration_worker::orchestrator::parse_size` (kept private there).
/// Inlining a 20-line helper is cheaper than promoting a new public
/// API across two crates for one call site.
fn parse_size(s: &str) -> Option<u64> {
    let s = s.trim();
    let (num, unit) = match s.find(|c: char| c.is_alphabetic()) {
        Some(i) => (s[..i].trim(), s[i..].trim()),
        None => (s, ""),
    };
    let n: u64 = num.parse().ok()?;
    let mult: u64 = match unit {
        "" | "B" => 1,
        "KB" => 1_000,
        "MB" => 1_000_000,
        "GB" => 1_000_000_000,
        "KiB" => 1 << 10,
        "MiB" => 1 << 20,
        "GiB" => 1 << 30,
        _ => return None,
    };
    n.checked_mul(mult)
}
