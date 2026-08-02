//! Vamoose logging.
//!
//! Output routing is selected per subcommand via [`LogMode`] (F39,
//! COORD_PLAN §3.7):
//!
//! In [`LogMode::Standard`] (every subcommand except the TUI), two
//! layers always run:
//!   * stderr (`tracing_subscriber::fmt`) — preserves the dev/harness
//!     workflow where operators read logs as the worker prints them.
//!   * rotating file — active file is plain text so `tail -F` and
//!     `grep` work; archives are gzipped by `file_rotate` on rotation.
//!
//! In [`LogMode::TuiQuiet`] (`vamoose tui`), tracing-fmt output is
//! suppressed entirely — a single stderr line would corrupt the
//! ratatui alternate screen — and events route to a rotating file
//! only when the operator passed `--log-file`. The S3 uploader is
//! never spawned in this mode, regardless of `[logging].s3_upload`.
//!
//! When `[logging].s3_upload = true` (Standard mode only), a
//! background tokio task polls the log directory for `.gz` archives
//! and uploads them to
//! `s3://{bucket}/{s3_prefix}/{hostname}-{pid}/{startup_ts}/...`. The
//! task owns its own cancellation token (decoupled from the
//! orchestrator fence) so non-worker subcommands can use the same
//! logging path.
//!
//! Shutdown ordering: drop the non-blocking writer guard (drains the
//! event channel into the file) → flush the file → cancel the
//! uploader → wait with a deadline. The deadline bounds the async
//! uploader join; the guard drain and file flush happen synchronously
//! before that wait.

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
use tracing_subscriber::EnvFilter;

use crate::config::{Logging, StorageSettings};

type SharedRotator = Arc<Mutex<FileRotate<AppendCount>>>;

/// Output routing per subcommand (F39, COORD_PLAN §3.7). An enum
/// rather than a bool so the TUI variant can carry its `--log-file`.
#[derive(Debug, Clone)]
pub enum LogMode {
    /// stderr + rotating file + config-gated S3 uploader — every
    /// subcommand except the TUI.
    Standard,
    /// `vamoose tui`: no stderr layer, ever (a single fmt line
    /// corrupts the ratatui alternate screen), and never the S3
    /// uploader. Events route to a rotating file appender only when
    /// `--log-file` was passed; otherwise they are discarded.
    TuiQuiet { log_file: Option<PathBuf> },
}

/// Handle returned by [`init`]; hold it for the lifetime of the
/// process and call [`LoggingHandle::shutdown`] before exit so the
/// last batched events flush and the final archive uploads.
pub struct LoggingHandle {
    guard: Option<WorkerGuard>,
    writer: Option<SharedRotator>,
    uploader: Option<UploaderTask>,
}

struct UploaderTask {
    cancel: CancellationToken,
    handle: JoinHandle<()>,
}

/// Initialize tracing. Installs the global subscriber for `mode`,
/// opens the mode's rotating log file (if any), and — Standard mode
/// only — spawns the config-gated S3 uploader task.
///
/// `storage` is only consulted when the uploader actually spawns;
/// otherwise it is ignored.
pub fn init(
    filter: EnvFilter,
    logging: &Logging,
    storage: &StorageSettings,
    mode: &LogMode,
) -> Result<LoggingHandle> {
    let (dispatch, handle) = build(filter, logging, Some(storage), mode, std::io::stderr)?;
    tracing::dispatcher::set_global_default(dispatch)
        .map_err(|e| anyhow::anyhow!("install tracing subscriber: {e}"))?;
    Ok(handle)
}

/// Minimal-subscriber fallback for when the config failed to load.
///
/// Standard keeps today's behavior: a plain stderr fmt subscriber so
/// the eventual config error surfaces. TuiQuiet must not touch
/// stderr even here — it installs the quiet subscriber with default
/// rotation caps, honoring `--log-file` when given; with no file the
/// subscriber simply discards events.
pub fn init_fallback(filter: EnvFilter, mode: &LogMode) -> Option<LoggingHandle> {
    match mode {
        LogMode::Standard => {
            tracing_subscriber::fmt()
                .with_env_filter(filter)
                .try_init()
                .ok();
            None
        }
        LogMode::TuiQuiet { .. } => {
            // TuiQuiet cannot spawn the uploader. Failure to open
            // --log-file degrades to no subscriber at all — for the
            // TUI, silence beats a corrupted alternate screen.
            let (dispatch, handle) =
                build(filter, &Logging::default(), None, mode, std::io::stderr).ok()?;
            tracing::dispatcher::set_global_default(dispatch).ok()?;
            Some(handle)
        }
    }
}

/// Assemble the subscriber + handle for `mode` without installing
/// anything globally. `stderr_writer` is the injectable stderr seam:
/// production passes `std::io::stderr`, tests pass a capture buffer.
/// In [`LogMode::TuiQuiet`] no stderr layer is constructed at all —
/// quiet by construction, not by filtering.
fn build<W>(
    filter: EnvFilter,
    logging: &Logging,
    storage: Option<&StorageSettings>,
    mode: &LogMode,
    stderr_writer: W,
) -> Result<(tracing::Dispatch, LoggingHandle)>
where
    W: for<'a> MakeWriter<'a> + Send + Sync + 'static,
{
    // Which file (if any) this mode writes.
    let file_path: Option<&Path> = match mode {
        LogMode::Standard => Some(logging.path.as_path()),
        LogMode::TuiQuiet { log_file } => log_file.as_deref(),
    };

    let mut writer: Option<SharedRotator> = None;
    let mut guard: Option<WorkerGuard> = None;
    let mut file_nb: Option<NonBlocking> = None;
    if let Some(path) = file_path {
        // Parent dir must exist (or we must be able to create it).
        // Hard fail with a clear message — silent fallback to
        // stderr-only would hide a misconfigured operator setup.
        if let Some(parent) = path.parent() {
            if !parent.as_os_str().is_empty() {
                fs::create_dir_all(parent)
                    .with_context(|| format!("creating log directory {}", parent.display()))?;
            }
        }

        let max_bytes = parse_size(&logging.max_bytes)
            .with_context(|| format!("invalid [logging].max_bytes: {:?}", logging.max_bytes))?;

        let rotator = FileRotate::new(
            path,
            AppendCount::new(logging.max_archives),
            ContentLimit::Bytes(max_bytes as usize),
            // Compress every archive (0 plaintext archives kept).
            Compression::OnRotate(0),
            // Default file permissions.
            None,
        );
        let shared: SharedRotator = Arc::new(Mutex::new(rotator));

        // `non_blocking` runs writes on a dedicated thread so log
        // emission can't stall the worker on disk I/O. Lossy by
        // default — if the channel fills (slow disk, full disk),
        // events are dropped rather than blocking the publisher.
        let (nb, g) = tracing_appender::non_blocking(SharedWriter(shared.clone()));
        writer = Some(shared);
        guard = Some(g);
        file_nb = Some(nb);
    }

    let stderr_layer = match mode {
        LogMode::Standard => Some(tracing_subscriber::fmt::layer().with_writer(stderr_writer)),
        LogMode::TuiQuiet { .. } => None,
    };
    let file_layer = file_nb.map(|nb| {
        tracing_subscriber::fmt::layer()
            .with_ansi(false)
            .with_writer(NbMaker(nb))
    });
    let subscriber = tracing_subscriber::registry()
        .with(filter)
        .with(stderr_layer)
        .with(file_layer);
    let dispatch = tracing::Dispatch::new(subscriber);

    // The uploader is gated on the MODE first, config second: the
    // TUI must never spawn it no matter what the config says (F39) —
    // the machinery itself stays intact for Standard mode.
    let uploader = match mode {
        LogMode::Standard if logging.s3_upload => {
            let storage = storage.ok_or_else(|| {
                anyhow::anyhow!("standard logging initialization requires storage settings")
            })?;
            Some(spawn_uploader(logging, storage)?)
        }
        _ => None,
    };

    Ok((
        dispatch,
        LoggingHandle {
            guard,
            writer,
            uploader,
        },
    ))
}

impl LoggingHandle {
    /// Whether the S3 uploader task was spawned. Test seam for the
    /// F39 mode gate (nothing on the production path consults it —
    /// hence the allow — but it's the honest probe for "did init
    /// start an uploader" should a subcommand ever need it).
    #[cfg_attr(not(test), allow(dead_code))]
    pub fn uploader_running(&self) -> bool {
        self.uploader.is_some()
    }

    /// Flush pending events, finalize the active log, and drain the
    /// uploader task. `deadline` bounds only the async uploader join;
    /// the preceding guard drain and file flush are synchronous.
    pub async fn shutdown(mut self, deadline: Duration) {
        // 1. Drop the non-blocking guard: drains the channel into the
        //    file synchronously.
        self.guard.take();

        // 2. Flush the file so any buffered bytes hit disk before the
        //    uploader's final pass.
        if let Some(w) = &self.writer {
            if let Ok(mut w) = w.lock() {
                let _ = w.flush();
            }
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
        if let Some(w) = &self.writer {
            if let Ok(mut w) = w.lock() {
                let _ = w.flush();
            }
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

// ---------- uploader ---------------------------------------------------

fn spawn_uploader(logging: &Logging, storage: &StorageSettings) -> Result<UploaderTask> {
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
    let endpoint = storage.endpoint.clone();
    let region = storage.region.clone();
    let profile = storage.profile.clone();
    let verify_tls = storage.verify_tls;
    let bucket = storage.bucket.clone();
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{Logging, StorageSettings};
    use std::sync::{Arc, Mutex};

    /// Injectable stderr seam: captures everything the subscriber's
    /// stderr layer writes so the F39 tests can assert on it.
    #[derive(Clone, Default)]
    struct CaptureWriter(Arc<Mutex<Vec<u8>>>);

    impl CaptureWriter {
        fn contents(&self) -> Vec<u8> {
            self.0.lock().unwrap().clone()
        }
    }

    impl io::Write for CaptureWriter {
        fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
            self.0.lock().unwrap().extend_from_slice(buf);
            Ok(buf.len())
        }
        fn flush(&mut self) -> io::Result<()> {
            Ok(())
        }
    }

    impl<'a> MakeWriter<'a> for CaptureWriter {
        type Writer = CaptureWriter;
        fn make_writer(&'a self) -> Self::Writer {
            self.clone()
        }
    }

    fn storage_stub() -> StorageSettings {
        StorageSettings {
            bucket: "bucket".into(),
            endpoint: "http://127.0.0.1:1".into(),
            region: "us-east-1".into(),
            profile: None,
            verify_tls: true,
        }
    }

    fn logging_at(dir: &Path, s3_upload: bool) -> Logging {
        Logging {
            path: dir.join("vamoose.log"),
            max_bytes: "1 MiB".into(),
            max_archives: 2,
            s3_upload,
            s3_prefix: "logs".into(),
            poll_secs: 3600,
        }
    }

    /// F39 acceptance 9: in TuiQuiet mode the subscriber has no
    /// stderr layer at all — one log line from any dependency would
    /// corrupt the ratatui alternate screen (COORD_PLAN §3.7). The
    /// mode makes stderr impossible by construction; this smoke test
    /// pins it through the injected-writer seam.
    #[test]
    fn tui_subscriber_has_no_stderr_layer() {
        let stderr = CaptureWriter::default();
        let (dispatch, _handle) = build(
            EnvFilter::new("info"),
            &Logging::default(),
            None,
            &LogMode::TuiQuiet { log_file: None },
            stderr.clone(),
        )
        .expect("build");
        tracing::dispatcher::with_default(&dispatch, || {
            tracing::error!("this line would corrupt the alternate screen");
        });
        assert!(
            stderr.contents().is_empty(),
            "TuiQuiet must never write to stderr; got: {:?}",
            String::from_utf8_lossy(&stderr.contents())
        );
    }

    /// F39 acceptance 10: `--log-file` routes TUI events into the
    /// rotating-appender machinery — and still nothing on stderr.
    #[test]
    fn tui_log_file_flag_routes_to_file() {
        let dir = tempfile::tempdir().unwrap();
        let log_path = dir.path().join("tui.log");
        let stderr = CaptureWriter::default();
        let (dispatch, handle) = build(
            EnvFilter::new("info"),
            &logging_at(dir.path(), false),
            None,
            &LogMode::TuiQuiet {
                log_file: Some(log_path.clone()),
            },
            stderr.clone(),
        )
        .expect("build");
        tracing::dispatcher::with_default(&dispatch, || {
            tracing::info!("f39-file-routed");
        });
        // Dropping the handle drains the non-blocking channel and
        // flushes the rotator.
        drop(handle);
        let body = std::fs::read_to_string(&log_path).expect("log file exists");
        assert!(
            body.contains("f39-file-routed"),
            "event lands in file: {body:?}"
        );
        assert!(stderr.contents().is_empty(), "still nothing on stderr");
    }

    /// F39 acceptance 11: `[logging].s3_upload = true` must NOT
    /// start the uploader in TUI mode — the uploader is config-
    /// driven, and before F39 `vamoose tui` silently spawned it.
    #[tokio::test]
    async fn tui_never_starts_s3_uploader() {
        let dir = tempfile::tempdir().unwrap();
        let stderr = CaptureWriter::default();
        let (_dispatch, handle) = build(
            EnvFilter::new("info"),
            &logging_at(dir.path(), true),
            Some(&storage_stub()),
            &LogMode::TuiQuiet {
                log_file: Some(dir.path().join("tui.log")),
            },
            stderr,
        )
        .expect("build");
        assert!(
            !handle.uploader_running(),
            "vamoose tui must never spawn the S3 log uploader"
        );
    }

    /// F39 acceptance 12 (regression): Standard mode keeps stderr +
    /// file + the configured uploader exactly as today.
    #[tokio::test]
    async fn worker_logging_unchanged() {
        let dir = tempfile::tempdir().unwrap();
        let logging = logging_at(dir.path(), true);
        let stderr = CaptureWriter::default();
        let (dispatch, handle) = build(
            EnvFilter::new("info"),
            &logging,
            Some(&storage_stub()),
            &LogMode::Standard,
            stderr.clone(),
        )
        .expect("build");
        assert!(
            handle.uploader_running(),
            "Standard mode keeps the config-driven uploader"
        );
        tracing::dispatcher::with_default(&dispatch, || {
            tracing::info!("f39-standard-both");
        });
        drop(handle);
        let err = String::from_utf8_lossy(&stderr.contents()).to_string();
        assert!(
            err.contains("f39-standard-both"),
            "stderr layer intact: {err:?}"
        );
        let body = std::fs::read_to_string(&logging.path).expect("log file");
        assert!(
            body.contains("f39-standard-both"),
            "file layer intact: {body:?}"
        );
    }

    /// F31: table test over the size strings `[logging].max_bytes`
    /// accepts (and the ones it must refuse).
    #[test]
    fn parse_size_table() {
        // Accepted.
        assert_eq!(parse_size("50 MiB"), Some(50 << 20));
        assert_eq!(parse_size("1 GiB"), Some(1 << 30));
        assert_eq!(parse_size("2 KiB"), Some(2 << 10));
        assert_eq!(parse_size("10 KB"), Some(10_000));
        assert_eq!(parse_size("5 MB"), Some(5_000_000));
        assert_eq!(parse_size("2 GB"), Some(2_000_000_000));
        assert_eq!(parse_size("1024"), Some(1024), "bare number is bytes");
        assert_eq!(parse_size("512 B"), Some(512));
        assert_eq!(
            parse_size("  50 MiB  "),
            Some(50 << 20),
            "whitespace trimmed"
        );
        assert_eq!(parse_size("50MiB"), Some(50 << 20), "no space required");

        // Refused.
        assert_eq!(parse_size("garbage"), None);
        assert_eq!(parse_size(""), None);
        assert_eq!(parse_size("MiB"), None, "unit without a number");
        assert_eq!(parse_size("-1 MiB"), None, "negative");
        assert_eq!(parse_size("1.5 GiB"), None, "fractional not supported");
        assert_eq!(parse_size("50 XiB"), None, "unknown unit");
        assert_eq!(
            parse_size("18446744073709551615 GiB"),
            None,
            "overflow is caught, not wrapped",
        );
    }
}
