use flate2::write::GzEncoder;
use flate2::Compression;
use migration_core::s3::S3Client;
use std::fs::{self, File, OpenOptions};
use std::io::{self, Write};
use std::path::{Path, PathBuf};
use std::sync::mpsc::{self, Receiver, Sender};
use std::sync::{Arc, Mutex};
use std::thread;
use tracing::field::{Field, Visit};
use tracing::{Event, Level, Subscriber};
use tracing_subscriber::filter::EnvFilter;
use tracing_subscriber::layer::{Context, SubscriberExt};
use tracing_subscriber::registry::LookupSpan;
use tracing_subscriber::util::SubscriberInitExt;
use tracing_subscriber::Layer;

const DEFAULT_LOG_PATH: &str = "/var/log/vamoose.log.gz";
const DEFAULT_MAX_BYTES: u64 = 50 * 1024 * 1024;
const DEFAULT_MAX_ARCHIVES: usize = 10;
const DEFAULT_BUCKET_KEY_PREFIX: &str = "logs-migration-system";
const DEFAULT_BASENAME: &str = "vamoose";

#[derive(Debug, Clone)]
pub struct LoggingConfig {
    pub path: PathBuf,
    pub max_bytes: u64,
    pub max_archives: usize,
    pub bucket_key_prefix: String,
    pub basename: String,
}

impl Default for LoggingConfig {
    fn default() -> Self {
        Self {
            path: PathBuf::from(DEFAULT_LOG_PATH),
            max_bytes: DEFAULT_MAX_BYTES,
            max_archives: DEFAULT_MAX_ARCHIVES,
            bucket_key_prefix: DEFAULT_BUCKET_KEY_PREFIX.to_string(),
            basename: DEFAULT_BASENAME.to_string(),
        }
    }
}

pub fn init(filter: EnvFilter, cfg: LoggingConfig, s3: S3Client) -> anyhow::Result<LogGuard> {
    let (tx, rx) = mpsc::channel();
    let worker = LogWorker::open(cfg.clone(), rx)?;
    let handle = thread::Builder::new()
        .name("vamoose-log-writer".to_string())
        .spawn(move || worker.run(s3))?;

    let layer = RotatingGzipLayer {
        tx: Arc::new(Mutex::new(Some(tx))),
    };

    tracing_subscriber::registry()
        .with(filter)
        .with(layer)
        .init();

    Ok(LogGuard {
        tx: layer.tx.clone(),
        handle: Some(handle),
    })
}

pub struct LogGuard {
    tx: Arc<Mutex<Option<Sender<LogCommand>>>>,
    handle: Option<thread::JoinHandle<()>>,
}

impl Drop for LogGuard {
    fn drop(&mut self) {
        if let Ok(mut tx) = self.tx.lock() {
            tx.take();
        }
        if let Some(handle) = self.handle.take() {
            let _ = handle.join();
        }
    }
}

#[derive(Clone)]
struct RotatingGzipLayer {
    tx: Arc<Mutex<Option<Sender<LogCommand>>>>,
}

impl<S> Layer<S> for RotatingGzipLayer
where
    S: Subscriber + for<'a> LookupSpan<'a>,
{
    fn on_event(&self, event: &Event<'_>, _ctx: Context<'_, S>) {
        let mut fields = FieldVisitor::default();
        event.record(&mut fields);
        let meta = event.metadata();
        let line = format!(
            "{} {:<5} {}{}\n",
            chrono::Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Millis, true),
            level(meta.level()),
            meta.target(),
            fields.finish(),
        );

        if let Ok(tx) = self.tx.lock() {
            if let Some(tx) = tx.as_ref() {
                let _ = tx.send(LogCommand::Line(line));
            }
        }
    }
}

#[derive(Default)]
struct FieldVisitor {
    fields: Vec<String>,
}

impl FieldVisitor {
    fn finish(self) -> String {
        if self.fields.is_empty() {
            String::new()
        } else {
            format!(" {}", self.fields.join(" "))
        }
    }
}

impl Visit for FieldVisitor {
    fn record_debug(&mut self, field: &Field, value: &dyn std::fmt::Debug) {
        self.fields.push(format!("{}={:?}", field.name(), value));
    }
}

fn level(level: &Level) -> &'static str {
    match *level {
        Level::ERROR => "ERROR",
        Level::WARN => "WARN",
        Level::INFO => "INFO",
        Level::DEBUG => "DEBUG",
        Level::TRACE => "TRACE",
    }
}

enum LogCommand {
    Line(String),
}

struct LogWorker {
    cfg: LoggingConfig,
    rx: Receiver<LogCommand>,
    encoder: GzEncoder<File>,
    bytes_written: u64,
}

impl LogWorker {
    fn open(cfg: LoggingConfig, rx: Receiver<LogCommand>) -> anyhow::Result<Self> {
        if let Some(parent) = cfg.path.parent() {
            fs::create_dir_all(parent)?;
        }

        let bytes_written = fs::metadata(&cfg.path).map(|m| m.len()).unwrap_or(0);
        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&cfg.path)?;
        let encoder = GzEncoder::new(file, Compression::default());

        Ok(Self {
            cfg,
            rx,
            encoder,
            bytes_written,
        })
    }

    fn run(mut self, s3: S3Client) {
        while let Ok(cmd) = self.rx.recv() {
            match cmd {
                LogCommand::Line(line) => {
                    if let Err(e) = self.write_line(&line, &s3) {
                        let _ = writeln!(io::stderr(), "vamoose logger error: {e}");
                    }
                }
            }
        }

        if let Err(e) = self.encoder.try_finish() {
            let _ = writeln!(io::stderr(), "vamoose logger flush error: {e}");
        }
    }

    fn write_line(&mut self, line: &str, s3: &S3Client) -> anyhow::Result<()> {
        self.encoder.write_all(line.as_bytes())?;
        self.encoder.flush()?;
        self.bytes_written = fs::metadata(&self.cfg.path).map(|m| m.len()).unwrap_or(0);

        if self.bytes_written >= self.cfg.max_bytes {
            self.rotate(s3)?;
        }

        Ok(())
    }

    fn rotate(&mut self, s3: &S3Client) -> anyhow::Result<()> {
        self.encoder.try_finish()?;

        rotate_archives(&self.cfg)?;

        let first_archive = archive_path(&self.cfg, 0);
        if self.cfg.path.exists() {
            fs::rename(&self.cfg.path, &first_archive)?;
            archive_to_bucket(&self.cfg, &first_archive, 0, s3);
        }

        let file = OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(&self.cfg.path)?;
        self.encoder = GzEncoder::new(file, Compression::default());
        self.bytes_written = 0;
        Ok(())
    }
}

fn rotate_archives(cfg: &LoggingConfig) -> anyhow::Result<()> {
    if cfg.max_archives == 0 {
        return Ok(());
    }

    let last = cfg.max_archives - 1;
    let last_path = archive_path(cfg, last);
    if last_path.exists() {
        fs::remove_file(last_path)?;
    }

    for idx in (0..last).rev() {
        let from = archive_path(cfg, idx);
        if from.exists() {
            fs::rename(from, archive_path(cfg, idx + 1))?;
        }
    }

    Ok(())
}

fn archive_path(cfg: &LoggingConfig, idx: usize) -> PathBuf {
    let parent = cfg.path.parent().unwrap_or_else(|| Path::new("."));
    parent.join(format!("{}.{idx}.log.gz", cfg.basename))
}

fn archive_to_bucket(cfg: &LoggingConfig, path: &Path, idx: usize, s3: &S3Client) {
    let body = match fs::read(path) {
        Ok(body) => body,
        Err(e) => {
            let _ = writeln!(io::stderr(), "vamoose logger archive read failed: {e}");
            return;
        }
    };

    let key = format!(
        "{}-{}.{}.log.gz",
        cfg.bucket_key_prefix.trim_end_matches('/'),
        cfg.basename,
        idx,
    );

    let client = s3.clone();
    thread::spawn(move || {
        let rt = match tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
        {
            Ok(rt) => rt,
            Err(e) => {
                let _ = writeln!(io::stderr(), "vamoose logger archive runtime failed: {e}");
                return;
            }
        };
        if let Err(e) = rt.block_on(client.put(&key, body)) {
            let _ = writeln!(io::stderr(), "vamoose logger archive upload failed: {e}");
        }
    });
}
