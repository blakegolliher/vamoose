//! Progress of this `prepare`, published to the bucket
//! (`prepare/progress.json`) so the coord, `vamoose tui`, and
//! `vamoose status` can show the scan / index / publish stages before
//! the manifest exists. Advisory: a failed PUT is logged and the
//! prepare carries on — the manifest is the only object that matters.

use anyhow::{Context, Result};
use migration_coord::schema::{
    PrepareIndex, PreparePhase, PrepareProgress, PrepareScan, PREPARE_PROGRESS_SCHEMA_VERSION,
};
use migration_core::layout::PREPARE_PROGRESS_KEY;
use migration_core::s3::S3Client;
use std::path::Path;
use std::time::{Duration, Instant};

/// How often counter-only changes are written. Phase changes are
/// written at once.
const PUBLISH_EVERY: Duration = Duration::from_secs(5);

pub(crate) struct Reporter<'a> {
    s3: &'a S3Client,
    progress: PrepareProgress,
    last_put: Option<Instant>,
}

impl<'a> Reporter<'a> {
    pub(crate) fn new(s3: &'a S3Client, run_id: &str, source: String, dest: String) -> Self {
        let now = chrono::Utc::now();
        Self {
            s3,
            progress: PrepareProgress {
                schema_version: PREPARE_PROGRESS_SCHEMA_VERSION,
                run_id: run_id.to_string(),
                host: hostname::get()
                    .map(|h| h.to_string_lossy().into_owned())
                    .unwrap_or_else(|_| "unknown".to_string()),
                pid: std::process::id(),
                source,
                dest,
                phase: PreparePhase::Scan,
                started_utc: now,
                updated_utc: now,
                scan: PrepareScan::default(),
                index: PrepareIndex::default(),
                message: None,
            },
            last_put: None,
        }
    }

    /// Enter `phase` and write immediately.
    pub(crate) async fn set_phase(&mut self, phase: PreparePhase) {
        self.progress.phase = phase;
        self.publish().await;
    }

    /// Apply a counter update; written if the last write is older
    /// than [`PUBLISH_EVERY`].
    pub(crate) async fn update(&mut self, f: impl FnOnce(&mut PrepareProgress)) {
        f(&mut self.progress);
        if self.last_put.is_none_or(|t| t.elapsed() >= PUBLISH_EVERY) {
            self.publish().await;
        }
    }

    /// Record the failure and write immediately.
    pub(crate) async fn fail(&mut self, message: String) {
        self.progress.message = Some(message);
        self.set_phase(PreparePhase::Failed).await;
    }

    pub(crate) async fn publish(&mut self) {
        self.progress.updated_utc = chrono::Utc::now();
        self.last_put = Some(Instant::now());
        match serde_json::to_vec_pretty(&self.progress) {
            Ok(body) => {
                if let Err(e) = self.s3.put(PREPARE_PROGRESS_KEY, body).await {
                    tracing::warn!(error = %e, "prepare: progress write to the bucket failed; continuing");
                }
            }
            Err(e) => tracing::warn!(error = %e, "prepare: progress serialisation failed"),
        }
    }
}

/// The scan counters in the last line of nfs-walker's JSON progress
/// log (`--log … --log-fmt json`): one object per interval with
/// `files`, `dirs`, `errors`, `rate_per_sec`, `elapsed_secs`, and
/// `is_final` on the last one. `None` until the first line lands.
pub(crate) fn read_walker_progress(log: &Path) -> Result<Option<PrepareScan>> {
    let text = match std::fs::read_to_string(log) {
        Ok(t) => t,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(e) => return Err(e).with_context(|| format!("reading {}", log.display())),
    };
    let Some(line) = text.lines().rev().find(|l| !l.trim().is_empty()) else {
        return Ok(None);
    };
    // A line still being written is not an error: use the previous one.
    let value: serde_json::Value = match serde_json::from_str(line) {
        Ok(v) => v,
        Err(_) => match text.lines().rev().filter(|l| !l.trim().is_empty()).nth(1) {
            Some(prev) => serde_json::from_str(prev)
                .with_context(|| format!("parsing walker progress in {}", log.display()))?,
            None => return Ok(None),
        },
    };
    let num = |k: &str| value.get(k).and_then(|v| v.as_u64()).unwrap_or(0);
    Ok(Some(PrepareScan {
        files: num("files"),
        dirs: num("dirs"),
        errors: num("errors"),
        rate_per_sec: num("rate_per_sec"),
        elapsed_secs: num("elapsed_secs"),
        complete: value
            .get("is_final")
            .and_then(|v| v.as_bool())
            .unwrap_or(false),
    }))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn walker_progress_reads_the_last_complete_line() {
        let dir = tempfile::tempdir().unwrap();
        let log = dir.path().join("walker-progress.jsonl");
        assert!(read_walker_progress(&log).unwrap().is_none(), "no file yet");
        std::fs::write(&log, "").unwrap();
        assert!(read_walker_progress(&log).unwrap().is_none(), "empty file");
        std::fs::write(
            &log,
            concat!(
                r#"{"ts":"t","is_final":false,"elapsed_secs":25,"dirs":39295,"files":4586703,"bytes":1,"errors":0,"rate_per_sec":184938}"#,
                "\n",
                r#"{"ts":"t","is_final":true,"elapsed_secs":2829,"dirs":4208101,"files":603266804,"bytes":2,"errors":0,"rate_per_sec":213000}"#,
                "\n",
                r#"{"ts":"t","is_fi"#,
            ),
        )
        .unwrap();
        let scan = read_walker_progress(&log).unwrap().unwrap();
        assert_eq!(scan.files, 603_266_804);
        assert_eq!(scan.dirs, 4_208_101);
        assert_eq!(scan.elapsed_secs, 2829);
        assert!(
            scan.complete,
            "torn tail line falls back to the previous one"
        );
    }
}
