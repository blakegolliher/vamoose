//! Immutable artifact publication shared by the mismatch JSONL, the terminal
//! report, and the canonical risk-evidence artifact.
//!
//! Every artifact is written beside its final path, fsynced, hard-linked into
//! place, and never replaced: an existing file with a different digest is a
//! hard error rather than a silent overwrite.

use anyhow::{Context, Result};
use serde::Serialize;
use sha2::{Digest, Sha256};
use std::fs::{File, OpenOptions};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

pub(crate) struct FileDigest {
    pub sha256: String,
    pub bytes: u64,
}

pub(crate) fn digest_file(path: &Path) -> Result<FileDigest> {
    let mut file = File::open(path)?;
    let mut hasher = Sha256::new();
    let bytes = std::io::copy(&mut file, &mut hasher)?;
    Ok(FileDigest {
        sha256: hex::encode(hasher.finalize()),
        bytes,
    })
}

pub(crate) fn publish_json(path: &Path, value: &impl Serialize) -> Result<()> {
    let (temporary, mut file) = create_temp(path)?;
    serde_json::to_writer_pretty(&mut file, value)?;
    file.write_all(b"\n")?;
    file.sync_all()?;
    let digest = digest_file(&temporary)?;
    publish_existing_temp(&temporary, path, &digest.sha256)
}

pub(crate) fn publish_existing_temp(
    temporary: &Path,
    final_path: &Path,
    sha256: &str,
) -> Result<()> {
    match std::fs::hard_link(temporary, final_path) {
        Ok(()) => {}
        Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {
            let existing = digest_file(final_path)?;
            if existing.sha256 != sha256 {
                anyhow::bail!(
                    "refusing to replace existing terminal artifact {} (digest {} != {})",
                    final_path.display(),
                    existing.sha256,
                    sha256
                );
            }
        }
        Err(error) => {
            return Err(error).with_context(|| format!("publishing {}", final_path.display()))
        }
    }
    std::fs::remove_file(temporary)?;
    sync_parent(final_path)
}

pub(crate) fn sync_parent(path: &Path) -> Result<()> {
    if let Some(parent) = path.parent() {
        File::open(parent)?.sync_all()?;
    }
    Ok(())
}

static TEMP_COUNTER: AtomicU64 = AtomicU64::new(0);

pub(crate) fn create_temp(path: &Path) -> Result<(PathBuf, File)> {
    let name = path
        .file_name()
        .and_then(|name| name.to_str())
        .unwrap_or("verification-artifact");
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos();
    for _ in 0..100 {
        let sequence = TEMP_COUNTER.fetch_add(1, Ordering::Relaxed);
        let temporary = path.with_file_name(format!(
            ".{name}.{}.{}.{}.tmp",
            std::process::id(),
            now,
            sequence
        ));
        match OpenOptions::new()
            .create_new(true)
            .write(true)
            .open(&temporary)
        {
            Ok(file) => return Ok((temporary, file)),
            Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => continue,
            Err(error) => {
                return Err(error).with_context(|| format!("creating {}", temporary.display()))
            }
        }
    }
    anyhow::bail!(
        "could not allocate a temporary artifact beside {}",
        path.display()
    )
}

pub(crate) fn ensure_parent(path: &Path) -> Result<()> {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)
            .with_context(|| format!("creating {}", parent.display()))?;
    }
    Ok(())
}
