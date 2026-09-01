//! Small durable-file helpers: atomic JSON writes, tolerant reads,
//! SHA256, timestamps. Local sibling of the vamoose prepare
//! checkpoint helpers (which are private to that crate).

use anyhow::{Context, Result};
use serde::de::DeserializeOwned;
use serde::Serialize;
use sha2::{Digest, Sha256};
use std::io::Write;
use std::path::Path;

/// Write `value` as pretty JSON through a `.partial` sibling and an
/// atomic rename, fsyncing file and directory, so an interrupted write
/// never leaves a torn checkpoint behind.
pub fn write_json_atomic<T: Serialize>(path: &Path, value: &T) -> Result<()> {
    let parent = path
        .parent()
        .ok_or_else(|| anyhow::anyhow!("{} has no parent directory", path.display()))?;
    std::fs::create_dir_all(parent).with_context(|| format!("creating {}", parent.display()))?;
    let partial = path.with_extension("json.partial");
    {
        let mut file = std::fs::File::create(&partial)
            .with_context(|| format!("creating {}", partial.display()))?;
        file.write_all(&serde_json::to_vec_pretty(value)?)?;
        file.write_all(b"\n")?;
        file.sync_all()?;
    }
    std::fs::rename(&partial, path)
        .with_context(|| format!("renaming {} -> {}", partial.display(), path.display()))?;
    std::fs::File::open(parent)?.sync_all()?;
    Ok(())
}

/// `Ok(None)` when the file does not exist; parse failures are errors
/// (a corrupt checkpoint must be looked at, not silently redone).
pub fn read_json_opt<T: DeserializeOwned>(path: &Path) -> Result<Option<T>> {
    match std::fs::read(path) {
        Ok(bytes) => {
            Ok(Some(serde_json::from_slice(&bytes).with_context(|| {
                format!("parsing checkpoint {}", path.display())
            })?))
        }
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(e) => Err(e).with_context(|| format!("reading {}", path.display())),
    }
}

pub fn sha256_file(path: &Path) -> Result<String> {
    let mut file =
        std::fs::File::open(path).with_context(|| format!("opening {}", path.display()))?;
    let mut hasher = Sha256::new();
    std::io::copy(&mut file, &mut hasher)?;
    Ok(hex::encode(hasher.finalize()))
}

/// RFC 3339 UTC with second precision and a `Z` suffix.
pub fn utc_now() -> String {
    chrono::Utc::now().format("%Y-%m-%dT%H:%M:%SZ").to_string()
}

/// Raise the soft `RLIMIT_NOFILE` toward 1M (capped at the hard
/// limit). The embedded scan holds per-worker NFS sockets plus
/// per-shard parquet writers, and the copy holds a socket pair per
/// libnfs context; the default soft limit of 1024 causes "Too many
/// open files" at scale. Same policy as the standalone nfs-walker.
pub fn raise_fd_limit() {
    const TARGET: libc::rlim_t = 1_048_576;
    // SAFETY: getrlimit/setrlimit are thread-safe and we pass valid
    // pointers to initialized rlimit structs.
    unsafe {
        let mut current = libc::rlimit {
            rlim_cur: 0,
            rlim_max: 0,
        };
        if libc::getrlimit(libc::RLIMIT_NOFILE, &mut current) != 0 {
            tracing::warn!("could not read RLIMIT_NOFILE; large runs may hit fd limits");
            return;
        }
        let target = TARGET.min(current.rlim_max);
        if current.rlim_cur >= target {
            return;
        }
        let new = libc::rlimit {
            rlim_cur: target,
            rlim_max: current.rlim_max,
        };
        if libc::setrlimit(libc::RLIMIT_NOFILE, &new) != 0 {
            tracing::warn!(
                soft = current.rlim_cur,
                hard = current.rlim_max,
                requested = target,
                "could not raise RLIMIT_NOFILE; large runs may hit 'Too many open files'",
            );
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn atomic_write_then_read_round_trips_and_leaves_no_partial() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("nested").join("cp.json");
        write_json_atomic(&path, &serde_json::json!({"a": 1})).unwrap();
        let back: serde_json::Value = read_json_opt(&path).unwrap().unwrap();
        assert_eq!(back["a"], 1);
        assert!(!path.with_extension("json.partial").exists());
        let absent: Option<serde_json::Value> =
            read_json_opt(&dir.path().join("missing.json")).unwrap();
        assert!(absent.is_none());
    }

    #[test]
    fn corrupt_checkpoint_is_an_error_not_a_restart() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("cp.json");
        std::fs::write(&path, b"{not json").unwrap();
        let err = read_json_opt::<serde_json::Value>(&path).unwrap_err();
        assert!(format!("{err:#}").contains("parsing checkpoint"));
    }

    #[test]
    fn sha256_file_matches_known_digest() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("f");
        std::fs::write(&path, b"hello").unwrap();
        assert_eq!(
            sha256_file(&path).unwrap(),
            "2cf24dba5fb0a30e26e83b2ac5b9e29e1b161e5c1fa7425e73043362938b9824"
        );
    }
}
