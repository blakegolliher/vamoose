//! Small durable-file helpers shared by the prepare stages: atomic JSON
//! writes, tolerant reads, SHA256, and timestamps.

use anyhow::{Context, Result};
use serde::de::DeserializeOwned;
use serde::Serialize;
use sha2::{Digest, Sha256};
use std::io::Write;
use std::path::Path;

/// Write `value` as pretty JSON through a `.partial` sibling and an
/// atomic rename, fsyncing file and directory, so an interrupted write
/// never leaves a torn checkpoint behind.
pub(crate) fn write_json_atomic<T: Serialize>(path: &Path, value: &T) -> Result<()> {
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
pub(crate) fn read_json_opt<T: DeserializeOwned>(path: &Path) -> Result<Option<T>> {
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

pub(crate) fn sha256_file(path: &Path) -> Result<String> {
    let mut file =
        std::fs::File::open(path).with_context(|| format!("opening {}", path.display()))?;
    let mut hasher = Sha256::new();
    std::io::copy(&mut file, &mut hasher)?;
    Ok(hex::encode(hasher.finalize()))
}

pub(crate) fn sha256_bytes(bytes: &[u8]) -> String {
    hex::encode(Sha256::digest(bytes))
}

/// RFC 3339 UTC with second precision and a `Z` suffix.
pub(crate) fn utc_now() -> String {
    chrono::Utc::now().format("%Y-%m-%dT%H:%M:%SZ").to_string()
}

/// JSON bytes with object keys sorted recursively, so identities
/// hashed from JSON do not depend on struct field order.
pub(crate) fn canonical_json(value: &serde_json::Value) -> Vec<u8> {
    fn sort(value: &serde_json::Value) -> serde_json::Value {
        match value {
            serde_json::Value::Object(map) => {
                let mut entries: Vec<(&String, &serde_json::Value)> = map.iter().collect();
                entries.sort_by(|a, b| a.0.cmp(b.0));
                let mut out = serde_json::Map::new();
                for (k, v) in entries {
                    out.insert(k.clone(), sort(v));
                }
                serde_json::Value::Object(out)
            }
            serde_json::Value::Array(items) => {
                serde_json::Value::Array(items.iter().map(sort).collect())
            }
            other => other.clone(),
        }
    }
    let mut bytes = serde_json::to_vec_pretty(&sort(value)).expect("Value serializes");
    bytes.push(b'\n');
    bytes
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
    fn canonical_json_sorts_keys_recursively() {
        let a = serde_json::json!({"b": {"y": 1, "x": [ {"k": 2, "j": 1} ]}, "a": 0});
        let b = serde_json::json!({"a": 0, "b": {"x": [ {"j": 1, "k": 2} ], "y": 1}});
        assert_eq!(canonical_json(&a), canonical_json(&b));
        let text = String::from_utf8(canonical_json(&a)).unwrap();
        assert!(text.find("\"a\"").unwrap() < text.find("\"b\"").unwrap());
    }

    #[test]
    fn sha256_helpers_agree() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("f");
        std::fs::write(&path, b"hello").unwrap();
        assert_eq!(sha256_file(&path).unwrap(), sha256_bytes(b"hello"));
        assert_eq!(
            sha256_bytes(b"hello"),
            "2cf24dba5fb0a30e26e83b2ac5b9e29e1b161e5c1fa7425e73043362938b9824"
        );
    }
}
