//! S3 key layout for a migration run.
//!
//! All S3 keys produced or consumed by the system are constructed
//! through this module. Centralizing the layout means the worker,
//! aggregator, and any future tooling cannot drift.
//!
//! Layout (relative to the run bucket root):
//!
//! ```text
//! manifest.json
//! index/<shard>.parquet                   # immutable, written once
//! shards/<shard>.parquet.claim            # one per shard, conditional PUT
//! progress/host-<id>.json                 # per-host heartbeat & throughput
//! batches/host-<id>.jsonl                 # per-host audit trail (optional)
//! failures/host-<id>.jsonl                # per-host per-file failures
//! ```

pub const MANIFEST_KEY: &str = "manifest.json";

pub const INDEX_PREFIX:      &str = "index/";
pub const SHARDS_PREFIX:     &str = "shards/";
pub const PROGRESS_PREFIX:   &str = "progress/";
pub const BATCHES_PREFIX:    &str = "batches/";
pub const FAILURES_PREFIX:   &str = "failures/";
pub const DOWNGRADES_PREFIX: &str = "downgrades/";

pub const CLAIM_SUFFIX: &str = ".claim";

/// Key for the parquet index shard (e.g. `index/part-0042.parquet`).
pub fn index_key(shard_filename: &str) -> String {
    format!("{INDEX_PREFIX}{shard_filename}")
}

/// Key for the claim object of a shard. Convention is the parquet
/// filename plus `.claim`, e.g. `shards/part-0042.parquet.claim`.
pub fn claim_key(shard_filename: &str) -> String {
    format!("{SHARDS_PREFIX}{shard_filename}{CLAIM_SUFFIX}")
}

pub fn progress_key(host_id: &str) -> String {
    format!("{PROGRESS_PREFIX}host-{host_id}.json")
}

pub fn batches_key(host_id: &str) -> String {
    format!("{BATCHES_PREFIX}host-{host_id}.jsonl")
}

pub fn failures_key(host_id: &str) -> String {
    format!("{FAILURES_PREFIX}host-{host_id}.jsonl")
}

/// Key for the per-host downgrades log. A downgrade is a successful
/// copy that had to drop a piece of metadata the user asked for —
/// e.g. null source mtime, or a hardlink group falling back to
/// `inode`-only because `fsid` was null. See SCHEMA_CONTRACT.md
/// "Null attribute semantics".
pub fn downgrades_key(host_id: &str) -> String {
    format!("{DOWNGRADES_PREFIX}host-{host_id}.jsonl")
}

/// Recover the shard filename (e.g. `part-0042.parquet`) from a claim
/// key. Returns `None` if the key doesn't match the expected layout.
pub fn shard_from_claim_key(claim_key: &str) -> Option<&str> {
    let stem = claim_key.strip_prefix(SHARDS_PREFIX)?;
    stem.strip_suffix(CLAIM_SUFFIX)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn round_trip() {
        let shard = "part-0042.parquet";
        let key = claim_key(shard);
        assert_eq!(key, "shards/part-0042.parquet.claim");
        assert_eq!(shard_from_claim_key(&key), Some(shard));
    }

    #[test]
    fn rejects_non_claim_key() {
        assert_eq!(shard_from_claim_key("index/part-0000.parquet"), None);
        assert_eq!(shard_from_claim_key("shards/part-0000.parquet"), None);
    }
}
