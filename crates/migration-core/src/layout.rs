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
//! failures/host-<id>/<stem>-e<epoch>.jsonl    # per-file failures, one
//!                                             # object per shard flush
//! downgrades/host-<id>/<stem>-e<epoch>.jsonl  # metadata downgrades,
//!                                             # one object per shard flush
//! ```
//!
//! The failure/downgrade sinks get one object per (host, shard, claim
//! epoch) flush — never a rewrite of a shared per-host object, which
//! would lose earlier shards' records on every flush (F04). `<stem>`
//! is the shard filename without its `.parquet` extension; the epoch
//! discriminator keeps a post-reclaim re-run of the same shard from
//! colliding with the fenced run's records. Keys stay under the
//! per-host prefix so consumers list `failures/host-<id>/` (or the
//! top-level `failures/`) to collect everything.

pub const MANIFEST_KEY: &str = "manifest.json";
/// `vamoose prepare` overwrites this with its progress (see
/// `PrepareProgress` in the control protocol) so the coord, the TUI,
/// and `vamoose status` can show the scan/index/publish stages before
/// the manifest exists.
pub const PREPARE_PROGRESS_KEY: &str = "prepare/progress.json";

pub const INDEX_PREFIX: &str = "index/";
pub const SHARDS_PREFIX: &str = "shards/";
pub const PROGRESS_PREFIX: &str = "progress/";
pub const BATCHES_PREFIX: &str = "batches/";
pub const FAILURES_PREFIX: &str = "failures/";
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

/// Per-host prefix for failure-log objects. Consumers LIST this
/// prefix (or the top-level [`FAILURES_PREFIX`]) to collect a host's
/// failure records across all of its shard flushes.
pub fn failures_host_prefix(host_id: &str) -> String {
    format!("{FAILURES_PREFIX}host-{host_id}/")
}

/// Per-host prefix for downgrade-log objects. See
/// [`failures_host_prefix`].
pub fn downgrades_host_prefix(host_id: &str) -> String {
    format!("{DOWNGRADES_PREFIX}host-{host_id}/")
}

/// Key for one flush of the failure sink:
/// `failures/host-<id>/<shard-stem>-e<epoch>.jsonl`.
///
/// One object per (host, shard, claim epoch) — the orchestrator
/// flushes after each shard, and a fixed per-host key would make each
/// flush overwrite the previous shard's records (F04). The scheme is
/// deterministic (no timestamps — replay-safe) and collision-proof
/// across reclaims: a re-run of the same shard happens under a higher
/// claim epoch, so it gets a distinct key instead of clobbering the
/// fenced run's records. Each key therefore has exactly one legitimate
/// writer, which is what makes `put_if_absent` the right primitive
/// (an unexpected 412 is a bug surfacing, not contention).
pub fn failures_flush_key(host_id: &str, shard_filename: &str, epoch: u64) -> String {
    format!(
        "{}{}-e{epoch}.jsonl",
        failures_host_prefix(host_id),
        shard_stem(shard_filename),
    )
}

/// Key for one flush of the downgrade sink:
/// `downgrades/host-<id>/<shard-stem>-e<epoch>.jsonl`.
///
/// A downgrade is a successful copy that had to drop a piece of
/// metadata the user asked for — e.g. null source mtime, or a
/// hardlink group falling back to `inode`-only because `fsid` was
/// null. See SCHEMA_CONTRACT.md "Null attribute semantics". Key
/// scheme rationale is identical to [`failures_flush_key`].
pub fn downgrades_flush_key(host_id: &str, shard_filename: &str, epoch: u64) -> String {
    format!(
        "{}{}-e{epoch}.jsonl",
        downgrades_host_prefix(host_id),
        shard_stem(shard_filename),
    )
}

/// Shard filename without its `.parquet` extension, e.g.
/// `part-0042.parquet` → `part-0042`. Falls back to the full name if
/// the extension is absent.
fn shard_stem(shard_filename: &str) -> &str {
    shard_filename
        .strip_suffix(".parquet")
        .unwrap_or(shard_filename)
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

    /// F04 acceptance test 1: flush keys must be unique per
    /// (host, shard, claim epoch). Distinct shards get distinct keys;
    /// the same shard re-processed at a higher epoch (post-reclaim
    /// re-run) must not clobber the fenced run's records. No
    /// timestamps — the scheme is deterministic and replay-safe.
    #[test]
    fn failures_key_unique_per_flush() {
        let host = "vastdataubuntu2404-05bfb1cb";

        let shard1_e1 = failures_flush_key(host, "part-0001.parquet", 1);
        let shard2_e1 = failures_flush_key(host, "part-0002.parquet", 1);
        let shard1_e2 = failures_flush_key(host, "part-0001.parquet", 2);

        // Exact scheme: failures/host-<id>/<shard-stem>-e<epoch>.jsonl
        assert_eq!(
            shard1_e1,
            format!("failures/host-{host}/part-0001-e1.jsonl"),
        );

        // Distinct shards, same epoch → distinct keys.
        assert_ne!(shard1_e1, shard2_e1);
        // Same shard, higher epoch (reclaim re-run) → distinct keys.
        assert_ne!(shard1_e1, shard1_e2);

        // Deterministic: same inputs always produce the same key
        // (no timestamps or randomness).
        assert_eq!(shard1_e1, failures_flush_key(host, "part-0001.parquet", 1));

        // Same guarantees for the downgrade sink.
        let d1 = downgrades_flush_key(host, "part-0001.parquet", 1);
        let d2 = downgrades_flush_key(host, "part-0002.parquet", 1);
        let d3 = downgrades_flush_key(host, "part-0001.parquet", 2);
        assert_eq!(d1, format!("downgrades/host-{host}/part-0001-e1.jsonl"));
        assert_ne!(d1, d2);
        assert_ne!(d1, d3);
        assert_eq!(d1, downgrades_flush_key(host, "part-0001.parquet", 1));

        // Failure and downgrade keys never collide with each other.
        assert_ne!(shard1_e1, d1);
    }

    /// F04 acceptance test 5 (layout half): the per-flush keys still
    /// live under the per-host prefix consumers list today, and under
    /// the top-level failures/ / downgrades/ prefixes.
    #[test]
    fn flush_keys_live_under_host_prefix() {
        let key = failures_flush_key("A", "part-0042.parquet", 3);
        assert!(key.starts_with(FAILURES_PREFIX), "key: {key}");
        assert!(key.starts_with(&failures_host_prefix("A")), "key: {key}");
        assert!(key.starts_with("failures/host-A"), "key: {key}");

        let dkey = downgrades_flush_key("A", "part-0042.parquet", 3);
        assert!(dkey.starts_with(DOWNGRADES_PREFIX), "key: {dkey}");
        assert!(
            dkey.starts_with(&downgrades_host_prefix("A")),
            "key: {dkey}"
        );
        assert!(dkey.starts_with("downgrades/host-A"), "key: {dkey}");
    }
}
