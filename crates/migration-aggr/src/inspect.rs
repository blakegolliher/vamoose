//! Stub for `mig-aggr inspect`: bails with a clear error instead of
//! panicking. See main.rs for the full subcommand list.

pub async fn run(
    _endpoint: &str,
    _region: &str,
    _bucket: &str,
    _shard: &str,
) -> anyhow::Result<()> {
    anyhow::bail!("`mig-aggr inspect` is not implemented; no pre-flight shard analysis exists yet")
}
