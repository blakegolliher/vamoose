//! Stub for `mig-aggr metrics`: bails with a clear error instead of
//! panicking. See main.rs for the full subcommand list.

pub async fn run(
    _endpoint: &str,
    _region: &str,
    _bucket: &str,
    _listen: &str,
) -> anyhow::Result<()> {
    anyhow::bail!("`mig-aggr metrics` is not implemented; no Prometheus exporter exists yet")
}
