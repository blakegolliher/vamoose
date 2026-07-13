//! Stub for `mig-aggr watch`: bails with a clear error instead of
//! panicking. See main.rs for the full subcommand list.

pub async fn run(_endpoint: &str, _region: &str, _bucket: &str) -> anyhow::Result<()> {
    anyhow::bail!("`mig-aggr watch` is not implemented; use the migration-tui dashboard")
}
