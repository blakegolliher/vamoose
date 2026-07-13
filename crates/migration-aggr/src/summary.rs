//! Stub for `mig-aggr summary`: bails with a clear error instead of
//! panicking. See main.rs for the full subcommand list.

pub async fn run(
    _endpoint: &str,
    _region: &str,
    _bucket: &str,
    _format: &str,
) -> anyhow::Result<()> {
    anyhow::bail!(
        "`mig-aggr summary` is not implemented; use the migration-tui dashboard \
         or read the progress/ objects in the run bucket directly"
    )
}
