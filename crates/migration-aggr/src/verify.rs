//! Stub for `mig-aggr verify`: bails with a clear error instead of
//! panicking. See main.rs for the full subcommand list.

pub async fn run(_endpoint: &str, _region: &str, _bucket: &str) -> anyhow::Result<()> {
    anyhow::bail!(
        "`mig-aggr verify` is not implemented; src-vs-dst metadata diffing needs \
         access to both endpoints and is out of scope for this build"
    )
}
