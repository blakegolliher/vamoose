//! Stub for `mig-aggr verify`. See main.rs for the full subcommand list.

pub async fn run(_endpoint: &str, _region: &str, _bucket: &str) -> anyhow::Result<()> {
    todo!("verify")
}

pub async fn clean_partials(_endpoint: &str, _region: &str, _bucket: &str, _dry_run: bool) -> anyhow::Result<()> {
    todo!("clean partial files left by fenced workers")
}
