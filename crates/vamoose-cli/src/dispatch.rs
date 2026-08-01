//! Command dispatch and semantic process outcomes.

use std::path::PathBuf;

use crate::cli::Command;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum CommandOutcome {
    Success,
    WorkerFenced,
}

impl CommandOutcome {
    pub(crate) fn exit_code(self) -> u8 {
        match self {
            Self::Success => 0,
            Self::WorkerFenced => 3,
        }
    }
}

pub(crate) async fn run(
    command: Command,
    config_path: Option<PathBuf>,
) -> anyhow::Result<CommandOutcome> {
    match command {
        Command::Worker(args) => {
            let outcome = crate::cmd::worker::run(args, config_path).await?;
            Ok(match outcome {
                migration_worker::orchestrator::RunOutcome::Clean => CommandOutcome::Success,
                migration_worker::orchestrator::RunOutcome::Fenced => CommandOutcome::WorkerFenced,
            })
        }
        Command::Walker(args) => crate::cmd::walker::run(args, config_path)
            .await
            .map(|()| CommandOutcome::Success),
        Command::Rewrite(args) => crate::cmd::rewrite::run(args, config_path)
            .await
            .map(|()| CommandOutcome::Success),
        Command::Aggr(args) => crate::cmd::aggr::run(args, config_path)
            .await
            .map(|()| CommandOutcome::Success),
        Command::Status(args) => crate::cmd::status::run(args, config_path)
            .await
            .map(|()| CommandOutcome::Success),
        Command::Doctor(args) => crate::cmd::doctor::run(args, config_path)
            .await
            .map(|()| CommandOutcome::Success),
        Command::Init(args) => crate::cmd::init::run(args, config_path)
            .await
            .map(|()| CommandOutcome::Success),
        Command::Run(args) => crate::cmd::run::run(args, config_path)
            .await
            .map(|()| CommandOutcome::Success),
        Command::Coord(args) => crate::cmd::coord::run(args, config_path)
            .await
            .map(|()| CommandOutcome::Success),
        Command::Tui(args) => crate::cmd::tui::run(args, config_path)
            .await
            .map(|()| CommandOutcome::Success),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn successful_and_fenced_worker_exit_codes_are_preserved() {
        assert_eq!(CommandOutcome::Success.exit_code(), 0);
        assert_eq!(CommandOutcome::WorkerFenced.exit_code(), 3);
    }
}
