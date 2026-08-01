//! Command dispatch and semantic process outcomes.

use std::path::PathBuf;
use std::time::Duration;

use crate::cli::Command;
use crate::cmd::doctor::DoctorOutcome;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum CommandOutcome {
    Success,
    DoctorChecksFailed,
    DoctorUnableToComplete,
    WorkerFenced,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum LoggingShutdownPolicy {
    Ordinary,
    WorkerWatchdogArmed,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct CommandCompletion {
    outcome: CommandOutcome,
    logging: LoggingShutdownPolicy,
}

const ORDINARY_LOGGING_SHUTDOWN_DEADLINE: Duration = Duration::from_secs(10);
const WORKER_WATCHDOG_SAFETY_MARGIN: Duration = Duration::from_secs(1);

impl CommandCompletion {
    fn ordinary(outcome: CommandOutcome) -> Self {
        Self {
            outcome,
            logging: LoggingShutdownPolicy::Ordinary,
        }
    }

    fn completed_worker(outcome: migration_worker::orchestrator::RunOutcome) -> Self {
        let outcome = match outcome {
            migration_worker::orchestrator::RunOutcome::Clean => CommandOutcome::Success,
            migration_worker::orchestrator::RunOutcome::Fenced => CommandOutcome::WorkerFenced,
        };
        Self {
            outcome,
            logging: LoggingShutdownPolicy::WorkerWatchdogArmed,
        }
    }

    pub(crate) fn exit_code(self) -> u8 {
        self.outcome.exit_code()
    }
}

fn completed_worker_logging_deadline() -> Duration {
    migration_worker::orchestrator::HARD_EXIT_WATCHDOG_INTERVAL
        .checked_sub(WORKER_WATCHDOG_SAFETY_MARGIN)
        .expect("worker watchdog interval must exceed its logging safety margin")
}

/// Errors return before the worker arms its hard-exit watchdog, so
/// only a completed worker run receives the shortened uploader wait.
pub(crate) fn logging_shutdown_deadline(result: &anyhow::Result<CommandCompletion>) -> Duration {
    match result {
        Ok(CommandCompletion {
            logging: LoggingShutdownPolicy::WorkerWatchdogArmed,
            ..
        }) => completed_worker_logging_deadline(),
        Ok(_) | Err(_) => ORDINARY_LOGGING_SHUTDOWN_DEADLINE,
    }
}

impl CommandOutcome {
    pub(crate) fn exit_code(self) -> u8 {
        match self {
            Self::Success => 0,
            Self::DoctorChecksFailed => 1,
            Self::DoctorUnableToComplete => 2,
            Self::WorkerFenced => 3,
        }
    }
}

impl From<DoctorOutcome> for CommandOutcome {
    fn from(outcome: DoctorOutcome) -> Self {
        match outcome {
            DoctorOutcome::Healthy => Self::Success,
            DoctorOutcome::ChecksFailed => Self::DoctorChecksFailed,
            DoctorOutcome::Incomplete(_) => Self::DoctorUnableToComplete,
        }
    }
}

pub(crate) async fn run(
    command: Command,
    config_path: Option<PathBuf>,
) -> anyhow::Result<CommandCompletion> {
    match command {
        Command::Worker(args) => {
            let outcome = crate::cmd::worker::run(args, config_path).await?;
            Ok(CommandCompletion::completed_worker(outcome))
        }
        Command::Walker(args) => crate::cmd::walker::run(args, config_path)
            .await
            .map(|()| CommandCompletion::ordinary(CommandOutcome::Success)),
        Command::Rewrite(args) => crate::cmd::rewrite::run(args, config_path)
            .await
            .map(|()| CommandCompletion::ordinary(CommandOutcome::Success)),
        Command::Aggr(args) => crate::cmd::aggr::run(args, config_path)
            .await
            .map(|()| CommandCompletion::ordinary(CommandOutcome::Success)),
        Command::Status(args) => crate::cmd::status::run(args, config_path)
            .await
            .map(|()| CommandCompletion::ordinary(CommandOutcome::Success)),
        Command::Doctor(args) => crate::cmd::doctor::run(args, config_path)
            .await
            .map(CommandOutcome::from)
            .map(CommandCompletion::ordinary),
        Command::Init(args) => crate::cmd::init::run(args, config_path)
            .await
            .map(|()| CommandCompletion::ordinary(CommandOutcome::Success)),
        Command::Run(args) => crate::cmd::run::run(args, config_path)
            .await
            .map(|()| CommandCompletion::ordinary(CommandOutcome::Success)),
        Command::Coord(args) => crate::cmd::coord::run(args, config_path)
            .await
            .map(|()| CommandCompletion::ordinary(CommandOutcome::Success)),
        Command::Tui(args) => crate::cmd::tui::run(args, config_path)
            .await
            .map(|()| CommandCompletion::ordinary(CommandOutcome::Success)),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cmd::doctor::IncompleteReason;

    #[test]
    fn successful_and_fenced_worker_exit_codes_are_preserved() {
        let clean =
            CommandCompletion::completed_worker(migration_worker::orchestrator::RunOutcome::Clean);
        let fenced =
            CommandCompletion::completed_worker(migration_worker::orchestrator::RunOutcome::Fenced);
        assert_eq!(clean.exit_code(), 0);
        assert_eq!(fenced.exit_code(), 3);
    }

    #[test]
    fn every_doctor_terminal_condition_keeps_its_exit_code() {
        let cases = [
            (DoctorOutcome::Healthy, 0),
            (DoctorOutcome::ChecksFailed, 1),
            (
                DoctorOutcome::Incomplete(IncompleteReason::ConfigurationLoad),
                2,
            ),
            (
                DoctorOutcome::Incomplete(IncompleteReason::S3ClientConstruction),
                2,
            ),
            (
                DoctorOutcome::Incomplete(IncompleteReason::S3Reachability),
                2,
            ),
            (
                DoctorOutcome::Incomplete(IncompleteReason::ConditionalOperation),
                2,
            ),
        ];

        for (doctor_outcome, expected) in cases {
            assert_eq!(CommandOutcome::from(doctor_outcome).exit_code(), expected);
        }
    }

    #[test]
    fn completed_worker_log_deadline_precedes_hard_exit_watchdog() {
        for outcome in [
            migration_worker::orchestrator::RunOutcome::Clean,
            migration_worker::orchestrator::RunOutcome::Fenced,
        ] {
            let completion = Ok(CommandCompletion::completed_worker(outcome));
            let deadline = logging_shutdown_deadline(&completion);

            assert_eq!(deadline, Duration::from_secs(4));
            assert!(deadline < migration_worker::orchestrator::HARD_EXIT_WATCHDOG_INTERVAL);
        }
    }

    #[test]
    fn worker_error_before_watchdog_arming_keeps_ordinary_deadline() {
        let result: anyhow::Result<CommandCompletion> = Err(anyhow::anyhow!("worker failed"));
        assert_eq!(
            logging_shutdown_deadline(&result),
            ORDINARY_LOGGING_SHUTDOWN_DEADLINE
        );
    }

    #[test]
    fn non_worker_command_keeps_ordinary_deadline() {
        let completion = Ok(CommandCompletion::ordinary(CommandOutcome::Success));
        assert_eq!(
            logging_shutdown_deadline(&completion),
            ORDINARY_LOGGING_SHUTDOWN_DEADLINE
        );
    }
}
