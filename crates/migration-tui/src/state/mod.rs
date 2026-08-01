//! Client-side state for the TUI.
//!
//! The authoritative data model and reducer come from
//! [`migration_control_protocol::schema::Snapshot`]. This module adds UI state,
//! bounded client-side activity histories, connection status, and the SSE resume
//! cursor while keeping the existing `migration_tui::state` API stable.
//!
//! Every accepted envelope enters through [`AppState::apply_envelope`]. REST
//! bootstrap and Resync recovery enter through [`snapshot_from_rest`] and
//! [`AppState::replace_snapshot`].

mod activity;
mod model;
mod ui;

pub use activity::{
    ProgressDeltaHistory, RecentError, RecentErrors, RecentVerifyMismatch, RecentVerifyMismatches,
    VerifyStatus, RECENT_ERRORS_PER_JOB, RECENT_VERIFY_MISMATCHES_PER_JOB,
};
pub use model::{snapshot_from_rest, AppState};
pub use ui::{
    CommandStatus, CommandStatusKind, ConnectionStatus, InputMode, JobSort, Modal, Tab, UiState,
    View, WorkerSort, COMMAND_STATUS_TTL_SECS,
};

#[cfg(test)]
mod tests;
