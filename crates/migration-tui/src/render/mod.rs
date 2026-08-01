//! Deterministic rendering for the jobs list, job-detail tabs, and modal
//! overlays.
//!
//! [`render`] draws solely from [`crate::state::AppState`] and the supplied
//! wall clock. View-specific modules own their layouts while this module keeps
//! the established `migration_tui::render` API stable.

mod chrome;
mod common;
mod detail;
mod jobs;
mod modal;

use chrome::{render_bottom_hints, render_top_banner};
use detail::render_detail;
use jobs::render_jobs_table;
use modal::render_modal;

use crate::state::{AppState, View};
use chrono::{DateTime, Utc};
use ratatui::layout::{Constraint, Direction, Layout};
use ratatui::Frame;

pub use detail::workers::visible_workers;
pub use jobs::{aggregate_counts, visible_jobs, AggregateCounts};

/// Render the full TUI into `frame` using `state` and the current
/// wall-clock `now` (passed in so tests are deterministic).
///
/// Dispatches by [`View`]: List view is the Phase 4 jobs list;
/// Detail view (Phase 5) opens after the operator presses Enter on
/// a job row.
pub fn render(frame: &mut Frame, state: &AppState, now: DateTime<Utc>) {
    match &state.ui.view {
        View::List => render_list(frame, state, now),
        View::Detail { job_id, tab } => render_detail(frame, state, now, job_id, *tab),
    }
    // Modal overlays render LAST so they sit over whichever view
    // is underneath. Help is reachable from both List and Detail,
    // so it can't live inside the Detail-only `render_detail`
    // branch — and even modals that originate in Detail (like
    // WorkerDetail) gain robustness from being drawn here: if a
    // future code path leaves a modal set while the view switches,
    // the operator still sees it instead of a silently-empty
    // background.
    if let Some(modal) = &state.ui.modal {
        render_modal(frame, frame.area(), state, modal, now);
    }
}

fn render_list(frame: &mut Frame, state: &AppState, now: DateTime<Utc>) {
    let chunks = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Length(1), // top banner
            Constraint::Min(0),    // jobs table
            Constraint::Length(1), // bottom hints
        ])
        .split(frame.area());

    render_top_banner(frame, chunks[0], state, now);
    render_jobs_table(frame, chunks[1], state, now);
    render_bottom_hints(frame, chunks[2], state, now);
}

#[cfg(test)]
use detail::format_error_class;
#[cfg(test)]
use jobs::progress_bar;

#[cfg(test)]
mod tests;
