use super::common::{header_style, phase_span};
use crate::format::{format_count, format_elapsed, format_pct};
use crate::state::{AppState, JobSort, UiState};
use chrono::{DateTime, Utc};
use migration_control_protocol::schema::{Job, Phase};
use ratatui::layout::{Constraint, Rect};
use ratatui::style::{Modifier, Style};
use ratatui::text::Span;
use ratatui::widgets::{Cell, Row, Table};
use ratatui::Frame;

// =============================================================================
// Jobs table
// =============================================================================

pub(super) fn render_jobs_table(
    frame: &mut Frame,
    area: Rect,
    state: &AppState,
    now: DateTime<Utc>,
) {
    let jobs = visible_jobs(state);

    // Header row, then one row per job. Column widths sum to 51 of
    // fixed budget; the leading Job column takes whatever remains
    // (Min(15) reserves a sensible minimum for the smallest sane
    // terminal).
    let header = Row::new(vec![
        Cell::from(Span::styled("Job", header_style())),
        Cell::from(Span::styled("Phase", header_style())),
        Cell::from(Span::styled("Progress", header_style())),
        Cell::from(Span::styled("Workers", header_style())),
        Cell::from(Span::styled("Errs", header_style())),
        Cell::from(Span::styled("Updated", header_style())),
    ])
    .style(Style::default().add_modifier(Modifier::BOLD));

    let rows = jobs
        .iter()
        .map(|j| job_row(j, state, now))
        .collect::<Vec<_>>();

    let widths = [
        Constraint::Min(15),
        Constraint::Length(9),
        Constraint::Length(24),
        Constraint::Length(7),
        Constraint::Length(5),
        Constraint::Length(7),
    ];
    let table = Table::new(rows, widths).header(header).column_spacing(1);
    frame.render_widget(table, area);
}

fn job_row<'a>(job: &'a Job, state: &AppState, now: DateTime<Utc>) -> Row<'a> {
    let theme = &state.theme;
    let selected = state.ui.selected_job.as_ref() == Some(&job.id);
    let base_style = if selected {
        Style::default()
            .bg(theme.selection_bg)
            .add_modifier(Modifier::BOLD)
    } else {
        Style::default()
    };

    let progress_cell = progress_cell(job);
    let phase_span = phase_span(job.phase, theme);
    let errs = job.progress.errors_total;
    let errs_style = if errs > 0 {
        Style::default().fg(theme.err)
    } else {
        Style::default()
    };
    let workers = job.assigned_workers.len();
    // Derive a "last update" time: most recent phase transition if
    // any, otherwise creation. The coord doesn't currently maintain
    // an explicit `updated_at` field on Job; if/when it does, this
    // can switch to reading it directly.
    let updated_at = job
        .phase_history
        .last()
        .map(|t| t.at)
        .unwrap_or(job.created_at);
    let updated = format_elapsed(updated_at, now);

    Row::new(vec![
        Cell::from(job.id.as_str().to_string()),
        Cell::from(phase_span),
        progress_cell,
        Cell::from(format!("{workers}")),
        Cell::from(Span::styled(format!("{errs}"), errs_style)),
        Cell::from(updated),
    ])
    .style(base_style)
}

fn progress_cell(job: &Job) -> Cell<'static> {
    let p = &job.progress;
    let bar = progress_bar(p.files_done, p.files_total, 10);
    // Pick a right-hand label by what we actually know:
    //
    // - `files_total > 0` → percentage (the normal steady-state once
    //   a totals event has landed; bar fills proportionally).
    // - `files_total == 0 && files_done > 0` → running file count.
    //   The coord schema has no "scan completed" event today, so
    //   pre-scan progress would otherwise read a flat ` -- ` next to
    //   a flat bar even while events stream in. The compact count
    //   surfaces motion until a denominator arrives.
    // - both zero → ` -- ` placeholder so the column stays width-
    //   stable when nothing's happened.
    let rhs = if p.files_total > 0 {
        format_pct(p.files_done, p.files_total)
    } else if p.files_done > 0 {
        format_count(p.files_done)
    } else {
        format_pct(0, 0)
    };
    Cell::from(format!("{bar} {rhs}"))
}

/// ASCII progress bar of `width` cells (full ▓, empty ░). Caller is
/// expected to render the percentage to the right.
pub(super) fn progress_bar(done: u64, total: u64, width: usize) -> String {
    if total == 0 {
        return "░".repeat(width);
    }
    let pct = (done as f64 / total as f64).clamp(0.0, 1.0);
    let filled = (pct * width as f64).round() as usize;
    let filled = filled.min(width);
    let empty = width - filled;
    let mut s = String::with_capacity(width * 3);
    for _ in 0..filled {
        s.push('▓');
    }
    for _ in 0..empty {
        s.push('░');
    }
    s
}

// =============================================================================
// Aggregates + filter/sort
// =============================================================================

#[derive(Debug, Clone, Copy, Default)]
pub struct AggregateCounts {
    pub total: usize,
    pub running: usize,
    pub paused: usize,
    /// Terminal = Completed + Failed + Cancelled.
    pub terminal: usize,
    pub files_done: u64,
    pub bytes_done: u64,
    pub errors_total: u64,
}

pub fn aggregate_counts(state: &AppState) -> AggregateCounts {
    let mut agg = AggregateCounts::default();
    for j in state.snapshot.jobs.values() {
        agg.total += 1;
        match j.phase {
            Phase::Paused => agg.paused += 1,
            Phase::Completed | Phase::Failed | Phase::Cancelled => agg.terminal += 1,
            _ => agg.running += 1,
        }
        agg.files_done = agg.files_done.saturating_add(j.progress.files_done);
        agg.bytes_done = agg.bytes_done.saturating_add(j.progress.bytes_done);
        agg.errors_total = agg.errors_total.saturating_add(j.progress.errors_total);
    }
    agg
}

/// Apply [`UiState::filter`] and [`UiState::sort`] to the snapshot's
/// jobs. Returns owned references — cheap because Job is in a
/// HashMap and we just hand out borrows.
pub fn visible_jobs(state: &AppState) -> Vec<&Job> {
    let UiState { filter, sort, .. } = &state.ui;
    let filter_lower = filter.to_lowercase();
    let mut out: Vec<&Job> = state
        .snapshot
        .jobs
        .values()
        .filter(|j| {
            if filter.is_empty() {
                return true;
            }
            j.id.as_str().to_lowercase().contains(&filter_lower)
                || j.name.to_lowercase().contains(&filter_lower)
        })
        .collect();
    match sort {
        JobSort::ById => out.sort_by(|a, b| a.id.as_str().cmp(b.id.as_str())),
        JobSort::ByPhase => out.sort_by(|a, b| {
            phase_rank(a.phase)
                .cmp(&phase_rank(b.phase))
                .then_with(|| a.id.as_str().cmp(b.id.as_str()))
        }),
        JobSort::ByProgressDesc => out.sort_by(|a, b| {
            let af = progress_fraction(a);
            let bf = progress_fraction(b);
            // Higher fraction comes first.
            bf.partial_cmp(&af)
                .unwrap_or(std::cmp::Ordering::Equal)
                .then_with(|| a.id.as_str().cmp(b.id.as_str()))
        }),
        JobSort::ByErrorsDesc => out.sort_by(|a, b| {
            b.progress
                .errors_total
                .cmp(&a.progress.errors_total)
                .then_with(|| a.id.as_str().cmp(b.id.as_str()))
        }),
    }
    out
}

fn phase_rank(phase: Phase) -> u8 {
    match phase {
        Phase::Copying => 0,
        Phase::Scanning => 1,
        Phase::Verifying => 2,
        Phase::Cutover => 3,
        Phase::Planned => 4,
        Phase::Paused => 5,
        Phase::Failed => 6,
        Phase::Cancelled => 7,
        Phase::Completed => 8,
    }
}

fn progress_fraction(j: &Job) -> f64 {
    if j.progress.files_total == 0 {
        0.0
    } else {
        j.progress.files_done as f64 / j.progress.files_total as f64
    }
}

// =============================================================================
