mod errors;
mod overview;
mod plan;
mod verify;
pub(super) mod workers;

#[cfg(test)]
pub(in crate::render) use errors::format_error_class;

use self::errors::render_errors_tab;
use self::overview::render_overview_tab;
use self::plan::render_plan_tab;
use self::verify::render_verify_tab;
use self::workers::render_workers_tab;
use super::chrome::{render_bottom_hints, render_top_banner};
use crate::state::{AppState, Tab};
use crate::theme::Theme;
use chrono::{DateTime, Utc};
use migration_control_protocol::schema::JobId;
use ratatui::layout::{Constraint, Direction, Layout, Rect};
use ratatui::style::{Color, Modifier, Style};
use ratatui::text::{Line, Span, Text};
use ratatui::widgets::{Paragraph, Tabs};
use ratatui::Frame;

pub(super) fn render_detail(
    frame: &mut Frame,
    state: &AppState,
    now: DateTime<Utc>,
    job_id: &JobId,
    tab: Tab,
) {
    let chunks = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Length(1), // top banner (shared with List)
            Constraint::Length(1), // tab bar
            Constraint::Min(0),    // tab body
            Constraint::Length(1), // bottom hints (mode-aware)
        ])
        .split(frame.area());

    render_top_banner(frame, chunks[0], state, now);
    let counts = tab_counts(state, job_id);
    render_tab_bar(frame, chunks[1], tab, &counts, &state.theme);
    render_tab_body(frame, chunks[2], state, job_id, tab, now);
    render_bottom_hints(frame, chunks[3], state, now);
    // Modal rendering happens in the top-level `render()` so it
    // also applies to the List view's Help overlay.
}

// =============================================================================
// Detail view: tab bar + per-tab bodies
// =============================================================================

/// Live counters surfaced in the tab bar so the operator knows
/// where activity is happening without switching tabs. Computed
/// once per render against the current job's state.
#[derive(Debug, Default, Clone, Copy)]
struct TabCounts {
    workers: usize,
    error_classes: usize,
    verify_mismatches: usize,
}

fn tab_counts(state: &AppState, job_id: &JobId) -> TabCounts {
    let workers = state.workers_for_job(job_id).len();
    let error_classes = state.errors_for_job(job_id).len();
    let verify_mismatches = state
        .recent_verify_mismatches_for_job(job_id)
        .map(|r| r.len())
        .unwrap_or(0);
    TabCounts {
        workers,
        error_classes,
        verify_mismatches,
    }
}

fn render_tab_bar(frame: &mut Frame, area: Rect, current: Tab, counts: &TabCounts, theme: &Theme) {
    let titles: Vec<Line<'static>> = Tab::all()
        .into_iter()
        .map(|t| tab_label_with_counter(t, counts, theme))
        .collect();
    let selected = Tab::all()
        .into_iter()
        .position(|t| t == current)
        .unwrap_or(0);
    let tabs = Tabs::new(titles)
        .select(selected)
        .divider("│")
        .highlight_style(
            Style::default()
                .fg(theme.accent)
                .add_modifier(Modifier::BOLD | Modifier::UNDERLINED),
        );
    frame.render_widget(tabs, area);
}

/// Build the styled label for one tab: "Workers (3)" with the
/// counter parenthetical color-coded (red on Errors when nonzero,
/// yellow on Verify when nonzero, default otherwise). Tabs with no
/// natural counter (Overview, Plan) render plain.
fn tab_label_with_counter(t: Tab, c: &TabCounts, theme: &Theme) -> Line<'static> {
    let (n, color) = match t {
        Tab::Overview | Tab::Plan => (None, Color::Reset),
        Tab::Workers => (Some(c.workers), Color::Reset),
        Tab::Errors => {
            let style = if c.error_classes > 0 {
                theme.err
            } else {
                Color::Reset
            };
            (Some(c.error_classes), style)
        }
        Tab::Verify => {
            let style = if c.verify_mismatches > 0 {
                theme.warn
            } else {
                Color::Reset
            };
            (Some(c.verify_mismatches), style)
        }
    };
    let mut spans = vec![Span::raw(t.label())];
    if let Some(n) = n {
        if n == 0 {
            // No count shown when zero — keeps the bar clean for
            // freshly-opened jobs.
        } else {
            spans.push(Span::raw(" "));
            spans.push(Span::styled(format!("({n})"), Style::default().fg(color)));
        }
    }
    Line::from(spans)
}

fn render_tab_body(
    frame: &mut Frame,
    area: Rect,
    state: &AppState,
    job_id: &JobId,
    tab: Tab,
    now: DateTime<Utc>,
) {
    let theme = &state.theme;
    let job = match state.job(job_id) {
        Some(j) => j,
        None => {
            // Defensive: the operator might have entered Detail
            // just as the job was archived. Show a single line so
            // they know to press Esc.
            let text = Text::from(vec![
                Line::from(Span::styled(
                    format!("Job '{}' not found.", job_id.as_str()),
                    Style::default().fg(theme.err),
                )),
                Line::raw(""),
                Line::from(Span::raw("Press Esc to return to the jobs list.")),
            ]);
            frame.render_widget(Paragraph::new(text), area);
            return;
        }
    };
    match tab {
        Tab::Overview => render_overview_tab(frame, area, state, job, now),
        Tab::Workers => render_workers_tab(frame, area, state, job, now),
        Tab::Errors => render_errors_tab(frame, area, state, job, now),
        Tab::Plan => render_plan_tab(frame, area, job, theme),
        Tab::Verify => render_verify_tab(frame, area, state, job, now),
    }
}
