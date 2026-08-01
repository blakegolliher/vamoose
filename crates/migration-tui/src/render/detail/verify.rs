use super::super::common::{header_style, kv_key, kv_line, phase_span, section_header};
use crate::format::format_elapsed;
use crate::state::{AppState, RecentVerifyMismatch, VerifyStatus};
use crate::theme::Theme;
use chrono::{DateTime, Utc};
use migration_control_protocol::schema::Job;
use ratatui::layout::{Constraint, Direction, Layout, Rect};
use ratatui::style::{Modifier, Style};
use ratatui::text::{Line, Span, Text};
use ratatui::widgets::{Cell, Paragraph, Row, Table};
use ratatui::Frame;

pub(super) fn render_verify_tab(
    frame: &mut Frame,
    area: Rect,
    state: &AppState,
    job: &Job,
    now: DateTime<Utc>,
) {
    let theme = &state.theme;
    let status = state.verify_status_for_job(&job.id);
    let mismatches = state
        .recent_verify_mismatches_for_job(&job.id)
        .map(|r| r.tail(15).collect::<Vec<_>>())
        .unwrap_or_default();
    let never_run =
        status.last_started.is_none() && status.last_completed.is_none() && mismatches.is_empty();

    if never_run {
        let text = Text::from(vec![
            Line::from(Span::styled(
                "Verify phase has not run for this job.",
                Style::default().fg(theme.muted),
            )),
            Line::raw(""),
            Line::from(Span::raw(
                "When VerifyStarted streams in, status will appear here.",
            )),
            Line::raw(""),
            Line::from(Span::raw("Current phase: ")),
            Line::from(vec![Span::raw("  "), phase_span(job.phase, theme)]),
        ]);
        frame.render_widget(Paragraph::new(text), area);
        return;
    }

    let chunks = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Length(8), // status panel
            Constraint::Length(1), // tail header
            Constraint::Min(0),    // mismatches tail
        ])
        .split(area);

    render_verify_status_panel(frame, chunks[0], &status, job, now, theme);

    let tail_header = Line::from(vec![
        Span::styled(
            "Recent mismatches",
            Style::default()
                .fg(theme.accent)
                .add_modifier(Modifier::BOLD),
        ),
        Span::raw(format!("  ({} shown, newest last)", mismatches.len())),
    ]);
    frame.render_widget(Paragraph::new(tail_header), chunks[1]);
    render_verify_mismatches_tail(frame, chunks[2], &mismatches, now, theme);
}

fn render_verify_status_panel(
    frame: &mut Frame,
    area: Rect,
    status: &VerifyStatus,
    job: &Job,
    now: DateTime<Utc>,
    theme: &Theme,
) {
    let mut lines: Vec<Line<'static>> = Vec::new();
    lines.push(section_header("Status"));
    lines.push(Line::from(vec![
        kv_key("Phase"),
        phase_span(job.phase, theme),
    ]));
    let started = status
        .last_started
        .map(|t| format_elapsed(t, now))
        .unwrap_or_else(|| "(never)".into());
    lines.push(kv_line("Started", started));
    let completed = status
        .last_completed
        .map(|t| format_elapsed(t, now))
        .unwrap_or_else(|| "(in progress or never)".into());
    lines.push(kv_line("Completed", completed));
    let result_line: Line<'static> = match status.last_mismatches {
        None => Line::from(vec![
            kv_key("Result"),
            Span::styled("(pending)", Style::default().fg(theme.muted)),
        ]),
        Some(0) => Line::from(vec![
            kv_key("Result"),
            Span::styled(
                "0 mismatches — verify ok".to_string(),
                Style::default().fg(theme.ok),
            ),
        ]),
        Some(n) => Line::from(vec![
            kv_key("Result"),
            Span::styled(format!("{n} mismatches"), Style::default().fg(theme.err)),
        ]),
    };
    lines.push(result_line);
    frame.render_widget(Paragraph::new(Text::from(lines)), area);
}

fn render_verify_mismatches_tail(
    frame: &mut Frame,
    area: Rect,
    tail: &[&RecentVerifyMismatch],
    now: DateTime<Utc>,
    theme: &Theme,
) {
    if tail.is_empty() {
        frame.render_widget(
            Paragraph::new(Span::styled(
                "(no individual mismatches captured yet)",
                Style::default().fg(theme.muted),
            )),
            area,
        );
        return;
    }
    let header = Row::new(vec![
        Cell::from(Span::styled("When", header_style())),
        Cell::from(Span::styled("Path", header_style())),
        Cell::from(Span::styled("Expected", header_style())),
        Cell::from(Span::styled("Got", header_style())),
    ])
    .style(Style::default().add_modifier(Modifier::BOLD));
    let rows: Vec<Row> = tail
        .iter()
        .map(|m| {
            Row::new(vec![
                Cell::from(format_elapsed(m.at, now)),
                Cell::from(m.path.clone()),
                Cell::from(m.expected.clone()),
                Cell::from(m.got.clone()),
            ])
        })
        .collect();
    let widths = [
        Constraint::Length(6),
        Constraint::Min(20),
        Constraint::Length(20),
        Constraint::Length(20),
    ];
    let table = Table::new(rows, widths).header(header).column_spacing(1);
    frame.render_widget(table, area);
}
