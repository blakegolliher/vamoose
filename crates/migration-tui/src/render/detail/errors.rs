use super::super::common::header_style;
use crate::format::format_elapsed;
use crate::state::{AppState, RecentError};
use crate::theme::Theme;
use chrono::{DateTime, Utc};
use migration_control_protocol::schema::{ErrorBucket, ErrorClass, Job};
use ratatui::layout::{Constraint, Direction, Layout, Rect};
use ratatui::style::{Modifier, Style};
use ratatui::text::{Line, Span, Text};
use ratatui::widgets::{Cell, Paragraph, Row, Table};
use ratatui::Frame;

pub(super) fn render_errors_tab(
    frame: &mut Frame,
    area: Rect,
    state: &AppState,
    job: &Job,
    now: DateTime<Utc>,
) {
    let theme = &state.theme;
    // Sort buckets by count desc so the loudest class lands on top.
    let mut buckets: Vec<&ErrorBucket> = state.errors_for_job(&job.id).iter().collect();
    buckets.sort_by_key(|b| std::cmp::Reverse(b.count));
    let tail = state
        .recent_errors_for_job(&job.id)
        .map(|r| r.tail(15).collect::<Vec<_>>())
        .unwrap_or_default();

    if buckets.is_empty() && tail.is_empty() {
        let text = Text::from(vec![
            Line::from(Span::styled(
                "No errors recorded for this job.",
                Style::default().fg(theme.muted),
            )),
            Line::raw(""),
            Line::from(Span::raw(
                "ErrorEmitted events will appear here as they stream in.",
            )),
        ]);
        frame.render_widget(Paragraph::new(text), area);
        return;
    }

    // Split the body: buckets table on top, recent tail underneath.
    let chunks = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Length(1),                                       // legend
            Constraint::Length((buckets.len() as u16 + 1).clamp(2, 10)), // header + rows, capped
            Constraint::Length(1),                                       // tail header
            Constraint::Min(0),                                          // tail
        ])
        .split(area);

    let total_count: u64 = buckets.iter().map(|b| b.count).sum();
    let legend = Line::from(vec![
        Span::styled(
            format!("{} error classes", buckets.len()),
            Style::default().add_modifier(Modifier::BOLD),
        ),
        Span::raw("  ·  "),
        Span::styled(
            format!("{} total errors", total_count),
            Style::default().fg(theme.err),
        ),
        Span::raw("  ·  recent tail below"),
    ]);
    frame.render_widget(Paragraph::new(legend), chunks[0]);

    render_error_buckets_table(frame, chunks[1], &buckets, now, theme);

    let tail_header = Line::from(vec![
        Span::styled(
            "Recent",
            Style::default()
                .fg(theme.accent)
                .add_modifier(Modifier::BOLD),
        ),
        Span::raw(format!("  ({} shown, newest last)", tail.len())),
    ]);
    frame.render_widget(Paragraph::new(tail_header), chunks[2]);

    render_recent_errors_tail(frame, chunks[3], &tail, now, theme);
}

fn render_error_buckets_table(
    frame: &mut Frame,
    area: Rect,
    buckets: &[&ErrorBucket],
    now: DateTime<Utc>,
    theme: &Theme,
) {
    let header = Row::new(vec![
        Cell::from(Span::styled("Class", header_style())),
        Cell::from(Span::styled("Count", header_style())),
        Cell::from(Span::styled("First", header_style())),
        Cell::from(Span::styled("Last", header_style())),
        Cell::from(Span::styled("Retry", header_style())),
        Cell::from(Span::styled("Sample path", header_style())),
    ])
    .style(Style::default().add_modifier(Modifier::BOLD));

    let rows: Vec<Row> = buckets
        .iter()
        .map(|b| {
            let sample = b
                .sample_paths
                .first()
                .cloned()
                .unwrap_or_else(|| "—".to_string());
            let retry_label = if b.retryable { "yes" } else { "no" };
            let retry_style = if b.retryable {
                Style::default().fg(theme.warn)
            } else {
                Style::default().fg(theme.err)
            };
            Row::new(vec![
                Cell::from(format_error_class(&b.class)),
                Cell::from(Span::styled(
                    format!("{}", b.count),
                    Style::default().fg(theme.err),
                )),
                Cell::from(format_elapsed(b.first_seen, now)),
                Cell::from(format_elapsed(b.last_seen, now)),
                Cell::from(Span::styled(retry_label.to_string(), retry_style)),
                Cell::from(sample),
            ])
        })
        .collect();

    let widths = [
        Constraint::Length(18), // Class
        Constraint::Length(6),  // Count
        Constraint::Length(6),  // First
        Constraint::Length(6),  // Last
        Constraint::Length(5),  // Retry
        Constraint::Min(20),    // Sample path
    ];
    let table = Table::new(rows, widths).header(header).column_spacing(1);
    frame.render_widget(table, area);
}

fn render_recent_errors_tail(
    frame: &mut Frame,
    area: Rect,
    tail: &[&RecentError],
    now: DateTime<Utc>,
    theme: &Theme,
) {
    if tail.is_empty() {
        let p = Paragraph::new(Span::styled(
            "(no recent errors)",
            Style::default().fg(theme.muted),
        ));
        frame.render_widget(p, area);
        return;
    }
    let header = Row::new(vec![
        Cell::from(Span::styled("When", header_style())),
        Cell::from(Span::styled("Class", header_style())),
        Cell::from(Span::styled("Path", header_style())),
        Cell::from(Span::styled("Message", header_style())),
    ])
    .style(Style::default().add_modifier(Modifier::BOLD));

    let rows: Vec<Row> = tail
        .iter()
        .map(|e| {
            Row::new(vec![
                Cell::from(format_elapsed(e.at, now)),
                Cell::from(format_error_class(&e.class)),
                Cell::from(e.path.clone()),
                Cell::from(e.message.clone()),
            ])
        })
        .collect();
    let widths = [
        Constraint::Length(6),  // When
        Constraint::Length(18), // Class
        Constraint::Length(30), // Path
        Constraint::Min(20),    // Message
    ];
    let table = Table::new(rows, widths).header(header).column_spacing(1);
    frame.render_widget(table, area);
}

pub(in crate::render) fn format_error_class(c: &ErrorClass) -> String {
    match c {
        ErrorClass::Nfs3Err(code) => format!("nfs3:{code}"),
        ErrorClass::ClaimConflict => "claim-conflict".into(),
        ErrorClass::Permission => "permission".into(),
        ErrorClass::Timeout => "timeout".into(),
        ErrorClass::ChecksumMismatch => "checksum".into(),
        ErrorClass::Other(s) => format!("other:{s}"),
    }
}
