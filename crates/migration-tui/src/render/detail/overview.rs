use super::super::common::{
    bytes_summary, files_summary, kv_key, kv_line, phase_span, section_header,
};
use crate::format::{format_bytes, format_elapsed};
use crate::state::AppState;
use chrono::{DateTime, Utc};
use migration_control_protocol::schema::Job;
use ratatui::layout::Rect;
use ratatui::text::{Line, Text};
use ratatui::widgets::Paragraph;
use ratatui::Frame;

pub(super) fn render_overview_tab(
    frame: &mut Frame,
    area: Rect,
    state: &AppState,
    job: &Job,
    now: DateTime<Utc>,
) {
    let theme = &state.theme;
    let mut lines: Vec<Line<'static>> = Vec::new();

    // ---- Identity --------------------------------------------------
    lines.push(section_header("Identity"));
    lines.push(kv_line("ID", job.id.as_str().to_string()));
    lines.push(kv_line("Name", job.name.clone()));
    lines.push(kv_line(
        "Owner",
        if job.owner.is_empty() {
            "(unknown)".to_string()
        } else {
            job.owner.clone()
        },
    ));
    lines.push(kv_line("Created", format_elapsed(job.created_at, now)));
    lines.push(Line::raw(""));

    // ---- Source / Dest --------------------------------------------
    lines.push(section_header("Source / Dest"));
    lines.push(kv_line("Source", job.source.clone()));
    lines.push(kv_line("Dest", job.dest.clone()));
    lines.push(Line::raw(""));

    // ---- Status ---------------------------------------------------
    lines.push(section_header("Status"));
    lines.push(Line::from(vec![
        kv_key("Phase"),
        phase_span(job.phase, theme),
    ]));
    lines.push(kv_line("Files", files_summary(job)));
    lines.push(kv_line("Bytes", bytes_summary(job)));
    lines.push(kv_line("Errors", format!("{}", job.progress.errors_total)));

    // ---- Throughput (1s / 1m / 5m) --------------------------------
    let bps1 = state.job_bytes_per_sec(&job.id, 1, now).unwrap_or(0.0);
    let bps60 = state.job_bytes_per_sec(&job.id, 60, now).unwrap_or(0.0);
    let bps300 = state.job_bytes_per_sec(&job.id, 300, now).unwrap_or(0.0);
    lines.push(kv_line(
        "Throughput",
        format!(
            "1s {}/s   1m {}/s   5m {}/s",
            format_bytes(bps1 as u64),
            format_bytes(bps60 as u64),
            format_bytes(bps300 as u64),
        ),
    ));
    lines.push(Line::raw(""));

    // ---- Workers + Errors counts (full breakdowns in their tabs) ----
    lines.push(section_header("Activity"));
    lines.push(kv_line(
        "Workers",
        format!("{} assigned (see Workers tab)", job.assigned_workers.len()),
    ));
    let err_buckets = state.errors_for_job(&job.id);
    lines.push(kv_line(
        "Error classes",
        format!("{} (see Errors tab)", err_buckets.len()),
    ));

    let para = Paragraph::new(Text::from(lines));
    frame.render_widget(para, area);
}
