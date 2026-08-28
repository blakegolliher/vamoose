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
    // Run start = the moment work first flowed (Planned -> Copying
    // in phase_history). "Created" is the registry seed time and can
    // predate the actual run by hours; operators asked for the real
    // start, elapsed, and the honest average rate since then — the
    // instantaneous rate swings with per-region RPC cost (dir-dense
    // stretches read low even at a saturated server).
    if let Some(t) = job
        .phase_history
        .iter()
        .find(|t| t.to == migration_control_protocol::schema::Phase::Copying)
    {
        let secs = (now - t.at).num_seconds().max(1);
        let avg = job.progress.files_done as f64 / secs as f64;
        lines.push(kv_line(
            "Started",
            format!(
                "{} ({} ago)  ·  avg {:.0} files/s since start",
                t.at.format("%Y-%m-%d %H:%M:%SZ"),
                format_elapsed(t.at, now),
                avg,
            ),
        ));
        if job.progress.files_total > 0 && avg > 0.0 {
            let remaining = job
                .progress
                .files_total
                .saturating_sub(job.progress.files_done);
            let eta_secs = (remaining as f64 / avg) as i64;
            let eta_at = now + chrono::Duration::seconds(eta_secs);
            lines.push(kv_line(
                "ETA",
                format!(
                    "~{} ({})",
                    human_duration(eta_secs),
                    eta_at.format("%H:%M:%SZ"),
                ),
            ));
        }
    }
    lines.push(kv_line("Files", files_summary(job)));
    lines.push(kv_line("Bytes", bytes_summary(job)));
    lines.push(kv_line("Errors", format!("{}", job.progress.errors_total)));

    // ---- Throughput (1s / 1m / 5m) --------------------------------
    let bps1 = state.job_bytes_per_sec(&job.id, 1, now).unwrap_or(0.0);
    let bps60 = state.job_bytes_per_sec(&job.id, 60, now).unwrap_or(0.0);
    let bps300 = state.job_bytes_per_sec(&job.id, 300, now).unwrap_or(0.0);
    let fps1 = state.job_files_per_sec(&job.id, 1, now);
    let fps60 = state.job_files_per_sec(&job.id, 60, now);
    let fps300 = state.job_files_per_sec(&job.id, 300, now);
    // Files/s first: metadata-heavy migrations move millions of tiny
    // files, where bytes/s reads as noise (measured 59-byte average
    // files on the 600M rig run).
    lines.push(kv_line(
        "Files/s",
        format!("1s {fps1:.0}   1m {fps60:.0}   5m {fps300:.0}"),
    ));
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
        format!(
            "{} connected of {} assigned (see Workers tab)",
            state.connected_worker_count(&job.id),
            job.assigned_workers.len()
        ),
    ));
    let err_buckets = state.errors_for_job(&job.id);
    lines.push(kv_line(
        "Error classes",
        format!("{} (see Errors tab)", err_buckets.len()),
    ));

    let para = Paragraph::new(Text::from(lines));
    frame.render_widget(para, area);
}

/// Compact duration: "3h12m", "48m", "90s".
fn human_duration(secs: i64) -> String {
    if secs >= 3600 {
        format!("{}h{:02}m", secs / 3600, (secs % 3600) / 60)
    } else if secs >= 60 {
        format!("{}m", secs / 60)
    } else {
        format!("{}s", secs)
    }
}
