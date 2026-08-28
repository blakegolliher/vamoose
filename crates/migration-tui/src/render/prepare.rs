//! The list view before any job exists: what `vamoose prepare` is
//! doing (from the coord's `GET /prepare`), or how to start one.

use crate::format::{format_bytes, format_count, format_elapsed};
use crate::state::AppState;
use chrono::{DateTime, Utc};
use migration_control_protocol::schema::{PreparePhase, PrepareProgress};
use ratatui::layout::{Constraint, Direction, Layout, Rect};
use ratatui::style::{Modifier, Style};
use ratatui::text::{Line, Span};
use ratatui::widgets::{Block, Borders, Paragraph};
use ratatui::Frame;

/// Age after which a non-terminal progress object is called out as
/// stale: the reporter writes at least every five seconds.
const STALE_AFTER_SECS: i64 = 60;

pub(super) fn render_prepare_panel(
    frame: &mut Frame,
    area: Rect,
    state: &AppState,
    now: DateTime<Utc>,
) {
    let theme = &state.theme;
    let dim = Style::default().fg(theme.muted);
    let bold = Style::default().add_modifier(Modifier::BOLD);
    let lines: Vec<Line> = match state.prepare.as_ref().and_then(|r| r.progress.as_ref()) {
        None => vec![
            Line::from(""),
            Line::from(Span::styled("No job yet.", bold)),
            Line::from(""),
            Line::from(
                "Run `sudo vamoose prepare` on any host: it scans the source, builds the index, \
                 and publishes manifest.json — the run starts the moment that lands.",
            ),
            Line::from(Span::styled(
                "Its progress appears here while it runs.",
                dim,
            )),
        ],
        Some(p) => prepare_lines(p, now, theme),
    };
    // Size the panel to its content and give the rest of the screen a
    // purpose: an unbordered paragraph over the whole job-table area
    // read as "the TUI stopped drawing" below the last stage line.
    let border = Style::default().fg(theme.accent);
    let panel_h = (lines.len() as u16).saturating_add(2).min(area.height);
    let chunks = Layout::default()
        .direction(Direction::Vertical)
        .constraints([Constraint::Length(panel_h), Constraint::Min(0)])
        .split(area);
    frame.render_widget(
        Paragraph::new(lines).block(
            Block::default()
                .borders(Borders::ALL)
                .title(" prepare ")
                .border_style(border),
        ),
        chunks[0],
    );
    if chunks[1].height >= 3 {
        frame.render_widget(
            Paragraph::new(while_you_wait_lines(state))
                .block(
                    Block::default()
                        .borders(Borders::ALL)
                        .title(" while you wait ")
                        .border_style(border),
                )
                .wrap(ratatui::widgets::Wrap { trim: false }),
            chunks[1],
        );
    }
}

/// What the rest of the screen says before a job exists. Facts only:
/// where the workers are, what happens when the manifest lands, and
/// how to read the rates above.
fn while_you_wait_lines(state: &AppState) -> Vec<Line<'static>> {
    let dim = Style::default().fg(state.theme.muted);
    vec![
        Line::from(
            "Workers cannot register with the coord until the job exists; they are polling the \
             bucket for manifest.json. `systemctl is-active vamoose-worker@main` (and @b) on each \
             host is the liveness check until then.",
        ),
        Line::from(""),
        Line::from(
            "When manifest.json lands the coord seeds the job within ~15 s, every waiting worker \
             claims a shard, and this screen becomes the job list (Enter opens the job; the \
             Workers tab shows who is copying).",
        ),
        Line::from(""),
        Line::from(Span::styled(
            "The scan rate is the source cluster's READDIRPLUS rate and the index stage is local \
             CPU plus S3 PUTs — neither predicts copy throughput.",
            dim,
        )),
    ]
}

fn prepare_lines<'a>(
    p: &'a PrepareProgress,
    now: DateTime<Utc>,
    theme: &crate::theme::Theme,
) -> Vec<Line<'a>> {
    let dim = Style::default().fg(theme.muted);
    let bold = Style::default().add_modifier(Modifier::BOLD);
    let ok = Style::default().fg(theme.phase_completed);
    let run = Style::default().fg(theme.phase_copying);
    let bad = Style::default().fg(theme.phase_failed);

    let age = (now - p.updated_utc).num_seconds().max(0);
    let terminal = matches!(p.phase, PreparePhase::Done | PreparePhase::Failed);
    let mut head = vec![
        Span::styled(
            match p.phase {
                PreparePhase::Done => "Prepared ",
                PreparePhase::Failed => "Prepare FAILED ",
                _ => "Preparing ",
            },
            if p.phase == PreparePhase::Failed {
                bad
            } else {
                bold
            },
        ),
        Span::styled(p.run_id.as_str(), bold),
        Span::styled(
            format!(
                " on {} (pid {}) · started {} ago · updated {}s ago",
                p.host,
                p.pid,
                format_elapsed(p.started_utc, now),
                age
            ),
            dim,
        ),
    ];
    if !terminal && age > STALE_AFTER_SECS {
        head.push(Span::styled(
            "  NO UPDATE — prepare may have died; re-run it to resume",
            bad,
        ));
    }

    let step = |stage: PreparePhase| -> (String, Style) {
        let idx = |ph: PreparePhase| match ph {
            PreparePhase::Scan => 0,
            PreparePhase::Index => 1,
            PreparePhase::Publish => 2,
            PreparePhase::Done | PreparePhase::Failed => 3,
        };
        match (idx(stage), idx(p.phase)) {
            (s, c) if s < c => ("  ✓ ".into(), ok),
            (s, c) if s == c && p.phase == PreparePhase::Failed => ("  ✗ ".into(), bad),
            (s, c) if s == c => ("  ▶ ".into(), run),
            _ => ("    ".into(), dim),
        }
    };
    let (m1, s1) = step(PreparePhase::Scan);
    let (m2, s2) = step(PreparePhase::Index);
    let (m3, s3) = step(PreparePhase::Publish);
    let total = p
        .index
        .shards_total
        .map_or("?".to_string(), |n| n.to_string());
    // Index ETA from the upload rate so far. The progress object has
    // no index start time; it began when the scan finished, so its
    // elapsed time is (updated - started) minus the scan's own.
    let index_eta = match (p.phase, p.index.shards_total, p.index.shards_uploaded) {
        (PreparePhase::Index, Some(total), done) if done > 0 && total > done => {
            let run_secs = (p.updated_utc - p.started_utc).num_seconds().max(0) as u64;
            let index_secs = run_secs.saturating_sub(p.scan.elapsed_secs).max(1);
            let left = (total - done) * index_secs / done;
            format!(" · ~{} left", hms(left))
        }
        _ => String::new(),
    };

    let mut lines = vec![
        Line::from(head),
        Line::from(Span::styled(format!("{}  →  {}", p.source, p.dest), dim)),
        Line::from(""),
        Line::from(vec![
            Span::styled(m1, s1),
            Span::styled("1. scan     ", bold),
            Span::raw(format!(
                "{} files · {} dirs · {} errors · {} ({}/s)",
                format_count(p.scan.files),
                format_count(p.scan.dirs),
                p.scan.errors,
                hms(p.scan.elapsed_secs),
                format_count(p.scan.rate_per_sec)
            )),
        ]),
        Line::from(vec![
            Span::styled(m2, s2),
            Span::styled("2. index    ", bold),
            Span::raw(format!(
                "{}/{} shards rewritten · {} uploaded · {} rows · {}{}",
                p.index.shards_rewritten,
                total,
                p.index.shards_uploaded,
                format_count(p.index.rows_uploaded),
                format_bytes(p.index.bytes_uploaded),
                index_eta
            )),
        ]),
        Line::from(vec![
            Span::styled(m3, s3),
            Span::styled("3. publish  ", bold),
            Span::raw("manifest.json — workers start claiming the moment it lands"),
        ]),
    ];
    if let Some(msg) = &p.message {
        lines.push(Line::from(""));
        lines.push(Line::from(Span::styled(msg.as_str(), bad)));
    }
    if p.phase == PreparePhase::Done {
        lines.push(Line::from(""));
        lines.push(Line::from(Span::styled(
            "Published. The job appears here as soon as the coord seeds it.",
            dim,
        )));
    }
    lines
}

fn hms(secs: u64) -> String {
    let (h, m, s) = (secs / 3600, secs % 3600 / 60, secs % 60);
    if h > 0 {
        format!("{h}h{m:02}m")
    } else if m > 0 {
        format!("{m}m{s:02}s")
    } else {
        format!("{s}s")
    }
}
