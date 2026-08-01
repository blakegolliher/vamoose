use super::super::common::{kv_key, section_header};
use crate::theme::Theme;
use migration_control_protocol::schema::Job;
use ratatui::layout::Rect;
use ratatui::style::{Modifier, Style};
use ratatui::text::{Line, Span, Text};
use ratatui::widgets::Paragraph;
use ratatui::Frame;

pub(super) fn render_plan_tab(frame: &mut Frame, area: Rect, job: &Job, theme: &Theme) {
    // Config hash up top — operators correlate this with the
    // worker's `[coord].job_id` to confirm they're looking at the
    // same plan.
    let mut lines: Vec<Line<'static>> = vec![section_header("Config hash")];
    lines.push(Line::from(vec![
        kv_key("Hash"),
        Span::styled(
            job.config_hash.0.clone(),
            Style::default()
                .fg(theme.accent)
                .add_modifier(Modifier::BOLD),
        ),
    ]));
    lines.push(Line::raw(""));

    // Pretty JSON of the full JobConfig. The render layer is
    // line-oriented so split on '\n'; if serialization fails (it
    // can't with our types but we cover it defensively) fall back
    // to the Debug repr.
    lines.push(section_header("JobConfig"));
    let json = match serde_json::to_string_pretty(&job.config) {
        Ok(s) => s,
        Err(e) => format!("(failed to serialize: {e}) — {:?}", job.config),
    };
    for raw in json.lines() {
        lines.push(Line::raw(format!("  {raw}")));
    }

    let para = Paragraph::new(Text::from(lines));
    frame.render_widget(para, area);
}
