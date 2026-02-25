use ratatui::prelude::*;
use ratatui::widgets::*;

use flux_communication::registry::ShmemKind;

use super::app::App;

pub fn render(frame: &mut Frame, app: &mut App) {
    let area = frame.area();

    let chunks = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Length(3),
            Constraint::Min(0),
            Constraint::Length(1),
        ])
        .split(area);

    // Title
    let title = Paragraph::new(" flux-ctl — Shared Memory Monitor ")
        .style(Style::default().fg(Color::Cyan).bold())
        .block(Block::default().borders(Borders::ALL));
    frame.render_widget(title, chunks[0]);

    // Header
    let header = Row::new(vec!["Name", "Kind", "Details", "PID", "Status"])
        .style(Style::default().fg(Color::Cyan).bold())
        .bottom_margin(1);

    // Build rows
    let mut rows = Vec::new();
    for group in &app.groups {
        let icon = if group.expanded { "▼" } else { "▶" };
        rows.push(
            Row::new(vec![
                Cell::from(format!(
                    "{} {} ({} segments)",
                    icon,
                    group.name,
                    group.segments.len()
                )),
                Cell::from(""),
                Cell::from(""),
                Cell::from(""),
                Cell::from(""),
            ])
            .style(Style::default().fg(Color::Yellow).bold()),
        );

        if group.expanded {
            for seg in &group.segments {
                let status = if seg.alive { "🟢 alive" } else { "💀 dead" };
                let kind = format!("{}", seg.entry.kind);
                let details = match seg.entry.kind {
                    ShmemKind::Queue => {
                        let writes = seg
                            .queue_writes
                            .map(|w| format!(" writes={w}"))
                            .unwrap_or_default();
                        format!(
                            "cap={} elem={}B{}",
                            seg.entry.capacity, seg.entry.elem_size, writes
                        )
                    }
                    _ => format!("size={}B", seg.entry.elem_size),
                };
                rows.push(Row::new(vec![
                    Cell::from(format!("  {}", seg.entry.type_name.as_str())),
                    Cell::from(kind),
                    Cell::from(details),
                    Cell::from(format!("{}", seg.entry.pid)),
                    Cell::from(status.to_string()),
                ]));
            }
        }
    }

    if rows.is_empty() {
        rows.push(Row::new(vec![Cell::from(
            "No segments found. Is a flux app running?",
        )]));
    }

    let widths = [
        Constraint::Percentage(30),
        Constraint::Percentage(12),
        Constraint::Percentage(28),
        Constraint::Percentage(10),
        Constraint::Percentage(20),
    ];

    let table = Table::new(rows, widths)
        .header(header)
        .block(Block::default().borders(Borders::ALL))
        .row_highlight_style(Style::default().bg(Color::DarkGray));

    let mut state = TableState::default().with_selected(Some(app.selected));
    frame.render_stateful_widget(table, chunks[1], &mut state);

    // Status bar
    let status =
        Paragraph::new(" q:quit  ↑↓/jk:navigate  Enter:expand/collapse  r:refresh")
            .style(Style::default().fg(Color::DarkGray));
    frame.render_widget(status, chunks[2]);
}
