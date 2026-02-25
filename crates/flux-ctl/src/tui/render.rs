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
    let header = Row::new(vec!["Name", "Kind", "Details", "PIDs", "Status"])
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
                let pid_display = if seg.pid_count <= 1 {
                    format!("{}", seg.entry.creator_pid())
                } else {
                    format!("{} (×{})", seg.entry.creator_pid(), seg.pid_count)
                };
                rows.push(Row::new(vec![
                    Cell::from(format!("  {}", seg.entry.type_name.as_str())),
                    Cell::from(kind),
                    Cell::from(details),
                    Cell::from(pid_display),
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

    // Status bar — just a hint
    let status = Paragraph::new(" Press ? for help")
        .style(Style::default().fg(Color::DarkGray));
    frame.render_widget(status, chunks[2]);

    // Help popup
    if app.show_help {
        render_help_popup(frame, area);
    }
}

fn render_help_popup(frame: &mut Frame, area: Rect) {
    let lines = vec![
        Line::from(Span::styled(
            " Keybindings ",
            Style::default().fg(Color::Cyan).bold(),
        )),
        Line::from(""),
        Line::from(vec![
            Span::styled("  ↑ / k    ", Style::default().fg(Color::Yellow)),
            Span::raw("Move up"),
        ]),
        Line::from(vec![
            Span::styled("  ↓ / j    ", Style::default().fg(Color::Yellow)),
            Span::raw("Move down"),
        ]),
        Line::from(vec![
            Span::styled("  Enter    ", Style::default().fg(Color::Yellow)),
            Span::raw("Expand / collapse app group"),
        ]),
        Line::from(vec![
            Span::styled("  r        ", Style::default().fg(Color::Yellow)),
            Span::raw("Force refresh"),
        ]),
        Line::from(vec![
            Span::styled("  ?        ", Style::default().fg(Color::Yellow)),
            Span::raw("Toggle this help"),
        ]),
        Line::from(vec![
            Span::styled("  q / Esc  ", Style::default().fg(Color::Yellow)),
            Span::raw("Quit"),
        ]),
        Line::from(""),
        Line::from(Span::styled(
            "  Press any key to close",
            Style::default().fg(Color::DarkGray).italic(),
        )),
    ];

    let popup_height = lines.len() as u16 + 2; // +2 for border
    let popup_width = 40;

    let popup_area = centered_rect(popup_width, popup_height, area);

    // Clear the area behind the popup
    frame.render_widget(Clear, popup_area);

    let popup = Paragraph::new(lines).block(
        Block::default()
            .borders(Borders::ALL)
            .border_style(Style::default().fg(Color::Cyan))
            .title(" Help ")
            .title_alignment(Alignment::Center),
    );
    frame.render_widget(popup, popup_area);
}

/// Return a centered `Rect` of fixed `width` × `height` within `area`.
fn centered_rect(width: u16, height: u16, area: Rect) -> Rect {
    let x = area.x + area.width.saturating_sub(width) / 2;
    let y = area.y + area.height.saturating_sub(height) / 2;
    Rect::new(
        x,
        y,
        width.min(area.width),
        height.min(area.height),
    )
}
