use ratatui::prelude::*;
use ratatui::widgets::*;

use flux_communication::registry::ShmemKind;

use super::app::{App, View};

pub fn render(frame: &mut Frame, app: &mut App) {
    match &app.view {
        View::List => render_list(frame, app),
        View::Detail(_) => render_detail(frame, app),
    }

    // Help popup overlays everything
    if app.show_help {
        render_help_popup(frame, frame.area());
    }
}

// ─── List view ──────────────────────────────────────────────────────────────

fn render_list(frame: &mut Frame, app: &mut App) {
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

    // Status bar
    render_status_bar(frame, app, chunks[2]);
}

// ─── Detail view ────────────────────────────────────────────────────────────

fn render_detail(frame: &mut Frame, app: &mut App) {
    let area = frame.area();

    let chunks = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Length(3),  // title
            Constraint::Length(12), // segment info
            Constraint::Min(4),    // PID table
            Constraint::Length(1), // status bar
        ])
        .split(area);

    // Title bar
    let seg = app.detail_segment();
    let title_text = seg
        .map(|s| format!(" {} — {} ", s.entry.app_name.as_str(), s.entry.type_name.as_str()))
        .unwrap_or_else(|| " Segment Detail ".into());
    let title = Paragraph::new(title_text)
        .style(Style::default().fg(Color::Cyan).bold())
        .block(Block::default().borders(Borders::ALL));
    frame.render_widget(title, chunks[0]);

    // Segment info panel
    if let Some(seg) = seg.cloned() {
        render_segment_info(frame, &seg, chunks[1]);
    }

    // PID table
    if let View::Detail(ref detail) = app.view {
        render_pid_table(frame, detail, chunks[2]);
    }

    // Status bar
    render_status_bar(frame, app, chunks[3]);

    // Cleanup confirmation overlay
    if let View::Detail(ref detail) = app.view {
        if detail.confirm_cleanup {
            render_confirm_popup(frame, area);
        }
    }
}

fn render_segment_info(frame: &mut Frame, seg: &super::app::SegmentInfo, area: Rect) {
    let status = if seg.alive { "🟢 alive" } else { "💀 dead" };

    let created = if seg.entry.created_at_nanos > 0 {
        let secs = seg.entry.created_at_nanos / 1_000_000_000;
        let ts = std::time::UNIX_EPOCH + std::time::Duration::from_secs(secs);
        humantime::format_rfc3339_seconds(ts).to_string()
    } else {
        "unknown".into()
    };

    let mut lines = vec![
        Line::from(vec![
            Span::styled("  Kind:       ", Style::default().fg(Color::DarkGray)),
            Span::raw(format!("{}", seg.entry.kind)),
        ]),
        Line::from(vec![
            Span::styled("  Status:     ", Style::default().fg(Color::DarkGray)),
            Span::raw(status),
        ]),
        Line::from(vec![
            Span::styled("  Elem size:  ", Style::default().fg(Color::DarkGray)),
            Span::raw(format!("{} bytes", seg.entry.elem_size)),
        ]),
        Line::from(vec![
            Span::styled("  Capacity:   ", Style::default().fg(Color::DarkGray)),
            Span::raw(format!("{}", seg.entry.capacity)),
        ]),
        Line::from(vec![
            Span::styled("  Type hash:  ", Style::default().fg(Color::DarkGray)),
            Span::raw(format!("0x{:016x}", seg.entry.type_hash)),
        ]),
        Line::from(vec![
            Span::styled("  Created:    ", Style::default().fg(Color::DarkGray)),
            Span::raw(created),
        ]),
        Line::from(vec![
            Span::styled("  Flink:      ", Style::default().fg(Color::DarkGray)),
            Span::raw(seg.entry.flink.as_str().to_string()),
        ]),
    ];

    // Queue-specific stats
    if seg.entry.kind == ShmemKind::Queue {
        if let Some(writes) = seg.queue_writes {
            lines.push(Line::from(vec![
                Span::styled("  Writes:     ", Style::default().fg(Color::DarkGray)),
                Span::raw(format!("{}", writes)),
            ]));
        }
    }

    let block = Block::default()
        .borders(Borders::ALL)
        .title(" Segment Info ")
        .title_alignment(Alignment::Left)
        .border_style(Style::default().fg(Color::DarkGray));
    let para = Paragraph::new(lines).block(block);
    frame.render_widget(para, area);
}

fn render_pid_table(
    frame: &mut Frame,
    detail: &super::app::DetailState,
    area: Rect,
) {
    let header = Row::new(vec!["PID", "Status", "Process", "Command"])
        .style(Style::default().fg(Color::Cyan).bold())
        .bottom_margin(1);

    let rows: Vec<Row> = detail
        .pids
        .iter()
        .map(|p| {
            let status_str = if p.alive {
                "alive".to_string()
            } else {
                "dead".to_string()
            };
            let status_style = if p.alive {
                Style::default().fg(Color::Green)
            } else {
                Style::default().fg(Color::Red)
            };
            let name = if p.name.is_empty() { "—" } else { &p.name };
            let cmd = if p.cmdline.is_empty() {
                "—".to_string()
            } else {
                truncate_str(&p.cmdline, 60)
            };

            Row::new(vec![
                Cell::from(format!("{}", p.pid)),
                Cell::from(Span::styled(status_str, status_style)),
                Cell::from(name.to_string()),
                Cell::from(cmd),
            ])
        })
        .collect();

    let empty_msg = if rows.is_empty() {
        "  No PIDs attached"
    } else {
        ""
    };

    let n_alive = detail.pids.iter().filter(|p| p.alive).count();
    let n_dead = detail.pids.iter().filter(|p| !p.alive).count();
    let title = format!(
        " Attached Processes ({} alive, {} dead) ",
        n_alive, n_dead
    );

    let widths = [
        Constraint::Length(10),
        Constraint::Length(8),
        Constraint::Length(20),
        Constraint::Min(20),
    ];

    if rows.is_empty() {
        let block = Block::default()
            .borders(Borders::ALL)
            .title(title)
            .title_alignment(Alignment::Left)
            .border_style(Style::default().fg(Color::DarkGray));
        let para = Paragraph::new(empty_msg).block(block);
        frame.render_widget(para, area);
    } else {
        let table = Table::new(rows, widths)
            .header(header)
            .block(
                Block::default()
                    .borders(Borders::ALL)
                    .title(title)
                    .title_alignment(Alignment::Left)
                    .border_style(Style::default().fg(Color::DarkGray)),
            )
            .row_highlight_style(Style::default().bg(Color::DarkGray));

        let mut state = TableState::default().with_selected(Some(detail.selected_pid));
        frame.render_stateful_widget(table, area, &mut state);
    }
}

fn render_confirm_popup(frame: &mut Frame, area: Rect) {
    let lines = vec![
        Line::from(""),
        Line::from(Span::styled(
            "  Clean up this segment?",
            Style::default().fg(Color::Yellow).bold(),
        )),
        Line::from(""),
        Line::from("  This will unlink the shared memory backing"),
        Line::from("  file and remove the flink."),
        Line::from(""),
        Line::from(vec![
            Span::styled("  Enter ", Style::default().fg(Color::Red).bold()),
            Span::raw("Confirm  "),
            Span::styled("  Esc ", Style::default().fg(Color::DarkGray).bold()),
            Span::raw("Cancel"),
        ]),
        Line::from(""),
    ];

    let popup_height = lines.len() as u16 + 2;
    let popup_width = 50;
    let popup_area = centered_rect(popup_width, popup_height, area);

    frame.render_widget(Clear, popup_area);
    let popup = Paragraph::new(lines).block(
        Block::default()
            .borders(Borders::ALL)
            .border_style(Style::default().fg(Color::Red))
            .title(" ⚠ Confirm Cleanup ")
            .title_alignment(Alignment::Center),
    );
    frame.render_widget(popup, popup_area);
}

// ─── Status bar ─────────────────────────────────────────────────────────────

fn render_status_bar(frame: &mut Frame, app: &App, area: Rect) {
    let text = if let Some((ref msg, _)) = app.status_msg {
        msg.clone()
    } else {
        match &app.view {
            View::List => " ↑↓ navigate  Enter open  ? help  q quit".into(),
            View::Detail(detail) => {
                let alive = app
                    .detail_segment()
                    .map(|s| s.alive)
                    .unwrap_or(true);
                if detail.confirm_cleanup {
                    " Enter confirm cleanup  Esc cancel".into()
                } else if !alive {
                    " Esc back  c clean up  ? help  q quit".into()
                } else {
                    " Esc back  ? help  q quit".into()
                }
            }
        }
    };

    let style = if app.status_msg.is_some() {
        Style::default().fg(Color::Yellow)
    } else {
        Style::default().fg(Color::DarkGray)
    };
    let status = Paragraph::new(text).style(style);
    frame.render_widget(status, area);
}

// ─── Helpers ────────────────────────────────────────────────────────────────

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
            Span::raw("Open segment / toggle app group"),
        ]),
        Line::from(vec![
            Span::styled("  Esc      ", Style::default().fg(Color::Yellow)),
            Span::raw("Back / close popup / quit"),
        ]),
        Line::from(vec![
            Span::styled("  c        ", Style::default().fg(Color::Yellow)),
            Span::raw("Clean up dead segment (detail view)"),
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
            Span::styled("  q        ", Style::default().fg(Color::Yellow)),
            Span::raw("Quit"),
        ]),
        Line::from(""),
        Line::from(Span::styled(
            "  Press any key to close",
            Style::default().fg(Color::DarkGray).italic(),
        )),
    ];

    let popup_height = lines.len() as u16 + 2;
    let popup_width = 46;

    let popup_area = centered_rect(popup_width, popup_height, area);

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
    Rect::new(x, y, width.min(area.width), height.min(area.height))
}

fn truncate_str(s: &str, max: usize) -> String {
    if s.len() <= max {
        s.to_string()
    } else {
        format!("{}…", &s[..max - 1])
    }
}
