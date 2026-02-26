use ratatui::prelude::*;
use ratatui::widgets::*;

use flux_communication::registry::ShmemKind;

use super::app::{App, SelectedItem, View};

pub fn render(frame: &mut Frame, app: &mut App) {
    match &app.view {
        View::List => render_list(frame, app),
        View::Detail(_) => render_detail(frame, app),
    }

    if app.confirm_cleanup_all {
        render_confirm_all_popup(frame, app, frame.area());
    } else {
        let confirming = match &app.view {
            View::List => app.confirm_cleanup,
            View::Detail(d) => d.confirm_cleanup,
        };
        if confirming {
            render_confirm_popup(frame, frame.area());
        }
    }

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

    let n_segments: usize = app.groups.iter().map(|g| g.segments.len()).sum();
    let n_apps = app.groups.len();
    let elapsed = app.last_refresh.elapsed().as_secs_u64();
    let updated = if elapsed < 2 {
        String::new()
    } else {
        format!(" (updated {elapsed}s ago)")
    };
    let title_text = format!(
        " flux-ctl — Shared Memory Monitor ({n_segments} segments, {n_apps} apps)  [sort: {}]{updated} ",
        app.sort_mode.label()
    );
    let title = Paragraph::new(title_text)
        .style(Style::default().fg(Color::Cyan).bold())
        .block(Block::default().borders(Borders::ALL));
    frame.render_widget(title, chunks[0]);

    let header = Row::new(vec!["Name", "Kind", "Details", "PIDs", "Status"])
        .style(Style::default().fg(Color::Cyan).bold())
        .bottom_margin(1);

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
                let now_nanos = std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_nanos() as u64;
                let age_secs = if seg.entry.created_at_nanos > 0 {
                    (now_nanos.saturating_sub(seg.entry.created_at_nanos)) / 1_000_000_000
                } else {
                    0
                };
                let is_stale = !seg.alive && age_secs > 60;

                let status = if seg.poison.is_some() {
                    "☠ poisoned".to_string()
                } else if seg.alive {
                    "🟢 alive".to_string()
                } else if is_stale {
                    format!("💀 dead ({}s)", age_secs)
                } else {
                    "💀 dead".to_string()
                };
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
                let row_style = if is_stale {
                    Style::default().fg(Color::DarkGray)
                } else {
                    Style::default()
                };
                let status_style = if seg.poison.is_some() {
                    Style::default().fg(Color::Red).bold()
                } else if is_stale {
                    Style::default().fg(Color::DarkGray)
                } else {
                    Style::default()
                };
                rows.push(Row::new(vec![
                    Cell::from(format!("  {}", seg.entry.type_name.as_str())),
                    Cell::from(kind),
                    Cell::from(details),
                    Cell::from(pid_display),
                    Cell::from(Span::styled(status, status_style)),
                ]).style(row_style));
            }
        }
    }

    if rows.is_empty() {
        let msg = if app.filter_text.is_empty() {
            "No segments found. Is a flux app running?".to_string()
        } else {
            format!("No segments match filter '{}'", app.filter_text)
        };
        rows.push(Row::new(vec![Cell::from(msg)]));
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

    render_status_bar(frame, app, chunks[2]);
}

// ─── Detail view ────────────────────────────────────────────────────────────

fn render_detail(frame: &mut Frame, app: &mut App) {
    let area = frame.area();

    let chunks = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Length(3),
            Constraint::Length(14),
            Constraint::Min(4),
            Constraint::Length(1),
        ])
        .split(area);

    let seg = app.detail_segment();
    let elapsed = app.last_refresh.elapsed().as_secs_u64();
    let updated = if elapsed < 2 {
        String::new()
    } else {
        format!(" (updated {elapsed}s ago)")
    };
    let title_text = seg
        .map(|s| format!(" {} — {}{updated} ", s.entry.app_name.as_str(), s.entry.type_name.as_str()))
        .unwrap_or_else(|| format!(" Segment Detail{updated} "));
    let title = Paragraph::new(title_text)
        .style(Style::default().fg(Color::Cyan).bold())
        .block(Block::default().borders(Borders::ALL));
    frame.render_widget(title, chunks[0]);

    if let Some(seg) = seg.cloned() {
        render_segment_info(frame, &seg, chunks[1]);
    }

    if let View::Detail(ref detail) = app.view {
        render_pid_table(frame, detail, chunks[2]);
    }

    render_status_bar(frame, app, chunks[3]);
}

fn render_segment_info(frame: &mut Frame, seg: &super::app::SegmentInfo, area: Rect) {
    let status = if seg.poison.is_some() {
        "☠ poisoned"
    } else if seg.alive {
        "🟢 alive"
    } else {
        "💀 dead"
    };

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
        Line::from(vec![
            Span::styled("  OS ID:      ", Style::default().fg(Color::DarkGray)),
            Span::raw(
                std::fs::read_to_string(seg.entry.flink.as_str())
                    .unwrap_or_else(|_| "unknown".into())
                    .trim()
                    .to_string(),
            ),
        ]),
        Line::from(vec![
            Span::styled("  Backing:    ", Style::default().fg(Color::DarkGray)),
            Span::raw(
                crate::discovery::backing_file_size(seg.entry.flink.as_str())
                    .map(format_bytes)
                    .unwrap_or_else(|| "unavailable".into()),
            ),
        ]),
    ];

    if seg.entry.kind == ShmemKind::Queue
        && let Some(writes) = seg.queue_writes
    {
        lines.push(Line::from(vec![
            Span::styled("  Writes:     ", Style::default().fg(Color::DarkGray)),
            Span::raw(format!("{}", writes)),
        ]));

        if let (Some(fill), Some(cap)) = (seg.queue_fill, seg.queue_capacity) {
            let pct = if cap > 0 {
                (fill as f64 / cap as f64) * 100.0
            } else {
                0.0
            };
            let filled = (pct / 10.0).round() as usize;
            let filled = filled.min(10);
            let bar = format!("{}{}", "█".repeat(filled), "░".repeat(10 - filled));
            lines.push(Line::from(vec![
                Span::styled("  Fill:       ", Style::default().fg(Color::DarkGray)),
                Span::styled(
                    bar,
                    if pct > 80.0 {
                        Style::default().fg(Color::Red)
                    } else if pct > 50.0 {
                        Style::default().fg(Color::Yellow)
                    } else {
                        Style::default().fg(Color::Green)
                    },
                ),
                Span::raw(format!(" {:.0}% ({}/{})", pct, fill, cap)),
            ]));
        }
    }

    if let Some(ref poison) = seg.poison {
        lines.push(Line::from(vec![
            Span::styled("  ☠ Poison:   ", Style::default().fg(Color::Red).bold()),
            Span::styled(
                format!(
                    "{}/{} slots poisoned (first at slot {})",
                    poison.n_poisoned, poison.total_slots, poison.first_slot
                ),
                Style::default().fg(Color::Red),
            ),
        ]));
    }

    let block = Block::default()
        .borders(Borders::ALL)
        .title(" Segment Info ")
        .title_alignment(Alignment::Left)
        .border_style(Style::default().fg(Color::DarkGray));
    frame.render_widget(Paragraph::new(lines).block(block), area);
}

fn render_pid_table(frame: &mut Frame, detail: &super::app::DetailState, area: Rect) {
    let header = Row::new(vec!["PID", "Status", "Process", "Command"])
        .style(Style::default().fg(Color::Cyan).bold())
        .bottom_margin(1);

    let rows: Vec<Row> = detail
        .pids
        .iter()
        .map(|p| {
            let status_str = if p.alive { "alive" } else { "dead" };
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
                Cell::from(Span::styled(status_str.to_string(), status_style)),
                Cell::from(name.to_string()),
                Cell::from(cmd),
            ])
        })
        .collect();

    let n_alive = detail.pids.iter().filter(|p| p.alive).count();
    let n_dead = detail.pids.iter().filter(|p| !p.alive).count();
    let title = format!(" Attached Processes ({} alive, {} dead) ", n_alive, n_dead);

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
        frame.render_widget(
            Paragraph::new("  No PIDs attached").block(block),
            area,
        );
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

    let popup_area = centered_rect(50, lines.len() as u16 + 2, area);
    frame.render_widget(Clear, popup_area);
    frame.render_widget(
        Paragraph::new(lines).block(
            Block::default()
                .borders(Borders::ALL)
                .border_style(Style::default().fg(Color::Red))
                .title(" ⚠ Confirm Cleanup ")
                .title_alignment(Alignment::Center),
        ),
        popup_area,
    );
}

fn render_confirm_all_popup(frame: &mut Frame, app: &App, area: Rect) {
    let dead_count: usize = app
        .pending_cleanup_flinks
        .as_ref()
        .map(|f| f.len())
        .unwrap_or(0);

    let lines = vec![
        Line::from(""),
        Line::from(Span::styled(
            format!("  Destroy all {dead_count} stale segments?"),
            Style::default().fg(Color::Yellow).bold(),
        )),
        Line::from(""),
        Line::from("  This will unlink every dead segment's shared"),
        Line::from("  memory backing file and remove its flink."),
        Line::from(""),
        Line::from(vec![
            Span::styled("  Enter ", Style::default().fg(Color::Red).bold()),
            Span::raw("Confirm  "),
            Span::styled("  Esc ", Style::default().fg(Color::DarkGray).bold()),
            Span::raw("Cancel"),
        ]),
        Line::from(""),
    ];

    let popup_area = centered_rect(52, lines.len() as u16 + 2, area);
    frame.render_widget(Clear, popup_area);
    frame.render_widget(
        Paragraph::new(lines).block(
            Block::default()
                .borders(Borders::ALL)
                .border_style(Style::default().fg(Color::Red))
                .title(" ⚠ Confirm Destroy All ")
                .title_alignment(Alignment::Center),
        ),
        popup_area,
    );
}

// ─── Status bar ─────────────────────────────────────────────────────────────

fn render_status_bar(frame: &mut Frame, app: &App, area: Rect) {
    let confirming_single = match &app.view {
        View::List => app.confirm_cleanup,
        View::Detail(d) => d.confirm_cleanup,
    };

    let has_any_dead = app.groups.iter().any(|g| g.segments.iter().any(|s| !s.alive));

    let text = if app.filter_mode {
        format!(" / {}█", app.filter_text)
    } else if app.confirm_cleanup_all || confirming_single {
        " Enter confirm  Esc cancel".into()
    } else if let Some((ref msg, _)) = app.status_msg {
        msg.clone()
    } else {
        let filter_hint = if !app.filter_text.is_empty() {
            format!("  [filter: {}]", app.filter_text)
        } else {
            String::new()
        };
        match &app.view {
            View::List => {
                let on_dead_seg = matches!(
                    app.selected_item(),
                    Some(SelectedItem::Segment(_, _, seg)) if !seg.alive
                );
                let base = match (on_dead_seg, has_any_dead) {
                    (true, _) => " ↑↓ navigate  Enter open  d destroy  D destroy all  / filter  s sort  ? help  q quit",
                    (false, true) => " ↑↓ navigate  Enter open  D destroy all  / filter  s sort  ? help  q quit",
                    _ => " ↑↓ navigate  Enter open  / filter  s sort  ? help  q quit",
                };
                format!("{}{}", base, filter_hint)
            }
            View::Detail(_) => {
                let alive = app.detail_segment().map(|s| s.alive).unwrap_or(true);
                let base = match (!alive, has_any_dead) {
                    (true, _) => " Esc back  d destroy  D destroy all  ? help  q quit",
                    (false, true) => " Esc back  D destroy all  ? help  q quit",
                    _ => " Esc back  ? help  q quit",
                };
                base.into()
            }
        }
    };

    let style = if app.filter_mode || app.status_msg.is_some() {
        Style::default().fg(Color::Yellow)
    } else {
        Style::default().fg(Color::DarkGray)
    };
    frame.render_widget(Paragraph::new(text).style(style), area);
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
            Span::styled("  ↑ / k      ", Style::default().fg(Color::Yellow)),
            Span::raw("Move up"),
        ]),
        Line::from(vec![
            Span::styled("  ↓ / j      ", Style::default().fg(Color::Yellow)),
            Span::raw("Move down"),
        ]),
        Line::from(vec![
            Span::styled("  Home / g   ", Style::default().fg(Color::Yellow)),
            Span::raw("Jump to first"),
        ]),
        Line::from(vec![
            Span::styled("  End / G    ", Style::default().fg(Color::Yellow)),
            Span::raw("Jump to last"),
        ]),
        Line::from(vec![
            Span::styled("  PgUp       ", Style::default().fg(Color::Yellow)),
            Span::raw("Page up (10 rows)"),
        ]),
        Line::from(vec![
            Span::styled("  PgDn       ", Style::default().fg(Color::Yellow)),
            Span::raw("Page down (10 rows)"),
        ]),
        Line::from(vec![
            Span::styled("  Enter      ", Style::default().fg(Color::Yellow)),
            Span::raw("Open segment / toggle app group"),
        ]),
        Line::from(vec![
            Span::styled("  Esc        ", Style::default().fg(Color::Yellow)),
            Span::raw("Back / clear filter / quit"),
        ]),
        Line::from(vec![
            Span::styled("  /          ", Style::default().fg(Color::Yellow)),
            Span::raw("Filter segments by name"),
        ]),
        Line::from(vec![
            Span::styled("  s          ", Style::default().fg(Color::Yellow)),
            Span::raw("Cycle sort (name → kind → status)"),
        ]),
        Line::from(vec![
            Span::styled("  d          ", Style::default().fg(Color::Yellow)),
            Span::raw("Destroy dead segment"),
        ]),
        Line::from(vec![
            Span::styled("  D          ", Style::default().fg(Color::Yellow)),
            Span::raw("Destroy all dead segments"),
        ]),
        Line::from(vec![
            Span::styled("  r          ", Style::default().fg(Color::Yellow)),
            Span::raw("Force refresh"),
        ]),
        Line::from(vec![
            Span::styled("  ?          ", Style::default().fg(Color::Yellow)),
            Span::raw("Toggle this help"),
        ]),
        Line::from(vec![
            Span::styled("  q          ", Style::default().fg(Color::Yellow)),
            Span::raw("Quit"),
        ]),
        Line::from(""),
        Line::from(Span::styled(
            "  Press ? to close",
            Style::default().fg(Color::DarkGray).italic(),
        )),
    ];

    let popup_area = centered_rect(52, lines.len() as u16 + 2, area);
    frame.render_widget(Clear, popup_area);
    frame.render_widget(
        Paragraph::new(lines).block(
            Block::default()
                .borders(Borders::ALL)
                .border_style(Style::default().fg(Color::Cyan))
                .title(" Help ")
                .title_alignment(Alignment::Center),
        ),
        popup_area,
    );
}

fn centered_rect(width: u16, height: u16, area: Rect) -> Rect {
    let x = area.x + area.width.saturating_sub(width) / 2;
    let y = area.y + area.height.saturating_sub(height) / 2;
    Rect::new(x, y, width.min(area.width), height.min(area.height))
}

fn format_bytes(bytes: u64) -> String {
    const KIB: u64 = 1024;
    const MIB: u64 = 1024 * KIB;
    const GIB: u64 = 1024 * MIB;
    if bytes >= GIB {
        format!("{:.1} GiB", bytes as f64 / GIB as f64)
    } else if bytes >= MIB {
        format!("{:.1} MiB", bytes as f64 / MIB as f64)
    } else if bytes >= KIB {
        format!("{:.1} KiB", bytes as f64 / KIB as f64)
    } else {
        format!("{} B", bytes)
    }
}

fn truncate_str(s: &str, max: usize) -> String {
    if s.chars().count() <= max {
        s.to_string()
    } else {
        let truncated: String = s.chars().take(max.saturating_sub(1)).collect();
        format!("{truncated}…")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn truncate_str_ascii() {
        assert_eq!(truncate_str("hello", 10), "hello");
        assert_eq!(truncate_str("hello world", 5), "hell…");
        assert_eq!(truncate_str("", 5), "");
        assert_eq!(truncate_str("abc", 3), "abc");
    }

    #[test]
    fn truncate_str_multibyte_utf8() {
        // CJK characters (3 bytes each)
        let cjk = "你好世界测试";
        assert_eq!(truncate_str(cjk, 6), cjk);
        assert_eq!(truncate_str(cjk, 4), "你好世…");

        // Emoji (4 bytes each)
        let emoji = "🔥🚀💥🎉🌍";
        assert_eq!(truncate_str(emoji, 5), emoji);
        assert_eq!(truncate_str(emoji, 3), "🔥🚀…");

        // Mixed ASCII + multi-byte
        let mixed = "café résumé";
        assert_eq!(truncate_str(mixed, 5), "café…");
    }

    #[test]
    fn truncate_str_edge_cases() {
        assert_eq!(truncate_str("a", 1), "a");
        assert_eq!(truncate_str("ab", 1), "…");
        assert_eq!(truncate_str("abc", 0), "…");
    }
}
