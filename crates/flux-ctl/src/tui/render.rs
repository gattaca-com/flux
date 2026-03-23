use flux_communication::ShmemKind;
use ratatui::{prelude::*, widgets::*};

use super::app::{App, DetailFocus, FluxTab, SelectedItem, View};
use crate::discovery::format_bytes;

pub fn render(frame: &mut Frame, app: &mut App) {
    let area = frame.area();

    // Split the terminal area into a 1-row tab bar at the top and the
    // remaining content area below.
    let [tab_bar_area, content_area] =
        Layout::vertical([Constraint::Length(1), Constraint::Min(0)]).areas(area);

    render_tab_bar(frame, app, tab_bar_area);

    match app.tab {
        FluxTab::Apps => match &app.view {
            View::List | View::Tiles => render_list(frame, app, content_area),
            View::Detail(_) => render_detail(frame, app, content_area),
        },
        FluxTab::Tiles => render_tiles(frame, app, content_area),
    }

    if app.confirm_cleanup_all {
        render_confirm_all_popup(frame, app, area);
    } else {
        let confirming = match app.tab {
            FluxTab::Tiles => false,
            FluxTab::Apps => match &app.view {
                View::List | View::Tiles => app.confirm_cleanup,
                View::Detail(d) => d.confirm_cleanup,
            },
        };
        if confirming {
            render_confirm_popup(frame, area);
        }
    }

    if app.show_help {
        render_help_popup(frame, area, app.tab);
    }
}

fn render_tab_bar(frame: &mut Frame, app: &App, area: Rect) {
    use ratatui::style::palette::tailwind;

    let titles = [FluxTab::Apps, FluxTab::Tiles].map(FluxTab::title);

    let palette = app.tab.palette();
    let highlight_style = Style::new().fg(palette.c100).bg(palette.c600).bold();
    let bar_style = Style::new().bg(tailwind::SLATE.c950);

    let selected = app.tab as usize;
    Tabs::new(titles)
        .select(selected)
        .highlight_style(highlight_style)
        .style(bar_style)
        .padding("", "")
        .divider(" ")
        .render(area, frame.buffer_mut());
}
fn render_list(frame: &mut Frame, app: &mut App, area: Rect) {
    let chunks = Layout::default()
        .direction(Direction::Vertical)
        .constraints([Constraint::Length(3), Constraint::Min(0), Constraint::Length(1)])
        .split(area);

    let n_segments: usize = app.groups.iter().map(|g| g.segments.len()).sum();
    let n_apps = app.groups.len();
    let total_size: u64 =
        app.groups.iter().flat_map(|g| &g.segments).map(|s| s.entry.backing_size as u64).sum();
    let elapsed = app.last_refresh.elapsed().as_secs_u64();
    let updated = if elapsed < 2 { String::new() } else { format!(" (updated {elapsed}s ago)") };
    let title_text = format!(
        " flux-ctl — Shared Memory Monitor ({n_segments} segments, {n_apps} apps, {})  [sort: {}]{updated} ",
        format_bytes(total_size),
        app.sort_mode.label()
    );
    let title = Paragraph::new(title_text)
        .style(Style::default().fg(Color::Cyan).bold())
        .block(Block::default().borders(Borders::ALL));
    frame.render_widget(title, chunks[0]);

    let header = Row::new(vec!["Name", "Kind", "Details", "Conns", "Status"])
        .style(Style::default().fg(Color::Cyan).bold())
        .bottom_margin(1);

    let mut rows = Vec::new();
    for group in &app.groups {
        let icon = if group.expanded { "▼" } else { "▶" };
        rows.push(
            Row::new(vec![
                Cell::from(format!("{} {} ({} segments)", icon, group.name, group.segments.len())),
                Cell::from(""),
                Cell::from(""),
                Cell::from(""),
                Cell::from(""),
            ])
            .style(Style::default().fg(Color::Yellow).bold()),
        );

        if group.expanded {
            for seg in &group.segments {
                let is_stale = !seg.alive;

                let status = if seg.poison.is_some() {
                    "☠ poisoned".to_string()
                } else if seg.alive {
                    "🟢 alive".to_string()
                } else {
                    "💀 dead".to_string()
                };
                let kind = format!("{}", seg.entry.kind);
                let details = match seg.entry.kind {
                    ShmemKind::Queue => {
                        let rate = match seg.msgs_per_sec {
                            Some(r) if r >= 1_000_000.0 => format!(" {:.1}M/s", r / 1_000_000.0),
                            Some(r) if r >= 1_000.0 => format!(" {:.1}K/s", r / 1_000.0),
                            Some(r) => format!(" {:.0}msg/s", r),
                            None => String::new(),
                        };
                        let lagger = match seg.max_lagger_pct {
                            Some(pct) if pct >= 100.0 => " lag:⚠>100%".to_string(),
                            Some(pct) => format!(" lag:{:.0}%", pct),
                            None => String::new(),
                        };
                        format!(
                            "cap={} elem={}B{}{}",
                            seg.entry.capacity, seg.entry.elem_size, rate, lagger
                        )
                    }
                    _ => format!("size={}B", seg.entry.elem_size),
                };
                let n_pids = seg.pids.len();
                let conn_display = if n_pids > 0 { n_pids.to_string() } else { "—".to_string() };
                let row_style =
                    if is_stale { Style::default().fg(Color::DarkGray) } else { Style::default() };
                let status_style = if seg.poison.is_some() {
                    Style::default().fg(Color::Red).bold()
                } else if is_stale {
                    Style::default().fg(Color::DarkGray)
                } else {
                    Style::default()
                };
                rows.push(
                    Row::new(vec![
                        Cell::from(format!("  {}", seg.entry.type_name.as_str())),
                        Cell::from(kind),
                        Cell::from(details),
                        Cell::from(conn_display),
                        Cell::from(Span::styled(status, status_style)),
                    ])
                    .style(row_style),
                );
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
        Constraint::Percentage(30),
        Constraint::Percentage(8),
        Constraint::Percentage(20),
    ];

    let table = Table::new(rows, widths)
        .header(header)
        .block(Block::default().borders(Borders::ALL))
        .row_highlight_style(Style::default().bg(Color::DarkGray));

    app.table_state.select(Some(app.selected));
    frame.render_stateful_widget(table, chunks[1], &mut app.table_state);

    render_status_bar(frame, app, chunks[2]);
}
fn render_detail(frame: &mut Frame, app: &mut App, area: Rect) {
    // Top-level vertical layout: title | segment info | bottom panels | status bar
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
    let updated = if elapsed < 2 { String::new() } else { format!(" (updated {elapsed}s ago)") };
    let title_text = seg
        .map(|s| {
            format!(" {} — {}{updated} ", s.entry.app_name.as_str(), s.entry.type_name.as_str())
        })
        .unwrap_or_else(|| format!(" Segment Detail{updated} "));
    let title = Paragraph::new(title_text)
        .style(Style::default().fg(Color::Cyan).bold())
        .block(Block::default().borders(Borders::ALL));
    frame.render_widget(title, chunks[0]);

    if let Some(seg) = seg.cloned() {
        render_segment_info(frame, &seg, chunks[1]);
    }

    if let View::Detail(ref detail) = app.view {
        let has_groups = !detail.consumer_groups.is_empty();
        if has_groups {
            // Horizontal split: consumer groups (left) | processes (right)
            let bottom = Layout::default()
                .direction(Direction::Horizontal)
                .constraints([Constraint::Percentage(50), Constraint::Percentage(50)])
                .split(chunks[2]);

            let seg = app.detail_segment();
            let write_pos = seg.and_then(|s| s.queue_writes);
            let capacity = seg.and_then(|s| s.queue_capacity);
            render_consumer_groups(frame, detail, write_pos, capacity, bottom[0]);
            render_pid_table(frame, detail, bottom[1]);
        } else {
            render_pid_table(frame, detail, chunks[2]);
        }
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

    if seg.entry.kind == ShmemKind::Queue &&
        let Some(writes) = seg.queue_writes
    {
        let rate_str = match seg.msgs_per_sec {
            Some(r) if r >= 1_000_000.0 => format!("  ({:.1} M/s)", r / 1_000_000.0),
            Some(r) if r >= 1_000.0 => format!("  ({:.1} K/s)", r / 1_000.0),
            Some(r) => format!("  ({:.0} msg/s)", r),
            None => String::new(),
        };
        lines.push(Line::from(vec![
            Span::styled("  Writes:     ", Style::default().fg(Color::DarkGray)),
            Span::raw(format!("{writes}")),
            Span::styled(rate_str, Style::default().fg(Color::Cyan)),
        ]));

        if let (Some(pos), Some(cap)) = (seg.queue_fill, seg.queue_capacity) {
            let bar_width: usize = 20;
            let filled = if cap > 0 {
                ((pos as f64 / cap as f64) * bar_width as f64).round() as usize
            } else {
                0
            };
            let filled = filled.min(bar_width);

            // Position of the first poisoned slot within the bar (if any).
            // Use total_slots from PoisonInfo (same read as first_slot) to
            // avoid races between the QueueStats and PoisonInfo reads.
            let poison_col = seg.poison.as_ref().map(|p| {
                if p.total_slots > 0 {
                    ((p.first_slot as f64 / p.total_slots as f64) * bar_width as f64).round()
                        as usize
                } else {
                    0
                }
                .min(bar_width.saturating_sub(1))
            });

            let cyan = Style::default().fg(Color::Cyan);
            let red = Style::default().fg(Color::Red).bold();

            let mut bar_spans =
                vec![Span::styled("  Write Pos:  ", Style::default().fg(Color::DarkGray))];
            for i in 0..bar_width {
                let ch = if i < filled { "█" } else { "░" };
                let style = if poison_col == Some(i) { red } else { cyan };
                bar_spans.push(Span::styled(ch, style));
            }
            bar_spans.push(Span::raw(format!(" {pos}/{cap}")));
            lines.push(Line::from(bar_spans));
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

fn render_consumer_groups(
    frame: &mut Frame,
    detail: &super::app::DetailState,
    write_pos: Option<usize>,
    capacity: Option<usize>,
    area: Rect,
) {
    let focused = detail.focus == DetailFocus::ConsumerGroups;

    let header = Row::new(vec!["Group", "Cursor", "msgs/s", "Lag", "Backlog"])
        .style(Style::default().fg(Color::Cyan).bold())
        .bottom_margin(1);

    let cap = capacity.unwrap_or(0);
    let bar_width: usize = 12;

    let rows: Vec<Row> = detail
        .consumer_groups
        .iter()
        .map(|cg| {
            let lag = write_pos.map(|w| w.saturating_sub(cg.cursor));
            let lag_str = lag.map(|l| format!("{l}")).unwrap_or_else(|| "—".into());
            let lag_style = if cap > 0 {
                match lag {
                    Some(l) if l * 4 >= cap * 3 => Style::default().fg(Color::Red),
                    Some(l) if l * 4 >= cap => Style::default().fg(Color::Yellow),
                    _ => Style::default().fg(Color::Green),
                }
            } else {
                match lag {
                    Some(l) if l > 1000 => Style::default().fg(Color::Red),
                    Some(l) if l > 100 => Style::default().fg(Color::Yellow),
                    _ => Style::default(),
                }
            };

            let rate_str = match cg.msgs_per_sec {
                Some(r) if r >= 1_000.0 => format!("{:.1}", r),
                Some(r) if r >= 1.0 => format!("{:.1}", r),
                Some(r) if r > 0.0 => format!("{:.2}", r),
                Some(_) => "0".into(),
                None => "—".into(),
            };

            // Backlog bar: filled portion = lag / capacity
            let bar = if cap > 0 {
                let filled = lag
                    .map(|l| ((l as f64 / cap as f64) * bar_width as f64).round() as usize)
                    .unwrap_or(0)
                    .min(bar_width);
                let empty = bar_width - filled;
                let bar_str = format!("{}{}", "█".repeat(filled), "░".repeat(empty));
                Span::styled(bar_str, lag_style)
            } else {
                Span::raw("—")
            };

            Row::new(vec![
                Cell::from(truncate_str(&cg.label, 40)),
                Cell::from(format!("{}", cg.cursor)),
                Cell::from(rate_str),
                Cell::from(Span::styled(lag_str, lag_style)),
                Cell::from(bar),
            ])
        })
        .collect();

    let title = format!(" Consumer Groups ({}) ", detail.consumer_groups.len());
    let border_style = if focused {
        Style::default().fg(Color::Cyan)
    } else {
        Style::default().fg(Color::DarkGray)
    };

    let widths = [
        Constraint::Percentage(40),
        Constraint::Length(14),
        Constraint::Length(12),
        Constraint::Length(10),
        Constraint::Min(14),
    ];

    let table = Table::new(rows, widths)
        .header(header)
        .block(
            Block::default()
                .borders(Borders::ALL)
                .title(title)
                .title_alignment(Alignment::Left)
                .border_style(border_style),
        )
        .row_highlight_style(Style::default().bg(Color::DarkGray));

    if focused {
        let mut state = TableState::default().with_selected(Some(detail.selected_group));
        frame.render_stateful_widget(table, area, &mut state);
    } else {
        frame.render_widget(table, area);
    }
}

fn render_pid_table(frame: &mut Frame, detail: &super::app::DetailState, area: Rect) {
    let focused = detail.focus == DetailFocus::Processes;
    let border_style = if focused {
        Style::default().fg(Color::Cyan)
    } else {
        Style::default().fg(Color::DarkGray)
    };

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
            let cmd =
                if p.cmdline.is_empty() { "—".to_string() } else { truncate_str(&p.cmdline, 60) };

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
            .border_style(border_style);
        frame.render_widget(Paragraph::new("  No PIDs attached").block(block), area);
    } else {
        let table = Table::new(rows, widths)
            .header(header)
            .block(
                Block::default()
                    .borders(Borders::ALL)
                    .title(title)
                    .title_alignment(Alignment::Left)
                    .border_style(border_style),
            )
            .row_highlight_style(Style::default().bg(Color::DarkGray));

        if focused {
            let mut state = TableState::default().with_selected(Some(detail.selected_pid));
            frame.render_stateful_widget(table, area, &mut state);
        } else {
            frame.render_widget(table, area);
        }
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
    let dead_count: usize = app.pending_cleanup_flinks.as_ref().map(|f| f.len()).unwrap_or(0);

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

fn render_tiles(frame: &mut Frame, app: &mut App, area: Rect) {
    let chunks = Layout::default()
        .direction(Direction::Vertical)
        .constraints([Constraint::Length(3), Constraint::Min(0), Constraint::Length(1)])
        .split(area);

    let n_tiles = app.tile_metrics.total_tiles();
    let n_apps = app.tile_metrics.groups().iter().filter(|g| !g.is_empty()).count();
    let title_text = format!(
        " flux-ctl — Tile Metrics ({n_tiles} tiles, {n_apps} apps)  [window: {:.0}s] ",
        app.tile_metrics.stats_window_secs()
    );
    let title = Paragraph::new(title_text)
        .style(Style::default().fg(Color::Magenta).bold())
        .block(Block::default().borders(Borders::ALL));
    frame.render_widget(title, chunks[0]);

    // Build rows — app headers + per-tile gauge rows
    let inner = chunks[1];
    let block = Block::default().borders(Borders::ALL);
    let inner_area = block.inner(inner);
    frame.render_widget(block, inner);

    if n_tiles == 0 {
        frame.render_widget(
            Paragraph::new("No tile metrics found. Is a flux app running?")
                .style(Style::default().fg(Color::DarkGray)),
            inner_area,
        );
        render_status_bar(frame, app, chunks[2]);
        return;
    }

    // Each tile gets 2 rows: stats line + gauge bar.
    // App headers get 1 row.
    let window_secs = app.tile_metrics.stats_window_secs();
    let mut row_specs: Vec<TileRow> = Vec::new();
    for group in app.tile_metrics.groups() {
        if group.is_empty() {
            continue;
        }
        row_specs.push(TileRow::AppHeader(group.app_name.clone(), group.tile_order.len()));
        for name in &group.tile_order {
            let stats = group.tile_stats(name, window_secs);
            row_specs.push(TileRow::Tile(name.clone(), stats));
        }
    }

    // Calculate how many rows we can show (2 lines per tile, 1 per header)
    let available_height = inner_area.height as usize;
    let selected = app.tile_metrics.selected;

    // Scroll offset calculation
    let mut cumulative_heights: Vec<(usize, usize)> = Vec::new(); // (row_idx, y_start)
    let mut y = 0usize;
    for (i, row) in row_specs.iter().enumerate() {
        cumulative_heights.push((i, y));
        y += match row {
            TileRow::AppHeader(..) => 1,
            TileRow::Tile(..) => 2,
        };
    }
    let total_content_height = y;

    // Find y position of selected row
    let selected_y = cumulative_heights.get(selected).map(|(_, y)| *y).unwrap_or(0);
    let selected_height = row_specs
        .get(selected)
        .map(|r| match r {
            TileRow::AppHeader(..) => 1,
            TileRow::Tile(..) => 2,
        })
        .unwrap_or(1);

    // Simple scroll: ensure selected is visible
    let scroll_offset = if selected_y + selected_height > available_height {
        (selected_y + selected_height).saturating_sub(available_height)
    } else {
        0
    };

    // Render visible rows
    let mut current_y = 0usize;
    for (i, row) in row_specs.iter().enumerate() {
        let row_height = match row {
            TileRow::AppHeader(..) => 1,
            TileRow::Tile(..) => 2,
        };

        if current_y < scroll_offset {
            current_y += row_height;
            continue;
        }
        if current_y >= scroll_offset + available_height {
            break;
        }

        let render_y = (current_y - scroll_offset) as u16;
        let is_selected = i == selected;

        match row {
            TileRow::AppHeader(name, count) => {
                let area = Rect::new(
                    inner_area.x,
                    inner_area.y + render_y,
                    inner_area.width,
                    1.min(inner_area.height.saturating_sub(render_y)),
                );
                let style = if is_selected {
                    Style::default().fg(Color::Yellow).bold().bg(Color::DarkGray)
                } else {
                    Style::default().fg(Color::Yellow).bold()
                };
                frame.render_widget(
                    Paragraph::new(format!("▶ {} ({count} tiles)", name)).style(style),
                    area,
                );
            }
            TileRow::Tile(name, stats) => {
                let lines_available = inner_area.height.saturating_sub(render_y);
                if lines_available == 0 {
                    current_y += row_height;
                    continue;
                }

                // Stats line
                let stats_area =
                    Rect::new(inner_area.x, inner_area.y + render_y, inner_area.width, 1);
                let (stats_text, stats_color) = match stats {
                    Some(s) => (
                        format!(
                            "  {:<30} util:{:5.1}%  avg:{:<10} min:{:<10} max:{:<10} loops:{}",
                            truncate_str(name, 30),
                            s.utilisation * 100.0,
                            s.busy_avg,
                            s.busy_min,
                            s.busy_max,
                            s.loop_count,
                        ),
                        Color::White,
                    ),
                    None => (
                        format!("  {:<30} waiting for data…", truncate_str(name, 30)),
                        Color::DarkGray,
                    ),
                };
                let text_style = if is_selected {
                    Style::default().fg(stats_color).bg(Color::DarkGray)
                } else {
                    Style::default().fg(stats_color)
                };
                frame.render_widget(Paragraph::new(stats_text).style(text_style), stats_area);

                // Gauge bar (if we have room)
                if lines_available >= 2 {
                    let gauge_area = Rect::new(
                        inner_area.x + 2,
                        inner_area.y + render_y + 1,
                        inner_area.width.saturating_sub(4),
                        1,
                    );
                    let util = stats.map(|s| s.utilisation).unwrap_or(0.0);
                    let gauge = Gauge::default()
                        .ratio(util.clamp(0.0, 1.0))
                        .gauge_style(util_colour(util))
                        .label(format!("{:5.1}%", util * 100.0));
                    frame.render_widget(gauge, gauge_area);
                }
            }
        }

        current_y += row_height;
    }

    // Scroll indicator
    if total_content_height > available_height {
        let pct = if total_content_height <= available_height {
            0
        } else {
            (scroll_offset * 100) / (total_content_height - available_height)
        };
        let indicator = Paragraph::new(format!(" {pct}%"))
            .style(Style::default().fg(Color::DarkGray))
            .alignment(Alignment::Right);
        let indicator_area = Rect::new(
            inner_area.x,
            inner_area.y + inner_area.height.saturating_sub(1),
            inner_area.width,
            1,
        );
        frame.render_widget(indicator, indicator_area);
    }

    render_status_bar(frame, app, chunks[2]);
}

/// Intermediate type for tile view row rendering.
enum TileRow {
    AppHeader(String, usize),
    Tile(String, Option<crate::tui::tile_metrics::TileStats>),
}

fn util_colour(util: f64) -> Color {
    if util < 0.3 {
        Color::Green
    } else if util < 0.7 {
        Color::Yellow
    } else {
        Color::Red
    }
}

fn render_status_bar(frame: &mut Frame, app: &App, area: Rect) {
    let confirming_single = match app.tab {
        FluxTab::Tiles => false,
        FluxTab::Apps => match &app.view {
            View::List | View::Tiles => app.confirm_cleanup,
            View::Detail(d) => d.confirm_cleanup,
        },
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
        match app.tab {
            FluxTab::Tiles => " ↑↓ navigate  ←→ tabs  Esc back  ? help  q quit".into(),
            FluxTab::Apps => match &app.view {
                View::List | View::Tiles => {
                    let on_dead_seg = matches!(
                        app.selected_item(),
                        Some(SelectedItem::Segment(_, _, seg)) if !seg.alive
                    );
                    let dead_toggle = if app.hide_dead { "a show dead" } else { "a hide dead" };
                    let base = match (on_dead_seg, has_any_dead) {
                        (true, _) => {
                            format!(
                                " ↑↓ navigate  ←→ tabs  Enter open  d/D destroy  {dead_toggle}  / filter  s sort  ? help  q quit"
                            )
                        }
                        (false, true) => {
                            format!(
                                " ↑↓ navigate  ←→ tabs  Enter open  D destroy all  {dead_toggle}  / filter  s sort  ? help  q quit"
                            )
                        }
                        _ => format!(
                            " ↑↓ navigate  ←→ tabs  Enter open  {dead_toggle}  / filter  s sort  ? help  q quit"
                        ),
                    };
                    format!("{}{}", base, filter_hint)
                }
                View::Detail(detail) => {
                    let alive = app.detail_segment().map(|s| s.alive).unwrap_or(true);
                    let tab_hint =
                        if !detail.consumer_groups.is_empty() { "Tab switch panel  " } else { "" };
                    match (!alive, has_any_dead) {
                        (true, _) => {
                            format!(" Esc back  {tab_hint}d destroy  D destroy all  ? help  q quit")
                        }
                        (false, true) => {
                            format!(" Esc back  {tab_hint}D destroy all  ? help  q quit")
                        }
                        _ => format!(" Esc back  {tab_hint}? help  q quit"),
                    }
                }
            },
        }
    };

    let style = if app.filter_mode || app.status_msg.is_some() {
        Style::default().fg(Color::Yellow)
    } else {
        Style::default().fg(Color::DarkGray)
    };
    frame.render_widget(Paragraph::new(text).style(style), area);
}
fn render_help_popup(frame: &mut Frame, area: Rect, tab: FluxTab) {
    let key = |k: &str| Span::styled(format!("  {k:<13}"), Style::default().fg(Color::Yellow));

    let mut lines = vec![
        Line::from(Span::styled(" Keybindings ", Style::default().fg(Color::Cyan).bold())),
        Line::from(""),
        // ── Navigation (common to all tabs) ──
        Line::from(vec![key("↑ / k"), Span::raw("Move up")]),
        Line::from(vec![key("↓ / j"), Span::raw("Move down")]),
        Line::from(vec![key("Home / g"), Span::raw("Jump to first")]),
        Line::from(vec![key("End / G"), Span::raw("Jump to last")]),
        Line::from(vec![key("PgUp"), Span::raw("Page up (10 rows)")]),
        Line::from(vec![key("PgDn"), Span::raw("Page down (10 rows)")]),
        Line::from(""),
        // ── Tab switching (common) ──
        Line::from(vec![key("← / →"), Span::raw("Switch tab")]),
        Line::from(vec![key("1 / 2"), Span::raw("Jump to tab (Apps / Tiles)")]),
    ];

    match tab {
        FluxTab::Apps => {
            lines.push(Line::from(""));
            lines.push(Line::from(Span::styled(
                " Apps tab ",
                Style::default().fg(Color::Cyan).bold(),
            )));
            lines
                .push(Line::from(vec![key("Enter"), Span::raw("Open segment / toggle app group")]));
            lines
                .push(Line::from(vec![key("Esc / Bksp"), Span::raw("Back / clear filter / quit")]));
            lines.push(Line::from(vec![key("/"), Span::raw("Filter segments by name")]));
            lines.push(Line::from(vec![
                key("s"),
                Span::raw("Cycle sort (name → kind → status → activity)"),
            ]));
            lines.push(Line::from(vec![key("a"), Span::raw("Toggle show/hide dead segments")]));
            lines
                .push(Line::from(vec![key("Tab"), Span::raw("Switch focus (groups ↔ processes)")]));
            lines.push(Line::from(vec![key("d"), Span::raw("Destroy dead segment")]));
            lines.push(Line::from(vec![key("D"), Span::raw("Destroy all dead segments")]));
            lines.push(Line::from(vec![key("r"), Span::raw("Force refresh")]));
        }
        FluxTab::Tiles => {
            lines.push(Line::from(""));
            lines.push(Line::from(Span::styled(
                " Tiles tab ",
                Style::default().fg(Color::Cyan).bold(),
            )));
            lines.push(Line::from(vec![key("Esc / t"), Span::raw("Back to Apps tab")]));
        }
    }

    lines.push(Line::from(""));
    lines.push(Line::from(vec![key("?"), Span::raw("Toggle this help")]));
    lines.push(Line::from(vec![key("q"), Span::raw("Quit")]));
    lines.push(Line::from(""));
    lines.push(Line::from(Span::styled(
        "  Press ? to close",
        Style::default().fg(Color::DarkGray).italic(),
    )));

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
