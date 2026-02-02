use std::collections::HashMap;

use flux::{
    communication::{
        queue::{Consumer, Queue},
        shmem_dir_queues_string,
    },
    tile::metrics::TileSample,
    timing::{Duration, Nanos, Repeater},
};
use ratatui::{
    Frame,
    layout::{Constraint, Layout, Rect},
    style::{Color, Style, Stylize},
    text::{Line, Span},
    widgets::{Block, Borders, Gauge, Paragraph},
};

use crate::{data::BucketedDataCache, tui::PlotSettings};

#[derive(Clone, Debug)]
struct TileData {
    consumer: Consumer<TileSample>,
    samples: BucketedDataCache<Nanos, TileSample>,
    cached_stats: Option<(f64, f64, TileStats)>,
}

impl TileData {
    fn new(consumer: Consumer<TileSample>) -> Self {
        Self { consumer, samples: BucketedDataCache::default(), cached_stats: None }
    }

    fn update(&mut self) {
        let mut got_new = false;
        while self.consumer.consume(|sample| {
            self.samples.insert(sample.window_end, *sample);
            got_new = true;
        }) {}

        if got_new {
            self.cached_stats = None;
        }
    }

    fn stats_in_range(&mut self, x_min: f64, x_max: f64) -> Option<TileStats> {
        if let Some((cached_min, cached_max, stats)) = self.cached_stats &&
            (cached_min - x_min).abs() < 1.0 &&
            (cached_max - x_max).abs() < 1.0
        {
            return Some(stats);
        }

        let mut total_busy = 0u64;
        let mut total_ticks = 0u64;
        let mut busy_min = u64::MAX;
        let mut busy_max = 0u64;
        let mut busy_sum = 0u64;
        let mut busy_count = 0u32;
        let mut sample_count = 0usize;

        for (nanos, sample) in self.samples.iter() {
            let sample_t = nanos.0 as f64;
            if sample_t < x_min || sample_t > x_max {
                continue;
            }

            sample_count += 1;
            total_busy += sample.busy_ticks;
            total_ticks += sample.total_ticks();

            if sample.busy_count > 0 {
                busy_min = busy_min.min(sample.busy_min());
                busy_max = busy_max.max(sample.busy_max);
                busy_sum += sample.busy_sum;
                busy_count += sample.busy_count;
            }
        }

        if sample_count == 0 {
            self.cached_stats = None;
            return None;
        }

        let stats = TileStats {
            utilisation: if total_ticks > 0 { total_busy as f64 / total_ticks as f64 } else { 0.0 },
            busy_avg: if busy_count > 0 {
                Duration(busy_sum / busy_count as u64)
            } else {
                Duration(0)
            },
            busy_min: Duration(if busy_min == u64::MAX { 0 } else { busy_min }),
            busy_max: Duration(busy_max),
        };

        self.cached_stats = Some((x_min, x_max, stats));
        Some(stats)
    }
}

#[derive(Clone, Copy, Debug)]
struct TileStats {
    utilisation: f64,
    busy_avg: Duration,
    busy_min: Duration,
    busy_max: Duration,
}

#[derive(Clone, Debug)]
pub struct TileMetricsView {
    tiles: HashMap<String, TileData>,
    tile_order: Vec<String>,
    discover_repeater: Repeater,
}

impl Default for TileMetricsView {
    fn default() -> Self {
        Self {
            tiles: HashMap::default(),
            tile_order: Vec::default(),
            discover_repeater: Repeater::every(Duration::from_secs(10)),
        }
    }
}

impl TileMetricsView {
    pub fn discover(&mut self, app_name: &str) {
        if !self.discover_repeater.fired() {
            return;
        }

        let dir = shmem_dir_queues_string(app_name);
        let Ok(entries) = std::fs::read_dir(&dir) else {
            return;
        };

        for entry in entries.flatten() {
            let path = entry.path();
            let Some(fname) = path.file_name().and_then(|s| s.to_str()) else {
                continue;
            };

            if !fname.starts_with("tilemetrics-") {
                continue;
            }

            let tile_name = fname.strip_prefix("tilemetrics-").unwrap().to_string();
            if self.tiles.contains_key(&tile_name) {
                continue;
            }

            let Ok(queue) = std::panic::catch_unwind(|| Queue::<TileSample>::open_shared(&path))
            else {
                continue;
            };

            let consumer = Consumer::from(queue).without_log();
            self.tiles.insert(tile_name.clone(), TileData::new(consumer));
            self.tile_order.push(tile_name);
        }

        self.tile_order.sort();
    }

    pub fn update(&mut self, app_name: &str) {
        self.discover(app_name);
        for tile in self.tiles.values_mut() {
            tile.update();
        }
    }

    pub fn render(&mut self, frame: &mut Frame, area: Rect, plot_settings: &PlotSettings) {
        let block = Block::default().borders(Borders::ALL).title("Tile Metrics");
        let inner = block.inner(area);
        frame.render_widget(block, area);

        if self.tiles.is_empty() {
            frame.render_widget(Paragraph::new("No tile metrics available"), inner);
            return;
        }

        let (x_min, x_max) = plot_settings.range();

        let mut rows: Vec<(&str, Option<TileStats>)> = Vec::new();
        for name in &self.tile_order {
            if let Some(tile) = self.tiles.get_mut(name) {
                let stats = tile.stats_in_range(x_min, x_max);
                rows.push((name, stats));
            }
        }

        if rows.is_empty() {
            frame.render_widget(Paragraph::new("No data in time range"), inner);
            return;
        }

        let max_name = rows.iter().map(|(n, _)| n.len().min(40)).max().unwrap_or(10);

        let constraints: Vec<_> =
            rows.iter().flat_map(|_| [Constraint::Length(1), Constraint::Length(1)]).collect();

        let row_areas = Layout::vertical(constraints).split(inner);

        for (i, (name, range_stats)) in rows.iter().enumerate() {
            let row_base = i * 2;
            if row_base + 1 >= row_areas.len() {
                break;
            }

            let line = format_stats_line(name, max_name, *range_stats);
            frame.render_widget(Paragraph::new(line), row_areas[row_base]);

            let util = range_stats.map(|s| s.utilisation).unwrap_or(0.0);
            let gauge = Gauge::default()
                .ratio(util.clamp(0.0, 1.0))
                .gauge_style(util_colour(util))
                .label(format!("{:5.1}%", util * 100.0));
            frame.render_widget(gauge, row_areas[row_base + 1]);
        }
    }
}

fn format_stats_line(name: &str, max_name: usize, range_stats: Option<TileStats>) -> Line<'static> {
    // truncate long names
    let display_name = if name.len() > max_name {
        format!("{}…", &name[..max_name - 1])
    } else {
        format!("{:<width$}", name, width = max_name)
    };

    let (stats_str, stats_color) = match range_stats {
        Some(s) => (
            format!(
                "util:{:5.1}%  avg:{:<10} min:{:<10} max:{:<10}",
                s.utilisation * 100.0,
                s.busy_avg,
                s.busy_min,
                s.busy_max
            ),
            Color::White,
        ),
        None => ("no data in range".to_string(), Color::DarkGray),
    };

    Line::from(vec![
        Span::styled(display_name, Style::default().bold()),
        Span::raw(" │ "),
        Span::styled(stats_str, Style::default().fg(stats_color)),
    ])
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
