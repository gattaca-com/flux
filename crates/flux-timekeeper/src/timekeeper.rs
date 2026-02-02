use std::collections::HashMap;

use crossterm::event::KeyCode;
use flux::{
    TimingMessage,
    communication::{
        queue::{Consumer, Queue},
        shmem_dir_queues_string,
    },
    persistence::Persistable,
    timing::{Duration, Instant, Nanos, Repeater},
};
use ratatui::{
    prelude::*,
    widgets::{HighlightSpacing, LineGauge, List, ListItem, ListState, Paragraph},
};
use serde::{Deserialize, Serialize};
use strum::AsRefStr;
use style::palette::tailwind;

use crate::{
    data::{self, BucketedDataCache, Statistics, TileMetricsView},
    tui::{Plot, PlotSettings, RenderFlags},
    types::{DataPoint, MsgPer10Sec},
};

const NUM_DATAPOINTS: usize = 750;
const SAMPLES_PER_PERCENTILE: usize = 128;

#[derive(Clone, Debug)]
pub struct TimerDataState {
    pub latency_consumer: Consumer<TimingMessage>,
    pub processing_consumer: Consumer<TimingMessage>,
    pub direction: Direction,

    reuse_buf: Vec<f64>,
}

impl TimerDataState {
    pub fn new(
        latency_consumer: Consumer<TimingMessage>,
        processing_consumer: Consumer<TimingMessage>,
    ) -> Self {
        Self {
            latency_consumer: latency_consumer.without_log(),
            processing_consumer: processing_consumer.without_log(),
            direction: Direction::Vertical,
            reuse_buf: Vec::with_capacity(1024),
        }
    }

    pub fn report(
        &mut self,
        data: &mut TimerData,
        frame: &mut Frame,
        rect: Rect,
        plot_settings: &PlotSettings,
    ) {
        if data.data_latency.is_empty() && data.data_processing.is_empty() {
            frame.render_widget(Paragraph::new("No timing data to display"), rect);
            return;
        }
        let latency_plot = (!data.data_latency.is_empty()).then(|| {
            let mut latency = Plot::<Duration>::new("");
            if data.stats_latency.flags.contains(RenderFlags::ShowAverages) {
                data.data_latency.add_to_plot(
                    plot_settings,
                    format!("latency_avg_{}", data.name),
                    |bid| Some((bid.time as f64, bid.avg as f64)),
                    data::utils::mean,
                    &mut latency,
                    Color::White,
                    Some("avg".to_string()),
                    None,
                    None,
                );
            }
            if data.stats_latency.flags.contains(RenderFlags::ShowMin) {
                data.data_latency.add_to_plot(
                    plot_settings,
                    format!("latency_min_{}", data.name),
                    |bid| Some((bid.time as f64, bid.min as f64)),
                    data::utils::mean,
                    &mut latency,
                    Color::Green,
                    Some("min".to_string()),
                    None,
                    None,
                );
            }
            if data.stats_latency.flags.contains(RenderFlags::ShowMax) {
                data.data_latency.add_to_plot(
                    plot_settings,
                    format!("latency_max_{}", data.name),
                    |bid| Some((bid.time as f64, bid.max as f64)),
                    data::utils::mean,
                    &mut latency,
                    Color::Red,
                    Some("max".to_string()),
                    None,
                    None,
                );
            }
            if data.stats_latency.flags.contains(RenderFlags::ShowMedian) {
                data.data_latency.add_to_plot(
                    plot_settings,
                    format!("latency_med_{}", data.name),
                    |bid| Some((bid.time as f64, bid.median as f64)),
                    data::utils::mean,
                    &mut latency,
                    Color::Yellow,
                    Some("med".to_string()),
                    None,
                    None,
                );
            }
            latency
        });
        let processing_plot = (!data.data_processing.is_empty()).then(|| {
            let mut processing = Plot::<Duration>::new("");
            if data.stats_processing.flags.contains(RenderFlags::ShowAverages) {
                data.data_processing.add_to_plot(
                    plot_settings,
                    format!("processing_avg_{}", data.name),
                    |bid| Some((bid.time as f64, bid.avg as f64)),
                    data::utils::mean,
                    &mut processing,
                    Color::White,
                    Some("avg".to_string()),
                    None,
                    None,
                );
            }
            if data.stats_processing.flags.contains(RenderFlags::ShowMin) {
                data.data_processing.add_to_plot(
                    plot_settings,
                    format!("processing_min_{}", data.name),
                    |bid| Some((bid.time as f64, bid.min as f64)),
                    data::utils::mean,
                    &mut processing,
                    Color::Green,
                    Some("min".to_string()),
                    None,
                    None,
                );
            }
            if data.stats_processing.flags.contains(RenderFlags::ShowMax) {
                data.data_processing.add_to_plot(
                    plot_settings,
                    format!("processing_max_{}", data.name),
                    |bid| Some((bid.time as f64, bid.max as f64)),
                    data::utils::mean,
                    &mut processing,
                    Color::Red,
                    Some("max".to_string()),
                    None,
                    None,
                );
            }
            if data.stats_processing.flags.contains(RenderFlags::ShowMedian) {
                data.data_processing.add_to_plot(
                    plot_settings,
                    format!("processing_med_{}", data.name),
                    |bid| Some((bid.time as f64, bid.median as f64)),
                    data::utils::mean,
                    &mut processing,
                    Color::Yellow,
                    Some("med".to_string()),
                    None,
                    None,
                );
            }
            processing
        });
        let msgs_per_sec = if !data.data_processing.is_empty() {
            &mut data.data_processing
        } else {
            &mut data.data_latency
        };

        let mut msgs_per_sec_plot = Plot::<MsgPer10Sec>::new("");
        msgs_per_sec.add_to_plot(
            plot_settings,
            format!("msgs_per_sec_{}", data.name),
            |bid| Some((bid.time as f64, bid.rate.0 as f64)),
            data::utils::mean,
            &mut msgs_per_sec_plot,
            Color::White,
            None,
            None,
            None,
        );

        let percentiles = self.window_percentiles(msgs_per_sec, plot_settings, |d| d.rate.0 as f64);
        let msgs_title = if let Some((p90, p99)) = percentiles {
            format!("  Msg/s (mean plotted)  |  sample p90={p90:.0}  sample p99={p99:.0}  ")
        } else {
            "  Msg/s  ".to_string()
        };

        let percentiles =
            self.window_percentiles(&data.data_processing, plot_settings, |d| d.avg as f64);
        let processing_title = if let Some((p90, p99)) = percentiles {
            format!(
                "  Processing (mean plotted)  |  sample p90={}  sample p99={}  ",
                Nanos::from(p90 as u128),
                Nanos::from(p99 as u128)
            )
        } else {
            "  Processing  ".to_string()
        };

        let percentiles =
            self.window_percentiles(&data.data_latency, plot_settings, |d| d.avg as f64);
        let latency_title = if let Some((p90, p99)) = percentiles {
            format!(
                "  Latency (mean plotted)  |  sample p90={}  sample p99={}  ",
                Nanos::from(p90 as u128),
                Nanos::from(p99 as u128)
            )
        } else {
            "  Latency  ".to_string()
        };

        match (latency_plot, processing_plot) {
            (None, None) => unreachable!(),
            (None, Some(mut plot)) => {
                let layout = Layout::new(self.direction, [
                    Constraint::Percentage(80),
                    Constraint::Percentage(20),
                ])
                .split(rect);
                plot.render(frame, layout[0], Some(processing_title), plot_settings);
                msgs_per_sec_plot.render(frame, layout[1], Some(msgs_title), plot_settings);
            }
            (Some(mut plot), None) => {
                let layout = Layout::new(self.direction, [
                    Constraint::Percentage(80),
                    Constraint::Percentage(20),
                ])
                .split(rect);
                plot.render(frame, layout[0], Some(latency_title), plot_settings);
                msgs_per_sec_plot.render(frame, layout[1], Some(msgs_title), plot_settings);
            }
            (Some(mut latency), Some(mut processing)) => {
                let layout = Layout::new(self.direction, [
                    Constraint::Percentage(40),
                    Constraint::Percentage(40),
                    Constraint::Percentage(20),
                ])
                .split(rect);
                latency.render(frame, layout[0], Some(latency_title), plot_settings);
                processing.render(frame, layout[1], Some(processing_title), plot_settings);
                msgs_per_sec_plot.render(frame, layout[2], Some(msgs_title), plot_settings);
            }
        }
    }

    fn window_percentiles(
        &mut self,
        data: &BucketedDataCache<Nanos, DataPoint<Duration>>,
        plot: &PlotSettings,
        extract: impl Fn(&DataPoint<Duration>) -> f64,
    ) -> Option<(f64, f64)> {
        self.reuse_buf.clear();

        let (x0, x1) = plot.range();

        for (_, d) in data.iter() {
            let x = d.time as f64;
            if x >= x0 && x <= x1 {
                self.reuse_buf.push(extract(d));
            }
        }

        let n = self.reuse_buf.len();
        if n < 8 {
            return None;
        }

        let p90_idx = ((n - 1) as f64 * 0.90).ceil() as usize;
        let p99_idx = ((n - 1) as f64 * 0.99).ceil() as usize;

        self.reuse_buf.select_nth_unstable_by(p90_idx, |a, b| a.partial_cmp(b).unwrap());
        let p90 = self.reuse_buf[p90_idx];

        self.reuse_buf.select_nth_unstable_by(p99_idx, |a, b| a.partial_cmp(b).unwrap());
        let p99 = self.reuse_buf[p99_idx];

        Some((p90, p99))
    }
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct TimerData {
    pub name: String,
    pub stats_latency: Statistics<Duration>,
    pub data_latency: BucketedDataCache<Nanos, DataPoint<Duration>>,
    pub stats_processing: Statistics<Duration>,
    pub data_processing: BucketedDataCache<Nanos, DataPoint<Duration>>,
    pub tot_processing: Duration,
}

impl TimerData {
    pub fn new(
        name: String,
        samples_per_median: usize,
        n_datapoints: usize,
        clock_overhead: Duration,
    ) -> Self {
        Self {
            name,
            stats_latency: Statistics::new(
                "Latency".into(),
                samples_per_median,
                n_datapoints,
                clock_overhead,
            ),
            data_latency: BucketedDataCache::default(),
            stats_processing: Statistics::new(
                "Processing".into(),
                samples_per_median,
                n_datapoints,
                clock_overhead,
            ),
            data_processing: BucketedDataCache::default(),
            tot_processing: Duration::ZERO,
        }
    }

    pub fn handle_messages(&mut self, state: &mut TimerDataState) {
        self.stats_latency.handle_messages(&mut state.latency_consumer);
        self.stats_processing.handle_messages(&mut state.processing_consumer);
        self.tot_processing += Duration(self.stats_processing.tot());
        self.stats_latency.register_datapoint(state.latency_consumer.queue_message_count());
        self.stats_processing.register_datapoint(state.processing_consumer.queue_message_count());
        if let Some(datapoint) = self
            .stats_latency
            .datapoints
            .last()
            .filter(|d| d.tot != 0 && self.data_latency.last().is_none_or(|d2| d2.1.time != d.time))
            .copied()
        {
            self.data_latency.insert(Nanos(datapoint.time), datapoint);
        }
        if let Some(datapoint) = self
            .stats_processing
            .datapoints
            .last()
            .filter(|d| {
                d.tot != 0 && self.data_processing.last().is_none_or(|d2| d2.1.time != d.time)
            })
            .copied()
        {
            self.data_processing.insert(Nanos(datapoint.time), datapoint);
        }
    }

    pub fn is_empty(&self) -> bool {
        self.stats_latency.is_empty() && self.stats_processing.is_empty()
    }

    pub fn toggle_render_options(&mut self, flags: RenderFlags) {
        self.stats_latency.toggle(flags);
        self.stats_processing.toggle(flags);
    }

    pub fn clear_bucketed(&mut self) {
        self.data_latency.clear_bucketed();
        self.data_processing.clear_bucketed();
    }

    pub(crate) fn clear_data(&mut self) {
        self.tot_processing = Duration::ZERO;
        self.data_latency.clear();
        self.data_processing.clear();
    }
}

#[derive(Clone, Debug, Default, Deserialize, Serialize)]
pub struct TimerDatas {
    pub data: Vec<TimerData>,
}

impl TimerDatas {
    pub fn push(&mut self, data: TimerData) {
        self.data.push(data);
        self.data.sort_unstable_by(|t1, t2| t1.name.cmp(&t2.name))
    }

    pub fn contains(&self, name: &str) -> bool {
        self.data.iter().any(|d| d.name == name)
    }

    pub fn clear(&mut self) {
        self.data.iter_mut().for_each(|d| d.clear_data());
    }

    pub fn toggle_render_options(&mut self, options: RenderFlags) {
        self.data.iter_mut().for_each(|d| d.toggle_render_options(options))
    }
}

impl Persistable for TimerDatas {
    const PERSIST_DIR: &'static str = "timer_updates";
}

fn black_box<T>(dummy: T) -> T {
    unsafe { std::ptr::read_volatile(&dummy) }
}

pub fn clock_overhead() -> Duration {
    let start = Instant::now();
    for _ in 0..1_000_000 {
        black_box(Instant::now());
    }
    let end = Instant::now();
    Duration((end.0 - start.0) / 1_000_000)
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, AsRefStr, Default)]
#[repr(u8)]
pub enum TimeKeeperMode {
    #[default]
    Timeline,
    Aggregate,
    TileMetrics,
}

impl TimeKeeperMode {
    pub fn next(self) -> Self {
        match self {
            Self::Timeline => Self::Aggregate,
            Self::Aggregate => Self::TileMetrics,
            Self::TileMetrics => Self::Timeline,
        }
    }

    pub fn label(self) -> &'static str {
        match self {
            Self::Timeline => "Timeline",
            Self::Aggregate => "Aggregate",
            Self::TileMetrics => "TileMetrics",
        }
    }
}

#[derive(Clone, Debug)]
pub struct TimeKeeper {
    mode: TimeKeeperMode,
    update_queue_checker: Repeater,
    clock_overhead: Duration,

    timer_data: TimerDatas,
    tile_metrics: TileMetricsView,

    pub timers: HashMap<String, TimerDataState>,
    pub timers_list_state: ListState,
}

impl Default for TimeKeeper {
    fn default() -> Self {
        Self {
            mode: TimeKeeperMode::default(),
            update_queue_checker: Repeater::every(Duration::from_secs(10)),
            clock_overhead: clock_overhead(),
            timer_data: TimerDatas::default(),
            tile_metrics: TileMetricsView::default(),
            timers_list_state: Default::default(),
            timers: HashMap::default(),
        }
    }
}

impl TimeKeeper {
    fn render_timeline(&mut self, area: Rect, frame: &mut Frame, plot_settings: &PlotSettings) {
        let mut max = 0;

        let namelist: Vec<ListItem> = self
            .timer_data
            .data
            .iter()
            .filter(|d| !d.is_empty())
            .map(|data| {
                let name = data.name.clone();
                max = max.max(name.len());
                ListItem::from(name)
            })
            .collect();
        let mut slot_time_data = self.timer_data.data.iter_mut().filter(|d| !d.is_empty());

        let layout = Layout::default()
            .direction(Direction::Horizontal)
            .constraints([Constraint::Length(max as u16 + 4), Constraint::Fill(1)])
            .split(area);

        let selected_style = Style::default().bold();
        let list = List::new(namelist)
            .highlight_style(selected_style)
            .highlight_symbol(">")
            .highlight_spacing(HighlightSpacing::Always)
            .scroll_padding(2);

        frame.render_stateful_widget(list, layout[0], &mut self.timers_list_state);
        if let Some(time_data) =
            self.timers_list_state.selected_mut().and_then(|curid| slot_time_data.nth(curid))
        {
            let timer = self.timers.get_mut(&time_data.name).unwrap();
            timer.report(time_data, frame, layout[1], plot_settings);
        }
    }

    fn render_aggregate(
        data: &[TimerData],
        plot: &PlotSettings,
        area: Rect,
        frame: &mut Frame<'_>,
    ) {
        let rows: Vec<_> = data
            .iter()
            .filter(|d| !d.is_empty())
            .map(|d| {
                let tot = windowed_total_processing(&d.data_processing, plot);
                (d.name.as_str(), tot)
            })
            .collect();

        if rows.is_empty() {
            return;
        }

        let mut max_total = rows.iter().map(|(_, t)| t.0).max().unwrap_or(1);
        if max_total == 0 {
            max_total = 1;
        }

        let name_w = rows.iter().map(|(n, _)| n.len()).max().unwrap() + 2;
        let val_w = rows.iter().map(|(_, t)| t.to_string().len()).max().unwrap();

        let layout = Layout::vertical(vec![Constraint::Length(1); rows.len()]);
        let areas = layout.split(area);

        for ((name, total), area) in rows.into_iter().zip(areas.iter()) {
            let ratio = (total.0 as f64 / max_total as f64).clamp(0.0, 1.0);

            let label = format!("{name:<name_w$} {total:>val_w$}",);

            frame.render_widget(
                LineGauge::default().filled_style(tailwind::BLUE.c800).ratio(ratio).label(label),
                *area,
            );
        }
    }

    pub fn render(&mut self, area: Rect, frame: &mut Frame, plot_settings: &PlotSettings) {
        use ratatui::layout::Constraint::{Length, Min};

        let layout = Layout::vertical([Length(3), Min(0)]).split(area);

        self.render_timekeeper_header(layout[0], frame);

        match self.mode {
            TimeKeeperMode::Timeline => self.render_timeline(layout[1], frame, plot_settings),
            TimeKeeperMode::Aggregate => {
                Self::render_aggregate(&self.timer_data.data, plot_settings, layout[1], frame)
            }
            TimeKeeperMode::TileMetrics => {
                self.tile_metrics.render(frame, layout[1], plot_settings);
            }
        }
    }

    pub fn handle_key_events(&mut self, code: KeyCode) {
        match (code, self.mode) {
            (KeyCode::Char('v'), _) => {
                self.mode = self.mode.next();
            }

            (KeyCode::Down, TimeKeeperMode::Timeline) => self.timers_list_state.select_next(),
            (KeyCode::Up, TimeKeeperMode::Timeline) => self.timers_list_state.select_previous(),

            (KeyCode::Char('l'), TimeKeeperMode::Timeline) => {
                self.timer_data.toggle_render_options(RenderFlags::ShowMin)
            }
            (KeyCode::Char('m'), TimeKeeperMode::Timeline) => {
                self.timer_data.toggle_render_options(RenderFlags::ShowMedian)
            }
            (KeyCode::Char('M'), TimeKeeperMode::Timeline) => {
                self.timer_data.toggle_render_options(RenderFlags::ShowMax)
            }
            (KeyCode::Char('a'), TimeKeeperMode::Timeline) => {
                self.timer_data.toggle_render_options(RenderFlags::ShowAverages)
            }
            _ => {}
        }
    }

    pub fn check_new_queues(&mut self, app_name: &str) {
        let queues_dir: &str = &shmem_dir_queues_string(app_name);
        let Ok(entries) = std::fs::read_dir(queues_dir) else {
            return;
        };
        for entry in entries.filter_map(|e| e.ok()) {
            let name = entry.path().as_os_str().to_str().unwrap().to_string();
            if let Some((_dir, real_name)) = name.split_once("latency-") {
                if !self.timer_data.contains(real_name) {
                    let latency_q = Queue::open_shared(format!("{queues_dir}/latency-{real_name}"));
                    let processing_q =
                        Queue::open_shared(format!("{queues_dir}/timing-{real_name}"));

                    // only display queues that have entries
                    if latency_q.count() == 0 && processing_q.count() == 0 {
                        continue;
                    }

                    let view = TimerDataState::new(latency_q.into(), processing_q.into());
                    let data = TimerData::new(
                        real_name.to_string().clone(),
                        SAMPLES_PER_PERCENTILE,
                        NUM_DATAPOINTS,
                        self.clock_overhead,
                    );

                    self.timers.insert(data.name.clone(), view);
                    self.timer_data.push(data);
                }
            }
        }
    }

    pub fn update(&mut self, app_name: &str) {
        if self.update_queue_checker.fired() {
            self.check_new_queues(app_name);
        }

        for data in &mut self.timer_data.data {
            if let Some(state) = self.timers.get_mut(&data.name) {
                data.handle_messages(state);
            }
        }

        self.tile_metrics.update(app_name);
    }

    fn render_timekeeper_header(&self, area: Rect, frame: &mut Frame) {
        use ratatui::widgets::{Block, Borders, Paragraph};

        let in_timeline = matches!(self.mode, TimeKeeperMode::Timeline);

        let mut spans =
            vec![Span::raw("[v] view: "), Span::styled(self.mode.label(), Style::default().bold())];

        if in_timeline {
            let render_flags =
                self.timer_data.data.first().map(|d| d.stats_latency.flags).unwrap_or_default();

            spans.extend([
                Span::raw("   "),
                styled_key("[a] avg", render_flags.contains(RenderFlags::ShowAverages)),
                Span::raw(" "),
                styled_key("[m] med", render_flags.contains(RenderFlags::ShowMedian)),
                Span::raw(" "),
                styled_key("[M] max", render_flags.contains(RenderFlags::ShowMax)),
                Span::raw(" "),
                styled_key("[l] min", render_flags.contains(RenderFlags::ShowMin)),
            ]);
        }

        let text = Line::from(spans);
        let block = Block::default().borders(Borders::BOTTOM).title("Timekeeper");
        Paragraph::new(text).block(block).render(area, frame.buffer_mut());
    }
}

fn styled_key(label: &str, active: bool) -> Span<'_> {
    if active { Span::styled(label, Style::default().bold()) } else { Span::raw(label) }
}

fn windowed_total_processing(
    data: &BucketedDataCache<Nanos, DataPoint<Duration>>,
    plot: &PlotSettings,
) -> Duration {
    let (x0, x1) = plot.range();
    let mut total = 0;

    for (_, d) in data.iter() {
        let t = d.time as f64;
        if t >= x0 && t <= x1 {
            total += d.tot;
        }
    }

    Duration(total)
}
