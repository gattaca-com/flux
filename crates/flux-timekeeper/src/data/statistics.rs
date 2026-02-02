use std::fmt::Debug;

use flux::{
    communication::queue::Consumer,
    timing::{Duration, Instant, Nanos},
};
use ratatui::{Frame, layout::Rect, style::Color, symbols::Marker, widgets::GraphType};
use serde::{Deserialize, Serialize};

use crate::{
    data::circular_buffer::CircularBuffer,
    tui::{Plot, PlotSeries, PlotSettings, RenderFlags},
    types::{DataPoint, MsgPer10Sec, Statisticable},
};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Statistics<T: Statisticable> {
    pub title: String,
    // These are updated when Start
    measurements: CircularBuffer<u64>,
    pub datapoints: CircularBuffer<DataPoint<T>>,

    // Running values, to go into the next datapoint
    avg: u64,
    min: u64,
    max: u64,
    tot: u64,
    samples: usize,

    // tot_count does not necessarily need to be equal to the sum of the samples
    // of the datapoints. If a timer is spamming messages, the timekeeper
    // won't be able to register all of them as samples, yet the
    // count of the timer queue will reflect the amount of messages
    // that were processed and is what is reflected here.
    // Using the delta from the queue count with this value + the
    // elapsed since last_t, we can know what the msgs/s was even
    // if we're not registering each of them as a sample
    tot_count: usize,

    last_t: Instant,

    // This will be subtracted from each measurement, useful for e.g. clock overhead
    offset: u64,
    pub flags: RenderFlags,
    got_one: bool,
}

impl<T: Statisticable> Statistics<T> {
    pub fn bytesize(&self) -> usize {
        std::mem::size_of::<Self>() + self.datapoints.len() * std::mem::size_of::<DataPoint<T>>()
    }
    pub fn new(title: String, samples_per_median: usize, n_datapoints: usize, offset: T) -> Self {
        let measurements = CircularBuffer::new(samples_per_median);

        Self {
            title,
            measurements,
            datapoints: CircularBuffer::new(n_datapoints),
            min: u64::MAX,
            max: 0,
            avg: 0,
            tot: 0,
            offset: offset.into(),
            samples: 0,
            tot_count: 0,
            flags: RenderFlags::ShowMedian,
            last_t: Instant::now(),
            got_one: false,
        }
    }

    fn corrected_or_zero(&self, t: u64) -> u64 {
        t.saturating_sub(self.offset)
    }

    pub fn set_measurements_len(&mut self, len: usize) {
        self.measurements = CircularBuffer::new(len);
    }

    pub fn register_datapoint(&mut self, mut tot_count: usize) {
        if tot_count == 0 {
            tot_count = self.samples;
        }
        let median = self.measurements.percentile(0.5);

        let count_delta = tot_count.saturating_sub(self.tot_count) as u64;
        let elapsed_since_last = self.last_t.elapsed().0;
        let rate = if count_delta > elapsed_since_last * 1000 {
            MsgPer10Sec(Duration::from_secs(10).0.saturating_mul(count_delta / elapsed_since_last))
        } else {
            MsgPer10Sec(Duration::from_secs(10).0.saturating_mul(count_delta) / elapsed_since_last)
        };

        self.datapoints.push(DataPoint {
            avg: self.corrected_or_zero(self.avg),
            min: self.corrected_or_zero(self.min),
            max: self.corrected_or_zero(self.max),
            tot: self.corrected_or_zero(self.tot),
            median: self.corrected_or_zero(median),

            n_samples: self.samples,
            rate,
            time: Nanos::now().0,
            vline: bool::default(),
            _p: std::marker::PhantomData,
        });
        self.tot_count = tot_count;
        self.reset();
    }

    pub fn tot(&self) -> u64 {
        self.tot
    }

    pub fn reset(&mut self) {
        self.measurements.clear();
        self.min = u64::MAX;
        self.max = 0;
        self.samples = 0;
        self.got_one = true;
        self.tot = 0;
        self.last_t = Instant::now();
    }

    pub fn clear(&mut self) {
        self.reset();
        self.datapoints.clear();
    }

    pub fn last(&self) -> Option<u64> {
        self.datapoints.last().and_then(|t| (t.avg != 0).then_some(t.avg))
    }

    pub fn track(&mut self, el: T) {
        let el = el.into();

        if el > self.max {
            self.max = el;
        }

        if el < self.min {
            self.min = el;
        }

        let avg = self.avg * self.samples as u64;

        self.samples += 1;
        self.avg = (avg + el) / self.samples as u64;
        self.tot += el;

        self.measurements.push(el);
    }

    pub fn tot_samples(&self) -> usize {
        self.datapoints.iter().map(|d| d.n_samples).sum()
    }

    pub fn is_empty(&self) -> bool {
        !self.got_one
    }

    pub fn handle_messages<M: 'static + Copy + Default + Into<T>>(
        &mut self,
        consumer: &mut Consumer<M>,
    ) {
        let curt = Instant::now();
        let max = Duration::from_millis(1);
        // loops until either a full datapoint was captured or no messages are pending
        let mut n_read = 0;
        while n_read < 4096 {
            if !consumer.consume(|&mut msg| {
                n_read += 1;
                self.track(msg.into());
            }) || curt.elapsed() > max
            {
                break;
            }
        }
    }

    pub fn toggle(&mut self, flags: RenderFlags) {
        self.flags ^= flags
    }
}

impl<T: Statisticable + Default> Statistics<T> {
    #[allow(clippy::filter_map_bool_then)]
    pub fn add_block_starts<D: Default + Statisticable>(&self, plot: &mut Plot<D>) {
        let block_starts = self
            .datapoints
            .iter()
            .filter_map(move |d| d.vline.then(|| vec![(d.time as f64, 0.0), (d.time as f64, 0.0)]));
        for d in block_starts {
            plot.push(
                PlotSeries::default()
                    .color(Color::Red)
                    .marker(Marker::Braille)
                    .graph_type(GraphType::Line)
                    .data(d.into_iter()),
            );
        }
    }

    pub fn to_plot_data<'a, F: Fn(&DataPoint<T>) -> u64 + 'a>(
        &'a self,
        f: F,
    ) -> impl Iterator<Item = (f64, f64)> + 'a {
        self.datapoints
            .iter()
            .filter(|t| t.n_samples != 0)
            .map(move |t| (t.time as f64, f(t) as f64))
    }

    /// works like stacked chart
    #[allow(clippy::filter_map_bool_then)]
    pub fn add_to_plot_stacked(
        &self,
        plot: &mut Plot<T>,
        color: Option<Color>,
        _label_suffix: Option<String>,
    ) {
        let mut offsets = vec![0.0f64; self.datapoints.len()];
        for prev_data in plot.plot_series.iter() {
            for p in &prev_data.data {
                if let Some(offset) = offsets.get_mut(p.0 as usize) {
                    *offset = (*offset).max(p.1);
                }
            }
        }
        let data = self.datapoints.iter().enumerate().filter_map(move |(i, d)| {
            (d.tot != 0)
                .then(|| vec![(i as f64, offsets[i]), (i as f64, offsets[i] + d.avg as f64)])
        });

        for d in data {
            plot.push(
                PlotSeries::default()
                    .color(color.unwrap_or_default())
                    .graph_type(GraphType::Line)
                    .marker(Marker::Block)
                    .data(d.into_iter()),
            );
        }
    }

    #[allow(clippy::filter_map_bool_then)]
    pub fn add_to_plot(
        &self,
        plot: &mut Plot<T>,
        color: Option<Color>,
        label_suffix: Option<String>,
        marker: Option<Marker>,
    ) {
        if !plot.has_block_starts() {
            self.add_block_starts(plot);
        }

        let marker = marker.unwrap_or(Marker::Braille);
        if self.flags.contains(RenderFlags::ShowAverages) {
            let label = if self.flags == RenderFlags::ShowAverages {
                label_suffix.clone().unwrap_or_default().to_string()
            } else {
                format!("avg{}", label_suffix.clone().unwrap_or_default())
            };
            plot.push(
                PlotSeries::default()
                    .label(label)
                    .color(color.unwrap_or_default())
                    .graph_type(GraphType::Scatter)
                    .marker(marker)
                    .data(self.to_plot_data(|t| t.avg)),
            );
        }

        if self.flags.contains(RenderFlags::ShowMin) {
            plot.push(
                PlotSeries::default()
                    .label(format!("min{}", label_suffix.clone().unwrap_or_default()))
                    .color(Color::Green)
                    .graph_type(GraphType::Scatter)
                    .marker(marker)
                    .data(self.to_plot_data(|t| t.min)),
            );
        }

        if self.flags.contains(RenderFlags::ShowMax) {
            plot.push(
                PlotSeries::default()
                    .label(format!("max{}", label_suffix.clone().unwrap_or_default()))
                    .color(Color::Red)
                    .graph_type(GraphType::Scatter)
                    .marker(marker)
                    .data(self.to_plot_data(|t| t.max)),
            );
        }

        if self.flags.contains(RenderFlags::ShowMedian) {
            plot.push(
                PlotSeries::default()
                    .label(format!("med{}", label_suffix.unwrap_or_default()))
                    .color(Color::Yellow)
                    .marker(marker)
                    .graph_type(GraphType::Scatter)
                    .data(self.to_plot_data(|t| t.median)),
            );
        }
    }

    #[allow(clippy::filter_map_bool_then)]
    pub fn report(&self, name: &str, frame: &mut Frame, rect: Rect, plot_settings: &PlotSettings)
    where
        T: Default,
    {
        let mut avg = 0;
        let mut tot = 0;
        for d in self.datapoints.iter().filter(|d| d.n_samples != 0) {
            avg += d.avg;
            tot += 1;
        }
        if tot != 0 {
            avg /= tot;
        }

        let title = if name.is_empty() {
            self.title.to_string()
        } else {
            format!("{} report for {name}", self.title)
        };
        let mut plot = Plot::<T>::new(title);

        self.add_to_plot(&mut plot, None, None, None);

        plot.render(frame, rect, Some(format!("Running avg: {}", T::from(avg))), plot_settings);
    }

    pub fn report_msg_per_sec(&self, frame: &mut Frame, rect: Rect, plot_settings: &PlotSettings) {
        let mut plot = Plot::<MsgPer10Sec>::new("");

        plot.push(
            PlotSeries::default()
                .label("msg/s")
                .graph_type(GraphType::Line)
                .data(self.datapoints.iter().enumerate().map(|(i, t)| (i as f64, t.rate.0 as f64))),
        );
        self.add_block_starts(&mut plot);
        plot.render(frame, rect, Some("Msg/s".to_string()), plot_settings);
    }
}
