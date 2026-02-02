use flux::timing::Nanos;
use ratatui::{style::Color, symbols::Marker, widgets::GraphType};

use crate::tui::{Plot, PlotSeries};

#[derive(Clone, Copy, Debug)]
pub enum DurationUnit {
    Ns,
    Us,
    Ms,
}

#[derive(Clone, Copy, Debug)]
pub enum XAxisSpec {
    /// Absolute wall-clock time
    UtcTime,

    /// Relative duration since some zero (slot start, tx start, etc)
    Duration { unit: DurationUnit },
}

#[derive(Clone, Debug)]
pub struct PlotSettings {
    xaxis_spec: XAxisSpec,

    // these are the actual bounds on the plot xaxis
    xmin_plot: f64,
    xmax_plot: f64,

    // these are the absolute times for the buckets,
    // this is done so the data doesn't shift between buckets
    // while smoothly updating plot bounds
    xmin_bucket: f64,
    xmax_bucket: f64,

    n_intervals: u64,
    zoom_speed: f64,
    pan_speed: f64,
    vlines: Vec<f64>,
}

impl PlotSettings {
    pub fn new(
        xmin: impl Into<f64>,
        xmax: impl Into<f64>,
        n_intervals: u64,
        xaxis_spec: XAxisSpec,
    ) -> Self {
        let xmin = xmin.into();
        let xmax = xmax.into();
        // Zoom in / out by 64 buckets at a time (16 on each side)
        let zoom_speed = 1.0 - 32.0 / (n_intervals as f64);

        // Pan by 16 buckets at a time
        let pan_speed = 16.0;

        let mut o = Self {
            xaxis_spec,
            xmin_plot: xmin,
            xmax_plot: xmax,
            zoom_speed,
            pan_speed,
            n_intervals,
            xmin_bucket: xmin,
            xmax_bucket: xmax,
            vlines: Default::default(),
        };
        o.maybe_update_bucket_bounds();
        o
    }

    pub fn time_range(&self) -> (f64, f64, f64) {
        (self.xmin_bucket, self.xmax_bucket, self.current_interval())
    }

    pub fn set_range_to_now(&mut self) {
        let now_t: f64 = Nanos::now().into();
        let cur_delta = self.xmax_plot - self.xmin_plot;
        self.set_range(now_t - cur_delta, now_t);
    }

    pub fn set_range(&mut self, xmin: impl Into<f64>, xmax: impl Into<f64>) {
        self.xmin_plot = xmin.into();
        self.xmax_plot = xmax.into();
        self.xmin_bucket = self.xmin_plot;
        self.xmax_bucket = self.xmax_plot;
        self.vlines.retain(|v| *v >= self.xmin_plot);
        self.maybe_update_bucket_bounds();
    }

    pub fn current_interval(&self) -> f64 {
        self.span() / self.n_intervals.max(1) as f64
    }

    pub fn span(&self) -> f64 {
        self.xmax_bucket - self.xmin_bucket
    }

    pub fn range(&self) -> (f64, f64) {
        (self.xmin_plot, self.xmax_plot)
    }

    pub fn n_intervals(&self) -> u64 {
        self.n_intervals
    }

    pub fn add_block_starts<T: Clone + Into<u64> + Default + std::fmt::Display>(
        &self,
        plot: &mut Plot<T>,
    ) {
        let block_starts = self.vlines.iter().map(|d| vec![(*d, 0.0), (*d, 0.0)]);
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

    pub fn pan_left(&mut self, factor: f64) {
        let delta = self.pan_speed * self.current_interval() * factor;
        self.xmin_plot -= delta;
        self.xmax_plot -= delta;

        self.maybe_update_bucket_bounds()
    }

    pub fn pan_right(&mut self, factor: f64) {
        let delta = self.pan_speed * self.current_interval() * factor;
        self.xmin_plot += delta;
        self.xmax_plot += delta;
        self.maybe_update_bucket_bounds()
    }

    pub fn maybe_update_bucket_bounds(&mut self) {
        let interval = self.current_interval();
        let n_buckets_shifted =
            ((self.xmin_plot - self.xmin_bucket).abs() / self.current_interval()).floor();

        let shift = interval * n_buckets_shifted;
        if self.xmin_plot > self.xmin_bucket {
            self.xmin_bucket += shift;
        } else {
            self.xmin_bucket -= shift;
        }
        self.xmax_bucket = self.xmin_bucket;
        for _ in 0..self.n_intervals {
            self.xmax_bucket += interval;
        }
    }

    pub fn zoom_in(&mut self) {
        self.zoom(self.zoom_speed);
    }

    pub fn zoom_out(&mut self) {
        self.zoom(1.0 / self.zoom_speed);
    }

    fn zoom(&mut self, factor: f64) {
        let centre = (self.xmax_plot + self.xmin_plot) / 2.0;
        let scaled_half_span = (centre - self.xmin_plot) * factor;
        self.set_range(centre - scaled_half_span, centre + scaled_half_span);
    }

    pub fn update_plot_xmax(&mut self, new_xmax: f64) {
        let delta = new_xmax - self.xmax_plot;
        self.xmin_plot += delta;
        self.xmax_plot += delta;
        self.maybe_update_bucket_bounds();
    }

    pub fn push_vline(&mut self, slot_t: f64) {
        self.vlines.push(slot_t);
        self.vlines.retain(|v| *v >= self.xmin_plot);
    }

    pub fn bucket_bounds(&self, x: f64) -> (f64, f64) {
        let bucket_id = ((x - self.xmin_bucket) / self.current_interval()).floor();
        (
            self.xmin_bucket + bucket_id * self.current_interval(),
            self.xmin_bucket + (bucket_id + 1.0) * self.current_interval(),
        )
    }

    pub fn set_n_intervals(&mut self, n_intervals: u64) {
        self.n_intervals = n_intervals;
        self.maybe_update_bucket_bounds();
    }

    pub fn xlabels(&self) -> Vec<String> {
        let (x_min, x_max) = self.range();
        let x_diff = x_max - x_min;
        let x_q1 = x_min + 0.25 * x_diff;
        let x_q2 = x_min + 0.50 * x_diff;
        let x_q3 = x_min + 0.75 * x_diff;

        let xlabels: Vec<String> = match self.xaxis_spec {
            XAxisSpec::UtcTime => vec![
                Nanos(x_min as u64).with_fmt_utc("%H:%M:%S"),
                Nanos(x_q1 as u64).with_fmt_utc(":%S%.3f"),
                Nanos(x_q2 as u64).with_fmt_utc(":%S%.3f"),
                Nanos(x_q3 as u64).with_fmt_utc(":%S%.3f"),
                Nanos(x_max as u64).with_fmt_utc("%H:%M:%S"),
            ],

            XAxisSpec::Duration { unit } => {
                let nanos_per_unit = match unit {
                    DurationUnit::Ns => 1.0,
                    DurationUnit::Us => 1e3,
                    DurationUnit::Ms => 1e6,
                };
                let suffix = match unit {
                    DurationUnit::Ns => "ns",
                    DurationUnit::Us => "µs",
                    DurationUnit::Ms => "ms",
                };

                vec![
                    format!("{:.3}{}", x_min / nanos_per_unit, suffix),
                    format!("{:.3}{}", x_q1 / nanos_per_unit, suffix),
                    format!("{:.3}{}", x_q2 / nanos_per_unit, suffix),
                    format!("{:.3}{}", x_q3 / nanos_per_unit, suffix),
                    format!("{:.3}{}", x_max / nanos_per_unit, suffix),
                ]
            }
        };

        xlabels
    }
}
