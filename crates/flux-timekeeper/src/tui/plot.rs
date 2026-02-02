use std::fmt::{Debug, Display};

use ratatui::{
    Frame,
    layout::{Constraint, Direction, Layout, Rect},
    prelude::*,
    style::{Color, Style},
    widgets::{Axis, Block, Borders, Chart, Dataset, GraphType},
};
use symbols::Marker;

use crate::{data, tui::PlotSettings};

pub trait Plottable: Into<u64> + From<u64> + Display + Copy + Default {}
impl<T: Into<u64> + From<u64> + Display + Copy + Default> Plottable for T {}

#[derive(Debug, Clone)]
pub struct Plot<T> {
    title: String,
    y_min: f64,
    y_max: f64,
    pub(crate) plot_series: Vec<PlotSeries<T>>,
}

impl<T> Plot<T> {
    pub fn has_block_starts(&self) -> bool {
        self.plot_series.iter().any(|series| {
            series.data.first().is_some_and(|data| data.0 != 0.0 && data.1 == 0.0) &&
                series.data.get(1).is_some_and(|data| data.0 != 0.0 && data.1 == 0.0)
        })
    }
    pub fn push(&mut self, series: PlotSeries<T>) {
        if !(series.y_min == 0.0 && series.y_max == 0.0) {
            self.y_min = self.y_min.min(series.y_min);
            self.y_max = self.y_max.max(series.y_max);
        }
        self.plot_series.push(series);
    }
}

impl<T: Plottable> Plot<T> {
    pub fn new<S: ToString>(title: S) -> Self {
        Self {
            title: title.to_string(),
            y_min: f64::MAX,
            y_max: 0.0,
            plot_series: Default::default(),
        }
    }

    pub fn render(
        &mut self,
        frame: &mut Frame,
        rect: Rect,
        title: Option<String>,
        plot_settings: &PlotSettings,
    ) {
        let mut spans = Vec::new();
        let prefix = if self.title.is_empty() {
            "Last point: ".to_string()
        } else {
            format!("{}: Last point: ", self.title)
        };
        spans.push(Span::raw(prefix));

        let mut first = true;
        for s in self.plot_series.iter().filter(|s| s.last_val != 0.0) {
            if !first {
                spans.push(Span::raw(" - "));
            }
            first = false;

            spans.push(Span::styled(format!("{}:", s.label), Style::default().fg(s.color)));
            spans.push(Span::raw(format!(" {}", T::from(s.last_val.round() as u64))));
        }

        let text = Line::from(spans);

        let mut block = Block::new().borders(Borders::ALL);
        if let Some(title) = title {
            block = block.title(title);
        }
        let area = block.inner(rect);
        let sub_layout =
            Layout::new(Direction::Vertical, [Constraint::Length(1), Constraint::Fill(1)])
                .split(area);
        frame.render_widget(block, rect);

        let y_min = self.y_min.min(self.y_max);
        let y_max = self.y_max;

        let y_diff = (y_max - y_min).max(y_max / 100.0);
        let y_min_bound = y_min - 0.05 * y_diff;
        let y_max_bound = y_max + 0.05 * y_diff;
        let y_q1 = y_min + (0.25 * y_diff);
        let y_q2 = y_min + (0.5 * y_diff);
        let y_q3 = y_min + (0.75 * y_diff);
        let ylabels = vec![
            T::from(y_min.round() as u64).to_string(),
            format!("{}", T::from(y_q1.round() as u64)),
            format!("{}", T::from(y_q2.round() as u64)),
            format!("{}", T::from(y_q3.round() as u64)),
            T::from(y_max.round() as u64).to_string(),
        ];

        let (x_min, x_max) = plot_settings.range();
        let x_diff = plot_settings.span();
        let x_q1 = x_min + (0.25 * x_diff);
        let x_q2 = x_min + (0.5 * x_diff);
        let x_q3 = x_min + (0.75 * x_diff);

        let vlines = [x_q1, x_q2, x_q3]
            .iter()
            .map(|x| vec![(*x, y_min_bound), (*x, y_max_bound)])
            .collect::<Vec<_>>();
        let hlines =
            [y_q1, y_q2, y_q3].iter().map(|y| vec![(x_min, *y), (x_max, *y)]).collect::<Vec<_>>();
        let mut datasets: Vec<_> = vlines
            .iter()
            .map(|d| {
                Dataset::default()
                    .marker(Marker::Braille)
                    .style(Style::new().fg(Color::Indexed(239)))
                    .graph_type(GraphType::Line)
                    .data(d.as_slice())
            })
            .collect();
        datasets.extend(hlines.iter().map(|d| {
            Dataset::default()
                .marker(Marker::Braille)
                .style(Style::new().fg(Color::Indexed(239)))
                .graph_type(GraphType::Line)
                .data(d.as_slice())
        }));

        let xaxis = Axis::default()
            .bounds([x_min, x_max])
            .style(Style::default().fg(Color::LightBlue))
            .labels(plot_settings.xlabels());
        datasets
            .extend(self.plot_series.iter_mut().map(|d| d.to_dataset(y_min_bound, y_max_bound)));
        let yaxis = Axis::default()
            .bounds([y_min_bound, y_max_bound])
            .style(Style::default().fg(Color::LightBlue))
            .labels(ylabels);

        let chart = Chart::new(datasets)
            .x_axis(xaxis)
            .y_axis(yaxis)
            .legend_position(Some(ratatui::widgets::LegendPosition::TopLeft));
        frame.render_widget(text, sub_layout[0]);
        frame.render_widget(chart, sub_layout[1]);
    }

    pub fn stack_by_x(&mut self) {
        let mut offsets = vec![0.0f64; self.plot_series.len()];
        for prev_data in self.plot_series.iter() {
            for p in &prev_data.data {
                if let Some(offset) = offsets.get_mut(p.0 as usize) {
                    *offset = (*offset).max(p.1);
                }
            }
        }
        let data: Vec<_> = self
            .plot_series
            .iter()
            .enumerate()
            .map(move |(i, d)| {
                vec![
                    (i as f64, offsets[i]),
                    (i as f64, offsets[i] + data::utils::mean(d.data.as_slice())),
                ]
            })
            .collect();

        self.plot_series.clear();

        for d in data {
            self.push(
                PlotSeries::default()
                    .graph_type(GraphType::Line)
                    .marker(Marker::Block)
                    .data(d.into_iter()),
            );
        }
    }
}

#[derive(Debug, Clone, Default)]
pub struct PlotSeries<T> {
    color: Color,
    label: String,
    graph_type: GraphType,
    marker: Marker,
    pub(crate) data: Vec<(f64, f64)>,
    y_min: f64,
    y_max: f64,
    last_val: f64,
    _p: std::marker::PhantomData<T>,
}
impl<T: Display + Into<u64> + Default + Clone> PlotSeries<T> {
    pub fn color(self, color: Color) -> Self {
        Self { color, ..self }
    }

    pub fn label<S: ToString>(self, label: S) -> Self {
        Self { label: label.to_string(), ..self }
    }

    pub fn graph_type(self, graph_type: GraphType) -> Self {
        Self { graph_type, ..self }
    }

    pub fn marker(self, marker: Marker) -> Self {
        Self { marker, ..self }
    }

    pub fn data<X: Into<f64>>(self, data: impl Iterator<Item = (X, f64)>) -> Self {
        let mut y_min = f64::MAX;
        let mut y_max = f64::MIN;
        let mut last_val = Default::default();
        let data = data
            .map(|(x, y)| {
                last_val = y;
                y_min = y_min.min(y);
                y_max = y_max.max(y);

                (x.into(), y)
            })
            .collect();

        Self { data, y_min, y_max, last_val, ..self }
    }

    pub fn data_raw(self, data: &[(f64, f64)]) -> Self {
        let mut y_min = f64::MAX;
        let mut y_max = f64::MIN;
        let mut last_val = Default::default();
        for &(_, y) in data {
            if y_min > y {
                y_min = y
            }
            if y_max < y {
                y_max = y
            }
            last_val = y
        }

        Self { data: data.to_vec(), y_min, y_max, last_val, ..self }
    }

    pub fn len(&self) -> usize {
        self.data.len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    #[allow(clippy::wrong_self_convention)]
    fn to_dataset(&mut self, y_min: f64, y_max: f64) -> Dataset<'_> {
        if self.len() == 2 && self.data[0].1 == 0.0 && self.data[1].1 == 0.0 {
            self.data[0].1 = y_min;
            self.data[1].1 = y_max;
        }
        let mut o = Dataset::default()
            .marker(self.marker)
            .style(Style::new().fg(self.color))
            .graph_type(self.graph_type)
            .data(&self.data);
        if !self.label.is_empty() {
            o = o.name(self.label.as_str());
        }
        o
    }
}
