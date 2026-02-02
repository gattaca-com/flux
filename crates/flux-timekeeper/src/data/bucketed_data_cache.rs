use std::{cmp::Eq, collections::HashMap, fmt::Display, hash::Hash};

use auto_impl::auto_impl;
use flux::timing::{Instant, InternalMessage};
use indexmap::IndexMap;
use ratatui::{style::Color, symbols::Marker, widgets::GraphType};
use serde::{Deserialize, Serialize};

#[auto_impl(&, Rc, Arc, Box)]
pub trait HasKey {
    type Key: Hash + Eq + for<'a> Deserialize<'a> + Serialize;
    fn key(&self) -> Self::Key;
}

impl<T: HasKey> HasKey for Vec<T> {
    type Key = T::Key;

    fn key(&self) -> Self::Key {
        self[0].key()
    }
}

impl<T> HasKey for InternalMessage<T> {
    type Key = Instant;

    fn key(&self) -> Self::Key {
        self.publish_t_internal()
    }
}

use crate::{
    data::bucketed_data::BucketedData,
    tui::{Plot, PlotSeries, PlotSettings},
};

// TODO: Remove Serialize and Deserialize. Shouldn't be saving this directly?
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct BucketedDataCache<K: Hash + Eq, T> {
    /// Underlying data which can be rebucketed any number of times.
    /// Ordered by [HasKey] ascending
    data: IndexMap<K, T>,
    /// Buckets over datasets coming from the same underlying data
    cache: HashMap<String, BucketedData>,
}

impl<K: Hash + Eq, T> std::ops::Deref for BucketedDataCache<K, T> {
    type Target = IndexMap<K, T>;

    fn deref(&self) -> &Self::Target {
        &self.data
    }
}

impl<K: Hash + Eq, T> std::ops::DerefMut for BucketedDataCache<K, T> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.data
    }
}

impl<K: Hash + Eq, T> BucketedDataCache<K, T> {
    pub fn clear_bucketed(&mut self) {
        self.cache.clear()
    }
    pub fn clear(&mut self) {
        self.data.clear();
        for b in self.cache.values_mut() {
            b.data_bucketed_start_id = 0;
            b.data_bucketed_last_id = 0;
        }
    }

    pub fn clear_cached(&mut self, label: &str) {
        if let Some(cached) = self.cache.get_mut(label) {
            cached.clear();
        }
    }
}

impl<T: HasKey> BucketedDataCache<T::Key, T> {
    pub fn push(&mut self, data: T) {
        self.data.insert(data.key(), data);
    }
}

impl<K: Hash + PartialOrd + Eq, T> BucketedDataCache<K, T> {
    #[allow(clippy::too_many_arguments)]
    pub fn add_to_plot<L: Into<u64> + From<u64> + Display + Copy + Default, E, A>(
        &mut self,
        plotsettings: &PlotSettings,
        dataset_identifier: String,
        extract_fn: E,
        aggregate_fn: A,
        plot: &mut Plot<L>,
        colour: Color,
        label: Option<String>,
        marker: Option<Marker>,
        graph_type: Option<GraphType>,
    ) where
        E: Fn(&T) -> Option<(f64, f64)>,
        A: Fn(&[(f64, f64)]) -> f64,
    {
        let dataset = self.cache.entry(dataset_identifier).or_default();
        dataset.maybe_rebucket(self.data.values(), plotsettings, extract_fn, aggregate_fn);
        let (dataset, _) = dataset.bucketed.as_slices();
        if dataset.is_empty() {
            return;
        }
        let marker = marker.unwrap_or(Marker::Braille);
        let graph_type = graph_type.unwrap_or(GraphType::Scatter);
        let mut series = PlotSeries::default()
            .color(colour)
            .marker(marker)
            .graph_type(graph_type)
            .data_raw(dataset);
        if let Some(label) = label {
            series = series.label(label)
        }
        if !plot.has_block_starts() {
            plotsettings.add_block_starts(plot);
        }
        plot.push(series);
    }

    pub fn prune_data(&mut self, lower_cutoff: K, upper_cutoff: K) {
        self.data.retain(|k, _| lower_cutoff < *k && *k < upper_cutoff);
    }
}
