//! Tile metrics discovery and display for the TUI.
//!
//! Discovers `tilemetrics-*` shared-memory queues, consumes [`TileSample`]
//! snapshots, and exposes per-tile utilisation / busy-time statistics grouped
//! by application.

use std::{
    collections::{HashMap, VecDeque},
    path::{Path, PathBuf},
};

use flux::tile::metrics::TileSample;
use flux_communication::{
    queue::{Consumer, Queue},
    shmem_dir_queues_string_with_base,
};
use flux_timing::{Duration, Instant};

/// How often we re-scan the filesystem for new `tilemetrics-*` files (10s).
fn discover_interval() -> Duration {
    Duration::from_secs(10)
}

/// Maximum number of samples to retain per tile.
const MAX_SAMPLES: usize = 512;

/// Default window (seconds) for computing aggregate stats.
const DEFAULT_STATS_WINDOW_SECS: f64 = 10.0;

// ── Per-tile data ────────────────────────────────────────────────────────

/// Aggregated statistics for a tile over a time window.
#[derive(Clone, Copy, Debug)]
pub struct TileStats {
    pub utilisation: f64,
    pub busy_avg: Duration,
    pub busy_min: Duration,
    pub busy_max: Duration,
    pub loop_count: u64,
}

/// Internal state for a single tile.
struct TileData {
    consumer: Consumer<TileSample>,
    /// Ring of recent samples.
    samples: VecDeque<TileSample>,
}

impl TileData {
    fn new(consumer: Consumer<TileSample>) -> Self {
        Self { consumer, samples: VecDeque::with_capacity(MAX_SAMPLES) }
    }

    /// Drain all pending samples from the shared-memory queue.
    fn update(&mut self) {
        while self.consumer.consume(|sample| {
            if self.samples.len() >= MAX_SAMPLES {
                self.samples.pop_front();
            }
            self.samples.push_back(*sample);
        }) {}
    }

    /// Compute aggregate stats over the most recent `window_secs` seconds.
    fn stats(&self, window_secs: f64) -> Option<TileStats> {
        if self.samples.is_empty() {
            return None;
        }

        // Use the latest sample's window_end as "now" for the time range.
        let latest_ns = self.samples.back()?.window_end.0 as f64;
        let cutoff = window_secs.mul_add(-1e9, latest_ns);

        let mut total_busy = 0u64;
        let mut total_ticks = 0u64;
        let mut busy_min = u64::MAX;
        let mut busy_max = 0u64;
        let mut busy_sum = 0u64;
        let mut busy_count = 0u32;
        let mut loop_count = 0u64;
        let mut sample_count = 0usize;

        for sample in &self.samples {
            if (sample.window_end.0 as f64) < cutoff {
                continue;
            }
            sample_count += 1;
            total_busy += sample.busy_ticks;
            total_ticks += sample.total_ticks();
            loop_count += sample.loop_count as u64;

            if sample.busy_count > 0 {
                busy_min = busy_min.min(sample.busy_min());
                busy_max = busy_max.max(sample.busy_max);
                busy_sum += sample.busy_sum;
                busy_count += sample.busy_count;
            }
        }

        if sample_count == 0 {
            return None;
        }

        Some(TileStats {
            utilisation: if total_ticks > 0 { total_busy as f64 / total_ticks as f64 } else { 0.0 },
            busy_avg: if busy_count > 0 {
                Duration(busy_sum / busy_count as u64)
            } else {
                Duration(0)
            },
            busy_min: Duration(if busy_min == u64::MAX { 0 } else { busy_min }),
            busy_max: Duration(busy_max),
            loop_count,
        })
    }
}

// ── Per-app group ────────────────────────────────────────────────────────

/// All tile metrics for a single application.
pub struct TileGroup {
    pub app_name: String,
    tiles: HashMap<String, TileData>,
    pub tile_order: Vec<String>,
}

impl TileGroup {
    fn new(app_name: String) -> Self {
        Self { app_name, tiles: HashMap::new(), tile_order: Vec::new() }
    }

    pub fn is_empty(&self) -> bool {
        self.tiles.is_empty()
    }

    /// Scan the app's shmem/queues directory for `tilemetrics-*` files.
    fn discover(&mut self, base_dir: &Path) {
        let dir = shmem_dir_queues_string_with_base(base_dir, &self.app_name);
        let Ok(entries) = std::fs::read_dir(&dir) else {
            return;
        };

        for entry in entries.flatten() {
            let path = entry.path();
            let Some(fname) = path.file_name().and_then(|s| s.to_str()) else {
                continue;
            };
            let Some(tile_name) = fname.strip_prefix("tilemetrics-") else {
                continue;
            };
            if self.tiles.contains_key(tile_name) {
                continue;
            }

            let Ok(queue) = Queue::<TileSample>::try_open_shared(&path) else { continue };

            let consumer = Consumer::new(queue, "flux-ctl-tile").without_log();
            let tile_name = tile_name.to_string();
            self.tiles.insert(tile_name.clone(), TileData::new(consumer));
            self.tile_order.push(tile_name);
        }

        self.tile_order.sort();
    }

    /// Consume pending samples for all tiles.
    fn update(&mut self) {
        for tile in self.tiles.values_mut() {
            tile.update();
        }
    }

    /// Get stats for a single tile by name.
    pub fn tile_stats(&self, name: &str, window_secs: f64) -> Option<TileStats> {
        self.tiles.get(name)?.stats(window_secs)
    }
}

// ── Top-level store ──────────────────────────────────────────────────────

/// Discovers and updates tile metrics across all applications.
pub struct TileMetricsStore {
    groups: Vec<TileGroup>,
    last_discover: Option<Instant>,
    base_dir: PathBuf,
    stats_window_secs: f64,
    pub selected: usize,
}

impl TileMetricsStore {
    pub fn new(base_dir: &Path) -> Self {
        Self {
            groups: Vec::new(),
            last_discover: None,
            base_dir: base_dir.to_path_buf(),
            stats_window_secs: DEFAULT_STATS_WINDOW_SECS,
            selected: 0,
        }
    }

    /// Discover new apps + tiles and consume pending samples.
    pub fn tick(&mut self) {
        let should_discover = self.last_discover.is_none_or(|t| t.elapsed() > discover_interval());

        if should_discover {
            self.discover_apps();
            for group in &mut self.groups {
                group.discover(&self.base_dir);
            }
            self.last_discover = Some(Instant::now());
        }

        for group in &mut self.groups {
            group.update();
        }
    }

    /// Scan `base_dir` for app directories containing shmem/queues.
    fn discover_apps(&mut self) {
        let shmem_root = &self.base_dir;
        let Ok(entries) = std::fs::read_dir(shmem_root) else {
            return;
        };
        for entry in entries.flatten() {
            if !entry.file_type().map(|ft| ft.is_dir()).unwrap_or(false) {
                continue;
            }
            let Some(name) = entry.file_name().to_str().map(String::from) else {
                continue;
            };
            if self.groups.iter().any(|g| g.app_name == name) {
                continue;
            }
            self.groups.push(TileGroup::new(name));
        }
        self.groups.sort_by(|a, b| a.app_name.cmp(&b.app_name));
    }

    pub fn groups(&self) -> &[TileGroup] {
        &self.groups
    }

    pub fn stats_window_secs(&self) -> f64 {
        self.stats_window_secs
    }

    /// Total number of tiles found across all apps (for display purposes).
    pub fn total_tiles(&self) -> usize {
        self.groups.iter().map(|g| g.tile_order.len()).sum()
    }

    /// Total number of displayable rows (for navigation).
    pub fn total_rows(&self) -> usize {
        self.groups.iter().filter(|g| !g.is_empty()).map(|g| 1 + g.tile_order.len()).sum()
    }

    pub fn select_next(&mut self) {
        let total = self.total_rows();
        if total > 0 {
            self.selected = (self.selected + 1).min(total - 1);
        }
    }

    pub fn select_prev(&mut self) {
        self.selected = self.selected.saturating_sub(1);
    }

    pub fn select_home(&mut self) {
        self.selected = 0;
    }

    pub fn select_end(&mut self) {
        let total = self.total_rows();
        self.selected = total.saturating_sub(1);
    }

    pub fn select_page_down(&mut self) {
        let total = self.total_rows();
        if total > 0 {
            self.selected = (self.selected + 10).min(total - 1);
        }
    }

    pub fn select_page_up(&mut self) {
        self.selected = self.selected.saturating_sub(10);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn empty_store_has_zero_rows() {
        let store = TileMetricsStore::new(Path::new("/nonexistent"));
        assert_eq!(store.total_tiles(), 0);
        assert_eq!(store.total_rows(), 0);
    }

    #[test]
    fn navigation_empty_store_does_not_panic() {
        let mut store = TileMetricsStore::new(Path::new("/nonexistent"));
        store.select_next();
        store.select_prev();
        store.select_home();
        store.select_end();
        store.select_page_down();
        store.select_page_up();
        assert_eq!(store.selected, 0);
    }
}
