pub mod metrics;

use core::sync::atomic::Ordering;

use flux_timing::{Duration, IngestionTime};
use flux_utils::{ShortTypename, ThreadNiceness, get_tid, short_typename, thread_boot, vsync};
use tracing::{Level, info, span};

use crate::{
    spine::{FluxSpine, ScopedSpine, SpineAdapter},
    tile::metrics::TileMetrics,
};

pub type TileID = u16;
pub type TileName = ShortTypename;

#[derive(Clone, Debug)]
pub struct TileConfig {
    cores: Vec<usize>,
    thread_niceness: Option<ThreadNiceness>,
    min_loop_duration: Option<Duration>,
    metrics: bool,
    idle_backoff: u32,
    #[cfg_attr(not(feature = "park"), allow(dead_code))]
    park: bool,
}

impl TileConfig {
    pub fn new(core: usize, thread_niceness: Option<ThreadNiceness>) -> Self {
        Self {
            cores: vec![core],
            thread_niceness,
            min_loop_duration: None,
            metrics: true,
            idle_backoff: 0,
            park: false,
        }
    }

    /// Boot a tile with a background (non-hot-path) config.
    /// Supports optional vsync pacing and inherits the process niceness.
    pub fn background(core: Option<usize>, min_loop_duration: Option<Duration>) -> Self {
        Self::background_on_cores(core.into_iter().collect(), min_loop_duration)
    }

    /// Background config pinned to a set of cores. Empty pins nowhere.
    pub fn background_on_cores(cores: Vec<usize>, min_loop_duration: Option<Duration>) -> Self {
        Self {
            cores,
            thread_niceness: None,
            min_loop_duration,
            metrics: true,
            idle_backoff: 0,
            park: false,
        }
    }

    /// Cores the tile is pinned to on startup. Empty means unpinned.
    pub fn cores(&self) -> &[usize] {
        &self.cores
    }

    pub fn without_metrics(mut self) -> Self {
        self.metrics = false;
        self
    }

    /// Issue `pauses` spin hints after an iteration with no recorded work.
    /// Defaults to zero. This can reduce polling contention at the cost of
    /// latency for new work after idle period; the delay depends on the CPU.
    /// Report work outside adapter operations with [`SpineAdapter::mark_work`].
    /// Parking takes precedence when enabled and eligible.
    pub fn with_idle_backoff(mut self, pauses: u32) -> Self {
        self.idle_backoff = pauses;
        self
    }

    /// Parked tiles wake only on spine producer signals; never for tiles that
    /// poll sockets or disk.
    pub fn with_park(mut self) -> Self {
        self.park = true;
        self
    }
}

/// Tile is a fixed execution unit pinned to a CPU core.
pub trait Tile<S: FluxSpine>: Send + Sized {
    /// This is tile’s primary business logic. Called repeatedly until stop flag
    /// is set.
    ///
    /// Work tracking is automatic via adapter.consume/produce* methods.
    /// For non-consume/produce work (e.g. business logic ticks), call
    /// `adapter.mark_work()`.
    fn loop_body(&mut self, _adapter: &mut SpineAdapter<S>);

    /// Called once on the attaching thread after adapter construction, before
    /// the worker starts. Use this to establish broadcast subscriptions
    /// before starting producers.
    fn on_attach(&mut self, _adapter: &mut SpineAdapter<S>) {}

    /// User init before loop. State setup etc.
    /// Called repeatedly until it returns true.
    fn try_init(&mut self, _adapter: &mut SpineAdapter<S>) -> bool {
        true
    }

    /// User teardown after `scoped_stop_flag` is flipped on.
    fn teardown(self, _adapter: &mut SpineAdapter<S>) {}

    /// Tile name for logging, tracing, metrics. No heap allocation.
    fn name(&self) -> TileName {
        short_typename::<Self>()
    }
}

/// Boot and run a tile thread.
/// Configures affinity and niceness, then executes the tile lifecycle.
/// Does not exit until the global stop flag is set.
pub fn attach_tile<'a, S, T>(tile: T, spine: &mut ScopedSpine<'a, '_, S>, config: TileConfig)
where
    S: FluxSpine,
    T: Tile<S> + 'a,
{
    let name = tile.name();
    let run = tile_runner(tile, spine, config);

    if name.as_str().is_empty() {
        spine.scope.spawn(run);
    } else {
        std::thread::Builder::new()
            .name(name.as_str().to_owned())
            .spawn_scoped(spine.scope, run)
            .expect("spawn tile thread");
    }
}

/// The tile's whole life as one closure.
///
/// Call it to run the tile on the calling thread, which is what a single-tile
/// process wants rather than leaving that thread parked in
/// [`start`](crate::spine::FluxSpine). [`attach_tile`] spawns it instead.
pub fn tile_runner<'a, S, T>(
    mut tile: T,
    spine: &mut ScopedSpine<'a, '_, S>,
    config: TileConfig,
) -> impl FnOnce() + Send + use<'a, S, T>
where
    S: FluxSpine,
    T: Tile<S> + 'a,
{
    let stop_flag = spine.stop_flag.clone();
    let mut adapter =
        SpineAdapter::connect_tile_with_stop_flag(&tile, spine.spine, stop_flag.clone());
    tile.on_attach(&mut adapter);
    let mut metrics = if config.metrics {
        Some(TileMetrics::new(spine.spine.base_dir(), S::app_name(), tile.name()))
    } else {
        None
    };

    move || {
        let _span = span!(Level::INFO, "", tile = %tile.name()).entered();
        thread_boot(&config.cores, config.thread_niceness);

        while !tile.try_init(&mut adapter) {
            if stop_flag.load(Ordering::Relaxed) != 0 {
                tile.teardown(&mut adapter);
                info!("Tile exited before initialisation. teardown complete");
                return;
            }
            std::hint::spin_loop();
        }
        info!(tid = get_tid(), "Tile init complete");

        #[cfg(feature = "park")]
        let mut expected = crate::park::SIGNAL.read_counter();

        loop {
            let ingestion_t = IngestionTime::now();

            if let Some(m) = &mut metrics {
                m.begin(ingestion_t);
            }

            vsync(config.min_loop_duration, || {
                adapter.begin_loop(ingestion_t);
                tile.loop_body(&mut adapter);
            });

            let worked = adapter.did_work();
            if let Some(m) = &mut metrics {
                m.end(worked);
            }

            if stop_flag.load(Ordering::Relaxed) != 0 {
                break;
            }

            #[cfg(feature = "park")]
            {
                if config.park && !worked && !adapter.waker_registered() {
                    crate::park::SIGNAL.park(expected);
                    expected = crate::park::SIGNAL.read_counter();
                    continue;
                }
                expected = crate::park::SIGNAL.read_counter();
            }

            if !worked {
                for _ in 0..config.idle_backoff {
                    std::hint::spin_loop();
                }
            }
        }

        tile.teardown(&mut adapter);

        #[cfg(feature = "park")]
        crate::park::SIGNAL.signal();

        info!("Tile teardown complete");
    }
}

#[derive(Clone, Copy, Debug)]
#[repr(C)]
pub struct TileInfo {
    pub tiles: [TileName; 255],
}

impl TileInfo {
    pub fn register_tile(&mut self, name: TileName) -> u16 {
        let mut i = 0;
        while i < 255 {
            let slot = &mut self.tiles[i];
            if *slot == name {
                return i as u16;
            } else if slot.is_empty() {
                *slot = name;
                return i as u16;
            }
            i += 1;
        }
        unreachable!("had more than 255 tiles!")
    }
}

impl Default for TileInfo {
    fn default() -> Self {
        Self { tiles: [TileName::new(); 255] }
    }
}
