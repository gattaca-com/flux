pub mod metrics;

use core::sync::atomic::Ordering;

use flux_timing::{Duration, IngestionTime};
use flux_utils::{ShortTypename, ThreadPriority, short_typename, thread_boot, vsync};
use tracing::{Level, info, span};

use crate::{
    spine::{FluxSpine, ScopedSpine, SpineAdapter},
    tile::metrics::TileMetrics,
};

pub type TileID = u16;
pub type TileName = ShortTypename;

#[derive(Clone, Copy, Debug)]
pub struct TileConfig {
    core: Option<usize>,
    thread_prio: ThreadPriority,
    min_loop_duration: Option<Duration>,
    metrics: bool,
}

impl TileConfig {
    pub fn new(core: usize, thread_prio: ThreadPriority) -> Self {
        Self { core: Some(core), thread_prio, min_loop_duration: None, metrics: true }
    }

    /// Boot a tile with a background (non-hot-path) config.
    /// Supports optional vsync pacing and relaxed scheduling.
    pub fn background(core: Option<usize>, min_loop_duration: Option<Duration>) -> Self {
        Self { core, thread_prio: ThreadPriority::OSDefault, min_loop_duration, metrics: true }
    }

    pub fn without_metrics(mut self) -> Self {
        self.metrics = false;
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
    /// adapter.mark_work().
    fn loop_body(&mut self, _adapter: &mut SpineAdapter<S>);

    /// User init before loop. State setup etc.
    /// Called repeatedly until it returns true.
    fn try_init(&mut self, _adapter: &mut SpineAdapter<S>) -> bool {
        true
    }

    /// User teardown after scoped_stop_flag is flipped on.
    fn teardown(self, _adapter: &mut SpineAdapter<S>) {}

    /// Tile name for logging, tracing, metrics. No heap allocation.
    fn name(&self) -> TileName {
        short_typename::<Self>()
    }
}

/// Boot and run a tile thread.
/// Configures affinity and priority, then executes the tile lifecycle.
/// Does not exit until the global stop flag is set.
pub fn attach_tile<'a, S, T>(mut tile: T, spine: &mut ScopedSpine<'a, '_, S>, config: TileConfig)
where
    S: FluxSpine,
    T: Tile<S> + 'a,
{
    let stop_flag = spine.stop_flag.clone();
    let mut adapter =
        SpineAdapter::connect_tile_with_stop_flag(&tile, spine.spine, stop_flag.clone());

    let mut metrics =
        if config.metrics { Some(TileMetrics::new(S::app_name(), tile.name())) } else { None };

    spine.scope.spawn(move || {
        let _span = span!(Level::INFO, "", tile = %tile.name()).entered();
        thread_boot(config.core, config.thread_prio);

        while !tile.try_init(&mut adapter) {
            if stop_flag.load(Ordering::Relaxed) != 0 {
                tile.teardown(&mut adapter);
                info!("Tile exited before initialisation. teardown complete");
                return;
            }
            std::hint::spin_loop();
        }
        info!("Tile init complete");

        loop {
            let ingestion_t = IngestionTime::now();

            if let Some(m) = &mut metrics {
                m.begin(ingestion_t);
            }

            vsync(config.min_loop_duration, || {
                adapter.begin_loop(ingestion_t);
                tile.loop_body(&mut adapter)
            });

            if let Some(m) = &mut metrics {
                m.end(adapter.did_work());
            }

            if stop_flag.load(Ordering::Relaxed) != 0 {
                break;
            }
        }

        tile.teardown(&mut adapter);
        info!("Tile teardown complete");
    });
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
