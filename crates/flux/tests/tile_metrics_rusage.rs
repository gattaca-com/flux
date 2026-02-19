use std::{sync::atomic::Ordering, thread, time::Duration as StdDuration};

use flux::{
    communication::{
        ShmemData,
        queue::{Consumer, Queue},
        shmem_dir_queues_string,
    },
    spine::{FluxSpine, SpineAdapter, SpineQueue},
    tile::{Tile, TileConfig, TileInfo, TileName, attach_tile, metrics::TileSample},
    timing::{Duration, Instant, Nanos},
    utils::directories::local_share_dir,
};
use spine_derive::from_spine;

#[derive(Clone, Copy, Default, Debug)]
#[repr(C)]
struct NoopMsg(u8);

#[from_spine("test-app-rusage-a")]
#[derive(Debug)]
struct RusageSpineA {
    pub tile_info: ShmemData<TileInfo>,
    #[queue(size(256))]
    pub noop: SpineQueue<NoopMsg>,
}

#[from_spine("test-app-rusage-b")]
#[derive(Debug)]
struct RusageSpineB {
    pub tile_info: ShmemData<TileInfo>,
    #[queue(size(256))]
    pub noop: SpineQueue<NoopMsg>,
}

#[derive(Clone, Copy, Debug)]
struct BusyTile {
    run_for: Duration,
    start: Option<Instant>,
    tile_name: TileName,
}

impl<S: FluxSpine> Tile<S> for BusyTile {
    fn loop_body(&mut self, adapter: &mut SpineAdapter<S>) {
        let start = self.start.get_or_insert_with(Instant::now);
        adapter.mark_work();

        if start.elapsed() >= self.run_for {
            adapter.request_stop_scope();
            return;
        }

        std::hint::spin_loop();
    }

    fn name(&self) -> TileName {
        self.tile_name
    }
}

fn drain_samples(consumer: &mut Consumer<TileSample>, out: &mut Vec<TileSample>) -> bool {
    let before = out.len();
    while consumer.consume(|sample| out.push(*sample)) {}
    out.len() > before
}

fn run_tile_and_collect_samples<S: FluxSpine>(
    tile_name: TileName,
    run_for: Duration,
) -> Vec<TileSample> {
    S::remove_all_files();
    let mut spine = S::new_in_base_dir(local_share_dir());
    let queue_file = format!("{}/tilemetrics-{tile_name}", shmem_dir_queues_string(S::app_name()));
    let mut samples = Vec::new();

    thread::scope(|scope| {
        let mut scoped = flux::spine::ScopedSpine::new(&mut spine, scope, None, None);

        attach_tile(
            BusyTile { run_for, start: None, tile_name },
            &mut scoped,
            TileConfig::background(None, None),
        );

        let mut consumer =
            Consumer::from(Queue::<TileSample>::open_shared(&queue_file)).without_log();

        while scoped.stop_flag.load(Ordering::Relaxed) == 0 {
            let _ = drain_samples(&mut consumer, &mut samples);
            thread::sleep(StdDuration::from_millis(10));
        }

        for _ in 0..20 {
            if !drain_samples(&mut consumer, &mut samples) {
                thread::sleep(StdDuration::from_millis(5));
            }
        }
    });

    S::remove_all_files();
    samples
}

#[test]
fn tile_metrics_collects_optional_rusage() {
    let samples = run_tile_and_collect_samples::<RusageSpineA>(
        TileName::from_str_truncate("rusage-tile-a"),
        Duration::from_millis(1300),
    );

    assert!(!samples.is_empty(), "expected tile metrics samples");

    #[cfg(unix)]
    assert!(
        samples.iter().any(|sample| sample.rusage.is_some()),
        "expected at least one rusage snapshot"
    );
}

#[test]
fn tile_metrics_rusage_is_rate_limited_and_printed() {
    let samples = run_tile_and_collect_samples::<RusageSpineB>(
        TileName::from_str_truncate("rusage-tile-b"),
        Duration::from_millis(2300),
    );

    let rusage_samples: Vec<_> = samples
        .iter()
        .filter_map(|sample| sample.rusage.map(|usage| (sample.window_end, usage)))
        .collect();

    #[cfg(unix)]
    {
        assert!(!rusage_samples.is_empty(), "expected at least one rusage snapshot");

        for (idx, (window_end, usage)) in rusage_samples.iter().take(5).enumerate() {
            println!(
                "rusage[{idx}] t={} user={}us sys={}us rss={} minflt={} majflt={} nvcsw={} nivcsw={}",
                window_end.0,
                usage.user_micros,
                usage.system_micros,
                usage.max_rss,
                usage.minor_page_faults,
                usage.major_page_faults,
                usage.voluntary_ctx_switches,
                usage.involuntary_ctx_switches,
            );
        }

        for pair in rusage_samples.windows(2) {
            let delta_ns = pair[1].0.0.saturating_sub(pair[0].0.0);
            assert!(
                delta_ns >= Nanos::from_millis(900).0,
                "rusage sampled too frequently (delta_ns={delta_ns})"
            );
        }
    }
}
