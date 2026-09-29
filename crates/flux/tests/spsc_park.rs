#![cfg(feature = "park")]

// ScopedSpine installs a process-wide panic hook that stops runners even for
// caught panics. Keep runner tests separate from tests of callback unwinding.
use std::{
    sync::{
        Arc,
        atomic::{AtomicU64, AtomicUsize, Ordering},
    },
    time::Duration,
};

use flux::{
    communication::{ShmemData, cleanup_shmem},
    spine::SpineAdapter,
    tile::{Tile, TileConfig, TileInfo, tile_runner},
};
use spine_derive::from_spine;

#[derive(Clone, Copy, Debug)]
#[repr(C)]
struct SpscMessage(u64);

#[from_spine("spsc-park")]
struct ParkSpine {
    tile_info: ShmemData<TileInfo>,
    #[queue(size(2), flavour("spsc"))]
    spsc: flux::spine::SpineQueue<SpscMessage>,
}

#[derive(Clone)]
struct ParkConsumer {
    empty_polls: Arc<AtomicUsize>,
    received: Arc<AtomicU64>,
    done: std::sync::mpsc::Sender<()>,
}

impl Tile<ParkSpine> for ParkConsumer {
    fn loop_body(&mut self, adapter: &mut SpineAdapter<ParkSpine>) {
        let received = self.received.clone();
        let consumed = adapter
            .try_consume_one(|message: SpscMessage, _| received.store(message.0, Ordering::Relaxed))
            .expect("consumer owns SPSC role");
        if consumed {
            adapter.request_stop_scope();
            self.done.send(()).expect("watchdog is listening");
        } else {
            self.empty_polls.fetch_add(1, Ordering::Release);
        }
    }
}

struct ParkProducer {
    consumer_empty_polls: Arc<AtomicUsize>,
    sent: bool,
}

impl Tile<ParkSpine> for ParkProducer {
    fn try_init(&mut self, _: &mut SpineAdapter<ParkSpine>) -> bool {
        self.consumer_empty_polls.load(Ordering::Acquire) >= 2
    }

    fn loop_body(&mut self, adapter: &mut SpineAdapter<ParkSpine>) {
        if !self.sent {
            adapter.try_produce(SpscMessage(101)).expect("producer owns SPSC role");
            self.sent = true;
        }
    }
}

#[test]
fn parked_tile_runner_keeps_claimed_spsc_endpoints_polling() {
    let tmp = tempfile::tempdir().expect("create temp directory");
    // SAFETY: isolated fixture, one schema, and no inherited endpoints.
    let mut spine = unsafe { ParkSpine::new_with_base_dir(tmp.path(), None) };
    let empty_polls = Arc::new(AtomicUsize::new(0));
    let received = Arc::new(AtomicU64::new(0));
    let (done, completion) = std::sync::mpsc::channel();

    let completion = std::thread::scope(|scope| {
        let mut scoped = flux::spine::ScopedSpine::new(&mut spine, scope, None, None);
        // Construct both runners before starting either: timer setup must not
        // provide a process-local signal that masks a parked SPSC consumer.
        let consumer = tile_runner(
            ParkConsumer { empty_polls: empty_polls.clone(), received: received.clone(), done },
            &mut scoped,
            TileConfig::background(None, None).without_metrics().with_park(),
        );
        let producer = tile_runner(
            ParkProducer { consumer_empty_polls: empty_polls, sent: false },
            &mut scoped,
            TileConfig::background(None, None).without_metrics().with_park(),
        );
        let stop = scoped.stop_flag.clone();
        let watchdog = scope.spawn(move || {
            let result = completion.recv_timeout(Duration::from_secs(5));
            if result.is_err() {
                stop.store(signal_hook::consts::SIGINT as usize, Ordering::Relaxed);
                flux::park::SIGNAL.signal();
            }
            result
        });
        scope.spawn(consumer);
        scope.spawn(producer);
        watchdog.join().expect("watchdog panicked")
    });

    completion.expect("SPSC runners must deliver without an IPC wakeup");
    assert_eq!(received.load(Ordering::Relaxed), 101);
    drop(spine);
    cleanup_shmem(tmp.path());
}
