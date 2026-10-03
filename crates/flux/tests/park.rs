#[cfg(feature = "park")]
mod tests {
    use std::{
        sync::{
            Arc,
            atomic::{AtomicBool, Ordering},
        },
        thread,
        time::Duration,
    };

    use flux::park::Signal;

    #[test]
    fn test_park_unpark() {
        let signal = Arc::new(Signal::new());
        let signal_clone = signal.clone();
        let parked = Arc::new(AtomicBool::new(false));
        let parked_clone = parked.clone();

        let counter = signal.read_counter();

        let handle = thread::spawn(move || {
            parked_clone.store(true, Ordering::Release);
            signal_clone.park(counter);
            parked_clone.store(false, Ordering::Release);
        });

        // Wait for thread to run and park
        thread::sleep(Duration::from_millis(50));
        assert!(parked.load(Ordering::Acquire));

        // Signal to unpark
        signal.signal();

        handle.join().unwrap();
        assert!(!parked.load(Ordering::Acquire));
    }

    #[test]
    fn test_sticky_behavior() {
        let signal = Signal::new();
        let counter = signal.read_counter();

        // Signal first (sticky)
        signal.signal();

        // Park with old counter should not block
        let start = std::time::Instant::now();
        signal.park(counter);
        assert!(start.elapsed() < Duration::from_millis(50));
    }

    #[test]
    fn test_mio_waker() {
        let signal = Signal::new();
        let mut poll = mio::Poll::new().unwrap();
        let waker = mio::Waker::new(poll.registry(), mio::Token(42)).unwrap();

        signal.register_waker(waker);

        signal.signal();

        let mut events = mio::Events::with_capacity(10);
        poll.poll(&mut events, Some(Duration::from_millis(500))).unwrap();

        let mut found = false;
        for event in &events {
            if event.token() == mio::Token(42) {
                found = true;
            }
        }

        assert!(found, "mio waker was not woken");
    }
}

#[cfg(feature = "park")]
mod runner {
    use std::{
        sync::{atomic::Ordering, mpsc},
        time::Duration,
    };

    use flux::{
        communication::{ShmemData, cleanup_shmem},
        park::SIGNAL,
        spine::{ScopedSpine, SpineAdapter, SpineQueue},
        tile::{Tile, TileConfig, TileInfo, tile_runner},
    };
    use serde::{Deserialize, Serialize};
    use spine_derive::from_spine;

    #[derive(Clone, Copy, Default, Debug, Serialize, Deserialize)]
    #[repr(C)]
    struct Message(u64);

    #[from_spine("external-parking-test")]
    #[derive(Debug)]
    struct TestSpine {
        pub tile_info: ShmemData<TileInfo>,
        pub messages: SpineQueue<Message>,
    }

    struct ExternalWaitTile {
        loops: usize,
        finished: mpsc::Sender<()>,
        poll: Option<mio::Poll>,
    }

    impl Tile<TestSpine> for ExternalWaitTile {
        fn try_init(&mut self, _adapter: &mut SpineAdapter<TestSpine>) -> bool {
            if let Some(poll) = &self.poll {
                let waker = mio::Waker::new(poll.registry(), mio::Token(0)).unwrap();
                SIGNAL.register_waker(waker);
            }
            true
        }

        fn loop_body(&mut self, adapter: &mut SpineAdapter<TestSpine>) {
            if let Some(poll) = &mut self.poll {
                let mut events = mio::Events::with_capacity(4);
                poll.poll(&mut events, Some(Duration::from_millis(20))).unwrap();
            }
            self.loops += 1;
            if self.loops == 2 {
                self.finished.send(()).unwrap();
                adapter.request_stop_scope();
            }
        }
    }

    #[test]
    fn disabled_parking_runs_again_without_a_spine_signal() {
        // Exercise both external wait styles under the same config. Run them
        // serially so their shutdown signals cannot mask accidental parking.
        for with_mio in [false, true] {
            let directory = tempfile::tempdir().unwrap();
            let mut spine = TestSpine::new_with_base_dir(directory.path(), None);
            let (finished, receiver) = mpsc::channel();
            let poll = with_mio.then(|| mio::Poll::new().unwrap());

            std::thread::scope(|scope| {
                let mut scoped = ScopedSpine::new(&mut spine, scope, None, None);
                let run = tile_runner(
                    ExternalWaitTile { loops: 0, finished, poll },
                    &mut scoped,
                    TileConfig::background(None, None).with_park().without_park().without_metrics(),
                );
                let runner = scope.spawn(run);
                let result = receiver.recv_timeout(Duration::from_secs(2));

                // Release a mistakenly parked runner before reporting the regression.
                scoped.stop_flag.store(1, Ordering::Relaxed);
                SIGNAL.signal();
                runner.join().unwrap();
                assert!(
                    result.is_ok(),
                    "runner parked despite parking being disabled (mio={with_mio})"
                );
            });
            cleanup_shmem(directory.path());
        }
    }
}
