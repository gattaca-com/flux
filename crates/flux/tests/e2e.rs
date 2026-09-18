use std::sync::{
    Arc,
    atomic::{AtomicU64, Ordering},
};

use flux::{
    communication::{ShmemData, cleanup_shmem},
    spine::{SpineAdapter, SpineQueue},
    tile::{Tile, TileConfig, TileInfo, attach_tile},
};
use flux_timing::Duration;
use serde::{Deserialize, Serialize};
use spine_derive::from_spine;

#[derive(Clone, Copy, Default, Debug, Serialize, Deserialize)]
#[repr(C)]
struct TestMsg(u64);

#[derive(Clone, Copy, Default, Debug, Serialize, Deserialize)]
#[repr(C)]
struct OtherTestMsg(u8);

#[from_spine("test-app")]
#[derive(Debug)]
struct MySpine {
    pub tile_info: ShmemData<TileInfo>,
    #[queue(size(2usize.pow(14)))]
    pub q: SpineQueue<TestMsg>,
    #[queue(size(2usize.pow(14)))]
    pub q2: SpineQueue<OtherTestMsg>,
}

#[derive(Clone, Copy, Default)]
struct ProducerTile {
    val: u64,
}

impl Tile<MySpine> for ProducerTile {
    fn loop_body(&mut self, adapter: &mut SpineAdapter<MySpine>) {
        adapter.produce(TestMsg(self.val));
    }
}

#[derive(Clone)]
struct ConsumerTile {
    received: Arc<AtomicU64>,
}

impl Tile<MySpine> for ConsumerTile {
    fn loop_body(&mut self, adapter: &mut SpineAdapter<MySpine>) {
        let mut received_one = false;
        adapter.consume(|m: TestMsg, _producers| {
            self.received.store(m.0, Ordering::Relaxed);
            received_one = true;
        });
        if received_one {
            adapter.request_stop_scope();
        }
    }
}

#[test]
fn end_to_end_send_receive_and_exit() {
    let tmp = tempfile::tempdir().expect("create temp dir");
    let base = tmp.path();
    let mut spine = MySpine::new_with_base_dir(base, None);

    let got = Arc::new(AtomicU64::new(0));
    let want = 42u64;

    std::thread::scope(|scope| {
        let mut scoped = flux::spine::ScopedSpine::new(&mut spine, scope, None, None);

        attach_tile(
            ProducerTile { val: want },
            &mut scoped,
            TileConfig::background(None, Some(Duration::from_millis(10))),
        );
        attach_tile(
            ConsumerTile { received: got.clone() },
            &mut scoped,
            TileConfig::background(None, None),
        );
    });

    cleanup_shmem(base);

    assert_eq!(got.load(Ordering::Relaxed), want);
}

#[test]
fn idle_backoff_allows_delivery_after_idle_and_scope_stop() {
    use std::sync::mpsc::{Receiver, SyncSender, sync_channel};

    struct DelayedWriter {
        ready: Receiver<()>,
        sent: bool,
        value: u64,
    }

    impl Tile<MySpine> for DelayedWriter {
        fn try_init(&mut self, _adapter: &mut SpineAdapter<MySpine>) -> bool {
            self.ready.try_recv().is_ok()
        }

        fn loop_body(&mut self, adapter: &mut SpineAdapter<MySpine>) {
            if !self.sent {
                adapter.produce(TestMsg(self.value));
                self.sent = true;
            }
        }
    }

    struct IdleReader {
        ready: Option<SyncSender<()>>,
        idle_iterations: usize,
        received: Arc<AtomicU64>,
    }

    impl Tile<MySpine> for IdleReader {
        fn on_attach(&mut self, adapter: &mut SpineAdapter<MySpine>) {
            adapter.subscribe_broadcast::<TestMsg>();
        }

        fn loop_body(&mut self, adapter: &mut SpineAdapter<MySpine>) {
            let mut received = false;
            adapter.consume(|m: TestMsg, _| {
                self.received.store(m.0, Ordering::Relaxed);
                received = true;
            });
            if received {
                adapter.request_stop_scope();
            } else {
                self.idle_iterations += 1;
                if self.idle_iterations >= 2 &&
                    let Some(ready) = self.ready.take()
                {
                    ready.send(()).unwrap();
                }
            }
        }
    }

    for pauses in [0, 16, 32] {
        let tmp = tempfile::tempdir().unwrap();
        let mut spine = MySpine::new_with_base_dir(tmp.path(), None);
        let got = Arc::new(AtomicU64::new(0));
        let want = 42;
        let (ready_tx, ready_rx) = sync_channel(1);

        std::thread::scope(|scope| {
            let mut scoped = flux::spine::ScopedSpine::new(&mut spine, scope, None, None);
            attach_tile(
                IdleReader { ready: Some(ready_tx), idle_iterations: 0, received: got.clone() },
                &mut scoped,
                TileConfig::background(None, None).without_metrics().with_idle_backoff(pauses),
            );
            attach_tile(
                DelayedWriter { ready: ready_rx, sent: false, value: want },
                &mut scoped,
                TileConfig::background(None, None).without_metrics(),
            );
        });

        assert_eq!(got.load(Ordering::Relaxed), want);
        cleanup_shmem(tmp.path());
    }
}
