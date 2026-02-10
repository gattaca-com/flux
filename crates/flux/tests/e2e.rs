use std::sync::{
    Arc,
    atomic::{AtomicU64, Ordering},
};

use flux::{
    communication::ShmemData,
    persistence::Persistable,
    spine::{FluxSpine, SpineAdapter, SpineQueue},
    tile::{Tile, TileConfig, TileInfo, attach_tile},
};
use flux_timing::Duration;
use serde::{Deserialize, Serialize};
use spine_derive::from_spine;

#[derive(Clone, Copy, Default, Debug, Serialize, Deserialize)]
#[repr(C)]
struct TestMsg(u64);

impl Persistable for TestMsg {
    const PERSIST_DIR: &'static str = "test_msg";
}

#[derive(Clone, Copy, Default, Debug, Serialize, Deserialize)]
#[repr(C)]
struct OtherTestMsg(u8);

impl Persistable for OtherTestMsg {
    const PERSIST_DIR: &'static str = "other_test_msg";
}
#[from_spine("test-app")]
#[derive(Debug)]
struct MySpine {
    pub tile_info: ShmemData<TileInfo>,
    #[queue(persist, size(2usize.pow(14)))]
    pub q: SpineQueue<TestMsg>,
    #[queue(persist, size(2usize.pow(14)))]
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
    let mut spine = MySpine::new(None);

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

    MySpine::remove_all_files();

    assert_eq!(got.load(Ordering::Relaxed), want);
}
