use std::sync::{
    Arc,
    atomic::{AtomicU64, Ordering},
};

use flux::{
    communication::{ShmemData, cleanup_shmem},
    persistence::Persistable,
    spine::{SpineAdapter, SpineQueue},
    tile::{Tile, TileConfig, TileInfo, attach_tile},
};
use flux_timing::Duration;
use flux_utils::directories::{shmem_dir_data_with_base, shmem_dir_queues_with_base};
use serde::{Deserialize, Serialize};
use spine_derive::from_spine;

#[derive(Clone, Copy, Default, Debug, Serialize, Deserialize)]
#[repr(C)]
struct MsgA(u64);

impl Persistable for MsgA {
    const PERSIST_DIR: &'static str = "msg_a";
}

#[derive(Clone, Copy, Default, Debug, Serialize, Deserialize)]
#[repr(C)]
struct MsgB(u8);

impl Persistable for MsgB {
    const PERSIST_DIR: &'static str = "msg_b";
}

#[from_spine("spine-test-app")]
#[derive(Debug)]
struct TestSpine {
    pub tile_info: ShmemData<TileInfo>,
    #[queue(size(2usize.pow(14)))]
    pub qa: SpineQueue<MsgA>,
    #[queue(size(2usize.pow(14)))]
    pub qb: SpineQueue<MsgB>,
}

#[derive(Clone, Copy, Default)]
struct Writer;

impl Tile<TestSpine> for Writer {
    fn loop_body(&mut self, adapter: &mut SpineAdapter<TestSpine>) {
        adapter.produce(MsgA(42));
    }
}

#[derive(Clone)]
struct Reader {
    received: Arc<AtomicU64>,
}

impl Tile<TestSpine> for Reader {
    fn loop_body(&mut self, adapter: &mut SpineAdapter<TestSpine>) {
        let mut got_one = false;
        adapter.consume(|m: MsgA, _| {
            self.received.store(m.0, Ordering::Relaxed);
            got_one = true;
        });
        if got_one {
            adapter.request_stop_scope();
        }
    }
}

fn all_files_under(root: &std::path::Path) -> Vec<std::path::PathBuf> {
    let mut out = Vec::new();
    if !root.exists() {
        return out;
    }
    for entry in std::fs::read_dir(root).unwrap() {
        let entry = entry.unwrap();
        let path = entry.path();
        if path.is_dir() {
            out.extend(all_files_under(&path));
        } else {
            out.push(path);
        }
    }
    out
}

/// Creates a spine with a custom temp `base_dir`, attaches two tiles
/// (writer + reader), runs until the reader receives a message, then
/// asserts that every shared-memory file was created inside the
/// specified `base_dir` and nowhere else.
#[test]
fn all_shmem_files_reside_in_base_dir() {
    let tmp = tempfile::tempdir().expect("create temp dir");
    let base = tmp.path();

    let mut spine = TestSpine::new_with_base_dir(base, None);

    let got = Arc::new(AtomicU64::new(0));

    std::thread::scope(|scope| {
        let mut scoped = flux::spine::ScopedSpine::new(&mut spine, scope, None, None);

        attach_tile(
            Writer,
            &mut scoped,
            TileConfig::background(None, Some(Duration::from_millis(10))),
        );
        attach_tile(
            Reader { received: got.clone() },
            &mut scoped,
            TileConfig::background(None, None),
        );
    });

    assert_eq!(got.load(Ordering::Relaxed), 42, "reader should have received the value");

    let files = all_files_under(base);
    assert!(!files.is_empty(), "expected at least one shmem file inside {}", base.display());

    let queues_dir = shmem_dir_queues_with_base(base, "spine-test-app");
    assert!(queues_dir.is_dir(), "queues dir should exist: {}", queues_dir.display());

    let data_dir = shmem_dir_data_with_base(base, "spine-test-app");
    assert!(data_dir.is_dir(), "data dir should exist: {}", data_dir.display());

    let tile_info_file = data_dir.join("TileInfo");
    assert!(tile_info_file.exists(), "TileInfo shmem should exist at {}", tile_info_file.display());

    let queue_files: Vec<_> = files
        .iter()
        .filter(|p| {
            let name = p.file_name().unwrap().to_string_lossy();
            name.contains("InternalMessage")
        })
        .collect();
    assert!(
        queue_files.len() >= 2,
        "expected at least 2 queue files (MsgA + MsgB), got {}: {:?}",
        queue_files.len(),
        queue_files
    );

    let timing_files: Vec<_> = files
        .iter()
        .filter(|p| {
            let name = p.file_name().unwrap().to_string_lossy();
            name.starts_with("timing-")
        })
        .collect();
    assert!(!timing_files.is_empty(), "expected timing shmem files, found none");

    let latency_files: Vec<_> = files
        .iter()
        .filter(|p| {
            let name = p.file_name().unwrap().to_string_lossy();
            name.starts_with("latency-")
        })
        .collect();
    assert!(!latency_files.is_empty(), "expected latency shmem files, found none");

    let metrics_files: Vec<_> = files
        .iter()
        .filter(|p| {
            let name = p.file_name().unwrap().to_string_lossy();
            name.starts_with("tilemetrics-")
        })
        .collect();
    assert!(
        !metrics_files.is_empty(),
        "expected tilemetrics shmem files inside base_dir, found none"
    );

    for f in &files {
        assert!(
            f.starts_with(base),
            "shmem file {} is NOT inside base_dir {}",
            f.display(),
            base.display()
        );
    }

    cleanup_shmem(base);
}
