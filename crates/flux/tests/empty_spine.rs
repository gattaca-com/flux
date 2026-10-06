use flux::{communication::ShmemData, spine_derive::from_spine, tile::TileInfo};

#[from_spine("test-empty")]
#[derive(Debug)]
struct EmptySpine {
    pub tile_info: ShmemData<TileInfo>,
}

#[test]
fn spine_without_queues_starts() {
    let dir = tempfile::tempdir().unwrap();
    let spine = EmptySpine::new_with_base_dir(dir.path(), None);
    spine.start(None, None, |_scoped| {});
}
