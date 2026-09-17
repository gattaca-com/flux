use flux::{spine::SpineAdapter, tile::Tile};
use flux_timing::{InternalMessage, Nanos};
use flux_utils::ArrayStr;
use flux_versioned_types::{BlobCache, HasVersionedLeaves};

use crate::{config::GatherConfig, meta::GatherMeta, shipper::BlobShipper, writer::BlobWriter};

/// Buffers gathered leaves and flushes them as one blob per leaf type.
pub struct GatherSender {
    cache: BlobCache,
    shipper: Option<BlobShipper>,
    writer: Option<BlobWriter>,
    meta: GatherMeta,
    zstd_level: i32,
    wire_skip: Vec<String>,
    disk_skip: Vec<String>,
    last_slot: Option<u64>,
}

impl GatherSender {
    /// `app` goes into `GatherMeta::app` (truncated to 32 bytes, as is
    /// `instance_id`).
    pub fn new(config: GatherConfig, app: &str) -> Self {
        let GatherConfig { instance_id, addrs, disk_dir, zstd_level, wire_skip, disk_skip } =
            config;
        Self {
            cache: BlobCache::new(),
            shipper: if addrs.is_empty() { None } else { Some(BlobShipper::new(addrs)) },
            writer: disk_dir.map(BlobWriter::new),
            meta: GatherMeta {
                slot: 0,
                flush_t: Nanos::ZERO,
                n_blobs: 0,
                instance_id: ArrayStr::from_str_truncate(&instance_id),
                app: ArrayStr::from_str_truncate(app),
            },
            zstd_level,
            wire_skip,
            disk_skip,
            last_slot: None,
        }
    }

    /// Buffer one message's leaves.
    pub fn push<T: HasVersionedLeaves>(&mut self, msg: &InternalMessage<T>) {
        self.cache.push(msg);
    }

    /// The buffer `GatherQueues::gather_into` drains into.
    pub fn cache_mut(&mut self) -> &mut BlobCache {
        &mut self.cache
    }

    /// Whether anything is buffered.
    pub fn is_empty(&self) -> bool {
        self.cache.is_empty()
    }

    /// One blob per pending leaf type, all carrying the same `GatherMeta`.
    /// Each blob goes to the shipper unless its `type_name` is in `wire_skip`,
    /// and to the writer unless it is in `disk_skip`. No-op when the cache
    /// is empty. Records `last_slot`.
    pub fn flush(&mut self, slot: u64) {
        self.last_slot = Some(slot);
        if self.cache.is_empty() {
            return;
        }
        let meta = GatherMeta {
            slot,
            flush_t: Nanos::now(),
            n_blobs: 0,
            instance_id: self.meta.instance_id,
            app: self.meta.app,
        };
        // TODO(merge): use BlobCache::n_blobs — set meta.n_blobs from cache.n_blobs()
        // after merge.
        self.cache.flush(&meta, self.zstd_level, |blob| {
            if !self.wire_skip.iter().any(|skip| skip.as_str() == blob.type_name()) {
                if let Some(shipper) = self.shipper.as_mut() {
                    shipper.ship(blob);
                }
            }
            if !self.disk_skip.iter().any(|skip| skip.as_str() == blob.type_name()) {
                if let Some(writer) = self.writer.as_mut() {
                    writer.write(blob);
                }
            }
        });
    }

    /// Progress TCP (connect retries + poll) and disk (completions). Call every
    /// loop iteration.
    pub fn drive(&mut self) {
        if let Some(shipper) = self.shipper.as_mut() {
            shipper.drive();
        }
        if let Some(writer) = self.writer.as_mut() {
            writer.poll();
        }
    }

    /// Flush whatever is pending as `last_slot + 1` (`0` if no boundary was
    /// ever seen), then wait for queued disk writes to land and give TCP a
    /// final poll.
    pub fn finish(mut self) {
        let slot = self.last_slot.map_or(0, |last| last + 1);
        self.flush(slot);
        if let Some(writer) = self.writer.as_mut() {
            writer.drain();
        }
        if let Some(shipper) = self.shipper.as_mut() {
            shipper.drive();
        }
    }

    fn set_app(&mut self, app: &str) {
        self.meta.app = ArrayStr::from_str_truncate(app);
    }
}

/// Generic tile: gathers every `queue(gather)` queue of `S`, flushes on
/// boundaries. Attach with `attach_tile(GatherTile::new(config), scoped,`
/// `TileConfig::background(..))`.
pub struct GatherTile {
    sender: GatherSender,
}

impl GatherTile {
    /// `app` is filled from `S::app_name()` in `on_attach`.
    pub fn new(config: GatherConfig) -> Self {
        Self { sender: GatherSender::new(config, "") }
    }
}

impl<S: crate::queues::GatherQueues> Tile<S> for GatherTile {
    fn on_attach(&mut self, _adapter: &mut SpineAdapter<S>) {
        self.sender.set_app(S::app_name());
    }

    fn loop_body(&mut self, adapter: &mut SpineAdapter<S>) {
        if let Some(slot) = S::gather_into(adapter, self.sender.cache_mut()) {
            self.sender.flush(slot);
        }
        self.sender.drive();
    }

    fn teardown(mut self, adapter: &mut SpineAdapter<S>) {
        S::gather_into(adapter, self.sender.cache_mut());
        self.sender.finish();
    }
}
