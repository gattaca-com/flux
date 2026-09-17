use flux::spine::{FluxSpine, SpineAdapter};
use flux_versioned_types::BlobCache;

/// The message type of the `queue(gather(boundary))` queue.
pub trait Boundary: Copy {
    /// Some(slot) when this message closes the batch belonging to slot.
    /// A slot-end style message returns its own slot; a slot-start style
    /// message returns slot - 1.
    fn gather_boundary(&self) -> Option<u64>;
}

/// Implemented by `from_spine` when the spine has `queue(gather)` fields.
pub trait GatherQueues: FluxSpine {
    /// Drains every gathered queue into cache: plain and dcache queues in
    /// declaration order, the boundary queue last. Returns the last
    /// boundary slot seen in this pass.
    fn gather_into(adapter: &mut SpineAdapter<Self>, cache: &mut BlobCache) -> Option<u64>;
}
