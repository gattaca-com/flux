use flux::spine::{FluxSpine, SpineAdapter};
use flux_versioned_types::BlobCache;

/// Implemented by `#[from_spine]` for spines with `#[queue(gather)]` fields.
pub trait GatherQueues: FluxSpine {
    fn gather_into(adapter: &mut SpineAdapter<Self>, cache: &mut BlobCache);
}
