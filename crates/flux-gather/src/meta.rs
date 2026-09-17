use std::path::{Path, PathBuf};

use flux_timing::Nanos;
use flux_utils::ArrayStr;
use flux_versioned_types::versioned_struct;
use type_hash_derive::type_hash_lock;

versioned_struct!(GatherMeta =>
    #[type_hash_lock(hash = 14068250002185293012)]
    GatherMetaV1 {
        pub slot: u64,
        /// Wall clock of the flush. Identical for every blob of one flush, so
        /// `(instance_id, app, flush_t)` identifies a batch and survives restarts.
        pub flush_t: Nanos,
        /// Blobs in this flush: the receiver knows when a batch is complete.
        pub n_blobs: u64,
        pub instance_id: ArrayStr<32>,
        pub app: ArrayStr<32>,
    }
);

const _: () = assert!(size_of::<GatherMeta>() == 104);

impl GatherMeta {
    /// `<base>/<instance_id>/<app>/<type_name>/<slot>_<flush_t nanos>.bin`
    pub fn path(&self, base: &Path, type_name: &str) -> PathBuf {
        base.join(self.instance_id.as_str())
            .join(self.app.as_str())
            .join(type_name)
            .join(format!("{}_{}.bin", self.slot, self.flush_t.0))
    }
}
