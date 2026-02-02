mod persistable;
mod persisting_tile;

use flux_timing::Nanos;
pub use persistable::Persistable;
pub use persisting_tile::PersistingQueueTile;

pub const PERSIST_INTERVAL: Nanos = Nanos::from_mins(1);
pub const TIMESTAMP_FORMAT_UTC: &str = "%Y-%m-%d_%H:%M_utc";
