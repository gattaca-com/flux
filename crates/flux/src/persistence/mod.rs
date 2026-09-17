mod persistable;

use flux_timing::Nanos;
pub use persistable::{Persistable, read, write};

pub const PERSIST_INTERVAL: Nanos = Nanos::from_mins(1);
pub const TIMESTAMP_FORMAT_UTC: &str = "%Y-%m-%d_%H:%M_utc";
