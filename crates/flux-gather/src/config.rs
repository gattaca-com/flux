use std::{net::SocketAddr, path::PathBuf};

/// What the gather sender ships and persists.
#[derive(Clone, Debug, serde::Deserialize)]
#[serde(default)]
pub struct GatherConfig {
    /// Goes into every `GatherMeta`; also selects sender-side file roots.
    pub instance_id: String,
    /// TCP destinations. Empty: nothing is shipped.
    pub addrs: Vec<SocketAddr>,
    /// Root of sender-side persistence. None: nothing is written.
    pub disk_dir: Option<PathBuf>,
    /// zstd level for blob payloads.
    pub zstd_level: i32,
    /// Blob `type_name`s never shipped (e.g. heavy decoded payloads).
    pub wire_skip: Vec<String>,
    /// Blob `type_name`s never written.
    pub disk_skip: Vec<String>,
}

impl Default for GatherConfig {
    fn default() -> Self {
        Self {
            instance_id: String::new(),
            addrs: Vec::new(),
            disk_dir: None,
            zstd_level: 3,
            wire_skip: Vec::new(),
            disk_skip: Vec::new(),
        }
    }
}
