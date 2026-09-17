use std::path::PathBuf;

use flux_disk::{DiskEvent, DiskIo, OpenOptions};
use flux_versioned_types::Blob;
use tracing::{error, warn};

use crate::meta::GatherMeta;

/// Persists each blob as its own file at `GatherMeta::path`.
pub struct BlobWriter {
    base: PathBuf,
    disk: DiskIo,
}

impl BlobWriter {
    /// Root all blob paths under base.
    pub fn new(base: PathBuf) -> Self {
        Self { base, disk: DiskIo::default() }
    }

    /// Queue blob as its own file. Returns false (after a warn!) when the blob
    /// has no `GatherMeta` or the open fails.
    pub fn write(&mut self, blob: &Blob) -> bool {
        let meta = match blob.user_metadata::<GatherMeta>() {
            Ok(meta) => meta,
            Err(error) => {
                warn!(?error, "gather blob without GatherMeta; skipping disk write");
                return false;
            }
        };
        let path = meta.path(&self.base, blob.type_name());
        if let Some(parent) = path.parent() {
            if let Err(error) = std::fs::create_dir_all(parent) {
                warn!(?path, %error, "couldn't create gather persistence directory");
                return false;
            }
        }
        let file =
            match self.disk.open(&path, OpenOptions::new().write(true).create(true).truncate(true))
            {
                Ok(file) => file,
                Err(error) => {
                    warn!(?path, %error, "couldn't open gather persistence file");
                    return false;
                }
            };
        let bytes = blob.as_bytes();
        self.disk.write_with(file, |buf| buf.extend_from_slice(bytes));
        self.disk.close(file);
        true
    }

    /// Reap completions, logging failures.
    pub fn poll(&mut self) {
        self.disk.poll_with(|event| {
            if let DiskEvent::Failed { file, op, error, .. } = event {
                error!(?file, ?op, %error, "gather disk write failed");
            }
        });
    }

    /// Poll until every queued write has landed or failed.
    pub fn drain(&mut self) {
        loop {
            self.disk.poll_with(|event| {
                if let DiskEvent::Failed { file, op, error, .. } = event {
                    error!(?file, ?op, %error, "gather disk write failed");
                }
            });
            if self.disk.is_idle() {
                return;
            }
            std::thread::sleep(std::time::Duration::from_millis(1));
        }
    }
}
