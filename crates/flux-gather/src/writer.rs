use std::path::Path;

use flux_disk::{DiskEvent, DiskIo, OpenOptions};
use flux_versioned_types::Blob;
use tracing::{error, warn};

#[derive(Default)]
pub struct BlobWriter {
    disk: DiskIo,
}

impl BlobWriter {
    pub fn new() -> Self {
        Self::default()
    }

    /// Open failures surface asynchronously through `poll`.
    pub fn write(&mut self, blob: &Blob, path: &Path) -> bool {
        if let Some(parent) = path.parent() {
            if let Err(error) = std::fs::create_dir_all(parent) {
                warn!(?path, %error, "couldn't create gather persistence directory");
                return false;
            }
        }
        let file = match self
            .disk
            .open(path, OpenOptions::new().write(true).create(true).truncate(true))
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

    pub fn poll(&mut self) -> bool {
        let mut reaped = false;
        self.disk.poll_with(|event| {
            reaped = true;
            if let DiskEvent::Failed { file, op, error, .. } = event {
                error!(?file, ?op, %error, "gather disk write failed");
            }
        });
        reaped
    }

    pub fn drain(&mut self) {
        loop {
            self.poll();
            if self.disk.is_idle() {
                return;
            }
            std::thread::sleep(std::time::Duration::from_millis(1));
        }
    }
}
