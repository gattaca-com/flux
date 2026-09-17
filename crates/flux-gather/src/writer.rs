use std::path::Path;

use flux_disk::{DiskEvent, DiskIo, OpenOptions};
use flux_versioned_types::Blob;
use tracing::{error, warn};

/// Persists each blob as the entire content of a caller-chosen file.
#[derive(Default)]
pub struct BlobWriter {
    disk: DiskIo,
}

impl BlobWriter {
    /// Empty queue; buffers grow on first use.
    pub fn new() -> Self {
        Self::default()
    }

    /// Queues `blob` as the entire content of `path`: parent directories are
    /// created (blocking, cheap), the file is created/truncated, written
    /// through `io_uring`, closed. `false` + `warn!` when the open fails.
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

    /// Reaps completions, `error!`-logging failures. Returns whether any
    /// completion was reaped.
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

    /// Polls until idle (1 ms sleeps between polls).
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
