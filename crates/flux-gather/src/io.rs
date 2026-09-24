use std::{io, path::Path};

use flux_disk::{DiskEvent, DiskIo, FileToken, OpenOptions};
use flux_versioned_types::Blob;
use tracing::error;

#[derive(Debug)]
pub enum BlobEvent<'a> {
    /// The file is written; a `load` of its path from here on sees the data.
    Written { file: FileToken },
    /// The whole file. Valid for the callback only; the caller copies it.
    Loaded { file: FileToken, bytes: &'a [u8] },
    /// The write or load failed.
    Failed { file: FileToken, error: io::Error },
}

#[derive(Default)]
pub struct BlobIo {
    disk: DiskIo,
    /// Tokens awaiting their one event.
    pending: Vec<FileToken>,
}

impl BlobIo {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn write(&mut self, blob: &Blob, path: &Path) -> io::Result<FileToken> {
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        let file =
            self.disk.open(path, OpenOptions::new().write(true).create(true).truncate(true))?;
        let bytes = blob.as_bytes();
        self.disk.write_with(file, |buf| buf.extend_from_slice(bytes));
        self.disk.close(file);
        self.pending.push(file);
        Ok(file)
    }

    pub fn load(&mut self, path: &Path) -> io::Result<FileToken> {
        let file = self.disk.open(path, OpenOptions::new().read(true))?;
        // `read_to_end` reports once, with the whole file.
        self.disk.read_to_end(file, 0);
        self.disk.close(file);
        self.pending.push(file);
        Ok(file)
    }

    /// Reaps completions; `true` when anything completed.
    pub fn poll_with(&mut self, mut on_event: impl FnMut(BlobEvent<'_>)) -> bool {
        let mut reaped = false;
        let pending = &mut self.pending;
        self.disk.poll_with(|event| {
            reaped = true;
            let (file, event) = match event {
                DiskEvent::Written { file, .. } => (file, BlobEvent::Written { file }),
                DiskEvent::Read { file, payload, .. } => {
                    (file, BlobEvent::Loaded { file, bytes: payload })
                }
                DiskEvent::Failed { file, op, error, .. } => {
                    error!(?file, ?op, %error, "gather disk io failed");
                    (file, BlobEvent::Failed { file, error })
                }
                _ => return,
            };
            // A failed open also fails the operations queued behind it; only
            // the first terminal event for a token is reported.
            let Some(i) = pending.iter().position(|&t| t == file) else { return };
            pending.swap_remove(i);
            on_event(event);
        });
        reaped
    }

    pub fn poll(&mut self) -> bool {
        self.poll_with(|_| {})
    }

    pub fn is_idle(&self) -> bool {
        self.disk.is_idle()
    }

    /// Polls until idle, discarding events.
    pub fn drain(&mut self) {
        loop {
            self.poll();
            if self.is_idle() {
                return;
            }
            std::thread::sleep(std::time::Duration::from_millis(1));
        }
    }
}
