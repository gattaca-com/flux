use std::{
    io,
    os::unix::ffi::OsStrExt,
    path::{Path, PathBuf},
};

use flux_disk::{DiskEvent, DiskIo, FailedOp, FileToken, OpenOptions};
use flux_versioned_types::Blob;
use tracing::error;

#[derive(Debug)]
pub enum BlobEvent<'a> {
    /// The file is in place; a `read` of its path from here on sees the whole
    /// of it.
    Written { file: FileToken },
    /// The whole file. Valid for the callback only; the caller copies it.
    Read { file: FileToken, bytes: &'a [u8] },
    /// The write or read failed.
    Failed { file: FileToken, error: io::Error },
}

/// A write in its unnamed file, linked to `path` once its bytes are down.
struct Staged {
    file: FileToken,
    path: PathBuf,
    /// The link hit an existing name, which has been unlinked.
    relinked: bool,
}

/// Asynchronous blob file io. Every token `write` or `read` returns gets
/// exactly one `BlobEvent`; the caller keeps the token-to-file mapping.
#[derive(Default)]
pub struct BlobIo {
    disk: DiskIo,
    /// Tokens awaiting their one event.
    pending: Vec<FileToken>,
    staged: Vec<Staged>,
}

impl BlobIo {
    pub fn new() -> Self {
        Self::default()
    }

    /// Writes `blob` to an unnamed file in `path`'s directory and links it
    /// to `path` once the bytes are written. Needs a filesystem with
    /// `O_TMPFILE`.
    pub fn write(&mut self, blob: &Blob, path: &Path) -> io::Result<FileToken> {
        if path.as_os_str().as_bytes().contains(&0) {
            return Err(io::Error::new(io::ErrorKind::InvalidInput, "path contains a nul byte"));
        }
        let dir = match path.parent() {
            Some(dir) if !dir.as_os_str().is_empty() => dir,
            _ => Path::new("."),
        };
        std::fs::create_dir_all(dir)?;
        let file = self.disk.open(dir, OpenOptions::new().write(true).tmpfile(true))?;
        let bytes = blob.as_bytes();
        self.disk.write_with(file, |buf| buf.extend_from_slice(bytes));
        self.pending.push(file);
        self.staged.push(Staged { file, path: path.to_path_buf(), relinked: false });
        Ok(file)
    }

    /// Reads the whole of `path`. `Err` only when the open fails
    /// synchronously; a missing file surfaces as `Failed`.
    pub fn read(&mut self, path: &Path) -> io::Result<FileToken> {
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
        let mut link = Vec::new();
        let mut relink = Vec::new();
        let mut close = Vec::new();
        let pending = &mut self.pending;
        let staged = &mut self.staged;
        self.disk.poll_with(|event| {
            reaped = true;
            let (file, event) = match event {
                DiskEvent::Written { file, .. } => {
                    link.push(file);
                    return;
                }
                DiskEvent::Linked { file, .. } => {
                    if let Some(i) = staged.iter().position(|s| s.file == file) {
                        staged.swap_remove(i);
                    }
                    close.push(file);
                    (file, BlobEvent::Written { file })
                }
                DiskEvent::Read { file, payload, .. } => {
                    (file, BlobEvent::Read { file, bytes: payload })
                }
                DiskEvent::Failed { file, op, error, .. } => {
                    let i = staged.iter().position(|s| s.file == file);
                    match (i, op, error.kind()) {
                        // A rewrite: clear the name and link again, once.
                        (Some(i), FailedOp::Link, io::ErrorKind::AlreadyExists)
                            if !staged[i].relinked =>
                        {
                            staged[i].relinked = true;
                            relink.push(file);
                            return;
                        }
                        (Some(_), FailedOp::Unlink, io::ErrorKind::NotFound) => return,
                        _ => {}
                    }
                    error!(?file, ?op, %error, "gather disk io failed");
                    if let Some(i) = i {
                        staged.swap_remove(i);
                        close.push(file);
                    }
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
        // The path was checked by `write` and the token is still open.
        for file in relink {
            let Some(s) = self.staged.iter().find(|s| s.file == file) else { continue };
            self.disk.unlink(file, &s.path).expect("path checked by write");
            self.disk.link(file, &s.path).expect("path checked by write");
        }
        for file in link {
            let Some(s) = self.staged.iter().find(|s| s.file == file) else { continue };
            self.disk.link(file, &s.path).expect("path checked by write");
        }
        for file in close {
            self.disk.close(file);
        }
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
