use std::path::{Path, PathBuf};

use flux_timing::InternalMessage;
use flux_versioned_types::{Blob, DecodeError, HasVersionedLeaves, Scratch};

use crate::meta::GatherMeta;

/// A persisted gather file that is not a blob of leaves of T.
#[derive(Debug)]
pub enum ReadError {
    /// The file could not be read.
    Io(std::io::Error),
    /// A blob in the file failed to decode.
    Decode(DecodeError),
    /// A blob in the file holds no leaf of T.
    ForeignType { type_name: String },
}

/// Reads back files written by `BlobWriter`.
pub struct BlobReader {
    file_bytes: Scratch,
    decode: Scratch,
}

impl BlobReader {
    /// Empty buffers; both grow on first use.
    pub fn new() -> Self {
        Self::default()
    }

    /// Files for one `(instance_id, app, type_name, slot)` under `base`, sorted
    /// by `flush_t`. Missing directory => empty `Vec`, not an error.
    pub fn slot_files(
        base: &Path,
        instance_id: &str,
        app: &str,
        type_name: &str,
        slot: u64,
    ) -> std::io::Result<Vec<PathBuf>> {
        let dir = base.join(instance_id).join(app).join(type_name);
        let entries = match std::fs::read_dir(&dir) {
            Ok(entries) => entries,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(Vec::new()),
            Err(error) => return Err(error),
        };
        let prefix = format!("{slot}_");
        let mut stamped = Vec::new();
        for entry in entries {
            let entry = entry?;
            let name = entry.file_name();
            let Some(name) = name.to_str() else { continue };
            let Some(rest) = name.strip_prefix(&prefix) else { continue };
            let Some(stamp) = rest.strip_suffix(".bin") else { continue };
            let Ok(flush_t) = stamp.parse::<u64>() else { continue };
            stamped.push((flush_t, entry.path()));
        }
        stamped.sort_unstable_by_key(|(flush_t, _)| *flush_t);
        Ok(stamped.into_iter().map(|(_, path)| path).collect())
    }

    /// Decode every blob in `path` as leaves of `T`.
    // The return nests exactly what `decode_blob` yields per blob; a type alias
    // would only rename it.
    #[allow(clippy::type_complexity)]
    pub fn read<T: HasVersionedLeaves>(
        &mut self,
        path: &Path,
    ) -> Result<Vec<(GatherMeta, Vec<InternalMessage<T>>)>, ReadError> {
        let bytes = std::fs::read(path).map_err(ReadError::Io)?;
        self.file_bytes.resize(bytes.len());
        self.file_bytes.as_mut_bytes().copy_from_slice(&bytes);
        let mut out = Vec::new();
        let mut rest: &[u8] = self.file_bytes.as_bytes();
        while !rest.is_empty() {
            let blob = Blob::from_bytes(rest).map_err(ReadError::Decode)?;
            let Some(decoded) = T::decode_blob::<GatherMeta>(blob, &mut self.decode) else {
                return Err(ReadError::ForeignType { type_name: blob.type_name().to_owned() });
            };
            let decoded = decoded.map_err(ReadError::Decode)?;
            rest = &rest[blob.as_bytes().len()..];
            out.push(decoded);
        }
        Ok(out)
    }
}

impl Default for BlobReader {
    fn default() -> Self {
        Self { file_bytes: Scratch::new(), decode: Scratch::new() }
    }
}
