use std::path::Path;

use flux_timing::InternalMessage;
use flux_versioned_types::{Blob, DecodeError, HasVersionedLeaves, Scratch, Versioned};

/// A persisted gather file that is not a blob of leaves of `T`.
#[derive(Debug)]
pub enum ReadError {
    /// The file could not be read.
    Io(std::io::Error),
    /// A blob in the file failed to decode.
    Decode(DecodeError),
    /// A blob in the file holds no leaf of `T`.
    ForeignType { type_name: String },
}

/// Reads back files written by [`BlobWriter`](crate::BlobWriter).
#[derive(Default)]
pub struct BlobReader {
    file: Scratch,
    decode: Scratch,
}

impl BlobReader {
    /// Empty buffers; both grow on first use.
    pub fn new() -> Self {
        Self::default()
    }

    /// Loads `path` into aligned storage and calls `f` on each blob in it (a
    /// file may hold concatenated blobs). `f` returning `Err` stops with
    /// `ReadError::Decode`. Replay a file to the wire with
    /// `reader.for_each_blob(path, |b| { shipper.ship(b); Ok(()) })`.
    pub fn for_each_blob(
        &mut self,
        path: &Path,
        mut f: impl FnMut(&Blob) -> Result<(), DecodeError>,
    ) -> Result<(), ReadError> {
        load_file(&mut self.file, path)?;
        visit_blobs(self.file.as_bytes(), |blob| f(blob).map_err(ReadError::Decode))
    }

    /// Decodes every blob in `path` as `U` metadata plus leaves of `T` (via
    /// `T::decode_blob::<U>`). A blob holding no leaf of `T` is
    /// `ReadError::ForeignType`.
    // The return nests exactly what `decode_blob` yields per blob; a type
    // alias would only rename it.
    #[allow(clippy::type_complexity)]
    pub fn read<U: Versioned, T: HasVersionedLeaves>(
        &mut self,
        path: &Path,
    ) -> Result<Vec<(U, Vec<InternalMessage<T>>)>, ReadError> {
        // Split borrows: blobs borrow `file` while decoding into `decode`.
        let Self { file, decode } = self;
        load_file(file, path)?;
        let mut out = Vec::new();
        visit_blobs(file.as_bytes(), |blob| {
            let Some(decoded) = T::decode_blob::<U>(blob, decode) else {
                return Err(ReadError::ForeignType { type_name: blob.type_name().to_owned() });
            };
            out.push(decoded.map_err(ReadError::Decode)?);
            Ok(())
        })?;
        Ok(out)
    }
}

/// Copies `path` straight into aligned scratch storage in a single read,
/// falling back to `fs::read` + `load` when the file changed size between
/// the metadata read and the read itself.
fn load_file(scratch: &mut Scratch, path: &Path) -> Result<(), ReadError> {
    if let Ok(meta) = std::fs::metadata(path) {
        scratch.resize(meta.len() as usize);
        if let Ok(mut file) = std::fs::File::open(path) {
            use std::io::Read as _;
            if file.read_exact(scratch.as_mut_bytes()).is_ok() {
                return Ok(());
            }
        }
    }
    let bytes = std::fs::read(path).map_err(ReadError::Io)?;
    scratch.load(&bytes).map(|_| ()).map_err(ReadError::Decode)
}

/// Walks the concatenated blobs in `bytes`, stopping at the first error.
fn visit_blobs(
    bytes: &[u8],
    mut f: impl FnMut(&Blob) -> Result<(), ReadError>,
) -> Result<(), ReadError> {
    let mut rest = bytes;
    while !rest.is_empty() {
        let blob = Blob::from_bytes(rest).map_err(ReadError::Decode)?;
        f(blob)?;
        rest = &rest[blob.as_bytes().len()..];
    }
    Ok(())
}
