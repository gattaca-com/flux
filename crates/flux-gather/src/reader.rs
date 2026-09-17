use std::path::Path;

use flux_timing::InternalMessage;
use flux_versioned_types::{Blob, DecodeError, HasVersionedLeaves, Scratch, Versioned};

#[derive(Debug)]
pub enum ReadError {
    Io(std::io::Error),
    Decode(DecodeError),
    ForeignType { type_name: String },
}

#[derive(Default)]
pub struct BlobReader {
    file: Scratch,
    decode: Scratch,
}

impl BlobReader {
    pub fn new() -> Self {
        Self::default()
    }

    /// A file may hold several concatenated blobs.
    pub fn for_each_blob(
        &mut self,
        path: &Path,
        mut f: impl FnMut(&Blob) -> Result<(), DecodeError>,
    ) -> Result<(), ReadError> {
        load_file(&mut self.file, path)?;
        visit_blobs(self.file.as_bytes(), |blob| f(blob).map_err(ReadError::Decode))
    }

    #[allow(clippy::type_complexity)]
    pub fn read<U: Versioned, T: HasVersionedLeaves>(
        &mut self,
        path: &Path,
    ) -> Result<Vec<(U, Vec<InternalMessage<T>>)>, ReadError> {
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
