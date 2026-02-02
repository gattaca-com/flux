use std::{
    io::Write,
    path::{Path, PathBuf},
    time::SystemTime,
};

use flux_timing::InternalMessage;
use flux_utils::directories::data_dir;
use serde::Serialize;
use tracing::error;

pub trait Persistable: for<'a> serde::Deserialize<'a> + serde::Serialize {
    const PERSIST_DIR: &'static str;

    fn filename(&self) -> String {
        "".to_string()
    }

    fn persist_dir<S: AsRef<str>>(app_name: S) -> PathBuf {
        data_dir(app_name).join(Self::PERSIST_DIR)
    }

    fn persist<S: AsRef<str>>(
        app_name: S,
        vals: &[Self],
        compression_level: Option<i32>,
        filename: Option<String>,
    ) {
        let Some(mut file) = vals.first().map(|v| v.filename()) else {
            return;
        };
        let dir = Self::persist_dir(app_name);
        if let Err(e) = std::fs::create_dir_all(&dir) {
            tracing::warn!("Not saving data because couldn't create persist dir ({:?}): {e}", dir);
            return;
        };
        if let Some(override_file) = filename {
            file = override_file;
        } else if file.is_empty() {
            file = format!("{}", std::fs::read_dir(&dir).unwrap().count());
        }
        let file = dir.join(file).with_added_extension("bin");
        write(file, vals, compression_level.unwrap_or(5));
    }

    fn load<S: AsRef<str>, P: AsRef<Path>>(app_name: S, filename: P) -> Option<Vec<Self>> {
        read(Self::persist_dir(app_name).join(filename.as_ref()).with_added_extension("bin"))
    }

    fn load_all<S: AsRef<str>>(app_name: S) -> Option<Vec<Self>> {
        let mut out = vec![];
        let mut paths = std::fs::read_dir(Self::persist_dir(app_name))
            .ok()?
            .filter_map(|d| d.ok().map(|p| (p.metadata().unwrap().modified().unwrap(), p.path())))
            .collect::<Vec<_>>();

        paths.sort_unstable_by_key(|(t, _)| *t);

        for (_, f) in paths {
            let Some(read) = read(f) else {
                continue;
            };
            out.extend(read);
        }
        Some(out)
    }

    fn load_from<S: AsRef<str>>(app_name: S, start_t: SystemTime) -> Option<Vec<Self>> {
        let mut out = vec![];
        let mut paths = std::fs::read_dir(Self::persist_dir(app_name))
            .ok()?
            .filter_map(|d| {
                d.ok().and_then(|p| {
                    let t = p.metadata().unwrap().modified().unwrap();
                    if t > start_t { Some((t, p.path())) } else { None }
                })
            })
            .collect::<Vec<_>>();

        paths.sort_unstable_by_key(|(t, _)| *t);

        for (_, f) in paths {
            let Some(read) = read(f) else {
                continue;
            };
            out.extend(read);
        }
        Some(out)
    }
}

pub fn read<T: serde::de::DeserializeOwned, P: AsRef<Path>>(path: P) -> Option<Vec<T>> {
    let f = std::fs::File::open(path.as_ref())
        .map_err(|e| tracing::warn!("issue opening {:?}: {e}", path.as_ref()))
        .ok()?;
    let b = zstd::decode_all(f)
        .map_err(|e| tracing::warn!("couldn't decode persistence file: {e}"))
        .ok()?;
    bitcode::deserialize(&b).map_err(|e| tracing::warn!("couldn't read persistence file: {e}")).ok()
}

pub fn write<T: Serialize, P: AsRef<Path> + std::fmt::Debug>(
    path: P,
    vals: &[T],
    compression_level: i32,
) {
    path.as_ref().parent().map(std::fs::create_dir_all);
    let file = match std::fs::File::create(&path) {
        Ok(file) => file,
        Err(e) => {
            error!("Couldn't open {path:#?}: {e}");
            return;
        }
    };

    let b = match bitcode::serialize(vals) {
        Ok(data) => data,
        Err(e) => {
            error!("Issue serializing {}: {e}", std::any::type_name::<T>());
            return;
        }
    };
    let mut e = match zstd::Encoder::new(&file, compression_level) {
        Ok(encoder) => encoder,
        Err(e) => {
            error!("Issue creating zstd encoder for {}: {e}", std::any::type_name::<T>());
            return;
        }
    };
    if let Err(e) = e.write_all(&b) {
        error!("Error writing serialized bytes {}: {e}", std::any::type_name::<T>());
        return;
    };
    if let Err(e) = e.finish() {
        error!("Error writing serialized bytes {}: {e}", std::any::type_name::<T>());
    }
}

impl<T: Persistable> Persistable for InternalMessage<T> {
    const PERSIST_DIR: &'static str = T::PERSIST_DIR;
}
