use std::path::{Path, PathBuf};

use directories::BaseDirs;
use tracing::warn;

pub fn local_share_dir() -> PathBuf {
    let Some(base_dirs) = BaseDirs::new() else {
        warn!("couldn't find basedirs, storing data in /tmp/<app_name>");
        return PathBuf::from("/tmp");
    };
    base_dirs.data_dir().to_path_buf()
}

pub fn data_dir<S: AsRef<Path>>(app_name: S) -> PathBuf {
    data_dir_with_base(local_share_dir(), app_name)
}

pub fn shmem_dir<S: AsRef<Path>>(app_name: S) -> PathBuf {
    shmem_dir_with_base(local_share_dir(), app_name)
}

pub fn logs_dir<S: AsRef<Path>>(app_name: S) -> PathBuf {
    logs_dir_with_base(local_share_dir(), app_name)
}

pub fn shmem_dir_queues<S: AsRef<Path>>(app_name: S) -> PathBuf {
    shmem_dir(app_name).join("queues")
}

pub fn shmem_dir_data<S: AsRef<Path>>(app_name: S) -> PathBuf {
    shmem_dir(app_name).join("data")
}

pub fn data_dir_with_base<D: AsRef<Path>, S: AsRef<Path>>(base_dir: D, app_name: S) -> PathBuf {
    base_dir.as_ref().join(app_name.as_ref()).join("data")
}

pub fn shmem_dir_with_base<D: AsRef<Path>, S: AsRef<Path>>(base_dir: D, app_name: S) -> PathBuf {
    base_dir.as_ref().join(app_name.as_ref()).join("shmem")
}

pub fn logs_dir_with_base<D: AsRef<Path>, S: AsRef<Path>>(base_dir: D, app_name: S) -> PathBuf {
    base_dir.as_ref().join(app_name.as_ref()).join("logs")
}

pub fn shmem_dir_queues_with_base<D: AsRef<Path>, S: AsRef<Path>>(
    base_dir: D,
    app_name: S,
) -> PathBuf {
    shmem_dir_with_base(base_dir, app_name).join("queues")
}

pub fn shmem_dir_data_with_base<D: AsRef<Path>, S: AsRef<Path>>(
    base_dir: D,
    app_name: S,
) -> PathBuf {
    shmem_dir_with_base(base_dir, app_name).join("data")
}
