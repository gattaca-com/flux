use std::path::PathBuf;

use directories::BaseDirs;
use tracing::warn;

pub fn local_share_dir<S: AsRef<str>>(app_name: S) -> PathBuf {
    let Some(base_dirs) = BaseDirs::new() else {
        warn!("couldn't find basedirs, storing data in /tmp/<app_name>");
        return PathBuf::from(format!("/tmp/{}", app_name.as_ref()));
    };
    base_dirs.data_dir().join(app_name.as_ref())
}

pub fn data_dir<S: AsRef<str>>(app_name: S) -> PathBuf {
    local_share_dir(app_name).join("data")
}

pub fn shmem_dir<S: AsRef<str>>(app_name: S) -> PathBuf {
    local_share_dir(app_name).join("shmem")
}

pub fn logs_dir<S: AsRef<str>>(app_name: S) -> PathBuf {
    local_share_dir(app_name).join("logs")
}

pub fn shmem_dir_queues<S: AsRef<str>>(app_name: S) -> PathBuf {
    shmem_dir(app_name).join("queues")
}

pub fn shmem_dir_data<S: AsRef<str>>(app_name: S) -> PathBuf {
    shmem_dir(app_name).join("data")
}
