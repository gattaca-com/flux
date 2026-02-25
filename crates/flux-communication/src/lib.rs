mod array;
mod error;
pub mod queue;
pub mod registry;
mod seqlock;
mod shmem_data;
pub mod timer;

use std::path::Path;

pub use array::SeqlockArray;
pub use error::{EmptyError, QueueError, ReadError};
use flux_utils::{
    directories::{local_share_dir, shmem_dir_queues, shmem_dir_queues_with_base},
    short_typename,
};
pub use registry::{ShmemEntry, ShmemKind, ShmemRegistry};
pub use seqlock::Seqlock;
pub use shmem_data::ShmemData;
pub use timer::{Timer, TimingMessage};

pub fn shmem_dir_queues_string<S: AsRef<Path>>(app_name: S) -> String {
    let queues_dir = shmem_dir_queues(app_name);
    queues_dir.to_string_lossy().to_string()
}

pub fn shmem_dir_queues_string_with_base<D: AsRef<Path>, S: AsRef<Path>>(
    base_dir: D,
    app_name: S,
) -> String {
    shmem_dir_queues_with_base(base_dir, app_name)
        .to_string_lossy()
        .to_string()
}

pub fn shmem_queue<S: AsRef<Path>, T: Copy>(
    app_name: S,
    len: usize,
    typ: queue::QueueType,
) -> queue::Queue<T> {
    shmem_queue_with_base_dir(local_share_dir(), app_name, len, typ)
}

pub fn shmem_queue_with_base_dir<D: AsRef<Path>, S: AsRef<Path>, T: Copy>(
    base_dir: D,
    app_name: S,
    len: usize,
    typ: queue::QueueType,
) -> queue::Queue<T> {
    let queue_name = short_typename::<T>();
    let flink_path =
        shmem_dir_queues_with_base(&base_dir, &app_name).join(queue_name.as_str());
    let queue = queue::Queue::create_or_open_shared(&flink_path, len, typ);

    // Auto-register in global shmem registry
    let reg = registry::ShmemRegistry::open_or_create(base_dir.as_ref());
    reg.register(registry::queue_entry(
        &app_name.as_ref().to_string_lossy(),
        queue_name.as_str(),
        &flink_path.to_string_lossy(),
        std::mem::size_of::<T>(),
        len,
    ));

    queue
}
