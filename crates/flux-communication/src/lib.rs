mod array;
mod error;
pub mod queue;
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
pub use seqlock::Seqlock;
pub use shmem_data::ShmemData;
pub use timer::{Timer, TimingMessage};

pub fn shmem_dir_queues_string<S: AsRef<Path>>(app_name: S) -> String {
    let queues_dir = shmem_dir_queues(app_name);
    queues_dir.to_string_lossy().to_string()
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
    queue::Queue::create_or_open_shared(
        shmem_dir_queues_with_base(base_dir, app_name).join(queue_name.as_str()),
        len,
        typ,
    )
}
