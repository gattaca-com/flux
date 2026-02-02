mod array;
mod error;
pub mod queue;
mod seqlock;
mod shmem_data;
pub mod timer;

pub use array::SeqlockArray;
pub use error::{EmptyError, QueueError, ReadError};
use flux_utils::{directories::shmem_dir_queues, short_typename};
pub use seqlock::Seqlock;
pub use shmem_data::ShmemData;
pub use timer::{Timer, TimingMessage};

pub fn shmem_dir_queues_string<S: AsRef<str>>(app_name: S) -> String {
    let queues_dir = shmem_dir_queues(app_name);
    queues_dir.to_string_lossy().to_string()
}

pub fn shmem_queue<S: AsRef<str>, T: Copy>(
    app_name: S,
    len: usize,
    typ: queue::QueueType,
) -> queue::Queue<T> {
    let queue_name = short_typename::<T>();
    queue::Queue::create_or_open_shared(
        shmem_dir_queues(app_name).join(queue_name.as_str()),
        len,
        typ,
    )
}
