pub mod array;
pub mod cleanup;
mod error;
#[cfg(feature = "park")]
pub mod park;
pub mod queue;
mod seqlock;
mod shmem_data;
pub mod timer;

use std::path::Path;

pub use array::SeqlockArray;
pub use cleanup::{cleanup_flink, cleanup_shmem, is_pid_alive};
pub use error::{EmptyError, QueueError, ReadError};
use flux_utils::{
    DCache, DCachePtr,
    directories::{
        local_share_dir, shmem_dir_arrays_with_base, shmem_dir_queues, shmem_dir_queues_with_base,
    },
    short_typename,
};
pub use seqlock::Seqlock;
pub use shmem_data::ShmemData;
pub use timer::{Timer, TimingMessage};
use type_hash::TypeHash;

/// Classification of shared-memory segment types.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
#[repr(u8)]
pub enum ShmemKind {
    #[default]
    Unknown = 0,
    Queue = 1,
    Data = 2,
    SeqlockArray = 3,
}

impl ShmemKind {
    /// Returns the lowercase string representation without allocation
    pub fn as_str_lowercase(&self) -> &'static str {
        match self {
            Self::Unknown => "unknown",
            Self::Queue => "queue",
            Self::Data => "data",
            Self::SeqlockArray => "seqlockarray",
        }
    }
}

impl std::fmt::Display for ShmemKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Unknown => write!(f, "Unknown"),
            Self::Queue => write!(f, "Queue"),
            Self::Data => write!(f, "Data"),
            Self::SeqlockArray => write!(f, "SeqlockArray"),
        }
    }
}

pub fn shmem_dir_queues_string<S: AsRef<Path>>(app_name: S) -> String {
    let queues_dir = shmem_dir_queues(app_name);
    queues_dir.to_string_lossy().to_string()
}

pub fn shmem_dir_queues_string_with_base<D: AsRef<Path>, S: AsRef<Path>>(
    base_dir: D,
    app_name: S,
) -> String {
    shmem_dir_queues_with_base(base_dir, app_name).to_string_lossy().to_string()
}

pub fn shmem_queue<S: AsRef<Path>, T: Copy + TypeHash>(
    app_name: S,
    len: usize,
    typ: queue::QueueType,
) -> queue::Queue<T> {
    shmem_queue_with_base_dir(local_share_dir(), app_name, len, typ)
}

pub fn shmem_queue_with_base_dir<D: AsRef<Path>, S: AsRef<Path>, T: Copy + TypeHash>(
    base_dir: D,
    app_name: S,
    len: usize,
    typ: queue::QueueType,
) -> queue::Queue<T> {
    let queue_name = short_typename::<T>();
    let flink_path = shmem_dir_queues_with_base(&base_dir, &app_name).join(queue_name.as_str());
    let q = queue::Queue::create_or_open_shared(&flink_path, len, typ);
    // Spine queues feed tiles that may be parked; set unconditionally since
    // another process may have created the queue without the flag.
    q.set_signal_on_produce(true);
    q
}

/// Allocates a spine queue and its dcache contiguously in a single shmem
/// region.
///
/// Layout: `[queue header | queue seqlocks | dcache]`
///
/// `mtu` is the maximum payload size in bytes; dcache capacity is derived as
/// `DCache::required_capacity(queue_len, mtu).next_power_of_two()`.
pub fn shmem_queue_dcache_with_base_dir<D, S, T>(
    base_dir: D,
    app_name: S,
    queue_len: usize,
    mtu: usize,
    typ: queue::QueueType,
) -> (queue::Queue<T>, DCachePtr)
where
    D: AsRef<Path>,
    S: AsRef<Path>,
    T: Copy + TypeHash,
{
    assert!(mtu > 0, "mtu must be > 0");

    let queue_name = short_typename::<T>();
    let flink_path = shmem_dir_queues_with_base(&base_dir, &app_name).join(queue_name.as_str());

    let real_len = queue_len.next_power_of_two();
    let queue_bytes = queue::Queue::<T>::byte_size(real_len);
    let queue_bytes_aligned = (queue_bytes + 63) & !63;
    let dcache_cap = DCache::required_capacity(real_len, mtu).next_power_of_two();
    // 64: DCache fixed prefix (reserved + cacheline pad) preceding the data slice.
    let total = queue_bytes_aligned + 64 + dcache_cap;
    // macOS reports shm sizes rounded up to a page, so there an mtu change that
    // stays within the last page goes unnoticed.
    let segment_size = if cfg!(target_os = "linux") {
        total
    } else {
        total.next_multiple_of(unsafe { libc::sysconf(libc::_SC_PAGESIZE) } as usize)
    };

    let (q, ptr) = queue::Flink(&flink_path).create_or_open::<T, _>(
        total,
        |ptr, mapped_len| {
            let q = queue::Queue::<T>::from_raw_validated(ptr, mapped_len, real_len)?;
            // The dcache offset and capacity derive from the slot count and mtu.
            if q.n_slots() != real_len || mapped_len != segment_size {
                return Err(QueueError::SizeChanged(mapped_len, segment_size));
            }
            q.check_poison()?;
            Ok((q, ptr))
        },
        |ptr| (queue::Queue::from_raw_init(ptr, real_len, typ), ptr),
    );

    q.set_signal_on_produce(true);

    let dcache_ptr = unsafe { ptr.add(queue_bytes_aligned) };
    let dc = unsafe { DCachePtr::from_raw(DCache::from_ptr(dcache_ptr, dcache_cap)) };

    (q, dc)
}

pub fn shmem_array<S: AsRef<Path>, T: Copy>(
    app_name: S,
    len: usize,
) -> Result<SeqlockArray<T>, error::QueueError> {
    shmem_array_with_base_dir(local_share_dir(), app_name, len)
}

pub fn shmem_array_with_base_dir<D: AsRef<Path>, S: AsRef<Path>, T: Copy>(
    base_dir: D,
    app_name: S,
    len: usize,
) -> Result<SeqlockArray<T>, error::QueueError> {
    let type_name = short_typename::<T>();
    let flink_path = shmem_dir_arrays_with_base(&base_dir, &app_name).join(type_name.as_str());
    let arr = SeqlockArray::create_or_open_shared(&flink_path, len)?;
    Ok(arr)
}
