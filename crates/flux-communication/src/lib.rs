pub mod array;
pub mod cleanup;
mod error;
pub mod queue;
mod seqlock;
mod shmem_data;
pub mod timer;

use std::path::Path;

pub use array::SeqlockArray;
pub use cleanup::{cleanup_flink, cleanup_shmem, is_pid_alive};
pub use error::{EmptyError, QueueError, ReadError};
use flux_utils::{
    DCache, DcachePtr,
    directories::{
        local_share_dir, shmem_dir_arrays_with_base, shmem_dir_queues, shmem_dir_queues_with_base,
    },
    short_typename,
};
pub use seqlock::Seqlock;
pub use shmem_data::ShmemData;
pub use timer::{Timer, TimingMessage};

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
    let flink_path = shmem_dir_queues_with_base(&base_dir, &app_name).join(queue_name.as_str());
    queue::Queue::create_or_open_shared(&flink_path, len, typ)
}

/// Allocates a spine queue and its dcache contiguously in a single shmem
/// region.
///
/// Layout: `[queue header | queue seqlocks | dcache header | dcache data]`
///
/// `mtu` is the maximum payload size in bytes; dcache capacity is derived as
/// `DCache::required_capacity(queue_len, mtu).next_power_of_two()`.
pub fn shmem_queue_dcache_with_base_dir<D, S, T>(
    base_dir: D,
    app_name: S,
    queue_len: usize,
    mtu: usize,
    typ: queue::QueueType,
) -> (queue::Queue<T>, DcachePtr)
where
    D: AsRef<Path>,
    S: AsRef<Path>,
    T: Copy,
{
    use shared_memory::{ShmemConf, ShmemError};

    let queue_name = short_typename::<T>();
    let flink_path = shmem_dir_queues_with_base(&base_dir, &app_name).join(queue_name.as_str());
    let _ = std::fs::create_dir_all(flink_path.parent().unwrap());

    let real_len = queue_len.next_power_of_two();
    let queue_bytes = queue::Queue::<T>::byte_size(real_len);
    // Align dcache start to cacheline boundary.
    let queue_bytes_aligned = (queue_bytes + 63) & !63;
    let dcache_cap = DCache::required_capacity(real_len, mtu).next_power_of_two();
    // DCache layout: 64-byte header + data region.
    let total = queue_bytes_aligned + 64 + dcache_cap;

    let (ptr, is_new): (*mut u8, bool) =
        match ShmemConf::new().size(total).flink(&flink_path).create() {
            Ok(shmem) => {
                let ptr = shmem.as_ptr();
                std::mem::forget(shmem);
                (ptr, true)
            }
            Err(ShmemError::LinkExists) => {
                let result = ShmemConf::new().flink(&flink_path).open();
                match result {
                    Ok(shmem) => {
                        let ptr = shmem.as_ptr();
                        std::mem::forget(shmem);
                        (ptr, false)
                    }
                    Err(_) => {
                        let _ = std::fs::remove_file(&flink_path);
                        return shmem_queue_dcache_with_base_dir(
                            base_dir, app_name, queue_len, mtu, typ,
                        );
                    }
                }
            }
            Err(e) => panic!("shmem create failed at {:?}: {e}", flink_path),
        };

    let q = if is_new {
        queue::Queue::from_raw_init(ptr, real_len, typ)
    } else {
        match queue::Queue::<T>::from_raw_open(ptr) {
            Ok(q) => q,
            Err(e) => {
                tracing::error!("invalid queue at {:?}: {e}. Removing and recreating.", flink_path);
                let _ = std::fs::remove_file(&flink_path);
                return shmem_queue_dcache_with_base_dir(base_dir, app_name, queue_len, mtu, typ);
            }
        }
    };

    let dcache_ptr = unsafe { ptr.add(queue_bytes_aligned) };
    let dc = unsafe { DcachePtr::from_raw(DCache::from_ptr(dcache_ptr, dcache_cap)) };

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
