use std::{path::Path, sync::atomic::AtomicUsize};

use shared_memory::{Shmem, ShmemConf};

#[repr(C, align(64))]
struct GroupCursor {
    cursor: AtomicUsize,
}

fn leak_cursor(shmem: Shmem) -> *const AtomicUsize {
    let ptr = shmem.as_ptr() as *const GroupCursor;
    std::mem::forget(shmem);
    unsafe { &raw const (*ptr).cursor }
}

/// Opens or creates a persistent collaborative cursor backed by a shared-memory
/// file at `path`. The shmem is leaked (same pattern as queues), so the
/// returned pointer is valid for the lifetime of the process.
pub fn open_or_create(path: impl AsRef<Path>) -> *const AtomicUsize {
    use shared_memory::ShmemError;
    let path = path.as_ref();
    let _ = std::fs::create_dir_all(path.parent().unwrap());

    match ShmemConf::new().size(std::mem::size_of::<GroupCursor>()).flink(path).create() {
        Ok(shmem) => {
            unsafe {
                std::ptr::write(
                    &raw mut (*(shmem.as_ptr() as *mut GroupCursor)).cursor,
                    AtomicUsize::new(0),
                );
            }
            leak_cursor(shmem)
        }
        Err(ShmemError::LinkExists) => match ShmemConf::new().flink(path).open() {
            Ok(shmem) => leak_cursor(shmem),
            Err(e) => panic!("Failed to open collaborative group at {}: {e}", path.display()),
        },
        Err(e) => panic!("Failed to create collaborative group at {}: {e}", path.display()),
    }
}
