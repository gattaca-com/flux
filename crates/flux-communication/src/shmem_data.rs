use std::{
    borrow::Borrow, ops::{Deref, DerefMut}, path::Path, ptr::NonNull
};

use flux_utils::{directories::{local_share_dir, shmem_dir_data_with_base}, short_typename};
use shared_memory::{Shmem, ShmemError};

#[repr(C)]
pub struct ShmemData<T> {
    inner: NonNull<T>,
}

impl<T: Default> ShmemData<T> {
    pub fn new(app_name: &str) -> Result<ShmemData<T>, ShmemError> {
        Self::open_or_init(app_name, T::default)
    }
}

impl<T> ShmemData<T> {
    pub fn open_or_init(
        app_name: &str,
        init_f: impl FnOnce() -> T,
    ) -> Result<ShmemData<T>, ShmemError> {
        Self::open_or_init_with_base_dir(local_share_dir(), app_name, init_f)
    }

    pub fn open_or_init_with_base_dir<D: AsRef<Path>, A: AsRef<Path>>(dir: D, app_name: A, init_f: impl FnOnce()->T)-> Result<ShmemData<T>, ShmemError> {
        use shared_memory::{ShmemConf, ShmemError};
        let shmem_file = shmem_dir_data_with_base(&dir, &app_name).join(short_typename::<T>().as_str());
        std::fs::create_dir_all(
            shmem_file
                .parent()
                .unwrap_or_else(|| panic!("no parent dir for {}", shmem_file.display())),
        )
        .unwrap_or_else(|_| panic!("couldn't create shmem dir for {}", shmem_file.display()));

        match ShmemConf::new().size(std::mem::size_of::<T>()).flink(&shmem_file).create() {
            Ok(shmem) => {
                let inner = Self::shmem_ptr(shmem);

                // Init ptr
                unsafe { std::ptr::write(inner.as_ptr(), init_f()) };

                Ok(Self { inner })
            }
            Err(ShmemError::LinkExists) => {

                let Ok(shmem) = ShmemConf::new().flink(&shmem_file).open() else {
                    if shmem_file.exists() {
                       tracing::warn!("couldn't open shmem file {}, recreating and retrying", shmem_file.display());
                       std::fs::remove_file(shmem_file).expect("couldn't remove shmem file for TileInfo");
                       return Self::open_or_init_with_base_dir(dir, app_name, init_f);
                    }
                    panic!("couldn't open shmem file {}", shmem_file.display());
                };

                let inner = Self::shmem_ptr(shmem);

                Ok(Self { inner })
            }
            Err(e) => Err(e),
        }
        
    }

    fn shmem_ptr(shmem: Shmem) -> NonNull<T> {
        let shmem_ptr = shmem.as_ptr() as *mut T;

        // Don't drop shmem on exit. Will just pick up same file descriptor from flink
        // on restart.
        std::mem::forget(shmem);

        NonNull::new(shmem_ptr)
            .expect("shmem pointer is null somehow. Probably regression in shared_memory crate.")
    }

    pub fn copy_ptr(&self) -> Self {
        Self { inner: self.inner }
    }
}

impl<T: std::fmt::Debug> std::fmt::Debug for ShmemData<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:?}", self.inner)
    }
}

unsafe impl<T: Send> Send for ShmemData<T> {}
unsafe impl<T: Sync> Sync for ShmemData<T> {}

impl<T> Borrow<T> for ShmemData<T> {
    fn borrow(&self) -> &T {
        unsafe { self.inner.as_ref() }
    }
}

impl<T> Deref for ShmemData<T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        unsafe { self.inner.as_ref() }
    }
}

// TODO: should probably favour an explicit method for mutable access. Something
// like get_mut.
impl<T> DerefMut for ShmemData<T> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        unsafe { self.inner.as_mut() }
    }
}

impl<T> AsRef<T> for ShmemData<T> {
    fn as_ref(&self) -> &T {
        unsafe { self.inner.as_ref() }
    }
}
