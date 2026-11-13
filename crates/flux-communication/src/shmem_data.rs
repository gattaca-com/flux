use std::{
    borrow::Borrow,
    ops::{Deref, DerefMut},
    path::Path,
    ptr::NonNull,
};

use flux_utils::{
    directories::{local_share_dir, shmem_dir_data_with_base},
    short_typename,
};
use shared_memory::{Shmem, ShmemError};

use crate::queue::Flink;

#[repr(C)]
pub struct ShmemData<T> {
    inner: NonNull<T>,
}

impl<T: Default> ShmemData<T> {
    pub fn new(app_name: &str) -> Result<Self, ShmemError> {
        Self::open_or_init(app_name, T::default)
    }
}

impl<T> ShmemData<T> {
    pub fn open_or_init(app_name: &str, init_f: impl FnOnce() -> T) -> Result<Self, ShmemError> {
        Self::open_or_init_with_base_dir(local_share_dir(), app_name, init_f)
    }

    pub fn open_or_init_with_base_dir<D: AsRef<Path>, A: AsRef<Path>>(
        dir: D,
        app_name: A,
        init_f: impl FnOnce() -> T,
    ) -> Result<Self, ShmemError> {
        let type_name = short_typename::<T>();
        let shmem_file = shmem_dir_data_with_base(&dir, &app_name).join(type_name.as_str());
        std::fs::create_dir_all(
            shmem_file
                .parent()
                .unwrap_or_else(|| panic!("no parent dir for {}", shmem_file.display())),
        )
        .unwrap_or_else(|_| panic!("couldn't create shmem dir for {}", shmem_file.display()));

        // Held across open, replace, create and init, so concurrent attachers end up on
        // one segment. A second flock on the directory blocks even in this process, so
        // the retries must not take it again.
        let _lock = Flink(&shmem_file).lock().map_err(ShmemError::LinkOpenFailed)?;
        Self::open_or_init_locked(&shmem_file, init_f)
    }

    fn open_or_init_locked(
        shmem_file: &Path,
        init_f: impl FnOnce() -> T,
    ) -> Result<Self, ShmemError> {
        use shared_memory::ShmemConf;
        match ShmemConf::new().size(std::mem::size_of::<T>()).flink(shmem_file).create() {
            Ok(shmem) => {
                let inner = Self::shmem_ptr(shmem);

                // Init ptr
                unsafe { std::ptr::write(inner.as_ptr(), init_f()) };

                Ok(Self { inner })
            }
            Err(ShmemError::LinkExists) => {
                let mut shmem = match ShmemConf::new().flink(shmem_file).open() {
                    Ok(shmem) => shmem,
                    Err(e) => {
                        tracing::warn!(
                            "couldn't open shmem file {}, removing and retrying: {e}",
                            shmem_file.display()
                        );
                        // A link that could not be removed would make the retry loop forever.
                        if let Err(removal) = crate::cleanup_flink(shmem_file) {
                            tracing::warn!("couldn't replace {}: {removal}", shmem_file.display());
                            return Err(e);
                        }
                        return Self::open_or_init_locked(shmem_file, init_f);
                    }
                };

                // `<`: some platforms round segment sizes up to a page.
                if shmem.len() < std::mem::size_of::<T>() {
                    tracing::warn!(
                        "shmem file {} maps {} bytes, {} needs {}, removing and recreating",
                        shmem_file.display(),
                        shmem.len(),
                        std::any::type_name::<T>(),
                        std::mem::size_of::<T>()
                    );
                    // Dropping an owned mapping unlinks the segment and its link file.
                    shmem.set_owner(true);
                    drop(shmem);
                    // A link that could not be removed would make the retry loop forever.
                    if shmem_file.exists() {
                        return Err(ShmemError::LinkExists);
                    }
                    return Self::open_or_init_locked(shmem_file, init_f);
                }

                let inner = Self::shmem_ptr(shmem);

                Ok(Self { inner })
            }
            Err(e) => Err(e),
        }
    }

    fn shmem_ptr(shmem: Shmem) -> NonNull<T> {
        let shmem_ptr = shmem.as_ptr().cast::<T>();

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

impl<T> Clone for ShmemData<T> {
    fn clone(&self) -> Self {
        self.copy_ptr()
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

#[cfg(test)]
mod tests {
    use flux_utils::{directories::shmem_dir_data_with_base, short_typename};
    use shared_memory::ShmemError;

    use super::ShmemData;

    mod small {
        pub struct Blob(pub u64);
    }
    mod large {
        // Larger than a 16 KiB page: macOS reports shm sizes rounded up to a page.
        pub struct Blob(pub [u64; 2048], pub u64);
    }

    #[test]
    fn reopening_as_a_larger_type_replaces_the_segment() {
        let dir = tempfile::tempdir().unwrap();
        let small =
            ShmemData::open_or_init_with_base_dir(dir.path(), "app", || small::Blob(1)).unwrap();
        assert_eq!(small.0, 1);
        let large =
            ShmemData::open_or_init_with_base_dir(dir.path(), "app", || large::Blob([7; 2048], 7))
                .unwrap();
        assert_eq!(large.0, [7; 2048]);
        assert_eq!(large.1, 7);
        crate::cleanup_shmem(dir.path());
    }

    #[test]
    fn a_link_that_cannot_be_replaced_is_an_error() {
        let dir = tempfile::tempdir().unwrap();
        let link = shmem_dir_data_with_base(dir.path(), "app")
            .join(short_typename::<small::Blob>().as_str());
        // A directory can neither be read as a link nor removed as a file.
        std::fs::create_dir_all(&link).unwrap();
        let opened = ShmemData::open_or_init_with_base_dir(dir.path(), "app", || small::Blob(1));
        assert!(matches!(opened, Err(ShmemError::LinkReadFailed(_))));
    }

    #[test]
    fn concurrent_replacements_attach_to_one_segment() {
        const THREADS: usize = 8;
        for _ in 0..200 {
            let dir = tempfile::tempdir().unwrap();
            ShmemData::open_or_init_with_base_dir(dir.path(), "app", || small::Blob(1)).unwrap();
            let start = std::sync::Barrier::new(THREADS);
            let mut attached: Vec<ShmemData<large::Blob>> = std::thread::scope(|s| {
                let threads: Vec<_> = (0..THREADS)
                    .map(|_| {
                        s.spawn(|| {
                            start.wait();
                            ShmemData::open_or_init_with_base_dir(dir.path(), "app", || {
                                large::Blob([7; 2048], 7)
                            })
                            .unwrap()
                        })
                    })
                    .collect();
                threads.into_iter().map(|t| t.join().unwrap()).collect()
            });
            attached[0].0[0] = 9;
            assert!(attached.iter().all(|a| a.0[0] == 9));
            crate::cleanup_shmem(dir.path());
        }
    }
}
