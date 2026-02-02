use shared_memory::ShmemError;
use thiserror::Error;

#[derive(Error, Debug, Copy, Clone, PartialEq)]
pub enum EmptyError {
    #[error("Lock empty")]
    Empty,
}

#[derive(Error, Debug, Copy, Clone, PartialEq)]
#[repr(u8)]
pub enum ReadError {
    #[error("Got sped past")]
    SpedPast,
    #[error("Lock empty")]
    Empty,
}

#[derive(Error, Debug)]
#[repr(u8)]
pub enum QueueError {
    #[error("Queue not initialized")]
    UnInitialized,
    #[error("Queue length not power of two")]
    LengthNotPowerOfTwo,
    #[error(
        "Element size changed from {0} to {1}. Need to reinit the queue after detaching processes"
    )]
    ElementSizeChanged(usize, usize),
    #[error("Element at {0} poisoned. Need to reinit the queue after detaching processes")]
    ElementPoisoned(usize),
    #[error("Shared memory file does not exist")]
    NonExistingFile,
    #[error("Preexisting shared memory too small")]
    TooSmall,
    #[error("Shmem error")]
    ShmemError(#[from] ShmemError),
}
