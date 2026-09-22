//! Gather versioned leaves into blobs, ship them over TCP or UDP, and persist
//! them to disk.
//!
//! One blob is one transport message is one file: payloads and file contents
//! are `blob.as_bytes()`. See `tests/e2e.rs` for the complete usage example.
pub mod queues;
pub mod reader;
pub mod receiver;
pub mod shipper;
pub mod writer;

pub use flux_versioned_types::{
    Blob, BlobCache, DecodeError, HasVersionedLeaves, Scratch, Versioned,
};
pub use mio::Token;
pub use queues::GatherQueues;
pub use reader::{BlobReader, ReadError};
pub use receiver::{BlobConsumer, BlobHandler, BlobReceiver, IncomingBlob};
pub use shipper::BlobShipper;
pub use writer::BlobWriter;
