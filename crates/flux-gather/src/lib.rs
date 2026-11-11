//! Gather versioned leaves into blobs, ship them over TCP, and persist them to
//! disk.
//!
//! One blob is one TCP frame is one file: frame payloads and file contents are
//! `blob.as_bytes()`. See `tests/e2e.rs` for the complete usage example.
//! Names and metadata from a peer must pass
//! `flux_versioned_types::leaves::name_is_path_component` before they form a
//! path.
pub mod io;
pub mod queues;
pub mod reader;
pub mod receiver;
pub mod shipper;

pub use flux_disk::FileToken;
pub use flux_versioned_types::{
    Blob, BlobCache, DecodeError, HasVersionedLeaves, Scratch, Versioned,
};
pub use io::{BlobEvent, BlobIo};
pub use mio::Token;
pub use queues::GatherQueues;
pub use reader::{BlobReader, ReadError};
pub use receiver::{BlobConsumer, BlobHandler, BlobReceiver, IncomingBlob};
pub use shipper::{BlobShipper, BlobShipperBuilder};
