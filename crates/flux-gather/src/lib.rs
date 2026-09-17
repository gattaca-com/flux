//! Building blocks for gathering versioned leaves into blobs, shipping them
//! over TCP, persisting them to disk, and routing them back on receipt.
//!
//! One `Blob` is one TCP frame is one file: frame payloads and file contents
//! are exactly `blob.as_bytes()`, so sender-side and receiver-side copies of
//! a blob are byte-identical.
//!
//! On the sender side, declare queues with `#[queue(gather)]`, drain them in
//! your tile with `S::gather_into(adapter, &mut cache)`, consume any queue
//! you want to react to by hand, and when *you* decide, flush:
//! `cache.flush(&your_meta, zstd_level, |blob| { shipper.ship(blob);
//! writer.write(blob, &path) })`. On the receiver side, run a spine with an
//! `IncomingBlob` dcache queue and a `Token` queue, listen with
//! `BlobReceiver`, and hand `(&YourMeta, &Blob)` to your `BlobSink` through
//! `BlobRouter::<YourMeta, _>::new(sink)`; `BlobReader` reads files back.
//! See `tests/e2e.rs` for the complete example.
//!
//! Under the `park` feature, tiles that only poll sockets or disk must be
//! attached with `TileConfig::without_park()`: with nothing to consume they
//! would park after one idle pass and never wake.

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
pub use receiver::{BlobReceiver, BlobRouter, BlobSink, IncomingBlob};
pub use shipper::BlobShipper;
pub use writer::BlobWriter;
