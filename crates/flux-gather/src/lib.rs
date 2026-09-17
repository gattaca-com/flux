//! Generic data-gather: buffer versioned leaves on the sender, flush them as
//! one `Blob` per leaf type, ship each blob as a single TCP frame and/or
//! persist it as a single file, then validate and route the frames on the
//! receiver. Sender-side and receiver-side files for the same blob are
//! byte-identical; the blob's `GatherMeta` carries everything the receiver
//! needs, so receivers hold no per-connection state.

// mio::Token has no repr(C), so the FFI check from_spine generates for the
// Token-carrying queues warns. Queues are shmem, never actual FFI.
#![allow(improper_ctypes)]

pub mod config;
pub mod meta;
pub mod queues;
pub mod reader;
pub mod receiver;
pub mod sender;
pub mod shipper;
pub mod writer;

pub use config::GatherConfig;
pub use flux_versioned_types::{Blob, BlobCache, DecodeError, HasVersionedLeaves, Scratch};
pub use meta::GatherMeta;
pub use mio::Token;
pub use queues::{Boundary, GatherQueues};
pub use reader::{BlobReader, ReadError};
pub use receiver::{BlobReceiver, BlobRouter, GatherReceiverSpine, IncomingBlob};
pub use sender::{GatherSender, GatherTile};
pub use shipper::BlobShipper;
pub use writer::BlobWriter;
