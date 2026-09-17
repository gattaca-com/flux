use std::{marker::PhantomData, net::SocketAddr};

use flux::{
    spine::{
        DCacheRead, FluxSpine, SpineAdapter, SpineConsumer, SpineDCacheConsumer, SpineProducer,
        SpineProducerWithDCache, SpineProducers,
    },
    tile::{Tile, TileName},
};
use flux_network::{NetworkDriver, PollEvent, TcpConfig, Transport};
use flux_versioned_types::{Blob, DecodeError, Scratch, Versioned};
use mio::Token;
use tracing::warn;

/// A frame landed in the dcache. Message type of the receiver spine's dcache
/// queue.
///
/// The dcache `mtu` is not enforced per frame: size it at least the largest
/// blob a sender can emit. A frame larger than `mtu` weakens the ring's lap
/// guarantee and surfaces as `Lost`.
#[repr(C)]
#[derive(Clone, Copy, Debug)]
pub struct IncomingBlob {
    /// Token of the connection the frame arrived on.
    pub token: Token,
}

/// Receives validated blobs.
///
/// Everything else in its life is an ordinary tile's:
/// `BlobConsumer` forwards `on_attach`, `try_init`, `loop_body`, `teardown` and
/// `name` to it, so I/O the handler owns (a `BlobWriter`, a replica spine's
/// producers) is driven from its own `loop_body` and finished in its
/// `teardown`.
pub trait BlobHandler<S: FluxSpine, U>: Tile<S> {
    /// One blob carrying `U` metadata arrived intact. `blob` lives in the
    /// consumer's scratch until the next frame.
    fn on_blob(&mut self, meta: &U, blob: &Blob, adapter: &mut SpineAdapter<S>);
}

/// TCP listener feeding frames into the dcache. No deserialization, no
/// per-connection state.
///
/// Tokens are per-driver integers starting at 0: run exactly one
/// `BlobReceiver` per spine, or a second one's `disconnect: Token` values
/// would address the wrong peer.
pub struct BlobReceiver {
    listen: SocketAddr,
    socket_buf_size: usize,
    driver: Option<NetworkDriver>,
}

impl BlobReceiver {
    /// Default `64 MiB` kernel socket buffer.
    pub fn new(listen: SocketAddr) -> Self {
        Self { listen, socket_buf_size: 64 * 1024 * 1024, driver: None }
    }

    /// Kernel `SO_SNDBUF`/`SO_RCVBUF` applied to the listener and accepted
    /// streams.
    pub fn with_socket_buf_size(mut self, bytes: usize) -> Self {
        self.socket_buf_size = bytes;
        self
    }
}

impl<S: FluxSpine> Tile<S> for BlobReceiver
where
    S::Producers: AsRef<SpineProducerWithDCache<IncomingBlob>>,
    S::Consumers: AsMut<SpineConsumer<Token>>,
{
    fn try_init(&mut self, adapter: &mut SpineAdapter<S>) -> bool {
        let producers: &SpineProducerWithDCache<IncomingBlob> = adapter.producers.as_ref();
        let mut driver = NetworkDriver::default()
            .with_transport(Transport::Tcp(TcpConfig::default()))
            .with_socket_buf_size(self.socket_buf_size)
            .with_dcache(producers.dcache_ptr());
        driver.listen_at(self.listen).expect("gather receiver couldn't listen");
        self.driver = Some(driver);
        true
    }

    fn loop_body(&mut self, adapter: &mut SpineAdapter<S>) {
        let Some(driver) = self.driver.as_mut() else { return };
        adapter.consume(|token: Token, _| driver.disconnect(token));
        if driver.poll_with_produce(&mut adapter.producers, |event| match event {
            PollEvent::Message { token, .. } => Some(IncomingBlob { token }),
            _ => None,
        }) {
            adapter.mark_work();
        }
    }
}

/// Validates dcache frames as blobs carrying `U` and hands them to the handler.
/// Never disconnects a peer for a missing payload: `NoRef`/`Lost` are
/// receiver-side faults.
///
/// Each frame is copied once into an internal scratch buffer while the dcache
/// slot is still held, so the epoch check covers the copy and a torn slot can
/// never reach the handler. `handle` does not copy: the caller owns the bytes.
pub struct BlobConsumer<U: Versioned, H> {
    handler: H,
    scratch: Scratch,
    // `fn() -> U` is always `Send`, so the tile stays `Send` with no `U:
    // Send` bound.
    meta: PhantomData<fn() -> U>,
}

impl<U: Versioned, H> BlobConsumer<U, H> {
    /// Route validated blobs into `handler`.
    pub fn new(handler: H) -> Self {
        Self { handler, scratch: Scratch::new(), meta: PhantomData }
    }

    /// Validates one frame (`Blob::from_bytes`, then exactly one blob per
    /// frame) and hands it to the handler. The caller owns `bytes`, so no copy
    /// is made. `Err`: the peer is not sending our blobs. Also usable as a
    /// component by an app that owns its own TCP tile.
    pub fn handle<S: FluxSpine>(
        &mut self,
        bytes: &[u8],
        adapter: &mut SpineAdapter<S>,
    ) -> Result<(), DecodeError>
    where
        H: BlobHandler<S, U>,
    {
        let blob = Blob::from_bytes(bytes)?;
        if blob.as_bytes().len() != bytes.len() {
            return Err(DecodeError::LengthMismatch {
                expected: blob.as_bytes().len(),
                got: bytes.len(),
            });
        }
        let meta = blob.user_metadata::<U>()?;
        self.handler.on_blob(&meta, blob, adapter);
        Ok(())
    }

    /// The handler, e.g. to inspect what it recorded.
    pub fn handler_mut(&mut self) -> &mut H {
        &mut self.handler
    }
}

impl<S: FluxSpine, U: Versioned, H: BlobHandler<S, U>> Tile<S> for BlobConsumer<U, H>
where
    S::Consumers: AsMut<SpineDCacheConsumer<IncomingBlob>>,
    S::Producers: AsRef<SpineProducer<Token>>,
{
    fn name(&self) -> TileName {
        // The handler is the tile's identity in tile_info/metrics.
        self.handler.name()
    }

    fn on_attach(&mut self, adapter: &mut SpineAdapter<S>) {
        self.handler.on_attach(adapter);
    }

    fn try_init(&mut self, adapter: &mut SpineAdapter<S>) -> bool {
        self.handler.try_init(adapter)
    }

    fn loop_body(&mut self, adapter: &mut SpineAdapter<S>) {
        loop {
            let mut pending: Option<U> = None;
            let scratch = &mut self.scratch;
            let more = adapter
                .consume_with_dcache_collaborative::<IncomingBlob, Result<U, DecodeError>, _, _>(
                    |_, payload| {
                        let blob = Blob::from_bytes(payload)?;
                        if blob.as_bytes().len() != payload.len() {
                            return Err(DecodeError::LengthMismatch {
                                expected: blob.as_bytes().len(),
                                got: payload.len(),
                            });
                        }
                        let meta = blob.user_metadata::<U>()?;
                        scratch.load(blob.as_bytes())?;
                        Ok(meta)
                    },
                    |result, producers| match result {
                        DCacheRead::Ok((_, Ok(meta))) => pending = Some(meta),
                        DCacheRead::Ok((msg, Err(error))) => {
                            warn!(?error, token = ?msg.token, "gather peer sent a non-blob frame");
                            producers.produce(msg.token);
                        }
                        DCacheRead::NoRef(msg) => {
                            warn!(token = ?msg.token, "gather frame payload missing");
                        }
                        DCacheRead::Lost(_) => {
                            warn!("gather frame lost to receiver-side overrun");
                        }
                        DCacheRead::SpedPast => {}
                    },
                );
            if let Some(meta) = pending {
                let blob = Blob::from_bytes(self.scratch.as_bytes())
                    .expect("copied from a validated blob");
                self.handler.on_blob(&meta, blob, adapter);
            }
            if !more {
                break;
            }
        }
        self.handler.loop_body(adapter);
    }

    fn teardown(mut self, adapter: &mut SpineAdapter<S>) {
        self.loop_body(adapter);
        self.handler.teardown(adapter);
    }
}
