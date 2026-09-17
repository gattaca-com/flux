use std::{marker::PhantomData, net::SocketAddr};

use flux::{
    spine::{
        DCacheRead, FluxSpine, SpineAdapter, SpineConsumer, SpineDCacheConsumer, SpineProducer,
        SpineProducerWithDCache, SpineProducers,
    },
    tile::Tile,
};
use flux_network::{NetworkDriver, PollEvent, TcpConfig, Transport};
use flux_versioned_types::{Blob, DecodeError, Versioned};
use mio::Token;
use tracing::warn;

/// A frame landed in the dcache. Message type of the receiver spine's dcache
/// queue.
#[repr(C)]
#[derive(Clone, Copy, Debug)]
pub struct IncomingBlob {
    /// Token of the connection the frame arrived on.
    pub token: Token,
}

/// Where [`BlobRouter`] delivers validated blobs.
pub trait BlobSink<U> {
    /// A blob carrying `U` metadata arrived intact.
    fn on_blob(&mut self, meta: &U, blob: &Blob);
    /// Called once per router loop; progress your own I/O here (e.g.
    /// `BlobWriter::poll`). Returns whether you did work.
    fn drive(&mut self) -> bool {
        false
    }
    /// Called from the router's teardown (e.g. `BlobWriter::drain`).
    fn finish(&mut self) {}
}

/// Closures are sinks with no I/O of their own.
impl<U, F: FnMut(&U, &Blob)> BlobSink<U> for F {
    fn on_blob(&mut self, meta: &U, blob: &Blob) {
        self(meta, blob);
    }
}

/// TCP listener feeding frames into the dcache. No deserialization, no
/// per-connection state.
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

/// Validates dcache frames as blobs carrying `U`, hands them to the sink,
/// disconnects peers that send anything else.
pub struct BlobRouter<U: Versioned, K: BlobSink<U>> {
    sink: K,
    // `fn() -> U` is always `Send`, so the tile stays `Send` with no `U:
    // Send` bound.
    meta: PhantomData<fn() -> U>,
}

impl<U: Versioned, K: BlobSink<U>> BlobRouter<U, K> {
    /// Route validated blobs into `sink`.
    pub fn new(sink: K) -> Self {
        Self { sink, meta: PhantomData }
    }

    /// `Blob::from_bytes(bytes)?` (zero-copy: dcache slots are 64-byte
    /// aligned), then `blob.user_metadata::<U>()?`, then
    /// `sink.on_blob(&meta, blob)`. `Err`: the peer is not sending our blobs.
    /// Also usable as a component by an app that owns its own TCP tile.
    pub fn handle(&mut self, bytes: &[u8]) -> Result<(), DecodeError> {
        let blob = Blob::from_bytes(bytes)?;
        let meta = blob.user_metadata::<U>()?;
        self.sink.on_blob(&meta, blob);
        Ok(())
    }

    /// The sink, e.g. to inspect what it recorded.
    pub fn sink_mut(&mut self) -> &mut K {
        &mut self.sink
    }
}

impl<S: FluxSpine, U: Versioned, K: BlobSink<U> + Send> Tile<S> for BlobRouter<U, K>
where
    S::Consumers: AsMut<SpineDCacheConsumer<IncomingBlob>>,
    S::Producers: AsRef<SpineProducer<Token>>,
{
    fn loop_body(&mut self, adapter: &mut SpineAdapter<S>) {
        adapter.consume_with_dcache::<IncomingBlob, Result<(), DecodeError>, _, _>(
            |_, payload| self.handle(payload),
            |result, producers| match result {
                DCacheRead::Ok((_, Ok(()))) | DCacheRead::SpedPast => {}
                DCacheRead::Ok((msg, Err(error))) => {
                    warn!(?error, token = ?msg.token, "gather peer sent a non-blob frame");
                    producers.produce(msg.token);
                }
                DCacheRead::NoRef(msg) | DCacheRead::Lost(msg) => {
                    warn!(token = ?msg.token, "gather frame payload missing");
                    producers.produce(msg.token);
                }
            },
        );
        if self.sink.drive() {
            adapter.mark_work();
        }
    }

    fn teardown(mut self, _adapter: &mut SpineAdapter<S>) {
        self.sink.finish();
    }
}
