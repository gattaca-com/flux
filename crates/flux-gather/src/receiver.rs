use std::{marker::PhantomData, net::SocketAddr};

use flux::{
    spine::{
        DCacheRead, FluxSpine, SpineAdapter, SpineConsumer, SpineDCacheConsumer, SpineProducer,
        SpineProducerWithDCache, SpineProducers,
    },
    tile::{Tile, TileName},
};
use flux_network::{NetworkDriver, PollEvent, TcpConfig, Transport};
use flux_versioned_types::{Blob, DecodeError, Versioned};
use mio::Token;
use tracing::warn;

#[repr(C)]
#[derive(Clone, Copy, Debug)]
pub struct IncomingBlob {
    pub token: Token,
}

pub trait BlobHandler<S: FluxSpine, U>: Tile<S> {
    /// `blob` borrows the gather mapping and is valid only for the call.
    /// Copy out whatever must outlive it; header-only work needs no copy.
    fn on_blob(&mut self, meta: &U, blob: &Blob, producers: &mut S::Producers);
}

pub struct BlobReceiver {
    listen: SocketAddr,
    socket_buf_size: usize,
    driver: Option<NetworkDriver>,
}

impl BlobReceiver {
    pub fn new(listen: SocketAddr) -> Self {
        Self { listen, socket_buf_size: 64 * 1024 * 1024, driver: None }
    }

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

/// Hands each frame to the handler borrowed from the dcache mapping, so a
/// torn slot never reaches it. The handler copies out whatever must outlive
/// the call; header-only work needs no copy.
pub struct BlobConsumer<U: Versioned, H> {
    handler: H,
    meta: PhantomData<fn() -> U>,
}

impl<U: Versioned, H> BlobConsumer<U, H> {
    pub fn new(handler: H) -> Self {
        Self { handler, meta: PhantomData }
    }

    /// `Err`: the peer is not sending our blobs.
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
        self.handler.on_blob(&meta, blob, &mut adapter.producers);
        Ok(())
    }

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
            let more = adapter
                .consume_with_dcache_collaborative::<IncomingBlob, Result<(), DecodeError>, _, _>(
                    |_, payload, producers| {
                        let blob = Blob::from_bytes(payload)?;
                        if blob.as_bytes().len() != payload.len() {
                            return Err(DecodeError::LengthMismatch {
                                expected: blob.as_bytes().len(),
                                got: payload.len(),
                            });
                        }
                        let meta = blob.user_metadata::<U>()?;
                        self.handler.on_blob(&meta, blob, producers);
                        Ok(())
                    },
                    |result, producers| match result {
                        DCacheRead::Ok((_, Ok(()))) | DCacheRead::SpedPast => {}
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
                    },
                );
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
