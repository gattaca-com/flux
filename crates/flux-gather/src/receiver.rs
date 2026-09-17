use std::{net::SocketAddr, path::PathBuf};

use flux::{
    communication::ShmemData,
    spine::{
        DCacheRead, FluxSpine, SpineAdapter, SpineConsumer, SpineDCacheConsumer, SpineProducer,
        SpineProducerWithDCache, SpineProducers, SpineQueue,
    },
    tile::{Tile, TileInfo},
};
use flux_network::{NetworkDriver, PollEvent, TcpConfig, Transport};
use flux_versioned_types::{Blob, DecodeError};
use mio::Token;
use spine_derive::from_spine;
use tracing::warn;

use crate::{meta::GatherMeta, writer::BlobWriter};

/// A frame landed in the dcache.
#[derive(Clone, Copy, Debug)]
#[repr(C)]
pub struct IncomingBlob {
    /// Token of the connection the frame arrived on.
    pub token: Token,
}

/// Ready-made spine for a plain receiver process.
#[from_spine("gather-receiver")]
#[derive(Debug)]
pub struct GatherReceiverSpine {
    /// Tile registry.
    pub tile_info: ShmemData<TileInfo>,
    /// `BlobReceiver` to `BlobRouter`. dcache = size x mtu bytes (zeroed alloc,
    /// lazily backed).
    #[queue(size(2usize.pow(9)), mtu(16 * 1024 * 1024))]
    pub blobs: SpineQueue<IncomingBlob>,
    /// `BlobRouter` to `BlobReceiver`: peers to force-close (they did not send
    /// blobs).
    #[queue(size(64))]
    pub disconnect: SpineQueue<Token>,
}

/// TCP listener. No deserialization, no per-connection state.
pub struct BlobReceiver {
    listen: SocketAddr,
    socket_buf_size: usize,
    max_frame_size: usize,
    driver: Option<NetworkDriver>,
}

impl BlobReceiver {
    /// Defaults: `64 MiB` socket buffer, `16 MiB` max frame matching the spine
    /// dcache mtu.
    pub fn new(listen: SocketAddr) -> Self {
        Self {
            listen,
            socket_buf_size: 64 * 1024 * 1024,
            max_frame_size: 16 * 1024 * 1024,
            driver: None,
        }
    }

    /// Kernel `SO_SNDBUF`/`SO_RCVBUF` applied to the listener and accepted
    /// streams.
    pub fn with_socket_buf_size(mut self, bytes: usize) -> Self {
        self.socket_buf_size = bytes;
        self
    }

    /// Largest accepted frame payload. Stored for symmetry with the sender-side
    /// dcache mtu; the driver itself enforces no limit.
    pub fn with_max_frame_size(mut self, bytes: usize) -> Self {
        self.max_frame_size = bytes;
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
        driver.poll_with_produce(&mut adapter.producers, |event| match event {
            PollEvent::Message { token, .. } => Some(IncomingBlob { token }),
            _ => None,
        });
    }
}

/// Validates frames as blobs, persists them, hands them to the app hook.
pub struct BlobRouter<H: FnMut(&GatherMeta, &Blob)> {
    writer: Option<BlobWriter>,
    hook: H,
}

impl<H: FnMut(&GatherMeta, &Blob)> BlobRouter<H> {
    /// `disk_dir` enables receiver-side persistence; `hook` sees every valid
    /// blob.
    pub fn new(disk_dir: Option<PathBuf>, hook: H) -> Self {
        Self { writer: disk_dir.map(BlobWriter::new), hook }
    }

    /// Zero-copy: dcache slots are 64-byte aligned, so `from_bytes` borrows
    /// directly. Err means the peer is not sending blobs.
    pub fn handle(&mut self, bytes: &[u8]) -> Result<(), DecodeError> {
        let blob = Blob::from_bytes(bytes)?;
        let meta = blob.user_metadata::<GatherMeta>()?;
        if let Some(writer) = self.writer.as_mut() {
            writer.write(blob);
        }
        (self.hook)(&meta, blob);
        Ok(())
    }

    /// Reap disk completions.
    pub fn drive(&mut self) {
        if let Some(writer) = self.writer.as_mut() {
            writer.poll();
        }
    }
}

impl<S: FluxSpine, H: FnMut(&GatherMeta, &Blob) + Send> Tile<S> for BlobRouter<H>
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
        self.drive();
    }

    fn teardown(mut self, _adapter: &mut SpineAdapter<S>) {
        if let Some(writer) = self.writer.as_mut() {
            writer.drain();
        }
    }
}
