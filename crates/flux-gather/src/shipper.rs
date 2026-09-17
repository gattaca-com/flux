use std::net::SocketAddr;

use flux_network::{NetworkDriver, SendBehavior, TcpConfig, Transport};
use flux_timing::{Duration, Repeater};
use flux_versioned_types::Blob;
use mio::Token;

/// Ships blobs as bare TCP frames. The first `drive()` dials every address;
/// never-connected addrs are retried by drive until the first success, after
/// which `NetworkDriver` owns reconnection.
pub struct BlobShipper {
    driver: NetworkDriver,
    addrs: Vec<SocketAddr>,
    tokens: Vec<Option<Token>>,
    retry: Repeater,
}

impl BlobShipper {
    /// TCP transport, nodelay: false, user timeout 5 s, no on-connect message.
    pub fn new(addrs: Vec<SocketAddr>) -> Self {
        let driver = NetworkDriver::default()
            .with_transport(Transport::Tcp(TcpConfig { nodelay: false, ..Default::default() }))
            .with_user_timeout(5000);
        let tokens = vec![None; addrs.len()];
        Self { driver, addrs, tokens, retry: Repeater::every(Duration::from_secs(2)) }
    }

    /// Disconnects peers whose send backlog exceeds `frames` for `timeout`;
    /// a disconnected outbound peer drops further frames until it reconnects.
    /// Forwards to the `NetworkDriver` builder; call before the first
    /// `drive()`.
    pub fn with_max_backlog(mut self, frames: usize, timeout: Duration) -> Self {
        let driver = std::mem::take(&mut self.driver);
        self.driver = driver.with_max_backlog(frames, timeout);
        self
    }

    /// Drops queued outbound frames when a connection drops instead of
    /// replaying them after reconnect; sends while disconnected are dropped.
    /// Forwards to the `NetworkDriver` builder; call before the first
    /// `drive()`.
    pub fn with_drop_outbound_backlog_on_disconnect(mut self, enabled: bool) -> Self {
        let driver = std::mem::take(&mut self.driver);
        self.driver = driver.with_drop_outbound_backlog_on_disconnect(enabled);
        self
    }

    /// Broadcast the blob bytes as one frame.
    ///
    /// Frames shipped while no address has ever connected are dropped; once
    /// connected, frames queue during reconnects without bound unless
    /// `with_max_backlog`/`with_drop_outbound_backlog_on_disconnect` is set.
    pub fn ship(&mut self, blob: &Blob) {
        let bytes = blob.as_bytes();
        self.driver
            .write_or_enqueue_with(SendBehavior::Broadcast, |buf| buf.extend_from_slice(bytes));
    }

    /// Dials never-connected addrs (the first call dials immediately: the
    /// retry timer starts at zero), then polls. Returns whether the poll did
    /// network work, so a tile can `adapter.mark_work()`.
    pub fn drive(&mut self) -> bool {
        if self.tokens.iter().any(Option::is_none) && self.retry.fired() {
            for (addr, token) in self.addrs.iter().zip(self.tokens.iter_mut()) {
                if token.is_none() {
                    *token = self.driver.connect(*addr);
                }
            }
        }
        self.driver.poll_with(|_| {})
    }
}
