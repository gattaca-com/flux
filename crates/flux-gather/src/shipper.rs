use std::net::SocketAddr;

use flux_network::{NetworkDriver, SendBehavior, TcpConfig, Transport};
use flux_timing::{Duration, Repeater};
use flux_versioned_types::Blob;
use mio::Token;

/// Ships blobs as bare TCP frames. Retries never-connected addrs until the
/// first success, after which `NetworkDriver` owns reconnection.
pub struct BlobShipper {
    driver: NetworkDriver,
    addrs: Vec<SocketAddr>,
    tokens: Vec<Option<Token>>,
    retry: Repeater,
}

impl BlobShipper {
    /// TCP transport, nodelay: false, user timeout 5 s, no on-connect message.
    /// Dials every address once; failures are retried by drive.
    pub fn new(addrs: Vec<SocketAddr>) -> Self {
        let mut driver = NetworkDriver::default()
            .with_transport(Transport::Tcp(TcpConfig { nodelay: false, ..Default::default() }))
            .with_user_timeout(5000);
        let mut tokens = Vec::with_capacity(addrs.len());
        for addr in &addrs {
            tokens.push(driver.connect(*addr));
        }
        Self { driver, addrs, tokens, retry: Repeater::every(Duration::from_secs(2)) }
    }

    /// Broadcast the blob bytes as one frame.
    pub fn ship(&mut self, blob: &Blob) {
        let bytes = blob.as_bytes();
        self.driver
            .write_or_enqueue_with(SendBehavior::Broadcast, |buf| buf.extend_from_slice(bytes));
    }

    /// Retry never-connected addrs, then poll.
    pub fn drive(&mut self) {
        if self.tokens.iter().any(Option::is_none) && self.retry.fired() {
            for (addr, token) in self.addrs.iter().zip(self.tokens.iter_mut()) {
                if token.is_none() {
                    *token = self.driver.connect(*addr);
                }
            }
        }
        self.driver.poll_with(|_| {});
    }
}
