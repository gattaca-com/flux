use std::net::SocketAddr;

use flux_network::{NetworkDriver, SendBehavior, TcpConfig, Transport};
use flux_timing::{Duration, Instant, Repeater};
use flux_versioned_types::Blob;
use mio::Token;
use tracing::warn;

/// A never-established endpoint may sit disconnected this long before its
/// backlog is shed. Covers slow receiver startups: a never-established peer
/// holds at most this much of queued frames.
const SHED_GRACE_SECS: u64 = 60;
/// An endpoint must look live this long before it counts as established. A
/// pending handshake looks exactly like a live connection until SYN timeout,
/// worst case ~127 s, so anything shorter would exempt peers that never
/// received a byte.
const ESTABLISH_GRACE_SECS: u64 = 300;

struct Endpoint {
    addr: SocketAddr,
    /// `None` until `connect` returns a token; the driver then owns
    /// reconnection.
    token: Option<Token>,
    established: bool,
    live_since: Option<(Token, Instant)>,
    dead_since: Option<(Token, Instant)>,
}

impl Endpoint {
    fn dial(&mut self, driver: &mut NetworkDriver) {
        if self.token.is_none() {
            self.token = driver.connect(self.addr);
        }
    }

    fn shed(&mut self, driver: &mut NetworkDriver, disconnected: &[Token], now: Instant) {
        let shed_grace = Duration::from_secs(SHED_GRACE_SECS);
        let establish_grace = Duration::from_secs(ESTABLISH_GRACE_SECS);
        let Some(token) = self.token else {
            self.live_since = None;
            self.dead_since = None;
            return;
        };
        if self.established {
            return;
        }
        if !disconnected.contains(&token) {
            if self
                .live_since
                .is_some_and(|(t, since)| t == token && now.elapsed_since(since) >= establish_grace)
            {
                self.established = true;
            } else if self.live_since.is_none_or(|(t, _)| t != token) {
                self.live_since = Some((token, now));
            }
            self.dead_since = None;
            return;
        }
        self.live_since = None;
        let since = match self.dead_since {
            Some((t, since)) if t == token => since,
            _ => {
                self.dead_since = Some((token, now));
                now
            }
        };
        if now.elapsed_since(since) < shed_grace {
            return;
        }
        let n = driver.clear_backlog(token);
        if n > 0 {
            warn!(?token, addr = ?self.addr, dropped = n, "cleared backlog for peer that never connected");
        }
    }
}

/// Ships blobs as bare TCP frames.
///
/// The first `drive()` dials every address; never-connected addrs are retried
/// by drive until the first success, after which `NetworkDriver` owns
/// reconnection. An endpoint that never held a live connection sheds its
/// queued frames after `SHED_GRACE_SECS` disconnected.
pub struct BlobShipper {
    driver: NetworkDriver,
    endpoints: Vec<Endpoint>,
    retry: Repeater,
}

impl BlobShipper {
    /// TCP transport, nodelay: false, user timeout 5 s, no on-connect message.
    pub fn new(addrs: Vec<SocketAddr>) -> Self {
        let driver = NetworkDriver::default()
            .with_transport(Transport::Tcp(TcpConfig { nodelay: false, ..Default::default() }))
            .with_user_timeout(5000);
        let endpoints = addrs
            .into_iter()
            .map(|addr| Endpoint {
                addr,
                token: None,
                established: false,
                live_since: None,
                dead_since: None,
            })
            .collect();
        Self { driver, endpoints, retry: Repeater::every(Duration::from_secs(2)) }
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
    /// Frames shipped before any endpoint ever connected are dropped; an
    /// endpoint that never held a live connection sheds its queued frames
    /// after `SHED_GRACE_SECS` disconnected; an endpoint that was live for
    /// `ESTABLISH_GRACE_SECS` keeps its backlog for gap-free replay, bounded
    /// only by `with_max_backlog`/
    /// `with_drop_outbound_backlog_on_disconnect`.
    pub fn ship(&mut self, blob: &Blob) {
        let bytes = blob.as_bytes();
        self.driver
            .write_or_enqueue_with(SendBehavior::Broadcast, |buf| buf.extend_from_slice(bytes));
    }

    /// Dials never-connected addrs and sheds dead-endpoint backlogs (the
    /// first call dials immediately: the retry timer starts at zero), then
    /// polls. Returns whether the poll did network work, so a tile can
    /// `adapter.mark_work()`.
    pub fn drive(&mut self) -> bool {
        if self.retry.fired() {
            let now = Instant::now();
            let disconnected: Vec<Token> = self.driver.currently_disconnected().collect();
            for ep in &mut self.endpoints {
                ep.dial(&mut self.driver);
                ep.shed(&mut self.driver, &disconnected, now);
            }
        }
        self.driver.poll_with(|_| {})
    }
}
