use std::net::SocketAddr;

use flux_network::{NetworkDriver, SendBehavior, TcpConfig, Transport};
use flux_timing::{Duration, Instant, Repeater};
use flux_versioned_types::Blob;
use mio::Token;
use tracing::warn;

/// A never-established peer holds at most this much backlog.
const SHED_GRACE_SECS: u64 = 60;
/// A pending handshake looks live until SYN timeout (~127 s).
const ESTABLISH_GRACE_SECS: u64 = 300;

struct Endpoint {
    addr: SocketAddr,
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

pub struct BlobShipper {
    driver: NetworkDriver,
    endpoints: Vec<Endpoint>,
    retry: Repeater,
}

impl BlobShipper {
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

    pub fn with_transport(mut self, transport: Transport) -> Self {
        let driver = std::mem::take(&mut self.driver);
        self.driver = driver.with_transport(transport);
        self
    }

    pub fn with_max_backlog(mut self, frames: usize, timeout: Duration) -> Self {
        let driver = std::mem::take(&mut self.driver);
        self.driver = driver.with_max_backlog(frames, timeout);
        self
    }

    pub fn with_drop_outbound_backlog_on_disconnect(mut self, enabled: bool) -> Self {
        let driver = std::mem::take(&mut self.driver);
        self.driver = driver.with_drop_outbound_backlog_on_disconnect(enabled);
        self
    }

    /// Dropped until an endpoint has connected once.
    pub fn ship(&mut self, blob: &Blob) {
        let bytes = blob.as_bytes();
        self.driver
            .write_or_enqueue_with(SendBehavior::Broadcast, |buf| buf.extend_from_slice(bytes));
    }

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
