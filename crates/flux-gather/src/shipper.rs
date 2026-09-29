use std::net::SocketAddr;

use flux_network::{Group, Network, NetworkEvent, ReplayPolicy, TcpGroupConfig};
use flux_timing::{Duration, Instant, Repeater};
use flux_versioned_types::Blob;
use mio::Token;
use tracing::warn;

/// A never-established peer holds at most this much backlog.
const SHED_GRACE_SECS: u64 = 60;

struct Endpoint {
    addr: SocketAddr,
    token: Token,
    established: bool,
    dead_since: Option<Instant>,
}

impl Endpoint {
    fn shed(&mut self, driver: &mut Network, disconnected: &[Token], now: Instant) {
        let shed_grace = Duration::from_secs(SHED_GRACE_SECS);
        if self.established {
            return;
        }
        if !disconnected.contains(&self.token) {
            self.dead_since = None;
            return;
        }
        let since = *self.dead_since.get_or_insert(now);
        if now.elapsed_since(since) < shed_grace {
            return;
        }
        let n = driver.clear_backlog(self.token);
        if n > 0 {
            warn!(token = ?self.token, addr = ?self.addr, dropped = n, "cleared backlog for peer that never connected");
        }
    }
}

/// Configures a [`BlobShipper`] before opening connections.
pub struct BlobShipperBuilder {
    addrs: Vec<SocketAddr>,
    config: TcpGroupConfig,
}

impl BlobShipperBuilder {
    pub fn with_max_backlog(mut self, frames: usize, timeout: Duration) -> Self {
        self.config.max_backlog_frames = Some((frames, timeout));
        self
    }

    pub fn with_drop_outbound_backlog_on_disconnect(mut self, enabled: bool) -> Self {
        self.config.replay = if enabled { ReplayPolicy::Drop } else { ReplayPolicy::Replay };
        self
    }

    /// Registers endpoints and starts nonblocking connections. Drive the
    /// returned shipper to complete establishment and retry failed attempts.
    pub fn connect(self) -> BlobShipper {
        let mut driver = Network::default();
        let group = driver.add_group(self.config);
        let endpoints = self
            .addrs
            .into_iter()
            .map(|addr| Endpoint {
                addr,
                token: driver.connect(group, addr),
                established: false,
                dead_since: None,
            })
            .collect();
        BlobShipper {
            driver,
            group,
            endpoints,
            shed_timer: Repeater::every(Duration::from_secs(2)),
        }
    }
}

pub struct BlobShipper {
    driver: Network,
    group: Group,
    endpoints: Vec<Endpoint>,
    shed_timer: Repeater,
}

impl BlobShipper {
    /// Configures a shipper without opening connections.
    pub fn builder(addrs: Vec<SocketAddr>) -> BlobShipperBuilder {
        let config = TcpGroupConfig {
            aligned_payloads: true,
            max_frame_size: u32::MAX as usize,
            nodelay: false,
            user_timeout_ms: 5000,
            replay: ReplayPolicy::Replay,
            ..Default::default()
        };
        BlobShipperBuilder { addrs, config }
    }

    /// Sends to all endpoints. Disconnected sends follow the configured replay
    /// policy.
    pub fn ship(&mut self, blob: &Blob) {
        let bytes = blob.as_bytes();
        self.driver.broadcast_with(self.group, |buf| buf.extend_from_slice(bytes));
    }

    /// One endpoint only, by token. A broadcast pause does not apply.
    pub fn ship_to(&mut self, token: Token, blob: &Blob) {
        let bytes = blob.as_bytes();
        self.driver.send_with(token, |buf| buf.extend_from_slice(bytes));
    }

    /// Takes an endpoint out of [`Self::ship`] until resumed; [`Self::ship_to`]
    /// still reaches it.
    pub fn pause_broadcast(&mut self, token: Token) {
        self.driver.pause_broadcast(token);
    }

    pub fn resume_broadcast(&mut self, token: Token) {
        self.driver.resume_broadcast(token);
    }

    pub fn is_broadcast_paused(&self, token: Token) -> bool {
        self.driver.is_broadcast_paused(token)
    }

    /// Which endpoint, in construction order, `token` belongs to.
    pub fn endpoint_of(&self, token: Token) -> Option<usize> {
        self.endpoints.iter().position(|ep| ep.token == token)
    }

    pub fn drive(&mut self) -> bool {
        self.drive_with(|_| {})
    }

    /// Hands the caller every lifecycle and message event. `Connected` reports
    /// initial establishment and each successful reconnect.
    pub fn drive_with(&mut self, mut on_event: impl for<'a> FnMut(NetworkEvent<'a>)) -> bool {
        if self.shed_timer.fired() {
            let now = Instant::now();
            let disconnected: Vec<Token> = self.driver.currently_disconnected().collect();
            for ep in &mut self.endpoints {
                ep.shed(&mut self.driver, &disconnected, now);
            }
        }
        self.driver.poll_with(|event| {
            if let NetworkEvent::Connected { token, .. } = event {
                if let Some(endpoint) =
                    self.endpoints.iter_mut().find(|endpoint| endpoint.token == token)
                {
                    endpoint.established = true;
                }
            }
            on_event(event);
        })
    }
}
