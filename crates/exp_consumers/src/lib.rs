//! Functional repros of how solana-builder and builder use the flux `main`
//! network stack, re-expressed on this branch's ownership model.
//!
//! Each module is one repro from the consumer brief, minimal but runnable:
//! the shape a real tile would take under the `Service` contract, with a test
//! that pins the behaviour the original relies on. One `poll_with` of the old
//! stack maps to one tile iteration here: sends staged from the previous
//! iteration go out first, then `drive(Some(Duration::ZERO), ..)` runs the
//! non-blocking network pass, then the tile pulls what its services recorded
//! and acts on it. Serialization is out of network scope, so raw byte
//! payloads stand in for the original wincode messages throughout.
//!
//! - [`broadcast_listener`]: a length-prefixed listener fanning a channel out
//!   by broadcast, unicasting the current filter to clients accepted in the
//!   same iteration, and dropping batches while nobody is connected
//!   (shredstream broadcaster + bundle-api tile).
//! - [`subscriber`]: one persistent keepalive endpoint, a blocking idle wait as
//!   the loop's only pacing, and reconnect proven end to end (analytics
//!   shredstream subscriber).
//! - [`three_group_tile`]: three heterogeneous services on one network in a
//!   caller-owned enum slice — per-token filtered unicast, group broadcast, and
//!   an accept-then-handshake-next-iteration protocol (the builder's
//!   `NetworkTile`).
//! - [`http_by_token`]: an `HttpService` server answering strictly on a later
//!   iteration by token, behind a generation guard (overseer `POST /sql`).
//! - [`http_client_pool`]: an `HttpService` client pool with bounded in-flight
//!   requests, a deadline, and persistent endpoints that are never removed,
//!   healing through reconnect (rpc simulation pool).
//! - [`raw_backpressure`]: a raw-framed server with a hard backlog cap, a
//!   per-connection input cap, round-robin processing and both close flavours
//!   (overseer `PostgreSQL` server).

pub mod broadcast_listener;
pub mod http_by_token;
pub mod http_client_pool;
pub mod raw_backpressure;
pub mod subscriber;
pub mod three_group_tile;

use flux_network::{
    Token,
    stream::{
        ConnectionGroup, ConnectionGroupId, Deadline, ReadinessOutcome, Service, StreamEvent,
        TickOutcome,
    },
};
use flux_timing::Instant;
use mio::event::Event;

/// The old `poll_with` closure body, as a leaf: every transport event is
/// recorded on the service for the tile to pull after `drive`.
pub struct RecordingLeaf {
    group: ConnectionGroup,
    /// Message payloads, oldest first, message boundaries as framed.
    pub inbox: Vec<Vec<u8>>,
    /// Tokens accepted by a listener since the tile last took them.
    pub accepted: Vec<Token>,
    /// Outbound endpoints that (re)connected since the tile last looked.
    pub connected: Vec<Token>,
    /// Connections that closed since the tile last took them.
    pub disconnected: Vec<Token>,
}

impl RecordingLeaf {
    pub fn new(group: ConnectionGroup) -> Self {
        Self {
            group,
            inbox: Vec::new(),
            accepted: Vec::new(),
            connected: Vec::new(),
            disconnected: Vec::new(),
        }
    }

    /// The application's handle on the transport: sends, closes, endpoints.
    pub fn group_mut(&mut self) -> &mut ConnectionGroup {
        &mut self.group
    }

    /// Every recorded message so far, leaving the inbox empty.
    pub fn drain_inbox(&mut self) -> Vec<Vec<u8>> {
        std::mem::take(&mut self.inbox)
    }
}

fn record(
    event: &StreamEvent<'_>,
    inbox: &mut Vec<Vec<u8>>,
    accepted: &mut Vec<Token>,
    connected: &mut Vec<Token>,
    disconnected: &mut Vec<Token>,
) {
    match event {
        StreamEvent::Accepted { token, .. } => accepted.push(*token),
        StreamEvent::Connected { token, .. } => connected.push(*token),
        StreamEvent::Message { payload, .. } => inbox.push(payload.to_vec()),
        StreamEvent::Disconnected { token, .. } => disconnected.push(*token),
    }
}

impl Service for RecordingLeaf {
    fn group_id(&self) -> &ConnectionGroupId {
        self.group.group_id()
    }

    fn handle_event(&mut self, event: &Event) -> ReadinessOutcome {
        let Self { group, inbox, accepted, connected, disconnected } = self;
        group.handle_event(event, &mut |event| {
            record(&event, inbox, accepted, connected, disconnected);
        })
    }

    fn tick(&mut self, now: Instant) -> TickOutcome {
        let Self { group, inbox, accepted, connected, disconnected } = self;
        group.maintain(now, &mut |event| {
            record(&event, inbox, accepted, connected, disconnected);
        })
    }

    fn next_deadline(&self) -> Deadline {
        self.group.next_deadline()
    }
}

#[cfg(test)]
pub(crate) mod harness {
    use std::net::SocketAddr;

    use flux_network::stream::{ConnectionGroupConfig, Endpoint, StreamNetwork};

    /// Longest a test loop may pump before it reports what never happened.
    pub const TIMEOUT: std::time::Duration = std::time::Duration::from_secs(10);

    pub fn expired(started: std::time::Instant) -> bool {
        started.elapsed() > TIMEOUT
    }

    /// A length-prefixed group with the given name, defaults otherwise.
    pub fn framed_group(name: &'static str) -> ConnectionGroupConfig {
        ConnectionGroupConfig { name, ..ConnectionGroupConfig::default() }
    }

    /// A raw-framed group: what an `HttpService` requires, since HTTP frames
    /// its own messages.
    pub fn raw_group(name: &'static str) -> ConnectionGroupConfig {
        ConnectionGroupConfig {
            name,
            framing: flux_network::stream::Framing::Raw,
            ..ConnectionGroupConfig::default()
        }
    }

    /// An ephemeral localhost TCP listener endpoint for `listen`.
    pub fn ephemeral() -> Endpoint {
        Endpoint::Tcp((std::net::Ipv4Addr::LOCALHOST, 0).into())
    }

    pub fn bound_addr(endpoint: &Endpoint) -> SocketAddr {
        let Endpoint::Tcp(addr) = endpoint else { panic!("a TCP listener was bound") };
        *addr
    }

    /// One non-blocking network pass over one service.
    pub fn pass<S: flux_network::stream::Service>(net: &mut StreamNetwork, service: &mut S) {
        let _ = net.drive(Some(flux_timing::Duration::ZERO), std::slice::from_mut(service));
    }
}
