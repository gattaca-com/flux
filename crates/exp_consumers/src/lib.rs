//! Functional repros of how solana-builder and builder use the flux `main`
//! network stack, re-expressed on this branch's ownership model.
//!
//! Each module is one repro from the consumer brief, minimal but runnable:
//! the shape a real tile takes, with a test that pins the behaviour the
//! original relies on. One `poll_with` of the old stack maps to one tile
//! iteration here: sends staged from the previous iteration go out first,
//! then `drive(Some(Duration::ZERO), ..)` runs the non-blocking network pass,
//! then the tile pulls what its services recorded and acts on it.
//! Serialization is out of network scope, so raw byte payloads stand in for
//! the original wincode messages throughout.
//!
//! No module implements `Service`: the leaf is flux's `StreamService` (or
//! `HttpService` for the two HTTP users), and everything here is tile logic —
//! token maps, staged sends, round-robin cursors — over the shipped pull.
//! The shredstream subscriber also demonstrates the sink mode: a
//! `StreamSink` moved into the service, receiving payloads zero-copy during
//! the iteration.
//!
//! - [`broadcast_listener`]: a length-prefixed listener fanning a channel out
//!   by broadcast, unicasting the current filter to clients accepted in the
//!   same iteration, and dropping batches while nobody is connected
//!   (shredstream broadcaster + bundle-api tile).
//! - [`subscriber`]: one persistent keepalive endpoint, a blocking idle wait as
//!   the loop's only pacing, reconnect proven end to end, in both the pulled
//!   and the sink mode (analytics shredstream subscriber).
//! - [`three_group_tile`]: three `StreamService`s on one network in a plain
//!   homogeneous slice — per-token filtered unicast, group broadcast, and an
//!   accept-then-handshake-next-iteration protocol, all as tile state (the
//!   builder's `NetworkTile`).
//! - [`http_by_token`]: an `HttpService` server answering strictly on a later
//!   iteration by token, behind a generation guard (overseer `POST /sql`).
//! - [`http_client_pool`]: an `HttpService` client pool with bounded in-flight
//!   requests, a deadline, and persistent endpoints that are never removed,
//!   healing through reconnect (rpc simulation pool).
//! - [`raw_backpressure`]: a raw-framed server with a hard backlog cap, a
//!   per-connection input and output cap, round-robin processing and both close
//!   flavours (overseer `PostgreSQL` server).

pub mod broadcast_listener;
pub mod http_by_token;
pub mod http_client_pool;
pub mod raw_backpressure;
pub mod subscriber;
pub mod three_group_tile;

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
