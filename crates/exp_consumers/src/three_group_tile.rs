//! The builder's `NetworkTile`: three protocols on one network, one poll —
//! and, on the shipped leaf, one plain slice.
//!
//! Every group runs a `StreamService<Retained>`, so the slice is homogeneous
//! `[StreamService; 3]` and there is no enum and no hand-written `Service`
//! anywhere. What the original's `poll_with` closure dispatched by group is
//! now the tile pulling each service in turn; the per-token `MempoolFilter`
//! map, the pending-handshake list and the session set are plain tile state
//! keyed by token, where the originals keep them anyway.
//!
//! The behaviours a faithful repro must keep, and the test pins: a per-token
//! filter steering unicast fan-out next to a group-wide broadcast on the
//! bundle-api group; the relay handshake going out at the start of the
//! iteration *after* the accept, exactly as `send_handshakes` runs before
//! `handle_network` in the original loop; and the cross-group reaction — a
//! relay message becomes a bundle-api broadcast within the same iteration,
//! right after the poll.
//!
//! One simplification against the original: every bundle-api inbound is
//! treated as a filter install, folding out the `Tx` and bundle-order
//! variants whose ingest is tile state, not network behaviour.

use flux_network::{
    Token,
    stream::{StreamEvent, StreamNetwork, StreamService},
};
use flux_timing::Duration;

/// Slice positions of the three groups, so the tile reads as the original's
/// per-group dispatch arms.
const BUNDLE_API: usize = 0;
const SHRED: usize = 1;
const RELAY: usize = 2;

pub const HANDSHAKE: &[u8] = b"handshake:v2";

/// The tile: three shipped services and the state their events feed.
pub struct NetworkTile {
    pub services: [StreamService; 3],
    /// The installed filters: token to the address bytes it subscribed to.
    pub filters: Vec<(Token, Vec<u8>)>,
    /// Relay sessions accepted but not yet handshaked: staged for the start
    /// of the next iteration.
    pub pending_handshakes: Vec<Token>,
    pub sessions: Vec<Token>,
    /// Shredstream batches seen, the `mark_work` stand-in.
    pub batches: usize,
}

impl NetworkTile {
    pub fn new(services: [StreamService; 3]) -> Self {
        Self {
            services,
            filters: Vec::new(),
            pending_handshakes: Vec::new(),
            sessions: Vec::new(),
            batches: 0,
        }
    }

    pub fn bundle_api_mut(&mut self) -> &mut StreamService {
        &mut self.services[BUNDLE_API]
    }

    pub fn relay_mut(&mut self) -> &mut StreamService {
        &mut self.services[RELAY]
    }

    /// Unicasts `payload` to every endpoint whose filter contains `addr`,
    /// returning how many sends went out — `forward_mempool_order`.
    pub fn forward_order(&mut self, addr: u8, payload: &[u8]) -> usize {
        let bundle_api = &mut self.services[BUNDLE_API];
        self.filters
            .iter()
            .filter(|(_, filter)| filter.contains(&addr))
            .filter(|(token, _)| bundle_api.send_with(*token, |buf| buf.extend_from_slice(payload)))
            .count()
    }

    /// One `loop_body`: staged handshakes first, then the network pass, then
    /// the pulls — bundle-api installs filters, the relay stages handshakes
    /// and accumulates choices, shredstream counts — and finally the
    /// cross-group reaction, every relayed choice broadcast to the bundle-api
    /// group before the iteration ends.
    pub fn step(&mut self, net: &mut StreamNetwork) {
        for token in self.pending_handshakes.drain(..) {
            self.services[RELAY].send_with(token, |buf| buf.extend_from_slice(HANDSHAKE));
        }

        let _ = net.drive(Some(Duration::ZERO), &mut self.services);

        while let Some(event) = self.services[BUNDLE_API].next_event() {
            match event {
                StreamEvent::Message { token, payload, .. } => {
                    // The first message installs the endpoint's filter, a
                    // later one replaces it, as MempoolFilter does.
                    if let Some((_, filter)) =
                        self.filters.iter_mut().find(|(owner, _)| *owner == token)
                    {
                        filter.clear();
                        filter.extend_from_slice(payload);
                    } else {
                        self.filters.push((token, payload.to_vec()));
                    }
                }
                StreamEvent::Disconnected { token, .. } => {
                    self.filters.retain(|(owner, _)| *owner != token);
                }
                StreamEvent::Connected { .. } => {}
                StreamEvent::Accepted { .. } => unreachable!("this group only dials out"),
            }
        }

        while let Some(event) = self.services[SHRED].next_event() {
            if let StreamEvent::Message { .. } = event {
                self.batches += 1;
            }
        }

        let mut chosen: Vec<Vec<u8>> = Vec::new();
        while let Some(event) = self.services[RELAY].next_event() {
            match event {
                StreamEvent::Accepted { token, .. } => {
                    self.pending_handshakes.push(token);
                    self.sessions.push(token);
                }
                StreamEvent::Message { payload, .. } => chosen.push(payload.to_vec()),
                StreamEvent::Disconnected { token, .. } => {
                    self.pending_handshakes.retain(|session| *session != token);
                    self.sessions.retain(|session| *session != token);
                }
                StreamEvent::Connected { .. } => unreachable!("this group only listens"),
            }
        }
        for choice in chosen {
            let _ = self.services[BUNDLE_API].broadcast_with(|buf| {
                buf.extend_from_slice(&choice);
            });
        }
    }
}

#[cfg(test)]
mod tests {
    use flux_network::stream::{Endpoint, StreamEvent, StreamNetwork, StreamService};
    use flux_timing::Duration;

    use super::{HANDSHAKE, NetworkTile};
    use crate::harness::{bound_addr, ephemeral, expired, framed_group};

    fn drain(peer: &mut StreamService, inbox: &mut Vec<Vec<u8>>) {
        while let Some(event) = peer.next_event() {
            if let StreamEvent::Message { payload, .. } = event {
                inbox.push(payload.to_vec());
            }
        }
    }

    /// One pass of an api peer: a single pull handles everything it sees —
    /// accepts answered with `filter`, messages collected — because a pull
    /// consumes; two loops over one service would swallow each other's events.
    fn serve_api(peer: &mut StreamService, filter: &[u8], inbox: &mut Vec<Vec<u8>>) {
        let mut accepted = Vec::new();
        while let Some(event) = peer.next_event() {
            match event {
                StreamEvent::Accepted { token, .. } => accepted.push(token),
                StreamEvent::Message { payload, .. } => inbox.push(payload.to_vec()),
                StreamEvent::Connected { .. } | StreamEvent::Disconnected { .. } => {}
            }
        }
        for token in accepted {
            let _ = peer.send_with(token, |buf| buf.extend_from_slice(filter));
        }
    }

    #[test]
    fn three_protocols_share_one_poll_with_their_own_rules() {
        let started = std::time::Instant::now();

        // The peers: two bundle-api servers, one shredstream feed, one relay
        // client, all on one peer network — all shipped services too.
        let mut peer_net = StreamNetwork::default();
        let mut api_a = StreamService::new(peer_net.add_group(framed_group("api-a")));
        let first_api = bound_addr(&api_a.listen(ephemeral()).unwrap());
        let mut api_b = StreamService::new(peer_net.add_group(framed_group("api-b")));
        let second_api = bound_addr(&api_b.listen(ephemeral()).unwrap());
        let mut feed = StreamService::new(peer_net.add_group(framed_group("feed")));
        let feed_addr = bound_addr(&feed.listen(ephemeral()).unwrap());
        let mut relay_client = StreamService::new(peer_net.add_group(framed_group("relay-client")));

        // The tile: three groups on one network, one homogeneous slice.
        let mut tile_net = StreamNetwork::default();
        let mut bundle_api = StreamService::new(tile_net.add_group(framed_group("bundle-api")));
        let _ = bundle_api.connect(Endpoint::Tcp(first_api));
        let _ = bundle_api.connect(Endpoint::Tcp(second_api));
        let mut shred = StreamService::new(tile_net.add_group(framed_group("shredstream")));
        let _ = shred.connect(Endpoint::Tcp(feed_addr));
        let mut relay = StreamService::new(tile_net.add_group(framed_group("relay")));
        let relay_addr = bound_addr(&relay.listen(ephemeral()).unwrap());
        let _ = relay_client.connect(Endpoint::Tcp(relay_addr));
        let mut tile = NetworkTile::new([bundle_api, shred, relay]);

        // One pass of the peer network: drive, then each api peer answers
        // its accepts with its filter and banks its messages.
        let (mut inbox_a, mut inbox_b) = (Vec::new(), Vec::new());
        macro_rules! peers_pass {
            () => {
                let _ = peer_net.drive(Some(Duration::ZERO), &mut [
                    &mut api_a,
                    &mut api_b,
                    &mut feed,
                    &mut relay_client,
                ]);
                serve_api(&mut api_a, b"\x0a", &mut inbox_a);
                serve_api(&mut api_b, b"\x0b", &mut inbox_b);
            };
        }

        // Each bundle-api peer installs its filter on the connection it
        // accepted; the relay client dials and waits.
        while tile.filters.len() < 2 {
            assert!(!expired(started), "the filters were never installed");
            tile.step(&mut tile_net);
            peers_pass!();
        }

        // A resolved order for address 0x0a reaches only the peer whose
        // filter holds it; a chosen mini-block reaches both.
        assert_eq!(tile.forward_order(0x0a, b"order:1"), 1);
        assert_eq!(tile.bundle_api_mut().broadcast_with(|buf| buf.extend(b"chosen:1")), 2);
        while inbox_a.len() < 2 || inbox_b.is_empty() {
            assert!(!expired(started), "the fan-out never arrived");
            tile.step(&mut tile_net);
            peers_pass!();
        }
        assert_eq!(inbox_a, [b"order:1".to_vec(), b"chosen:1".to_vec()]);
        assert_eq!(inbox_b, [b"chosen:1".to_vec()], "the order skipped the other filter");

        // The relay handshake goes out at the start of the iteration after
        // the accept, and the shredstream feed flows next to all of it.
        feed.broadcast_with(|buf| buf.extend_from_slice(b"batch:1"));
        let mut relay_inbox = Vec::new();
        while relay_inbox.is_empty() || tile.batches == 0 {
            assert!(!expired(started), "the handshake or the batch never arrived");
            tile.step(&mut tile_net);
            peers_pass!();
            drain(&mut relay_client, &mut relay_inbox);
        }
        assert_eq!(relay_inbox[0], HANDSHAKE);
        assert_eq!(tile.sessions.len(), 1);
        assert_eq!(tile.batches, 1);

        // The cross-group reaction: a relay message becomes a bundle-api
        // broadcast inside step itself.
        relay_client.broadcast_with(|buf| buf.extend_from_slice(b"chosen:9"));
        while inbox_a.len() < 3 || inbox_b.len() < 2 {
            assert!(!expired(started), "the relayed choice never reached the bundle-api peers");
            tile.step(&mut tile_net);
            peers_pass!();
        }
        assert_eq!(inbox_a[2], b"chosen:9");
        assert_eq!(inbox_b[1], b"chosen:9");
    }
}
