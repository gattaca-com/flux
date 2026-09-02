//! The builder's `NetworkTile`: three protocols on one network, one poll.
//!
//! The original's single `poll_with` closure dispatches on the event's group;
//! here each group is a leaf service and the dispatch is the caller-owned
//! enum the scheduler idiom asks for. The behaviours a faithful repro must
//! keep, and the tests pin: a per-token `MempoolFilter` steering unicast
//! fan-out next to a group-wide broadcast on the bundle-api group, the
//! relay handshake going out at the start of the iteration *after* the
//! accept, exactly as `send_handshakes` runs before `handle_network` in the
//! original loop, and the cross-group reaction — a relay message becomes a
//! bundle-api broadcast within the same iteration, right after the poll.
//!
//! One simplification against the original: every bundle-api inbound is
//! treated as a filter install, folding out the `Tx` and bundle-order
//! variants whose ingest is tile state, not network behaviour.

use flux_network::{
    Token,
    stream::{
        ConnectionGroup, ConnectionGroupId, Deadline, ReadinessOutcome, Service, StreamEvent,
        StreamNetwork, TickOutcome,
    },
};
use flux_timing::{Duration, Instant};
use mio::event::Event;

use crate::RecordingLeaf;

/// The bundle-api leaf: N persistent outbound endpoints, each installing a
/// filter with its first message and replacing it with every later one.
pub struct BundleApiService {
    group: ConnectionGroup,
    /// The installed filters: token to the address bytes it subscribed to.
    pub filters: Vec<(Token, Vec<u8>)>,
    pub connected: Vec<Token>,
}

impl BundleApiService {
    pub fn new(group: ConnectionGroup) -> Self {
        Self { group, filters: Vec::new(), connected: Vec::new() }
    }

    pub fn group_mut(&mut self) -> &mut ConnectionGroup {
        &mut self.group
    }

    /// Unicasts `payload` to every endpoint whose filter contains `addr`,
    /// returning how many sends went out — `forward_mempool_order`.
    pub fn forward_order(&mut self, addr: u8, payload: &[u8]) -> usize {
        let Self { group, filters, .. } = self;
        filters
            .iter()
            .filter(|(_, filter)| filter.contains(&addr))
            .filter(|(token, _)| group.send_with(*token, |buf| buf.extend_from_slice(payload)))
            .count()
    }

    /// Broadcasts a chosen mini-block to every endpoint —
    /// `forward_chosen_miniblock`.
    pub fn forward_chosen(&mut self, payload: &[u8]) -> usize {
        self.group.broadcast_with(|buf| buf.extend_from_slice(payload))
    }

    fn record(
        event: &StreamEvent<'_>,
        filters: &mut Vec<(Token, Vec<u8>)>,
        connected: &mut Vec<Token>,
    ) {
        match event {
            StreamEvent::Connected { token, .. } => connected.push(*token),
            StreamEvent::Message { token, payload, .. } => {
                // The first message installs the endpoint's filter, a later
                // one replaces it, as MempoolFilter does.
                if let Some((_, filter)) = filters.iter_mut().find(|(owner, _)| owner == token) {
                    filter.clear();
                    filter.extend_from_slice(payload);
                } else {
                    filters.push((*token, payload.to_vec()));
                }
            }
            StreamEvent::Disconnected { token, .. } => {
                filters.retain(|(owner, _)| owner != token);
            }
            StreamEvent::Accepted { .. } => unreachable!("this group only dials out"),
        }
    }
}

impl Service for BundleApiService {
    fn group_id(&self) -> &ConnectionGroupId {
        self.group.group_id()
    }

    fn handle_event(&mut self, event: &Event) -> ReadinessOutcome {
        let Self { group, filters, connected } = self;
        group.handle_event(event, &mut |event| Self::record(&event, filters, connected))
    }

    fn tick(&mut self, now: Instant) -> TickOutcome {
        let Self { group, filters, connected } = self;
        group.maintain(now, &mut |event| Self::record(&event, filters, connected))
    }

    fn next_deadline(&self) -> Deadline {
        self.group.next_deadline()
    }
}

/// The relay leaf: a listener whose handshake goes out on the iteration
/// after the accept, from a pending list the tile flushes first.
pub struct RelayService {
    group: ConnectionGroup,
    pub pending_handshakes: Vec<Token>,
    pub sessions: Vec<Token>,
    pub inbox: Vec<(Token, Vec<u8>)>,
}

impl RelayService {
    pub const HANDSHAKE: &'static [u8] = b"handshake:v2";

    pub fn new(group: ConnectionGroup) -> Self {
        Self { group, pending_handshakes: Vec::new(), sessions: Vec::new(), inbox: Vec::new() }
    }

    pub fn group_mut(&mut self) -> &mut ConnectionGroup {
        &mut self.group
    }

    /// Sends the handshake to every session accepted since the last call:
    /// the first thing the tile does, so an accept recorded during one
    /// iteration's network pass handshakes at the start of the next.
    pub fn send_handshakes(&mut self) -> usize {
        let Self { group, pending_handshakes, .. } = self;
        pending_handshakes
            .drain(..)
            .filter(|token| group.send_with(*token, |buf| buf.extend_from_slice(Self::HANDSHAKE)))
            .count()
    }

    fn record(
        event: &StreamEvent<'_>,
        pending: &mut Vec<Token>,
        sessions: &mut Vec<Token>,
        inbox: &mut Vec<(Token, Vec<u8>)>,
    ) {
        match event {
            StreamEvent::Accepted { token, .. } => {
                pending.push(*token);
                sessions.push(*token);
            }
            StreamEvent::Message { token, payload, .. } => inbox.push((*token, payload.to_vec())),
            StreamEvent::Disconnected { token, .. } => {
                pending.retain(|session| session != token);
                sessions.retain(|session| session != token);
            }
            StreamEvent::Connected { .. } => unreachable!("this group only listens"),
        }
    }
}

impl Service for RelayService {
    fn group_id(&self) -> &ConnectionGroupId {
        self.group.group_id()
    }

    fn handle_event(&mut self, event: &Event) -> ReadinessOutcome {
        let Self { group, pending_handshakes, sessions, inbox } = self;
        group.handle_event(event, &mut |event| {
            Self::record(&event, pending_handshakes, sessions, inbox);
        })
    }

    fn tick(&mut self, now: Instant) -> TickOutcome {
        let Self { group, pending_handshakes, sessions, inbox } = self;
        group.maintain(now, &mut |event| {
            Self::record(&event, pending_handshakes, sessions, inbox);
        })
    }

    fn next_deadline(&self) -> Deadline {
        self.group.next_deadline()
    }
}

/// The tile's three heterogeneous services, dispatched by exhaustive match:
/// the caller-owned enum the scheduler schedules as one slice.
pub enum TileService {
    BundleApi(BundleApiService),
    Shred(RecordingLeaf),
    Relay(RelayService),
}

impl Service for TileService {
    fn group_id(&self) -> &ConnectionGroupId {
        match self {
            Self::BundleApi(service) => service.group_id(),
            Self::Shred(service) => service.group_id(),
            Self::Relay(service) => service.group_id(),
        }
    }

    fn handle_event(&mut self, event: &Event) -> ReadinessOutcome {
        match self {
            Self::BundleApi(service) => service.handle_event(event),
            Self::Shred(service) => service.handle_event(event),
            Self::Relay(service) => service.handle_event(event),
        }
    }

    fn tick(&mut self, now: Instant) -> TickOutcome {
        match self {
            Self::BundleApi(service) => service.tick(now),
            Self::Shred(service) => service.tick(now),
            Self::Relay(service) => service.tick(now),
        }
    }

    fn next_deadline(&self) -> Deadline {
        match self {
            Self::BundleApi(service) => service.next_deadline(),
            Self::Shred(service) => service.next_deadline(),
            Self::Relay(service) => service.next_deadline(),
        }
    }
}

/// The `NetworkTile`'s loop, minimally: handshakes staged from the previous
/// iteration go out first, then the network pass runs all three services.
pub struct NetworkTile {
    pub services: [TileService; 3],
}

impl NetworkTile {
    /// One `loop_body`: `send_handshakes` before the network pass, exactly
    /// as the original orders them, then the cross-group reaction the
    /// original does right after its poll — every relay message recorded
    /// this iteration goes out as a bundle-api broadcast before the
    /// iteration ends.
    pub fn step(&mut self, net: &mut StreamNetwork) {
        self.relay_mut().send_handshakes();
        let _ = net.drive(Some(Duration::ZERO), &mut self.services);

        let [bundle_api, _, relay] = &mut self.services;
        let (TileService::BundleApi(bundle_api), TileService::Relay(relay)) = (bundle_api, relay)
        else {
            unreachable!("slots 0 and 2 hold the bundle-api and relay services")
        };
        for (_, chosen) in relay.inbox.drain(..) {
            let _ = bundle_api.forward_chosen(&chosen);
        }
    }

    pub fn bundle_api_mut(&mut self) -> &mut BundleApiService {
        match &mut self.services[0] {
            TileService::BundleApi(service) => service,
            _ => unreachable!("slot 0 holds the bundle-api service"),
        }
    }

    pub fn shred_mut(&mut self) -> &mut RecordingLeaf {
        match &mut self.services[1] {
            TileService::Shred(service) => service,
            _ => unreachable!("slot 1 holds the shredstream service"),
        }
    }

    pub fn relay_mut(&mut self) -> &mut RelayService {
        match &mut self.services[2] {
            TileService::Relay(service) => service,
            _ => unreachable!("slot 2 holds the relay service"),
        }
    }
}

#[cfg(test)]
mod tests {
    use flux_network::stream::{Endpoint, StreamNetwork};
    use flux_timing::Duration;

    use super::{BundleApiService, NetworkTile, RelayService, TileService};
    use crate::{
        RecordingLeaf,
        harness::{bound_addr, ephemeral, expired, framed_group},
    };

    #[test]
    fn three_protocols_share_one_poll_with_their_own_rules() {
        let started = std::time::Instant::now();

        // The peers: two bundle-api servers, one shredstream feed, and — for
        // the relay side — a client dialing in, all on one peer network.
        let mut peer_net = StreamNetwork::default();
        let mut api_a = RecordingLeaf::new(peer_net.add_group(framed_group("api-a")));
        let first_api = bound_addr(&api_a.group_mut().listen(ephemeral()).unwrap());
        let mut api_b = RecordingLeaf::new(peer_net.add_group(framed_group("api-b")));
        let second_api = bound_addr(&api_b.group_mut().listen(ephemeral()).unwrap());
        let mut feed = RecordingLeaf::new(peer_net.add_group(framed_group("feed")));
        let feed_addr = bound_addr(&feed.group_mut().listen(ephemeral()).unwrap());
        let mut relay_client = RecordingLeaf::new(peer_net.add_group(framed_group("relay-client")));

        // The tile: three groups on one network, one drive.
        let mut tile_net = StreamNetwork::default();
        let mut bundle_api = BundleApiService::new(tile_net.add_group(framed_group("bundle-api")));
        let _ = bundle_api.group_mut().connect(Endpoint::Tcp(first_api));
        let _ = bundle_api.group_mut().connect(Endpoint::Tcp(second_api));
        let mut shred = RecordingLeaf::new(tile_net.add_group(framed_group("shredstream")));
        let _ = shred.group_mut().connect(Endpoint::Tcp(feed_addr));
        let mut relay = RelayService::new(tile_net.add_group(framed_group("relay")));
        let relay_addr = bound_addr(&relay.group_mut().listen(ephemeral()).unwrap());
        let _ = relay_client.group_mut().connect(Endpoint::Tcp(relay_addr));
        let mut tile = NetworkTile {
            services: [
                TileService::BundleApi(bundle_api),
                TileService::Shred(shred),
                TileService::Relay(relay),
            ],
        };

        // Each bundle-api peer installs its filter on the connection it
        // accepted; the relay client just dials and waits.
        let filters_installed = |tile: &mut NetworkTile| tile.bundle_api_mut().filters.len() == 2;
        while !filters_installed(&mut tile) {
            assert!(!expired(started), "the filters were never installed");
            tile.step(&mut tile_net);
            let _ = peer_net.drive(Some(Duration::ZERO), &mut [
                &mut api_a,
                &mut api_b,
                &mut feed,
                &mut relay_client,
            ]);
            for (peer, filter) in [(&mut api_a, b"\x0a"), (&mut api_b, b"\x0b")] {
                for token in std::mem::take(&mut peer.accepted) {
                    let _ = peer.group_mut().send_with(token, |buf| buf.extend_from_slice(filter));
                }
            }
        }

        // A resolved order for address 0x0a reaches only the peer whose
        // filter holds it; a chosen mini-block reaches both.
        assert_eq!(tile.bundle_api_mut().forward_order(0x0a, b"order:1"), 1);
        assert_eq!(tile.bundle_api_mut().forward_chosen(b"chosen:1"), 2);
        while api_a.inbox.len() < 2 || api_b.inbox.is_empty() {
            assert!(!expired(started), "the fan-out never arrived");
            tile.step(&mut tile_net);
            let _ = peer_net.drive(Some(Duration::ZERO), &mut [
                &mut api_a,
                &mut api_b,
                &mut feed,
                &mut relay_client,
            ]);
        }
        assert_eq!(api_a.inbox, [b"order:1".to_vec(), b"chosen:1".to_vec()]);
        assert_eq!(api_b.inbox, [b"chosen:1".to_vec()], "the order skipped the other filter");

        // The relay handshake goes out at the start of the iteration after
        // the accept, and the shredstream feed flows next to all of it.
        feed.group_mut().broadcast_with(|buf| buf.extend_from_slice(b"batch:1"));
        while relay_client.inbox.is_empty() || tile.shred_mut().inbox.is_empty() {
            assert!(!expired(started), "the handshake or the batch never arrived");
            tile.step(&mut tile_net);
            let _ = peer_net.drive(Some(Duration::ZERO), &mut [
                &mut api_a,
                &mut api_b,
                &mut feed,
                &mut relay_client,
            ]);
        }
        assert_eq!(relay_client.inbox[0], super::RelayService::HANDSHAKE);
        assert_eq!(tile.shred_mut().inbox[0], b"batch:1");
        assert_eq!(tile.relay_mut().sessions.len(), 1);

        // The cross-group reaction: a relay message becomes a bundle-api
        // broadcast inside step itself — the inbox is consumed by the same
        // iteration that recorded it, never left for the tile to ferry.
        relay_client.group_mut().broadcast_with(|buf| buf.extend_from_slice(b"chosen:9"));
        while api_a.inbox.len() < 3 || api_b.inbox.len() < 2 {
            assert!(!expired(started), "the relayed choice never reached the bundle-api peers");
            tile.step(&mut tile_net);
            let _ = peer_net.drive(Some(Duration::ZERO), &mut [
                &mut api_a,
                &mut api_b,
                &mut feed,
                &mut relay_client,
            ]);
        }
        assert_eq!(api_a.inbox[2], b"chosen:9");
        assert_eq!(api_b.inbox[1], b"chosen:9");
        assert!(tile.relay_mut().inbox.is_empty(), "step consumed the relay inbox");
    }
}
