//! The shredstream broadcaster and the bundle-api tile, folded into one repro.
//!
//! A length-prefixed listener fans a channel of batches out by broadcast,
//! unicasts the current filter to every client accepted in the same tile
//! iteration — the ordering the bundle-api tile depends on — and drops
//! batches on the floor while nobody is connected, as the shredstream
//! broadcaster does. The leaf is `StreamService`; everything here is the tile
//! around it.

use std::collections::VecDeque;

use flux_network::{
    Token,
    stream::{StreamEvent, StreamNetwork, StreamService},
};
use flux_timing::Duration;

/// The tile state around the listener: who is connected, what the current
/// filter is, and what happened to every batch offered so far.
pub struct BroadcastTile {
    pub listener: StreamService,
    pub clients: Vec<Token>,
    pub filter: Vec<u8>,
    pub delivered: usize,
    pub dropped: usize,
}

impl BroadcastTile {
    pub fn new(listener: StreamService, filter: Vec<u8>) -> Self {
        Self { listener, clients: Vec::new(), filter, delivered: 0, dropped: 0 }
    }

    /// One tile iteration: the network pass, the pull, then the filter
    /// unicast to clients accepted in this very iteration, then the batch
    /// fan-out — broadcast to the connected, dropped for nobody.
    pub fn step(&mut self, net: &mut StreamNetwork, pending: &mut VecDeque<Vec<u8>>) {
        let _ = net.drive(Some(Duration::ZERO), std::slice::from_mut(&mut self.listener));

        // The pull borrows the service, so sends are staged and go out after.
        let mut accepted = Vec::new();
        while let Some(event) = self.listener.next_event() {
            match event {
                StreamEvent::Accepted { token, .. } => accepted.push(token),
                StreamEvent::Disconnected { token, .. } => {
                    self.clients.retain(|client| *client != token);
                }
                // The feed is one-directional: inbound is ignored, as the
                // original warns and drops.
                StreamEvent::Message { .. } => {}
                StreamEvent::Connected { .. } => unreachable!("this group only listens"),
            }
        }
        for token in accepted {
            self.clients.push(token);
            let filter = &self.filter;
            self.listener.send_with(token, |buf| buf.extend_from_slice(filter));
        }

        while let Some(batch) = pending.pop_front() {
            if self.clients.is_empty() {
                self.dropped += 1;
            } else {
                self.listener.broadcast_with(|buf| buf.extend_from_slice(&batch));
                self.delivered += 1;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::VecDeque;

    use flux_network::stream::{Endpoint, StreamEvent, StreamNetwork, StreamService};
    use flux_timing::Duration;

    use super::BroadcastTile;
    use crate::harness::{bound_addr, ephemeral, expired, framed_group};

    /// Drains a client service into (inbox, connected count).
    fn drain(client: &mut StreamService, inbox: &mut Vec<Vec<u8>>) {
        while let Some(event) = client.next_event() {
            if let StreamEvent::Message { payload, .. } = event {
                inbox.push(payload.to_vec());
            }
        }
    }

    #[test]
    fn filters_are_unicast_on_accept_and_batches_fan_out_or_drop() {
        let started = std::time::Instant::now();
        let mut server = StreamNetwork::default();
        let mut listener = StreamService::new(server.add_group(framed_group("builders")));
        let addr = bound_addr(&listener.listen(ephemeral()).unwrap());
        let mut tile = BroadcastTile::new(listener, b"filter:v1".to_vec());
        let mut pending = VecDeque::new();

        // Nobody is connected: the batch is dropped, never buffered.
        pending.push_back(b"batch:lost".to_vec());
        tile.step(&mut server, &mut pending);
        assert_eq!((tile.dropped, tile.delivered), (1, 0), "a clientless batch is dropped");

        // The first client connects and must see the filter before anything
        // else; a batch offered after that reaches it.
        let mut peers = StreamNetwork::default();
        let mut client_a = StreamService::new(peers.add_group(framed_group("client-a")));
        let _ = client_a.connect(Endpoint::Tcp(addr));
        let mut inbox_a = Vec::new();
        while tile.clients.is_empty() {
            assert!(!expired(started), "the first client was never accepted");
            tile.step(&mut server, &mut pending);
            let _ = peers.drive(Some(Duration::ZERO), &mut [&mut client_a]);
        }
        pending.push_back(b"batch:1".to_vec());
        while inbox_a.len() < 2 {
            assert!(!expired(started), "client A never got the filter and the batch");
            tile.step(&mut server, &mut pending);
            let _ = peers.drive(Some(Duration::ZERO), &mut [&mut client_a]);
            drain(&mut client_a, &mut inbox_a);
        }
        assert_eq!(inbox_a[0], b"filter:v1", "the filter precedes every batch");
        assert_eq!(inbox_a[1], b"batch:1");

        // A later client gets the current filter on the iteration it is
        // accepted, never the batch that predates it, and both clients get
        // the next batch.
        let mut client_b = StreamService::new(peers.add_group(framed_group("client-b")));
        let _ = client_b.connect(Endpoint::Tcp(addr));
        let mut inbox_b = Vec::new();
        while tile.clients.len() < 2 {
            assert!(!expired(started), "the second client was never accepted");
            tile.step(&mut server, &mut pending);
            let _ = peers.drive(Some(Duration::ZERO), &mut [&mut client_a, &mut client_b]);
        }
        pending.push_back(b"batch:2".to_vec());
        while inbox_b.len() < 2 || inbox_a.len() < 3 {
            assert!(!expired(started), "the second batch never fanned out");
            tile.step(&mut server, &mut pending);
            let _ = peers.drive(Some(Duration::ZERO), &mut [&mut client_a, &mut client_b]);
            drain(&mut client_a, &mut inbox_a);
            drain(&mut client_b, &mut inbox_b);
        }
        assert_eq!(inbox_b[0], b"filter:v1", "a late client still leads with the filter");
        assert_eq!(inbox_b[1], b"batch:2", "a late client never sees an earlier batch");
        assert_eq!(inbox_a[2], b"batch:2");
        assert_eq!((tile.dropped, tile.delivered), (1, 2));
    }
}
