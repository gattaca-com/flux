//! The shredstream broadcaster and the bundle-api tile, folded into one repro.
//!
//! A length-prefixed listener fans a channel of batches out by broadcast,
//! unicasts the current filter to every client accepted in the same tile
//! iteration — the ordering the bundle-api tile depends on — and drops
//! batches on the floor while nobody is connected, as the shredstream
//! broadcaster does. The old `poll_with` closure becomes the recording leaf;
//! the sends that followed it in `loop_body` follow `drive` here.

use std::collections::VecDeque;

use flux_network::{Token, stream::StreamNetwork};
use flux_timing::Duration;

use crate::RecordingLeaf;

/// The tile state around the listener: who is connected, what the current
/// filter is, and what happened to every batch offered so far.
pub struct BroadcastTile {
    pub listener: RecordingLeaf,
    pub clients: Vec<Token>,
    pub filter: Vec<u8>,
    pub delivered: usize,
    pub dropped: usize,
}

impl BroadcastTile {
    pub fn new(listener: RecordingLeaf, filter: Vec<u8>) -> Self {
        Self { listener, clients: Vec::new(), filter, delivered: 0, dropped: 0 }
    }

    /// One tile iteration: the network pass, then the filter unicast to
    /// clients accepted in this very iteration, then the batch fan-out —
    /// broadcast to the connected, dropped for nobody.
    pub fn step(&mut self, net: &mut StreamNetwork, pending: &mut VecDeque<Vec<u8>>) {
        let _ = net.drive(Some(Duration::ZERO), std::slice::from_mut(&mut self.listener));

        for token in std::mem::take(&mut self.listener.accepted) {
            self.clients.push(token);
            let filter = &self.filter;
            self.listener.group_mut().send_with(token, |buf| buf.extend_from_slice(filter));
        }
        for token in std::mem::take(&mut self.listener.disconnected) {
            self.clients.retain(|client| *client != token);
        }

        while let Some(batch) = pending.pop_front() {
            if self.clients.is_empty() {
                self.dropped += 1;
            } else {
                self.listener.group_mut().broadcast_with(|buf| buf.extend_from_slice(&batch));
                self.delivered += 1;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::VecDeque;

    use flux_network::stream::{Endpoint, StreamNetwork};
    use flux_timing::Duration;

    use super::BroadcastTile;
    use crate::{
        RecordingLeaf,
        harness::{bound_addr, ephemeral, expired, framed_group},
    };

    #[test]
    fn filters_are_unicast_on_accept_and_batches_fan_out_or_drop() {
        let started = std::time::Instant::now();
        let mut server = StreamNetwork::default();
        let mut listener = RecordingLeaf::new(server.add_group(framed_group("builders")));
        let addr = bound_addr(&listener.group_mut().listen(ephemeral()).unwrap());
        let mut tile = BroadcastTile::new(listener, b"filter:v1".to_vec());
        let mut pending = VecDeque::new();

        // Nobody is connected: the batch is dropped, never buffered.
        pending.push_back(b"batch:lost".to_vec());
        tile.step(&mut server, &mut pending);
        assert_eq!((tile.dropped, tile.delivered), (1, 0), "a clientless batch is dropped");

        // The first client connects and must see the filter before anything
        // else; a batch offered after that reaches it.
        let mut peers = StreamNetwork::default();
        let mut client_a = RecordingLeaf::new(peers.add_group(framed_group("client-a")));
        let _ = client_a.group_mut().connect(Endpoint::Tcp(addr));
        while tile.clients.is_empty() {
            assert!(!expired(started), "the first client was never accepted");
            tile.step(&mut server, &mut pending);
            let _ = peers.drive(Some(Duration::ZERO), &mut [&mut client_a]);
        }
        pending.push_back(b"batch:1".to_vec());
        while client_a.inbox.len() < 2 {
            assert!(!expired(started), "client A never got the filter and the batch");
            tile.step(&mut server, &mut pending);
            let _ = peers.drive(Some(Duration::ZERO), &mut [&mut client_a]);
        }
        assert_eq!(client_a.inbox[0], b"filter:v1", "the filter precedes every batch");
        assert_eq!(client_a.inbox[1], b"batch:1");

        // A later client gets the current filter on the iteration it is
        // accepted, never the batch that predates it, and both clients get
        // the next batch.
        let mut client_b = RecordingLeaf::new(peers.add_group(framed_group("client-b")));
        let _ = client_b.group_mut().connect(Endpoint::Tcp(addr));
        while tile.clients.len() < 2 {
            assert!(!expired(started), "the second client was never accepted");
            tile.step(&mut server, &mut pending);
            let _ = peers.drive(Some(Duration::ZERO), &mut [&mut client_a, &mut client_b]);
        }
        pending.push_back(b"batch:2".to_vec());
        while client_b.inbox.len() < 2 || client_a.inbox.len() < 3 {
            assert!(!expired(started), "the second batch never fanned out");
            tile.step(&mut server, &mut pending);
            let _ = peers.drive(Some(Duration::ZERO), &mut [&mut client_a, &mut client_b]);
        }
        assert_eq!(client_b.inbox[0], b"filter:v1", "a late client still leads with the filter");
        assert_eq!(client_b.inbox[1], b"batch:2", "a late client never sees an earlier batch");
        assert_eq!(client_a.inbox[2], b"batch:2");
        assert_eq!((tile.dropped, tile.delivered), (1, 2));
    }
}
