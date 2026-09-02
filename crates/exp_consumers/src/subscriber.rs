//! The analytics shredstream subscriber: one persistent keepalive endpoint in
//! a blocking loop, shown in both consumption modes.
//!
//! The original runs in a `spawn_blocking` closure — `poll_with`, forward
//! each batch, sleep a millisecond when nothing arrived. Here the blocking
//! `drive(Some(1 ms))` is the whole loop body: it parks for up to the
//! millisecond only when the transport is idle, wakes on readiness, and runs
//! the reconnect schedule from inside the same call. The endpoint is
//! persistent: nobody removes it, and after the server drops the connection
//! the group reconnects it and the batches resume.
//!
//! The pulled mode is the default shape. The sink mode is the original's
//! actual data path — deserialize and forward to a channel, consuming the
//! payload where it is lent, with no copy into retained storage — and is the
//! zero-copy mode the latency-sensitive consumers asked for.

use std::sync::mpsc;

use flux_network::stream::{StreamEvent, StreamSink};

/// The sink the original's closure body becomes.
///
/// Every batch is forwarded to a channel as it arrives, nothing is held.
/// Deserialization stands in as a copy into the message the channel owns,
/// exactly like `tx.blocking_send(..)`.
pub struct ChannelSink {
    pub sender: mpsc::Sender<Vec<u8>>,
    pub connects: usize,
}

impl StreamSink for ChannelSink {
    fn on_event(&mut self, event: StreamEvent<'_>) {
        match event {
            StreamEvent::Message { payload, .. } => {
                // A closed receiver means the task is shutting down; the
                // original stops its loop, the sink just drops the batch.
                let _ = self.sender.send(payload.to_vec());
            }
            StreamEvent::Connected { .. } => self.connects += 1,
            StreamEvent::Disconnected { .. } | StreamEvent::Accepted { .. } => {}
        }
    }

    // Everything is forwarded inside on_event: nothing is held.
    fn has_pending(&self) -> bool {
        false
    }
}

#[cfg(test)]
mod tests {
    use std::sync::mpsc;

    use flux_network::stream::{
        ConnectionGroupConfig, Endpoint, StreamEvent, StreamNetwork, StreamService, TcpOptions,
    };
    use flux_timing::Duration;

    use super::ChannelSink;
    use crate::harness::{bound_addr, ephemeral, expired, framed_group, pass};

    /// The subscriber's group: keepalive on and a short retry, as the
    /// analytics loop configures its one endpoint.
    fn subscriber_group() -> ConnectionGroupConfig {
        ConnectionGroupConfig {
            name: "subscriber",
            reconnect_interval: Duration::from_millis(25),
            tcp: TcpOptions { keepalive: true, ..TcpOptions::default() },
            ..ConnectionGroupConfig::default()
        }
    }

    /// The feed side: a listener that can broadcast and drop its client.
    fn feed() -> (StreamNetwork, StreamService, std::net::SocketAddr) {
        let mut net = StreamNetwork::default();
        let mut service = StreamService::new(net.add_group(framed_group("shredstream")));
        let addr = bound_addr(&service.listen(ephemeral()).unwrap());
        (net, service, addr)
    }

    /// Pulls the feed service, returning the first accepted token seen.
    fn feed_accepted(service: &mut StreamService) -> Option<flux_network::Token> {
        let mut accepted = None;
        while let Some(event) = service.next_event() {
            if let StreamEvent::Accepted { token, .. } = event {
                accepted.get_or_insert(token);
            }
        }
        accepted
    }

    #[test]
    fn a_persistent_endpoint_survives_the_server_dropping_it() {
        let started = std::time::Instant::now();
        let (mut server, mut feed, addr) = feed();

        let mut client = StreamNetwork::default();
        let mut subscriber = StreamService::new(client.add_group(subscriber_group()));
        let _ = subscriber.connect(Endpoint::Tcp(addr));
        let (mut inbox, mut connects, mut drops) = (Vec::<Vec<u8>>::new(), 0, 0);
        let drain = |subscriber: &mut StreamService,
                     inbox: &mut Vec<Vec<u8>>,
                     connects: &mut usize,
                     drops: &mut usize| {
            while let Some(event) = subscriber.next_event() {
                match event {
                    StreamEvent::Message { payload, .. } => inbox.push(payload.to_vec()),
                    StreamEvent::Connected { .. } => *connects += 1,
                    StreamEvent::Disconnected { .. } => *drops += 1,
                    StreamEvent::Accepted { .. } => unreachable!("this group only dials"),
                }
            }
        };

        // First connection: a batch flows.
        let mut first_token = None;
        while first_token.is_none() {
            assert!(!expired(started), "the subscriber never connected");
            let _ = client.drive(Some(Duration::from_millis(1)), &mut [&mut subscriber]);
            pass(&mut server, &mut feed);
            first_token = feed_accepted(&mut feed);
        }
        feed.broadcast_with(|buf| buf.extend_from_slice(b"batch:1"));
        while inbox.is_empty() {
            assert!(!expired(started), "the first batch never arrived");
            let _ = client.drive(Some(Duration::from_millis(1)), &mut [&mut subscriber]);
            pass(&mut server, &mut feed);
            drain(&mut subscriber, &mut inbox, &mut connects, &mut drops);
        }
        assert_eq!(inbox[0], b"batch:1");
        assert_eq!(connects, 1);

        // The server drops the connection; the persistent endpoint reconnects
        // by itself and the feed resumes — no remove, no new connect.
        feed.disconnect(first_token.unwrap());
        let mut reaccepted = None;
        while reaccepted.is_none() {
            assert!(!expired(started), "the subscriber never reconnected");
            let _ = client.drive(Some(Duration::from_millis(1)), &mut [&mut subscriber]);
            pass(&mut server, &mut feed);
            reaccepted = feed_accepted(&mut feed);
        }
        feed.broadcast_with(|buf| buf.extend_from_slice(b"batch:2"));
        while inbox.len() < 2 {
            assert!(!expired(started), "the feed never resumed after the reconnect");
            let _ = client.drive(Some(Duration::from_millis(1)), &mut [&mut subscriber]);
            pass(&mut server, &mut feed);
            drain(&mut subscriber, &mut inbox, &mut connects, &mut drops);
        }
        assert_eq!(inbox[1], b"batch:2");
        assert_eq!(connects, 2, "the same endpoint connected twice");
        assert!(drops >= 1, "the drop was observed, not papered over");
    }

    #[test]
    fn a_channel_sink_forwards_batches_without_the_tile_pulling() {
        let started = std::time::Instant::now();
        let (mut server, mut feed, addr) = feed();

        // The sink mode: the sender is moved into the service, batches are
        // consumed where they are lent, and the tile only drives.
        let (sender, receiver) = mpsc::channel();
        let mut client = StreamNetwork::default();
        let mut subscriber =
            StreamService::with_sink(client.add_group(subscriber_group()), ChannelSink {
                sender,
                connects: 0,
            });
        let _ = subscriber.connect(Endpoint::Tcp(addr));

        while feed_accepted(&mut feed).is_none() {
            assert!(!expired(started), "the sink subscriber never connected");
            let _ = client.drive(Some(Duration::from_millis(1)), &mut [&mut subscriber]);
            pass(&mut server, &mut feed);
        }
        feed.broadcast_with(|buf| buf.extend_from_slice(b"batch:zc"));
        loop {
            assert!(!expired(started), "the batch never reached the channel");
            let _ = client.drive(Some(Duration::from_millis(1)), &mut [&mut subscriber]);
            pass(&mut server, &mut feed);
            if let Ok(batch) = receiver.try_recv() {
                assert_eq!(batch, b"batch:zc");
                break;
            }
        }
        assert_eq!(subscriber.sink().connects, 1, "the sink saw its own lifecycle");
        // There is nothing to pull: next_event() does not exist on a
        // sink-mode service — the wrong use would not compile.
    }
}
