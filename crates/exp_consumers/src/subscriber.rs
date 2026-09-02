//! The analytics shredstream subscriber: one persistent keepalive endpoint in
//! a blocking loop.
//!
//! The original runs in a `spawn_blocking` closure — `poll_with`, forward
//! each batch, sleep a millisecond when nothing arrived. Here the blocking
//! `drive(Some(1 ms))` is the whole loop body: it parks for up to the
//! millisecond only when the transport is idle, wakes on readiness, and runs
//! the reconnect schedule from inside the same call — the sleep and the poll
//! stop being two separate steps. The endpoint is persistent: nobody removes
//! it, and after the server drops the connection the group reconnects it and
//! the batches resume.

use flux_network::stream::StreamNetwork;
use flux_timing::Duration;

use crate::RecordingLeaf;

/// One blocking loop iteration: at most a millisecond of idle wait, exactly
/// as the original's `poll_with` + `sleep(1 ms)` paced it.
pub fn blocking_iteration(net: &mut StreamNetwork, subscriber: &mut RecordingLeaf) -> bool {
    net.drive(Some(Duration::from_millis(1)), std::slice::from_mut(subscriber))
}

#[cfg(test)]
mod tests {
    use flux_network::stream::{ConnectionGroupConfig, Endpoint, StreamNetwork, TcpOptions};
    use flux_timing::Duration;

    use super::blocking_iteration;
    use crate::{
        RecordingLeaf,
        harness::{bound_addr, ephemeral, expired, framed_group, pass},
    };

    #[test]
    fn a_persistent_endpoint_survives_the_server_dropping_it() {
        let started = std::time::Instant::now();
        let mut server = StreamNetwork::default();
        let mut feed = RecordingLeaf::new(server.add_group(framed_group("shredstream")));
        let addr = bound_addr(&feed.group_mut().listen(ephemeral()).unwrap());

        // Keepalive on and a short retry, as the analytics loop configures
        // its one endpoint; the token is ignored, as in the original.
        let mut client = StreamNetwork::default();
        let mut subscriber = RecordingLeaf::new(client.add_group(ConnectionGroupConfig {
            name: "subscriber",
            reconnect_interval: Duration::from_millis(25),
            tcp: TcpOptions { keepalive: true, ..TcpOptions::default() },
            ..ConnectionGroupConfig::default()
        }));
        let _ = subscriber.group_mut().connect(Endpoint::Tcp(addr));

        // First connection: a batch flows.
        while feed.accepted.is_empty() {
            assert!(!expired(started), "the subscriber never connected");
            let _ = blocking_iteration(&mut client, &mut subscriber);
            pass(&mut server, &mut feed);
        }
        feed.group_mut().broadcast_with(|buf| buf.extend_from_slice(b"batch:1"));
        while subscriber.inbox.is_empty() {
            assert!(!expired(started), "the first batch never arrived");
            let _ = blocking_iteration(&mut client, &mut subscriber);
            pass(&mut server, &mut feed);
        }
        assert_eq!(subscriber.inbox[0], b"batch:1");
        assert_eq!(subscriber.connected.len(), 1);

        // The server drops the connection; the persistent endpoint reconnects
        // by itself and the feed resumes — no remove, no new connect.
        let dropped = feed.accepted[0];
        feed.group_mut().disconnect(dropped);
        while feed.accepted.len() < 2 {
            assert!(!expired(started), "the subscriber never reconnected");
            let _ = blocking_iteration(&mut client, &mut subscriber);
            pass(&mut server, &mut feed);
        }
        feed.group_mut().broadcast_with(|buf| buf.extend_from_slice(b"batch:2"));
        while subscriber.inbox.len() < 2 {
            assert!(!expired(started), "the feed never resumed after the reconnect");
            let _ = blocking_iteration(&mut client, &mut subscriber);
            pass(&mut server, &mut feed);
        }
        assert_eq!(subscriber.inbox[1], b"batch:2");
        assert_eq!(subscriber.connected.len(), 2, "the same endpoint connected twice");
        assert!(!subscriber.disconnected.is_empty(), "the drop was observed, not papered over");
    }
}
