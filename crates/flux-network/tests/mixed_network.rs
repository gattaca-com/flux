use std::{
    net::{Ipv4Addr, SocketAddr, TcpListener, UdpSocket},
    time::{Duration, Instant},
};

use flux_network::{
    Group, Network, NetworkCore, NetworkEvent, NetworkWithExternalPoll, TcpGroupConfig, UdpConfig,
    UdpGroupConfig,
};
use mio::{Events, Poll, Token};

fn addresses() -> [SocketAddr; 2] {
    [
        TcpListener::bind((Ipv4Addr::LOCALHOST, 0)).unwrap().local_addr().unwrap(),
        UdpSocket::bind((Ipv4Addr::LOCALHOST, 0)).unwrap().local_addr().unwrap(),
    ]
}

fn groups(network: &mut NetworkCore) -> [Group; 2] {
    [
        network.add_group(TcpGroupConfig { max_frame_size: 8, ..Default::default() }),
        network.add_group(UdpGroupConfig {
            udp: UdpConfig { max_message_size: 4096, max_datagram_size: 512, ..UdpConfig::lan() },
            ..Default::default()
        }),
    ]
}

#[derive(Debug, Default)]
struct Log {
    accepted: Vec<(Group, Token)>,
    connected: Vec<(Group, Token)>,
    messages: Vec<(Group, Token, Vec<u8>)>,
    disconnected: Vec<(Group, Token)>,
}

impl Log {
    fn record(&mut self, event: &NetworkEvent<'_>) {
        match *event {
            NetworkEvent::Accepted { group, token, .. } => self.accepted.push((group, token)),
            NetworkEvent::Connected { group, token, .. } => self.connected.push((group, token)),
            NetworkEvent::Message { group, token, payload, .. } => {
                assert!(
                    self.accepted.contains(&(group, token)) ||
                        self.connected.contains(&(group, token)),
                    "message preceded establishment"
                );
                self.messages.push((group, token, payload.to_vec()));
            }
            NetworkEvent::Disconnected { group, token, .. } => {
                self.disconnected.push((group, token));
            }
        }
    }
}

fn until(
    server: &mut Network,
    client: &mut Network,
    server_log: &mut Log,
    client_log: &mut Log,
    done: impl Fn(&Log, &Log) -> bool,
) {
    let deadline = Instant::now() + Duration::from_secs(5);
    while !done(server_log, client_log) {
        assert!(
            Instant::now() < deadline,
            "mixed exchange timed out: server={server_log:?}, client={client_log:?}"
        );
        server.poll_with(|event| server_log.record(&event));
        client.poll_with(|event| client_log.record(&event));
    }
}

#[test]
fn mixed_groups_keep_configuration_broadcasts_and_disconnects_independent() {
    let mut server = Network::default();
    let mut client = Network::default();
    let server_groups = groups(&mut server);
    let client_groups = groups(&mut client);
    let addresses = addresses();
    let tokens = std::array::from_fn::<_, 2, _>(|i| {
        server.listen(server_groups[i], addresses[i]).unwrap();
        client.connect(client_groups[i], addresses[i])
    });
    let (mut server_log, mut client_log) = (Log::default(), Log::default());
    until(&mut server, &mut client, &mut server_log, &mut client_log, |s, c| {
        s.accepted.len() == 2 && c.connected.len() == 2
    });
    assert!(client_log.connected.contains(&(client_groups[0], tokens[0])));
    assert!(client_log.connected.contains(&(client_groups[1], tokens[1])));

    // The TCP limit must not restrict a fragmented UDP message in another group.
    let large = vec![0x5a; 2048];
    assert!(!client.send_with(tokens[0], |buf| buf.extend_from_slice(&large)));
    assert!(client.send_with(tokens[1], |buf| buf.extend_from_slice(&large)));
    assert_eq!(server.broadcast_with(server_groups[0], |buf| buf.extend_from_slice(b"tcp")), 1);
    assert_eq!(server.broadcast_with(server_groups[1], |buf| buf.extend_from_slice(b"udp")), 1);
    until(&mut server, &mut client, &mut server_log, &mut client_log, |s, c| {
        s.messages.len() == 1 && c.messages.len() == 2
    });
    let accepted_udp = server_log.accepted.iter().find(|(g, _)| *g == server_groups[1]).unwrap().1;
    assert_eq!(server_log.messages, [(server_groups[1], accepted_udp, large)]);
    assert!(client_log.messages.contains(&(client_groups[0], tokens[0], b"tcp".to_vec())));
    assert!(client_log.messages.contains(&(client_groups[1], tokens[1], b"udp".to_vec())));

    let accepted_tcp = server_log.accepted.iter().find(|(g, _)| *g == server_groups[0]).unwrap().1;
    assert!(server.disconnect(accepted_tcp));
    until(&mut server, &mut client, &mut server_log, &mut client_log, |_, c| {
        c.disconnected.contains(&(client_groups[0], tokens[0]))
    });
    assert!(client.remove(tokens[0]));
    assert_eq!(
        server.broadcast_with(server_groups[1], |buf| buf.extend_from_slice(b"still-up")),
        1
    );
    until(&mut server, &mut client, &mut server_log, &mut client_log, |_, c| c.messages.len() == 3);
    assert_eq!(client_log.messages[2], (client_groups[1], tokens[1], b"still-up".to_vec()));
    assert_eq!(client_log.disconnected, [(client_groups[0], tokens[0])]);
}

#[test]
fn one_external_poll_observes_readiness_and_drives_both_transports() {
    let mut poll = Poll::new().unwrap();
    let mut events = Events::with_capacity(32);
    let mut network = NetworkWithExternalPoll::new(poll.registry().try_clone().unwrap(), 0..100);
    let groups = groups(&mut network);
    let addresses = addresses();
    let outbound = std::array::from_fn::<_, 2, _>(|i| {
        network.listen(groups[i], addresses[i]).unwrap();
        network.connect(groups[i], addresses[i])
    });
    let mut log = Log::default();
    let mut sent = false;
    let mut readable = [false; 2];
    let deadline = Instant::now() + Duration::from_secs(5);
    while log.messages.len() < 4 {
        assert!(Instant::now() < deadline, "shared poll exchange timed out");
        network.pre_poll(&mut |event| log.record(&event));
        poll.poll(&mut events, Some(network.max_poll_interval())).unwrap();
        for readiness in &events {
            network.handle_event(readiness, &mut |event| {
                if let NetworkEvent::Message { group, token, .. } = &event {
                    for i in 0..2 {
                        if *token == outbound[i] {
                            assert_eq!(*group, groups[i]);
                            assert_eq!(readiness.token(), outbound[i]);
                            assert!(readiness.is_readable());
                            readable[i] = true;
                        }
                    }
                }
                log.record(&event);
            });
        }
        network.post_poll(&mut |event| log.record(&event));
        if !sent && log.accepted.len() == 2 && log.connected.len() == 2 {
            for token in outbound {
                assert!(network.send_with(token, |buf| buf.extend_from_slice(b"request")));
            }
            for &(_, token) in &log.accepted {
                assert!(network.send_with(token, |buf| buf.extend_from_slice(b"reply")));
            }
            sent = true;
        }
    }
    assert_eq!(readable, [true, true]);
    assert!(log.disconnected.is_empty());
    assert_eq!(log.messages.len(), 4);
    for i in 0..2 {
        let accepted = log.accepted.iter().find(|(group, _)| *group == groups[i]).unwrap().1;
        assert!(log.messages.contains(&(groups[i], accepted, b"request".to_vec())));
        assert!(log.messages.contains(&(groups[i], outbound[i], b"reply".to_vec())));
    }
}

/// Token operations behave the same whichever transport a token came from:
/// nothing here depends on knowing which group is which.
#[test]
fn token_operations_do_not_distinguish_transports() {
    let mut server = Network::default();
    let mut client = Network::default();
    let server_groups = groups(&mut server);
    let client_groups = groups(&mut client);
    let addresses = addresses();
    let listeners =
        std::array::from_fn::<_, 2, _>(|i| server.listen(server_groups[i], addresses[i]).unwrap());
    let outbound =
        std::array::from_fn::<_, 2, _>(|i| client.connect(client_groups[i], addresses[i]));
    let (mut server_log, mut client_log) = (Log::default(), Log::default());
    let deadline = Instant::now() + Duration::from_secs(5);
    while server_log.accepted.len() < 2 {
        assert!(Instant::now() < deadline, "accept timed out");
        server.poll_with(|event| server_log.record(&event));
    }
    // Retry before consuming the handshake reply, leaving a duplicate UDP
    // hello queued at the server when we close the accepted sessions below.
    client.force_reconnect();
    while client_log.connected.len() < 2 {
        assert!(Instant::now() < deadline, "connect timed out");
        client.poll_with(|event| client_log.record(&event));
    }
    let accepted =
        server_groups.map(|group| server_log.accepted.iter().find(|(g, _)| *g == group).unwrap().1);

    for &token in accepted.iter().chain(&outbound) {
        assert_eq!(server.clear_backlog(token).max(client.clear_backlog(token)), 0);
        assert!(!server.is_broadcast_paused(token));
    }
    for &listener in &listeners {
        assert!(!server.disconnect(listener), "listeners are not sessions");
        assert_eq!(server.clear_backlog(listener), 0);
        assert!(!server.disconnect_when_drained(listener));
    }
    // Unknown tokens are simply rejected, never a panic.
    let unknown = Token(usize::MAX - 1);
    assert!(!server.disconnect(unknown));
    assert!(!server.disconnect_when_drained(unknown));
    assert_eq!(server.clear_backlog(unknown), 0);
    assert!(!server.remove(unknown));

    // A drained close on an idle session of either transport closes it now.
    for &token in &accepted {
        assert!(server.disconnect_when_drained(token));
        assert!(!server.send_with(token, |buf| buf.extend_from_slice(b"refused")));
    }
    until(&mut server, &mut client, &mut server_log, &mut client_log, |s, c| {
        (0..2).all(|i| {
            s.disconnected.contains(&(server_groups[i], accepted[i])) &&
                c.disconnected.contains(&(client_groups[i], outbound[i]))
        })
    });
    for i in 0..2 {
        assert!(server_log.disconnected.contains(&(server_groups[i], accepted[i])));
        assert!(client_log.disconnected.contains(&(client_groups[i], outbound[i])));
    }
}
