use std::{
    cell::Cell,
    net::{Ipv4Addr, SocketAddr},
    ops::Range,
    time::{Duration, Instant},
};

use flux_network::tcp::{TcpEvent, TcpGroupConfig, TcpNetworkWithExternalPoll};
use mio::{Events, Poll, Token};

const SERVER_TOKENS: Range<usize> = 100..200;
const CLIENT_TOKENS: Range<usize> = 200..300;

const SERVER_HELLO: &[u8] = b"server-hello";
const CLIENT_HELLO: &[u8] = b"client-hello";
const REQUEST: &[u8] = b"request-payload";
const RESPONSE: &[u8] = b"response-payload";
const AFTER_RECONNECT: &[u8] = b"after-reconnect";

/// The external-poll phase that invoked a handler.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Phase {
    Pre,
    Event,
    Post,
}

#[derive(Debug)]
enum Ev {
    Accepted(Token),
    Connected(Token),
    Message(Token, Vec<u8>),
    Disconnected(Token),
}

#[derive(Debug)]
struct Record {
    phase: Phase,
    event: Ev,
}

fn unused_addr() -> SocketAddr {
    let listener = std::net::TcpListener::bind((Ipv4Addr::LOCALHOST, 0)).unwrap();
    let addr = listener.local_addr().unwrap();
    drop(listener);
    addr
}

fn accepts(log: &[Record]) -> Vec<Token> {
    log.iter()
        .filter_map(|r| if let Ev::Accepted(token) = r.event { Some(token) } else { None })
        .collect()
}

fn connects(log: &[Record]) -> usize {
    log.iter().filter(|r| matches!(r.event, Ev::Connected(_))).count()
}

fn has_message(log: &[Record], payload: &[u8]) -> bool {
    log.iter().any(|r| matches!(&r.event, Ev::Message(_, p) if p == payload))
}

fn message_token(log: &[Record], payload: &[u8]) -> Option<Token> {
    log.iter().find_map(|r| match &r.event {
        Ev::Message(token, p) if p == payload => Some(*token),
        _ => None,
    })
}

fn disconnect_phase(log: &[Record], token: Token) -> Option<Phase> {
    log.iter().find_map(|r| match r.event {
        Ev::Disconnected(t) if t == token => Some(r.phase),
        _ => None,
    })
}

#[test]
#[allow(clippy::too_many_lines)]
fn one_poll_drives_two_networks_with_disjoint_ranges() {
    let addr = unused_addr();
    let mut poll = Poll::new().unwrap();
    let mut events = Events::with_capacity(128);

    let mut server =
        TcpNetworkWithExternalPoll::new(poll.registry().try_clone().unwrap(), SERVER_TOKENS);
    let mut client =
        TcpNetworkWithExternalPoll::new(poll.registry().try_clone().unwrap(), CLIENT_TOKENS);

    let server_group = server.add_group(TcpGroupConfig {
        name: "server",
        on_connect_msg: Some(SERVER_HELLO.to_vec()),
        ..TcpGroupConfig::default()
    });
    let client_group = client.add_group(TcpGroupConfig {
        name: "client",
        on_connect_msg: Some(CLIENT_HELLO.to_vec()),
        reconnect_interval: flux_timing::Duration::from_millis(1),
        ..TcpGroupConfig::default()
    });

    server.listen(server_group, addr).unwrap();
    let client_token = client.connect(client_group, addr);
    assert!(CLIENT_TOKENS.contains(&client_token.0));

    let phase = Cell::new(Phase::Pre);
    let mut server_log: Vec<Record> = Vec::new();
    let mut client_log: Vec<Record> = Vec::new();

    let mut request_sent = false;
    let mut response_sent = false;
    let mut disconnect_issued = false;
    let mut resent = false;
    let mut done = false;

    let deadline = Instant::now() + Duration::from_secs(10);
    while !done && Instant::now() < deadline {
        {
            let mut on_server = |event: TcpEvent<'_>| {
                let ev = match event {
                    TcpEvent::Accepted { group, token, .. } => {
                        assert_eq!(group, server_group);
                        Ev::Accepted(token)
                    }
                    TcpEvent::Message { group, token, payload, .. } => {
                        assert_eq!(group, server_group);
                        Ev::Message(token, payload.to_vec())
                    }
                    TcpEvent::Disconnected { group, token, .. } => {
                        assert_eq!(group, server_group);
                        Ev::Disconnected(token)
                    }
                    TcpEvent::Connected { .. } => {
                        panic!("server network has no outbound endpoints")
                    }
                };
                let token = match &ev {
                    Ev::Accepted(t) |
                    Ev::Message(t, _) |
                    Ev::Disconnected(t) |
                    Ev::Connected(t) => *t,
                };
                assert!(
                    SERVER_TOKENS.contains(&token.0),
                    "server event token {token:?} out of range"
                );
                server_log.push(Record { phase: phase.get(), event: ev });
            };
            let mut on_client = |event: TcpEvent<'_>| {
                let ev = match event {
                    TcpEvent::Connected { group, token, .. } => {
                        assert_eq!(group, client_group);
                        assert_eq!(token, client_token, "reconnect must keep the endpoint token");
                        Ev::Connected(token)
                    }
                    TcpEvent::Message { group, token, payload, .. } => {
                        assert_eq!(group, client_group);
                        assert_eq!(token, client_token);
                        Ev::Message(token, payload.to_vec())
                    }
                    TcpEvent::Disconnected { group, token, .. } => {
                        assert_eq!(group, client_group);
                        assert_eq!(token, client_token);
                        Ev::Disconnected(token)
                    }
                    TcpEvent::Accepted { .. } => panic!("client network has no listeners"),
                };
                client_log.push(Record { phase: phase.get(), event: ev });
            };

            phase.set(Phase::Pre);
            server.pre_poll(&mut on_server);
            client.pre_poll(&mut on_client);

            poll.poll(&mut events, Some(Duration::from_millis(1))).unwrap();
            phase.set(Phase::Event);
            for event in &events {
                let token = event.token().0;
                if SERVER_TOKENS.contains(&token) {
                    server.handle_event(event, &mut on_server);
                } else if CLIENT_TOKENS.contains(&token) {
                    client.handle_event(event, &mut on_client);
                } else {
                    panic!("event token {token} lies outside every network's range");
                }
            }

            phase.set(Phase::Post);
            server.post_poll(&mut on_server);
            client.post_poll(&mut on_client);
        }

        if !request_sent && connects(&client_log) >= 1 && has_message(&client_log, SERVER_HELLO) {
            assert!(client.send_with(client_token, |buf| buf.extend_from_slice(REQUEST)));
            request_sent = true;
        }
        if !response_sent && has_message(&server_log, REQUEST) {
            let token = accepts(&server_log)[0];
            assert!(server.send_with(token, |buf| buf.extend_from_slice(RESPONSE)));
            response_sent = true;
        }
        if !disconnect_issued && has_message(&client_log, RESPONSE) {
            assert!(server.disconnect(accepts(&server_log)[0]));
            disconnect_issued = true;
        }
        if disconnect_issued &&
            !resent &&
            accepts(&server_log).len() >= 2 &&
            connects(&client_log) >= 2
        {
            assert!(client.send_with(client_token, |buf| buf.extend_from_slice(AFTER_RECONNECT)));
            resent = true;
        }
        if resent && has_message(&server_log, AFTER_RECONNECT) {
            done = true;
        }
    }
    assert!(done, "the two-network exchange did not complete before the deadline");

    // Verify the initial bidirectional exchange.
    assert!(has_message(&server_log, CLIENT_HELLO));
    assert!(has_message(&client_log, SERVER_HELLO));
    assert!(has_message(&server_log, REQUEST));
    assert!(has_message(&client_log, RESPONSE));

    // The reconnect uses the next server-side token and preserves the client
    // token, which is checked in the handler.
    let accepted = accepts(&server_log);
    assert_eq!(accepted.len(), 2);
    assert_eq!(accepted[1].0, accepted[0].0 + 1);
    assert_eq!(message_token(&server_log, AFTER_RECONNECT), Some(accepted[1]));

    // A requested disconnect is reported by the next pre_poll; the peer close
    // is reported by handle_event.
    assert_eq!(disconnect_phase(&server_log, accepted[0]), Some(Phase::Pre));
    assert_eq!(disconnect_phase(&client_log, client_token), Some(Phase::Event));
}

#[test]
#[should_panic(expected = "tcp token range 100..102 exhausted")]
fn exhausted_token_range_panics_naming_the_range() {
    let poll = Poll::new().unwrap();
    let mut network =
        TcpNetworkWithExternalPoll::new(poll.registry().try_clone().unwrap(), 100..102);
    let group = network.add_group(TcpGroupConfig::default());

    network.listen(group, unused_addr()).unwrap();
    let second = network.connect(group, unused_addr());
    assert_eq!(second, Token(101), "tokens are assigned in ascending order from the range start");

    let _ = network.connect(group, unused_addr());
}

#[test]
#[cfg_attr(debug_assertions, should_panic(expected = "lies outside this network's token range"))]
fn foreign_token_trips_the_containment_assert() {
    let mut poll = Poll::new().unwrap();
    let mut network =
        TcpNetworkWithExternalPoll::new(poll.registry().try_clone().unwrap(), 100..200);

    // Register a readiness source outside the network's token range.
    let mut listener = mio::net::TcpListener::bind("127.0.0.1:0".parse().unwrap()).unwrap();
    poll.registry().register(&mut listener, Token(7), mio::Interest::READABLE).unwrap();
    let _client = std::net::TcpStream::connect(listener.local_addr().unwrap()).unwrap();

    let mut events = Events::with_capacity(4);
    let deadline = Instant::now() + Duration::from_secs(5);
    while events.is_empty() && Instant::now() < deadline {
        poll.poll(&mut events, Some(Duration::from_millis(10))).unwrap();
    }
    let event = events.iter().next().expect("listener readiness did not arrive");
    assert_eq!(event.token(), Token(7));

    // Debug builds reject the token; release builds emit no TcpEvent for it.
    network.handle_event(event, &mut |_| panic!("no TcpEvent expected for a foreign token"));
}
