//! The cross-network misuse the ownership split makes impossible to run.
//!
//! Two networks allocate their first groups in the same slot, so an identity
//! carried as a bare integer could not tell their services apart. The
//! identity is the network: every scheduling entry point rejects a service of
//! another network before any state changes, and closing a group checks the
//! same identity.
//!
//! A third form of the misuse cannot be written down at all: HTTP operations
//! go through the `ConnectionGroup` a service owns and take no network, so no
//! call pairs a service with the wrong network's operations.

use std::net::{Ipv4Addr, TcpStream};

use flux_network::{
    http::{HttpConfig, HttpService},
    stream::{ConnectionGroup, ConnectionGroupConfig, Endpoint, Framing, StreamNetwork},
};
use flux_timing::Duration;
use mio::{Events, Poll, Token};

/// The first group of `net`: slot 0, whichever network allocated it.
fn first_group(net: &mut StreamNetwork, name: &'static str) -> ConnectionGroup {
    net.add_group(ConnectionGroupConfig {
        name,
        framing: Framing::Raw,
        ..ConnectionGroupConfig::default()
    })
}

#[test]
#[should_panic(expected = "a service of another network was passed to this one")]
fn driving_a_service_of_another_network_panics() {
    let mut public_net = StreamNetwork::default();
    let mut other_net = StreamNetwork::default();
    // Both first groups sit in slot 0: what trips the panic is the network
    // the identity carries, not the slot integer.
    let mut wrong_http =
        HttpService::new(first_group(&mut other_net, "other"), HttpConfig::default());

    let _ = public_net.drive(Some(Duration::ZERO), &mut [&mut wrong_http]);
}

#[test]
#[should_panic(expected = "a service of another network was passed to this one")]
fn an_external_fold_rejects_a_service_of_another_network() {
    let poll = Poll::new().unwrap();
    let registry = poll.registry().try_clone().unwrap();
    let external = StreamNetwork::with_registry(registry, Token(1024));

    let mut other_net = StreamNetwork::default();
    let wrong_http = HttpService::new(first_group(&mut other_net, "other"), HttpConfig::default());

    let _ = external.next_deadline(&[wrong_http]);
}

#[test]
#[should_panic(expected = "a service of another network was passed to this one")]
fn an_external_tick_rejects_a_service_of_another_network() {
    let poll = Poll::new().unwrap();
    let mut external =
        StreamNetwork::with_registry(poll.registry().try_clone().unwrap(), Token(1024));

    let mut other_net = StreamNetwork::default();
    let mut wrong_http =
        HttpService::new(first_group(&mut other_net, "other"), HttpConfig::default());

    let _ = external.tick(&mut [&mut wrong_http]);
}

#[test]
#[should_panic(expected = "a service of another network was passed to this one")]
fn an_external_event_rejects_a_service_of_another_network() {
    let mut poll = Poll::new().unwrap();
    let mut external =
        StreamNetwork::with_registry(poll.registry().try_clone().unwrap(), Token(1024));
    // A listener of the external network's own, so a readiness event arrives
    // on a token the network recognises and validation is reached at all —
    // a foreign token is handed back before the services are looked at.
    let mut local = first_group(&mut external, "local");
    let Endpoint::Tcp(addr) = local.listen(Endpoint::Tcp((Ipv4Addr::LOCALHOST, 0).into())).unwrap()
    else {
        unreachable!("a TCP listener")
    };
    let _client = TcpStream::connect(addr).unwrap();

    let mut other_net = StreamNetwork::default();
    let mut wrong_http =
        HttpService::new(first_group(&mut other_net, "other"), HttpConfig::default());

    let mut events = Events::with_capacity(4);
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
    loop {
        assert!(std::time::Instant::now() < deadline, "the listener never became readable");
        poll.poll(&mut events, Some(std::time::Duration::from_millis(1))).unwrap();
        for event in &events {
            // The foreign service is checked before the event is routed, and
            // before the omission of the listener's own service could be.
            let _ = external.handle_event(event, &mut [&mut wrong_http]);
        }
    }
}

#[test]
#[should_panic(expected = "this connection group belongs to another network")]
fn closing_a_group_of_another_network_panics() {
    let mut public_net = StreamNetwork::default();
    let mut other_net = StreamNetwork::default();
    let group = first_group(&mut other_net, "other");

    public_net.remove_group(group);
}
