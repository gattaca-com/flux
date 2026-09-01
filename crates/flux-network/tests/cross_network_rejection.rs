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

use flux_network::{
    http::{HttpConfig, HttpService},
    stream::{ConnectionGroup, ConnectionGroupConfig, Framing, StreamNetwork},
};
use flux_timing::Duration;
use mio::{Poll, Token};

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
#[should_panic(expected = "this connection group belongs to another network")]
fn closing_a_group_of_another_network_panics() {
    let mut public_net = StreamNetwork::default();
    let mut other_net = StreamNetwork::default();
    let group = first_group(&mut other_net, "other");

    public_net.remove_group(group);
}
