//! The cross-network error the ownership split has to make impossible to run.
//!
//! Two networks each allocate their first group as the same integer, so a
//! Service built against one network validates against the other. These tests
//! record what that misuse does today; the refactor must reject it before any
//! state changes, at which point they invert to `#[should_panic]`.

use std::net::Ipv4Addr;

use flux_network::{
    http::{HttpConfig, HttpService},
    stream::{ConnectionGroupConfig, Endpoint, Framing, StreamNetwork},
};
use flux_timing::Duration;

fn raw_group(net: &mut StreamNetwork, name: &'static str) -> flux_network::stream::ConnectionGroup {
    net.add_group(ConnectionGroupConfig {
        name,
        framing: Framing::Raw,
        ..ConnectionGroupConfig::default()
    })
}

fn ephemeral() -> Endpoint {
    Endpoint::Tcp((Ipv4Addr::LOCALHOST, 0).into())
}

#[test]
fn two_networks_hand_out_the_same_first_group() {
    let mut one = StreamNetwork::default();
    let mut two = StreamNetwork::default();

    let first = raw_group(&mut one, "one");
    let second = raw_group(&mut two, "two");

    assert_eq!(
        format!("{first:?}"),
        format!("{second:?}"),
        "the group handle carries no network identity"
    );
}

#[test]
fn a_network_drives_a_service_that_belongs_to_another_network() {
    let mut public_net = StreamNetwork::default();
    let public_group = raw_group(&mut public_net, "public");
    let mut public_http = HttpService::new(&mut public_net, public_group, HttpConfig::default());
    public_http.listen(&mut public_net, ephemeral()).unwrap();

    let mut other_net = StreamNetwork::default();
    let other_group = raw_group(&mut other_net, "other");
    let mut wrong_http = HttpService::new(&mut other_net, other_group, HttpConfig::default());

    // The service reports the integer 0, which is also this network's claimed
    // group, so validation accepts it and the wrong service is driven.
    public_net.drive(Some(Duration::ZERO), &mut [wrong_http.as_service()], |_| {});
}

#[test]
fn a_foreign_network_answers_an_http_operation() {
    let mut public_net = StreamNetwork::default();
    let public_group = raw_group(&mut public_net, "public");
    let _public_http = HttpService::new(&mut public_net, public_group, HttpConfig::default());

    let mut other_net = StreamNetwork::default();
    let other_group = raw_group(&mut other_net, "other");
    let mut wrong_http = HttpService::new(&mut other_net, other_group, HttpConfig::default());

    // The normal HTTP interface takes a caller-selected network, so the error
    // continues past scheduling.
    assert!(wrong_http.listen(&mut public_net, ephemeral()).is_ok());
    assert!(wrong_http.next_event(&mut public_net).is_none());
}
