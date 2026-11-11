//! One-way UDP for [`crate::GroupConfig::UdpPublisher`] and
//! [`crate::GroupConfig::UdpSubscriber`] groups: a publisher sends one
//! stream, the same bytes to every subscriber, which joins at its live end.
//! A subscriber that falls a `send_window` behind or goes silent for
//! `peer_timeout` is dropped, and resubscribes at the live end once it hears
//! of it. The publisher pings every subscriber each `heartbeat_interval` and
//! records the round trip in telemetry; a subscriber that hears neither
//! packets nor pings for `peer_timeout` resubscribes.

mod group;
mod publisher;
mod subscriber;
mod wire;

pub(crate) use group::PubSubGroup;
