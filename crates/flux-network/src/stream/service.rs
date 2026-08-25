//! The scheduling hooks a service exposes to its network, and the carrier that
//! hands them over.

use flux_timing::Instant;

use super::{ConnectionGroup, StreamEvent, StreamNetwork};

pub(crate) mod private {
    use flux_timing::Instant;

    use super::{ConnectionGroup, StreamEvent, StreamNetwork};

    /// What a [`StreamNetwork`] calls on the service owning a group, in the
    /// order [`StreamNetwork::drive`] fixes.
    ///
    /// The trait is flux-internal so that the hooks stay the network's alone:
    /// a service reaches its network only through the opaque
    /// [`super::ServiceRef`] carrier.
    pub trait ServiceDriver {
        /// The group this service owns.
        fn group(&self) -> ConnectionGroup;
        /// Takes one transport event for that group.
        fn on_event(&mut self, event: &StreamEvent<'_>);
        /// Runs the service's own timers once per iteration, reporting whether
        /// protocol events are left to pull.
        fn tick(&mut self, net: &mut StreamNetwork, now: Instant) -> bool;
        /// The earliest instant this service needs a tick at.
        fn next_deadline(&self) -> Option<Instant>;
    }
}

/// One service, for the duration of a driver call.
///
/// A service hands its network a `ServiceRef` — `HttpService::as_service()` —
/// and the network drives it; the carrier itself offers a caller nothing.
pub struct ServiceRef<'a>(&'a mut dyn private::ServiceDriver);

impl<'a> ServiceRef<'a> {
    pub(crate) fn new(driver: &'a mut dyn private::ServiceDriver) -> Self {
        Self(driver)
    }

    pub(crate) fn group(&self) -> ConnectionGroup {
        self.0.group()
    }

    pub(crate) fn on_event(&mut self, event: &StreamEvent<'_>) {
        self.0.on_event(event);
    }

    pub(crate) fn tick(&mut self, net: &mut StreamNetwork, now: Instant) -> bool {
        self.0.tick(net, now)
    }

    pub(crate) fn next_deadline(&self) -> Option<Instant> {
        self.0.next_deadline()
    }
}

#[cfg(test)]
mod tests {
    use std::{
        cell::Cell,
        io::{Read as _, Write as _},
        net::{Ipv4Addr, SocketAddr, TcpStream as StdTcpStream},
        rc::Rc,
    };

    use flux_timing::{Duration, Instant};
    use mio::Token;

    use super::{ServiceRef, private::ServiceDriver};
    use crate::stream::{
        ConnectionGroup, ConnectionGroupConfig, Endpoint, Framing, StreamEvent, StreamNetwork,
    };

    const PATIENCE: std::time::Duration = std::time::Duration::from_secs(10);

    /// What the network did to a service, in the order it did it.
    #[derive(Clone, Debug, PartialEq, Eq)]
    enum Step {
        Accepted(Token),
        Connected(Token),
        Message(Vec<u8>),
        Disconnected(Token),
        Tick(usize),
    }

    /// A service that records how it was driven.
    struct Probe {
        group: ConnectionGroup,
        steps: Vec<Step>,
        tick_times: Vec<Instant>,
        clock: Rc<Cell<usize>>,
        deadline: Option<Instant>,
        pullable: bool,
    }

    impl Probe {
        fn new(group: ConnectionGroup) -> Self {
            Self {
                group,
                steps: Vec::new(),
                tick_times: Vec::new(),
                clock: Rc::new(Cell::new(0)),
                deadline: None,
                pullable: false,
            }
        }

        fn as_service(&mut self) -> ServiceRef<'_> {
            ServiceRef::new(self)
        }

        fn events(&self) -> Vec<Step> {
            self.steps.iter().filter(|step| !matches!(step, Step::Tick(_))).cloned().collect()
        }

        fn ticks(&self) -> Vec<usize> {
            self.steps
                .iter()
                .filter_map(|step| match step {
                    Step::Tick(order) => Some(*order),
                    _ => None,
                })
                .collect()
        }

        fn accepted(&self) -> Option<Token> {
            self.steps.iter().find_map(|step| match step {
                Step::Accepted(token) => Some(*token),
                _ => None,
            })
        }
    }

    impl ServiceDriver for Probe {
        fn group(&self) -> ConnectionGroup {
            self.group
        }

        fn on_event(&mut self, event: &StreamEvent<'_>) {
            self.steps.push(match *event {
                StreamEvent::Accepted { token, .. } => Step::Accepted(token),
                StreamEvent::Connected { token, .. } => Step::Connected(token),
                StreamEvent::Message { payload, .. } => Step::Message(payload.to_vec()),
                StreamEvent::Disconnected { token, .. } => Step::Disconnected(token),
            });
        }

        fn tick(&mut self, _net: &mut StreamNetwork, now: Instant) -> bool {
            let order = self.clock.get();
            self.clock.set(order + 1);
            self.steps.push(Step::Tick(order));
            self.tick_times.push(now);
            self.pullable
        }

        fn next_deadline(&self) -> Option<Instant> {
            self.deadline
        }
    }

    fn unused_addr() -> SocketAddr {
        let listener = std::net::TcpListener::bind((Ipv4Addr::LOCALHOST, 0)).unwrap();
        let addr = listener.local_addr().unwrap();
        drop(listener);
        addr
    }

    fn raw_group(network: &mut StreamNetwork, name: &'static str) -> ConnectionGroup {
        network.add_group(ConnectionGroupConfig {
            name,
            framing: Framing::Raw,
            ..ConnectionGroupConfig::default()
        })
    }

    /// Drives one service until it has seen what the test waits for.
    fn drive_until(network: &mut StreamNetwork, probe: &mut Probe, done: impl Fn(&Probe) -> bool) {
        let deadline = std::time::Instant::now() + PATIENCE;
        while std::time::Instant::now() < deadline {
            network.drive(Some(Duration::from_millis(1)), &mut [probe.as_service()], |_| {});
            if done(probe) {
                return;
            }
        }
        panic!("the service never saw what the test waits for: {:?}", probe.steps);
    }

    #[test]
    #[should_panic(expected = "service-owned group 0 has no service")]
    fn drive_rejects_a_claimed_group_left_without_a_service() {
        let mut network = StreamNetwork::default();
        let group = raw_group(&mut network, "orphan");
        network.claim_group(group);
        network.drive(Some(Duration::ZERO), &mut [], |_| {});
    }

    #[test]
    #[should_panic(expected = "duplicate service for group 0")]
    fn drive_rejects_two_services_for_one_group() {
        let mut network = StreamNetwork::default();
        let group = raw_group(&mut network, "shared");
        network.claim_group(group);
        let mut one = Probe::new(group);
        let mut two = Probe::new(group);
        network.drive(Some(Duration::ZERO), &mut [one.as_service(), two.as_service()], |_| {});
    }

    #[test]
    #[should_panic(expected = "no service owns connection group 0")]
    fn drive_rejects_a_service_of_an_unclaimed_group() {
        let mut network = StreamNetwork::default();
        let group = raw_group(&mut network, "raw");
        let mut probe = Probe::new(group);
        network.drive(Some(Duration::ZERO), &mut [probe.as_service()], |_| {});
    }

    #[test]
    #[should_panic(expected = "already has a service")]
    fn a_group_takes_one_claim() {
        let mut network = StreamNetwork::default();
        let group = raw_group(&mut network, "claimed");
        network.claim_group(group);
        network.claim_group(group);
    }

    #[test]
    fn releasing_a_claim_returns_the_group_to_raw() {
        let mut network = StreamNetwork::default();
        let group = raw_group(&mut network, "released");
        network.claim_group(group);
        network.release_group(group);
        network.drive(Some(Duration::ZERO), &mut [], |_| {});
    }

    #[test]
    fn events_reach_the_owning_service_and_unclaimed_groups_reach_the_handler() {
        let mut network = StreamNetwork::default();
        let owned = raw_group(&mut network, "owned");
        let raw = raw_group(&mut network, "raw");
        let owned_addr = unused_addr();
        let raw_addr = unused_addr();
        network.listen(owned, Endpoint::Tcp(owned_addr)).unwrap();
        network.listen(raw, Endpoint::Tcp(raw_addr)).unwrap();
        network.claim_group(owned);
        let mut probe = Probe::new(owned);

        let mut to_owned = StdTcpStream::connect(owned_addr).unwrap();
        let mut to_raw = StdTcpStream::connect(raw_addr).unwrap();
        to_owned.write_all(b"for the service").unwrap();
        to_raw.write_all(b"for the handler").unwrap();

        let mut lent = Vec::new();
        let deadline = std::time::Instant::now() + PATIENCE;
        while std::time::Instant::now() < deadline && (probe.events().len() < 2 || lent.is_empty())
        {
            network.drive(Some(Duration::from_millis(1)), &mut [probe.as_service()], |event| {
                if let StreamEvent::Message { group, payload, .. } = event {
                    assert_eq!(group, raw);
                    lent.push(payload.to_vec());
                }
            });
        }

        assert!(
            matches!(probe.events().as_slice(), [Step::Accepted(_), Step::Message(payload)]
            if payload == b"for the service"),
            "{:?}",
            probe.events()
        );
        assert_eq!(lent, [b"for the handler".to_vec()]);
    }

    #[test]
    fn a_disconnect_from_maintenance_reaches_the_service_before_its_tick() {
        let mut network = StreamNetwork::default();
        let group = raw_group(&mut network, "kicked");
        let addr = unused_addr();
        network.listen(group, Endpoint::Tcp(addr)).unwrap();
        network.claim_group(group);
        let mut probe = Probe::new(group);
        let client = StdTcpStream::connect(addr).unwrap();

        drive_until(&mut network, &mut probe, |probe| probe.accepted().is_some());
        let accepted = probe.accepted().unwrap();
        probe.steps.clear();

        // Disconnecting outside a driver call leaves the event pending, so the
        // next drive delivers it as maintenance, before any tick.
        assert!(network.disconnect(accepted));
        network.drive(Some(Duration::ZERO), &mut [probe.as_service()], |_| {});

        assert!(
            matches!(probe.steps.as_slice(), [Step::Disconnected(token), Step::Tick(_)]
                if *token == accepted),
            "{:?}",
            probe.steps
        );
        drop(client);
    }

    #[test]
    fn services_tick_once_each_in_slice_order() {
        let mut network = StreamNetwork::default();
        let first = raw_group(&mut network, "first");
        let second = raw_group(&mut network, "second");
        network.claim_group(first);
        network.claim_group(second);
        let clock = Rc::new(Cell::new(0));
        let mut one = Probe::new(first);
        let mut two = Probe::new(second);
        one.clock = Rc::clone(&clock);
        two.clock = Rc::clone(&clock);

        network.drive(Some(Duration::ZERO), &mut [one.as_service(), two.as_service()], |_| {});
        network.drive(Some(Duration::ZERO), &mut [two.as_service(), one.as_service()], |_| {});

        assert_eq!(one.ticks(), [0, 3]);
        assert_eq!(two.ticks(), [1, 2]);
    }

    #[test]
    fn work_is_reported_by_services_and_by_routed_events() {
        let mut network = StreamNetwork::default();
        let group = raw_group(&mut network, "work");
        let addr = unused_addr();
        network.listen(group, Endpoint::Tcp(addr)).unwrap();
        network.claim_group(group);
        let mut probe = Probe::new(group);

        assert!(!network.drive(Some(Duration::ZERO), &mut [probe.as_service()], |_| {}));

        probe.pullable = true;
        assert!(network.drive(Some(Duration::ZERO), &mut [probe.as_service()], |_| {}));
        probe.pullable = false;

        let _client = StdTcpStream::connect(addr).unwrap();
        let deadline = std::time::Instant::now() + PATIENCE;
        let mut worked = false;
        while std::time::Instant::now() < deadline && !worked {
            worked =
                network.drive(Some(Duration::from_millis(1)), &mut [probe.as_service()], |_| {});
        }
        assert!(worked, "an accepted connection is work");
    }

    #[test]
    fn the_poll_waits_no_longer_than_the_earliest_service_deadline() {
        let mut network = StreamNetwork::default();
        let group = raw_group(&mut network, "deadline");
        network.claim_group(group);
        let mut probe = Probe::new(group);
        probe.deadline = Some(Instant::now() + Duration::from_millis(50));

        let started = std::time::Instant::now();
        network.drive(Some(Duration::from_secs(5)), &mut [probe.as_service()], |_| {});
        let waited = started.elapsed();

        assert!(waited >= std::time::Duration::from_millis(40), "{waited:?}");
        assert!(waited < std::time::Duration::from_secs(1), "{waited:?}");
    }

    #[test]
    fn ticks_see_the_time_after_the_poll_wait() {
        let mut network = StreamNetwork::default();
        let group = raw_group(&mut network, "waited");
        network.claim_group(group);
        let mut probe = Probe::new(group);

        // Nothing is listening and no deadline falls sooner, so the poll waits
        // the timeout out in full.
        let timeout = Duration::from_millis(50);
        let before = Instant::now();
        network.drive(Some(timeout), &mut [probe.as_service()], |_| {});

        assert_eq!(probe.tick_times.len(), 1, "{:?}", probe.steps);
        let ticked = probe.tick_times[0];
        assert!(
            ticked >= before + timeout,
            "the tick came {}us into a {}us wait",
            (ticked - before).as_micros_u64(),
            timeout.as_micros_u64()
        );
    }

    #[test]
    fn poll_with_never_blocks() {
        let mut network = StreamNetwork::default();
        let group = raw_group(&mut network, "raw");
        network.listen(group, Endpoint::Tcp(unused_addr())).unwrap();

        let started = std::time::Instant::now();
        network.poll_with(|_| {});
        assert!(started.elapsed() < std::time::Duration::from_millis(500));
    }

    #[test]
    fn a_service_answers_the_bytes_the_network_lends_it() {
        let mut network = StreamNetwork::default();
        let group = raw_group(&mut network, "echo");
        let addr = unused_addr();
        network.listen(group, Endpoint::Tcp(addr)).unwrap();
        network.claim_group(group);
        let mut probe = Probe::new(group);
        let mut client = StdTcpStream::connect(addr).unwrap();
        client.write_all(b"ping").unwrap();

        drive_until(&mut network, &mut probe, |probe| {
            probe.events().contains(&Step::Message(b"ping".to_vec()))
        });

        let token = probe.accepted().unwrap();
        assert!(network.send_with(token, |out| out.extend_from_slice(b"pong")));
        let mut buffer = [0; 4];
        client.set_nonblocking(false).unwrap();
        client.set_read_timeout(Some(PATIENCE)).unwrap();
        client.read_exact(&mut buffer).unwrap();
        assert_eq!(&buffer, b"pong");
    }
}
