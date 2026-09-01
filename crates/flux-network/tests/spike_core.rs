//! Spike: the shared registration/token core the ownership split needs.
//!
//! Proves, before any of the crate is restructured, that a `ConnectionGroup`
//! holding a shared handle to its network's registry and token allocator keeps
//! the auto traits the current `StreamNetwork` has, that sockets registered
//! through that handle reach the network's own poll, and what a registry clone
//! does once the poll behind it is gone.

use std::{
    io,
    net::{Ipv4Addr, SocketAddr},
    sync::{
        Arc,
        atomic::{AtomicU64, AtomicUsize, Ordering},
    },
    time::Duration,
};

use mio::{Events, Interest, Poll, Registry, Token, net::TcpListener};

fn assert_send<T: Send>() {}
fn assert_sync<T: Sync>() {}

/// What every group of one network shares once group state moves out.
struct NetworkCore {
    registry: Registry,
    token_base: Token,
    next_token: AtomicUsize,
}

impl NetworkCore {
    fn next_token(&self) -> Token {
        Token(self.next_token.fetch_add(1, Ordering::Relaxed))
    }
}

/// The opaque identity a Service reports, carrying the obligation audit.
struct ConnectionGroupId {
    core: Arc<NetworkCore>,
    slot: usize,
    /// maintenance observed, deadline observed, deadline was `Some`.
    audit_flags: AtomicUsize,
    /// The deadline the group last reported, meaningful only with its flag.
    audit_deadline: AtomicU64,
}

/// The stateful group, reduced to what this probe needs.
struct ConnectionGroup {
    identity: ConnectionGroupId,
    listeners: Vec<(Token, TcpListener)>,
    /// Maintenance passes this group has run, standing in for the pending
    /// work the real operation drains.
    maintained: usize,
}

impl ConnectionGroup {
    fn listen(&mut self, addr: SocketAddr) -> io::Result<SocketAddr> {
        let mut socket = TcpListener::bind(addr)?;
        let bound = socket.local_addr()?;
        let token = self.identity.core.next_token();
        self.identity.core.registry.register(&mut socket, token, Interest::READABLE)?;
        self.listeners.push((token, socket));
        Ok(bound)
    }
}

fn core_and_group(poll: &Poll, token_base: Token) -> (Arc<NetworkCore>, ConnectionGroup) {
    let core = Arc::new(NetworkCore {
        registry: poll.registry().try_clone().expect("registry clone"),
        token_base,
        next_token: AtomicUsize::new(token_base.0),
    });
    let group = ConnectionGroup {
        identity: ConnectionGroupId {
            core: Arc::clone(&core),
            slot: 0,
            audit_flags: AtomicUsize::new(0),
            audit_deadline: AtomicU64::new(0),
        },
        listeners: Vec::new(),
        maintained: 0,
    };
    (core, group)
}

#[test]
fn the_shared_core_keeps_the_auto_traits_the_network_has_today() {
    assert_send::<Registry>();
    assert_sync::<Registry>();
    assert_send::<NetworkCore>();
    assert_sync::<NetworkCore>();
    assert_send::<Arc<NetworkCore>>();
    assert_send::<ConnectionGroupId>();
    assert_send::<ConnectionGroup>();
    assert_send::<flux_network::stream::StreamNetwork>();
}

#[test]
fn a_socket_registered_through_the_shared_core_reaches_the_networks_poll() {
    let mut poll = Poll::new().unwrap();
    let (core, mut group) = core_and_group(&poll, Token(0));
    let bound = group.listen((Ipv4Addr::LOCALHOST, 0).into()).unwrap();

    let _client = std::net::TcpStream::connect(bound).unwrap();

    let mut events = Events::with_capacity(8);
    poll.poll(&mut events, Some(Duration::from_secs(5))).unwrap();
    let seen: Vec<Token> = events.iter().map(|event| event.token()).collect();

    assert_eq!(seen, [group.listeners[0].0], "readiness reached the poll behind the shared core");
    assert!(core.next_token.load(Ordering::Relaxed) > core.token_base.0, "a token was allocated");
}

#[test]
fn the_audit_state_is_writable_through_a_shared_identity() {
    let poll = Poll::new().unwrap();
    let (_core, group) = core_and_group(&poll, Token(0));

    // The network holds only `&ConnectionGroupId`, which is what arming and
    // checking the obligation audit must be able to do.
    let identity: &ConnectionGroupId = &group.identity;
    identity.audit_flags.store(0b001, Ordering::Relaxed);
    identity.audit_deadline.store(42, Ordering::Relaxed);

    assert_eq!(identity.audit_flags.load(Ordering::Relaxed), 0b001);
    assert_eq!(identity.audit_deadline.load(Ordering::Relaxed), 42);
    assert_eq!(identity.slot, 0);
}

#[test]
fn a_registry_clone_outlives_its_poll_and_registers_into_nothing() {
    let poll = Poll::new().unwrap();
    let (_core, mut group) = core_and_group(&poll, Token(0));
    drop(poll);

    // Whether this errors or silently succeeds decides what the design can
    // promise about dropping a network before its Services.
    let outcome = group.listen((Ipv4Addr::LOCALHOST, 0).into());
    println!("register after the poll was dropped: {outcome:?}");
    assert!(outcome.is_ok(), "the clone accepts registrations with no poll behind it");
}

const MAINTAIN_ARMED: usize = 0b001;
const MAINTAIN_OBSERVED: usize = 0b010;

impl ConnectionGroupId {
    /// The network arms the audit through the shared reference `Service`
    /// hands it, before the tick it is about to make.
    fn arm_maintain(&self) {
        self.audit_flags.store(MAINTAIN_ARMED, Ordering::Relaxed);
    }

    /// The group satisfies it from inside the leaf's tick.
    fn observe_maintain(&self) {
        self.audit_flags.fetch_or(MAINTAIN_OBSERVED, Ordering::Relaxed);
    }

    fn assert_maintained(&self) {
        let flags = self.audit_flags.load(Ordering::Relaxed);
        assert!(
            flags & MAINTAIN_OBSERVED != 0,
            "group {} ticked without reaching ConnectionGroup::maintain",
            self.slot
        );
    }
}

impl ConnectionGroup {
    fn maintain(&mut self) -> bool {
        self.identity.observe_maintain();
        self.maintained += 1;
        false
    }
}

/// The scheduling contract, reduced to the call the audit brackets.
trait SpikeService {
    fn group_id(&self) -> &ConnectionGroupId;
    fn tick(&mut self) -> bool;
}

/// The blanket delegation that keeps borrowed Services ergonomic.
impl<S: SpikeService> SpikeService for &mut S {
    fn group_id(&self) -> &ConnectionGroupId {
        (**self).group_id()
    }

    fn tick(&mut self) -> bool {
        (**self).tick()
    }
}

struct DutifulLeaf {
    group: ConnectionGroup,
}

impl SpikeService for DutifulLeaf {
    fn group_id(&self) -> &ConnectionGroupId {
        &self.group.identity
    }

    fn tick(&mut self) -> bool {
        self.group.maintain()
    }
}

struct ForgetfulLeaf {
    group: ConnectionGroup,
}

impl SpikeService for ForgetfulLeaf {
    fn group_id(&self) -> &ConnectionGroupId {
        &self.group.identity
    }

    fn tick(&mut self) -> bool {
        false
    }
}

/// An outer Service owning a lower one: only the root is scheduled, and the
/// leaf's obligation is satisfied through delegation.
struct Composer {
    lower: DutifulLeaf,
}

impl SpikeService for Composer {
    fn group_id(&self) -> &ConnectionGroupId {
        self.lower.group_id()
    }

    fn tick(&mut self) -> bool {
        self.lower.tick()
    }
}

/// What the network does per root Service: arm through a shared borrow, tick
/// through a mutable one, then check through a shared borrow again.
fn tick_services<S: SpikeService>(services: &mut [S]) -> bool {
    let mut worked = false;
    for service in services.iter_mut() {
        service.group_id().arm_maintain();
        worked |= service.tick();
        service.group_id().assert_maintained();
    }
    worked
}

#[test]
fn the_audit_brackets_a_tick_without_a_borrow_conflict() {
    let poll = Poll::new().unwrap();
    let (_core, group) = core_and_group(&poll, Token(0));
    let mut leaf = DutifulLeaf { group };

    // Both the owned and the borrowed form go through the same driver.
    tick_services(&mut [&mut leaf]);
    tick_services(std::slice::from_mut(&mut leaf));
}

#[test]
fn a_composed_service_satisfies_the_leaf_obligation_by_delegating() {
    let poll = Poll::new().unwrap();
    let (_core, group) = core_and_group(&poll, Token(0));
    let mut root = Composer { lower: DutifulLeaf { group } };

    tick_services(&mut [&mut root]);

    assert_eq!(root.lower.group.maintained, 1, "the tick reached the leaf's group");
}

#[test]
#[should_panic(expected = "ticked without reaching ConnectionGroup::maintain")]
fn a_leaf_that_skips_maintain_is_caught_after_its_tick() {
    let poll = Poll::new().unwrap();
    let (_core, group) = core_and_group(&poll, Token(0));
    let mut leaf = ForgetfulLeaf { group };

    tick_services(&mut [&mut leaf]);
}

#[test]
fn an_external_core_clones_the_callers_registry_and_classifies_its_tokens() {
    // External mode: the caller owns the poll and the core registers on a
    // clone of its registry, with every network token at or above the base.
    let mut caller_poll = Poll::new().unwrap();
    let base = Token(1024);
    let (core, mut group) = core_and_group(&caller_poll, base);

    // A source of the caller's own, below the base.
    let waker = mio::Waker::new(caller_poll.registry(), Token(7)).unwrap();

    let bound = group.listen((Ipv4Addr::LOCALHOST, 0).into()).unwrap();
    let _client = std::net::TcpStream::connect(bound).unwrap();
    waker.wake().unwrap();

    let mut events = Events::with_capacity(8);
    caller_poll.poll(&mut events, Some(Duration::from_secs(5))).unwrap();

    let high_water = core.next_token.load(Ordering::Relaxed);
    let is_ours = |token: Token| (base.0..high_water).contains(&token.0);
    let (ours, theirs): (Vec<Token>, Vec<Token>) =
        events.iter().map(|event| event.token()).partition(|token| is_ours(*token));

    assert_eq!(ours, [group.listeners[0].0], "the listener token is the network's");
    assert_eq!(theirs, [Token(7)], "the caller's waker token is handed back");
}
