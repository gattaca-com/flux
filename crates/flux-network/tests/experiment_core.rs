//! Environmental probes for the shared registration core: what mio's
//! registry-clone semantics let the design promise, and the auto traits the
//! ownership experiment's types must keep.
//!
//! The two probed facts belong to the OS and mio rather than to flux: a
//! registry clone accepts registrations after its poll is gone, which is what
//! makes dropping a network before its services inert rather than an error;
//! and a caller's poll sees the network's tokens beside its own, classified
//! by nothing but the token range. Everything flux builds on those facts is
//! tested against the real crate.

use std::{
    io,
    net::{Ipv4Addr, SocketAddr},
    sync::atomic::{AtomicUsize, Ordering},
    time::Duration,
};

use flux_network::stream::{ConnectionGroup, ConnectionGroupId, StreamNetwork};
use mio::{Events, Interest, Poll, Registry, Token, net::TcpListener};

fn assert_send<T: Send>() {}
fn assert_sync<T: Sync>() {}

/// The registration half a network shares with its groups: a clone of the
/// poll's registry, and the token space allocated from it.
struct RegistrationCore {
    registry: Registry,
    next_token: AtomicUsize,
}

impl RegistrationCore {
    fn new(poll: &Poll, base: Token) -> Self {
        Self {
            registry: poll.registry().try_clone().expect("registry clone"),
            next_token: AtomicUsize::new(base.0),
        }
    }

    /// Registers a fresh listener through the clone, handing back the socket
    /// — dropping it would deregister — with its address and token.
    fn listen(&self, addr: SocketAddr) -> io::Result<(TcpListener, SocketAddr, Token)> {
        let mut socket = TcpListener::bind(addr)?;
        let bound = socket.local_addr()?;
        let token = Token(self.next_token.fetch_add(1, Ordering::Relaxed));
        self.registry.register(&mut socket, token, Interest::READABLE)?;
        Ok((socket, bound, token))
    }
}

#[test]
fn the_real_network_types_keep_their_auto_traits() {
    assert_send::<Registry>();
    assert_sync::<Registry>();
    assert_send::<ConnectionGroupId>();
    assert_sync::<ConnectionGroupId>();
    assert_send::<ConnectionGroup>();
    assert_send::<StreamNetwork>();
}

#[test]
fn a_registry_clone_outlives_its_poll_and_registers_into_nothing() {
    let poll = Poll::new().unwrap();
    let core = RegistrationCore::new(&poll, Token(0));
    drop(poll);

    // Whether this errors or silently succeeds decides what the design can
    // promise about dropping a network before its services.
    let outcome = core.listen((Ipv4Addr::LOCALHOST, 0).into());
    assert!(outcome.is_ok(), "the clone accepts registrations with no poll behind it");
}

#[test]
fn an_external_core_clones_the_callers_registry_and_classifies_its_tokens() {
    // External mode: the caller owns the poll, the core registers on a clone
    // of its registry, and every network token sits at or above the base.
    let mut caller_poll = Poll::new().unwrap();
    let base = Token(1024);
    let core = RegistrationCore::new(&caller_poll, base);

    // A source of the caller's own, below the base.
    let waker = mio::Waker::new(caller_poll.registry(), Token(7)).unwrap();

    let (_listener, bound, listener_token) = core.listen((Ipv4Addr::LOCALHOST, 0).into()).unwrap();
    let _client = std::net::TcpStream::connect(bound).unwrap();
    waker.wake().unwrap();

    let mut events = Events::with_capacity(8);
    caller_poll.poll(&mut events, Some(Duration::from_secs(5))).unwrap();

    let high_water = core.next_token.load(Ordering::Relaxed);
    let is_ours = |token: Token| (base.0..high_water).contains(&token.0);
    let (ours, theirs): (Vec<Token>, Vec<Token>) =
        events.iter().map(|event| event.token()).partition(|token| is_ours(*token));

    assert_eq!(ours, [listener_token], "the listener token is the network's");
    assert_eq!(theirs, [Token(7)], "the caller's waker token is handed back");
}
