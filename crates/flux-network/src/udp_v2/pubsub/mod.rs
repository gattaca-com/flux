//! One-way UDP: a [`Publisher`] sends one stream, the same bytes to every
//! [`Subscriber`], which joins at its live end. A subscriber that falls a
//! `send_window` behind or goes silent for `peer_timeout` is dropped, and
//! resubscribes at the live end once it hears of it. A subscriber never times
//! out its publisher.
//!
//! Both own a poll by default. To share one with a
//! [`crate::NetworkWithExternalPoll`] or each other, build them with
//! `with_external_poll`, give each its own token range, and route each
//! readiness event to the owner of its token.

use std::{io, net::SocketAddr, ops::Range};

use flux_timing::Nanos;
use mio::{Events, Poll, Registry, Token, event::Event as MioEvent};

use crate::{
    network::{PayloadBuf, UdpGroupConfigV2},
    udp_v2::sys::RecvBatch,
};

mod publisher;
mod subscriber;
mod wire;

use publisher::PublisherState;
use subscriber::SubscriberState;

const EVENTS_CAPACITY: usize = 128;

pub enum PublisherEvent<'a> {
    /// A subscriber joined at the live end, with its on-connect message,
    /// empty without one. [`Publisher::disconnect`] turns it away.
    Subscribed { token: Token, peer_addr: SocketAddr, msg: &'a [u8] },
    /// A subscriber timed out, fell a window behind, resubscribed or was
    /// disconnected.
    Unsubscribed { token: Token, peer_addr: SocketAddr },
}

pub enum SubscriberEvent<'a> {
    /// Joined the stream at its live end, with the publisher's on-connect
    /// message, empty without one.
    Subscribed { token: Token, peer_addr: SocketAddr, msg: &'a [u8] },
    /// The publisher dropped the subscription, or it was resubscribed
    /// locally. It resubscribes on its own.
    Unsubscribed { token: Token, peer_addr: SocketAddr },
    /// Messages arrive unordered.
    Message { token: Token, payload: &'a [u8], send_ts: Nanos },
}

pub(crate) struct TokenRange {
    next: usize,
    end: usize,
}

impl TokenRange {
    fn new(range: Range<usize>) -> Self {
        Self { next: range.start, end: range.end }
    }

    pub(crate) fn allocate(&mut self) -> Option<Token> {
        if self.next == self.end {
            return None;
        }
        self.next += 1;
        Some(Token(self.next - 1))
    }
}

fn token_range_exhausted() -> io::Error {
    io::Error::other("udp token range exhausted")
}

enum Poller {
    Owned(Poll, Events),
    External(Registry),
}

impl Poller {
    fn owned() -> Self {
        Self::Owned(
            Poll::new().expect("failed to create poll"),
            Events::with_capacity(EVENTS_CAPACITY),
        )
    }

    #[inline]
    fn registry(&self) -> &Registry {
        match self {
            Self::Owned(poll, _) => poll.registry(),
            Self::External(registry) => registry,
        }
    }

    /// One nonblocking poll; `true` if anything was ready.
    fn poll(&mut self, mut on_event: impl FnMut(&Registry, &MioEvent)) -> bool {
        let Self::Owned(poll, events) = self else {
            panic!("poll_with needs an owned poll; drive an external one with handle_event")
        };
        if let Err(err) = poll.poll(events, Some(std::time::Duration::ZERO)) {
            if err.kind() != io::ErrorKind::Interrupted {
                flux_utils::safe_panic!("couldn't poll udp: {err}");
            }
            return false;
        }
        for event in &*events {
            on_event(poll.registry(), event);
        }
        !events.is_empty()
    }
}

/// Sends one stream to every subscriber of its listeners.
pub struct Publisher {
    poller: Poller,
    recv: RecvBatch,
    state: PublisherState,
}

impl Publisher {
    pub fn new(config: UdpGroupConfigV2) -> Self {
        Self::with_poller(config, Poller::owned(), 0..usize::MAX)
    }

    /// Registers on `registry` with tokens from `tokens`; see the module docs.
    pub fn with_external_poll(
        config: UdpGroupConfigV2,
        registry: Registry,
        tokens: Range<usize>,
    ) -> Self {
        Self::with_poller(config, Poller::External(registry), tokens)
    }

    fn with_poller(config: UdpGroupConfigV2, poller: Poller, tokens: Range<usize>) -> Self {
        Self {
            poller,
            recv: RecvBatch::new(config.udp.max_datagram_size),
            state: PublisherState::new(config, TokenRange::new(tokens)),
        }
    }

    /// Returns the listener token, which can be passed to `remove`.
    pub fn listen(&mut self, addr: SocketAddr) -> io::Result<Token> {
        self.state.listen(self.poller.registry(), addr)
    }

    /// Returns the number of subscribers it goes to, 0 if none: a message
    /// published to no one is dropped.
    pub fn broadcast_with<F>(&mut self, serialise: F) -> usize
    where
        F: FnOnce(&mut PayloadBuf<'_>),
    {
        self.state.broadcast_with(self.poller.registry(), serialise)
    }

    pub fn broadcast_many_with<I, F>(&mut self, items: I, serialise: F) -> usize
    where
        I: IntoIterator,
        F: FnMut(&mut PayloadBuf<'_>, I::Item),
    {
        self.state.broadcast_many_with(self.poller.registry(), items, serialise)
    }

    /// Drops a subscriber. It hears of it once it next acks and resubscribes.
    pub fn disconnect(&mut self, token: Token) -> bool {
        self.state.disconnect(token)
    }

    /// Drops a subscriber once it acked everything published so far. It gets
    /// nothing newer meanwhile.
    pub fn disconnect_when_drained(&mut self, token: Token) -> bool {
        self.state.disconnect_when_drained(token)
    }

    /// Drops a subscriber without an `Unsubscribed`, or closes a listener and
    /// drops its subscribers.
    pub fn remove(&mut self, token: Token) -> bool {
        self.state.remove(self.poller.registry(), token)
    }

    /// One nonblocking poll with due timers. Returns whether anything was
    /// processed. Needs an owned poll.
    pub fn poll_with<F>(&mut self, mut handler: F) -> bool
    where
        F: FnMut(PublisherEvent<'_>),
    {
        let Self { poller, recv, state } = self;
        let mut work = state.pre_poll(poller.registry(), &mut handler);
        work |=
            poller.poll(|registry, event| state.handle_event(recv, registry, event, &mut handler));
        work | state.post_poll(&mut handler)
    }

    pub fn max_poll_interval(&self) -> std::time::Duration {
        self.state.tick_interval().into()
    }

    pub fn pre_poll<F>(&mut self, handler: &mut F) -> bool
    where
        F: FnMut(PublisherEvent<'_>),
    {
        self.state.pre_poll(self.poller.registry(), handler)
    }

    pub fn handle_event<F>(&mut self, event: &MioEvent, handler: &mut F)
    where
        F: FnMut(PublisherEvent<'_>),
    {
        self.state.handle_event(&mut self.recv, self.poller.registry(), event, handler);
    }

    pub fn post_poll<F>(&mut self, handler: &mut F) -> bool
    where
        F: FnMut(PublisherEvent<'_>),
    {
        self.state.post_poll(handler)
    }
}

impl Drop for Publisher {
    fn drop(&mut self) {
        self.state.close_all(self.poller.registry());
    }
}

/// Reads publishers' streams, one subscription per publisher.
pub struct Subscriber {
    poller: Poller,
    recv: RecvBatch,
    state: SubscriberState,
}

impl Subscriber {
    pub fn new(config: UdpGroupConfigV2) -> Self {
        Self::with_poller(config, Poller::owned(), 0..usize::MAX)
    }

    /// Registers on `registry` with tokens from `tokens`; see the module docs.
    pub fn with_external_poll(
        config: UdpGroupConfigV2,
        registry: Registry,
        tokens: Range<usize>,
    ) -> Self {
        Self::with_poller(config, Poller::External(registry), tokens)
    }

    fn with_poller(config: UdpGroupConfigV2, poller: Poller, tokens: Range<usize>) -> Self {
        Self {
            poller,
            recv: RecvBatch::new(config.udp.max_datagram_size),
            state: SubscriberState::new(config, TokenRange::new(tokens)),
        }
    }

    /// Subscribes from a socket of its own, retrying until the publisher
    /// answers.
    pub fn subscribe(&mut self, addr: SocketAddr) -> io::Result<Token> {
        self.state.subscribe(self.poller.registry(), addr)
    }

    pub fn is_subscribed(&self, token: Token) -> bool {
        self.state.is_subscribed(token)
    }

    /// Leaves the stream and joins again at its live end.
    pub fn resubscribe(&mut self, token: Token) -> bool {
        self.state.resubscribe(token)
    }

    /// Closes a subscription for good, without an `Unsubscribed`. The
    /// publisher drops it once it times out.
    pub fn unsubscribe(&mut self, token: Token) -> bool {
        self.state.unsubscribe(self.poller.registry(), token)
    }

    /// One nonblocking poll with due timers. Returns whether anything was
    /// processed. Needs an owned poll.
    pub fn poll_with<F>(&mut self, mut handler: F) -> bool
    where
        F: FnMut(SubscriberEvent<'_>),
    {
        let Self { poller, recv, state } = self;
        let mut work = state.pre_poll(poller.registry(), &mut handler);
        work |=
            poller.poll(|registry, event| state.handle_event(recv, registry, event, &mut handler));
        work | state.post_poll(&mut handler)
    }

    pub fn max_poll_interval(&self) -> std::time::Duration {
        self.state.tick_interval().into()
    }

    pub fn pre_poll<F>(&mut self, handler: &mut F) -> bool
    where
        F: FnMut(SubscriberEvent<'_>),
    {
        self.state.pre_poll(self.poller.registry(), handler)
    }

    pub fn handle_event<F>(&mut self, event: &MioEvent, handler: &mut F)
    where
        F: FnMut(SubscriberEvent<'_>),
    {
        self.state.handle_event(&mut self.recv, self.poller.registry(), event, handler);
    }

    pub fn post_poll<F>(&mut self, handler: &mut F) -> bool
    where
        F: FnMut(SubscriberEvent<'_>),
    {
        self.state.post_poll(handler)
    }
}

impl Drop for Subscriber {
    fn drop(&mut self) {
        self.state.close_all(self.poller.registry());
    }
}
