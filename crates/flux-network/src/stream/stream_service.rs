//! A service for a group whose messages are the protocol.
//!
//! [`StreamService`] owns one [`ConnectionGroup`] of either framing and
//! implements [`Service`] for it, so a tile that speaks its own message format
//! over the group writes no scheduling code: the group's transport work runs
//! in the service's tick and its deadline folds into the service's, once, in
//! flux. Where the group's events go is the service's one type parameter, a
//! [`StreamSink`], and the two ways of using it differ only there.
//!
//! **Pulled after the iteration** — the default sink, [`Retained`]. Every
//! event the group emits during a network iteration is recorded, each payload
//! copied once into storage the service owns, and the tile pulls them back
//! afterwards with [`StreamService::next_event`] in arrival order. This is the
//! shape for a handler that needs state the service cannot own, the tile's
//! own, and it reads like the HTTP service's pull loop.
//!
//! ```no_run
//! use flux_network::stream::{
//!     ConnectionGroupConfig, Endpoint, StreamEvent, StreamNetwork, StreamService,
//! };
//! use flux_timing::Duration;
//!
//! let mut net = StreamNetwork::default();
//! let config = ConnectionGroupConfig { name: "feed", ..ConnectionGroupConfig::default() };
//! let mut feed = StreamService::new(net.add_group(config));
//! feed.listen(Endpoint::Tcp("127.0.0.1:9000".parse().unwrap()))?;
//!
//! let mut greet = Vec::new();
//! let mut received = 0;
//! loop {
//!     net.drive(Some(Duration::from_millis(1)), &mut [&mut feed]);
//!     while let Some(event) = feed.next_event() {
//!         match event {
//!             StreamEvent::Accepted { token, .. } => greet.push(token),
//!             StreamEvent::Message { payload, .. } => received += payload.len(),
//!             _ => {}
//!         }
//!     }
//!     // The service is free again once the pull loop ends: sends go by token.
//!     for token in greet.drain(..) {
//!         feed.send_with(token, |out| out.extend_from_slice(b"hello"));
//!     }
//! }
//! # Ok::<(), std::io::Error>(())
//! ```
//!
//! **Delivered during the iteration** — a sink of the caller's, moved into the
//! service with [`StreamService::with_sink`]. The sink receives each event as
//! the group emits it, with a message's payload borrowed straight from the
//! connection's read buffer: no copy beyond the kernel's. The sink holds only
//! what was moved into it and cannot reach the group while it runs, so a
//! handler that forwards, counts or parses in place fits, and one that must
//! send back does so by token from the tile afterwards.
//!
//! ```no_run
//! use flux_network::stream::{
//!     ConnectionGroupConfig, Endpoint, StreamEvent, StreamNetwork, StreamService, StreamSink,
//! };
//! use flux_timing::Duration;
//!
//! /// Stands in for a channel sender, or anything else that consumes bytes
//! /// without keeping them.
//! struct Forward {
//!     forwarded: usize,
//! }
//!
//! impl StreamSink for Forward {
//!     fn on_event(&mut self, event: StreamEvent<'_>) {
//!         if let StreamEvent::Message { payload, .. } = event {
//!             self.forwarded += payload.len();
//!         }
//!     }
//!
//!     // Everything is consumed inside `on_event`: nothing is held.
//!     fn has_pending(&self) -> bool {
//!         false
//!     }
//! }
//!
//! let mut net = StreamNetwork::default();
//! let config = ConnectionGroupConfig { name: "shreds", ..ConnectionGroupConfig::default() };
//! let mut shreds = StreamService::with_sink(net.add_group(config), Forward { forwarded: 0 });
//! let upstream = shreds.connect(Endpoint::Tcp("10.0.0.1:9000".parse().unwrap()));
//!
//! loop {
//!     net.drive(Some(Duration::from_millis(1)), &mut [&mut shreds]);
//!     println!("{} bytes so far from {upstream:?}", shreds.sink().forwarded);
//! }
//! # Ok::<(), std::io::Error>(())
//! ```
//!
//! A tile hosting services of both kinds, or of different sink types, drives
//! them through a caller-owned enum that implements [`Service`] by matching;
//! services of one type go in a plain slice.
//!
//! The pulled mode is the default because its one copy is cheap against the
//! rest of an iteration at the frame sizes the consumers send, kilobytes to
//! tens of kilobytes, and stays a small share of a sub-millisecond budget
//! even at a megabyte. A sink is the choice for a handler that consumes in
//! place anyway, a forwarder or a parser, and for frames whose copy has
//! become a share of the budget worth measuring.
//!
//! # What a payload borrow is good for
//! In both modes a message's payload is lent for exactly one call: the sink's
//! [`StreamSink::on_event`], or the [`StreamService::next_event`] pull whose
//! event holds it. Nothing about where the bytes live is promised beyond that
//! call — not that they stay in the connection's buffer, nor that the buffer
//! is the caller's memory at all — so a payload kept for later is copied by
//! whoever keeps it. Which I/O engine fills the buffer is
//! [`ConnectionGroupConfig`](super::ConnectionGroupConfig) territory and
//! changes nothing here.
//!
//! # Storage in the pulled mode
//! [`Retained`] keeps one vector of fixed-size records and one byte arena.
//! Both start over, keeping their capacity, the first time an event arrives
//! after everything recorded has been pulled, so a tile that drains the
//! service every iteration allocates nothing once the two have grown to an
//! iteration's worth. A tile that pulls a bounded number of events per
//! iteration under sustained inbound never reaches that point: records and
//! bytes accumulate behind its cursor until it catches up, and neither store
//! shrinks afterwards.
//!
//! What is retained is an unread channel, and pulling it is the tile's
//! obligation: the service reports the unpulled events as work on every
//! iteration, so the loop that drives the network never sleeps while they
//! accumulate — a tile that stops pulling spins visibly rather than parking
//! over a growing store. Past a threshold of retained bytes the sink also
//! warns, at most once every ten seconds, the same posture and the same
//! default as the transport's own send backlog: growth is allowed and
//! reported, and a bound on what a peer may send is the transport's to
//! enforce, not the sink's. [`Retained::warn_at`] sets the threshold for a
//! sink passed to [`StreamService::with_sink`].

use std::io;

use flux_timing::{Duration, Instant, Nanos};
use mio::{Token, event::Event};
use tracing::warn;

use super::{
    ConnectionGroup, ConnectionGroupId, Deadline, Endpoint, PayloadBuf, Peer, ReadinessOutcome,
    Service, StreamEvent, StreamNetwork, TickOutcome,
};

/// Where a [`StreamService`] delivers the transport events of its group.
///
/// The service calls [`Self::on_event`] from inside the group's readiness
/// handling and its tick, so a sink runs during the network iteration, before
/// the tile regains control. It reaches nothing but itself: not the group,
/// whose borrow the call holds, and not the tile's state, unless that state
/// was moved into the sink.
pub trait StreamSink {
    /// Takes one transport event.
    ///
    /// A message's payload is lent for this call only. Keep the bytes and the
    /// sink copies them; parse or forward them and nothing is copied.
    fn on_event(&mut self, event: StreamEvent<'_>);

    /// Whether this sink holds anything that will be consumed later.
    ///
    /// Return `true` while anything taken in an earlier call is still
    /// waiting: a recorded event, a partial batch, a parked message. What a
    /// sink holds is work exposed upward, and it rides the network's did-work
    /// report, never the service's deadline: the loop driving the network
    /// keeps going while the report says so and parks only when nothing is
    /// held. A `false` over held work is therefore the one way a sink can
    /// strand what it holds — the loop sleeps until the next readiness event
    /// and the held work waits with it — which is why every sink states this
    /// itself. A sink that consumes everything inside [`Self::on_event`]
    /// holds nothing and returns `false`.
    ///
    /// ```compile_fail
    /// // A sink cannot leave its pending report unstated.
    /// use flux_network::stream::{StreamEvent, StreamSink};
    /// struct Forward;
    /// impl StreamSink for Forward {
    ///     fn on_event(&mut self, _: StreamEvent<'_>) {}
    /// }
    /// ```
    fn has_pending(&self) -> bool;
}

/// Retained bytes past which the default sink warns: the transport's own
/// backlog-warning default.
const DEFAULT_RETAINED_WARN_BYTES: usize = 64 * 1024 * 1024;
/// How long the sink stays quiet after one warning.
const RETAINED_WARNING_INTERVAL_SECS: u64 = 10;

/// The default sink: every event recorded during the iteration, pulled after
/// it in arrival order.
///
/// Records are fixed-size and `Copy`; a message's bytes go into one arena and
/// its record keeps their offset and length. The module documentation covers
/// when the two stores start over, when they grow, and when the sink warns.
pub struct Retained {
    records: Vec<Record>,
    /// The next record to pull; everything before it has been.
    cursor: usize,
    arena: Vec<u8>,
    /// Retained bytes at or past which the sink warns; `None` never warns.
    warn_bytes: Option<usize>,
    last_warning: Option<Instant>,
}

/// A sink warning at the transport's backlog default, as [`StreamService::new`]
/// makes it.
impl Default for Retained {
    fn default() -> Self {
        Self::warn_at(Some(DEFAULT_RETAINED_WARN_BYTES))
    }
}

/// One recorded event: which connection, and what.
#[derive(Clone, Copy)]
struct Record {
    token: Token,
    kind: Kind,
}

#[derive(Clone, Copy)]
enum Kind {
    Accepted(Peer),
    Connected(Peer),
    /// The payload is `arena[start..start + len]`.
    Message {
        start: usize,
        len: usize,
        send_ts: Nanos,
    },
    Disconnected(Peer),
}

impl Retained {
    /// A sink that warns while the bytes it holds unpulled are at or past
    /// `bytes`, at most once every ten seconds; `None` never warns. Pass the
    /// result to [`StreamService::with_sink`].
    pub fn warn_at(bytes: Option<usize>) -> Self {
        Self {
            records: Vec::new(),
            cursor: 0,
            arena: Vec::new(),
            warn_bytes: bytes,
            last_warning: None,
        }
    }

    fn pending(&self) -> usize {
        self.records.len() - self.cursor
    }

    /// Whether a warning is due at `now`, given the threshold is met, and
    /// records it as given.
    fn should_warn(&mut self, now: Instant) -> bool {
        let quiet = Duration::from_secs(RETAINED_WARNING_INTERVAL_SECS);
        if self.last_warning.is_some_and(|last| now.saturating_sub(last) < quiet) {
            return false;
        }
        self.last_warning = Some(now);
        true
    }

    fn next_event(&mut self) -> Option<StreamEvent<'_>> {
        let Record { token, kind } = *self.records.get(self.cursor)?;
        self.cursor += 1;
        Some(match kind {
            Kind::Accepted(peer) => StreamEvent::Accepted { token, peer },
            Kind::Connected(peer) => StreamEvent::Connected { token, peer },
            Kind::Message { start, len, send_ts } => {
                StreamEvent::Message { token, payload: &self.arena[start..start + len], send_ts }
            }
            Kind::Disconnected(peer) => StreamEvent::Disconnected { token, peer },
        })
    }
}

impl StreamSink for Retained {
    fn on_event(&mut self, event: StreamEvent<'_>) {
        // Everything recorded has been pulled: both stores start over and
        // keep their capacity, so a drained service reuses what it has.
        if self.cursor == self.records.len() {
            self.records.clear();
            self.arena.clear();
            self.cursor = 0;
        }
        let (token, kind) = match event {
            StreamEvent::Accepted { token, peer } => (token, Kind::Accepted(peer)),
            StreamEvent::Connected { token, peer } => (token, Kind::Connected(peer)),
            StreamEvent::Message { token, payload, send_ts } => {
                let start = self.arena.len();
                self.arena.extend_from_slice(payload);
                (token, Kind::Message { start, len: payload.len(), send_ts })
            }
            StreamEvent::Disconnected { token, peer } => (token, Kind::Disconnected(peer)),
        };
        self.records.push(Record { token, kind });
        if self.warn_bytes.is_some_and(|limit| self.arena.len() >= limit) &&
            self.should_warn(Instant::now())
        {
            warn!(
                retained_bytes = self.arena.len(),
                pending = self.pending(),
                "retained inbound is past its warning threshold: the tile is not pulling"
            );
        }
    }

    fn has_pending(&self) -> bool {
        self.cursor < self.records.len()
    }
}

/// A service over one [`ConnectionGroup`] whose messages the caller handles
/// as they are: raw chunks or length-prefixed frames, delivered to a
/// [`StreamSink`].
///
/// The network schedules the service — pass it to
/// [`StreamNetwork::drive`] — and the caller either pulls events with
/// [`Self::next_event`] under the default sink or reads its own sink back with
/// [`Self::sink`]. Normal operations act on the group the service owns, so
/// none of them takes a network.
pub struct StreamService<S: StreamSink = Retained> {
    group: ConnectionGroup,
    sink: S,
}

impl StreamService<Retained> {
    /// Takes over `group`, recording its events for the caller to pull after
    /// each iteration. Moving the group in is the claim: the service owns its
    /// transport state from here, and no other service can hold it.
    pub fn new(group: ConnectionGroup) -> Self {
        Self { group, sink: Retained::default() }
    }

    /// The next recorded event, oldest first.
    ///
    /// The event borrows the service until it is dropped, so a pull loop
    /// reaches the transport only once it ends: sends and disconnects go by
    /// token afterwards. A caller may stop pulling after any number of events
    /// and resume in a later iteration with nothing lost.
    pub fn next_event(&mut self) -> Option<StreamEvent<'_>> {
        self.sink.next_event()
    }

    /// Events recorded and not yet pulled.
    pub fn pending(&self) -> usize {
        self.sink.pending()
    }
}

impl<S: StreamSink> StreamService<S> {
    /// Takes over `group`, delivering its events to `sink` as they happen.
    /// Moving the group in is the claim: the service owns its transport state
    /// from here, and no other service can hold it.
    pub fn with_sink(group: ConnectionGroup, sink: S) -> Self {
        Self { group, sink }
    }

    /// The sink, to read back what it kept.
    pub fn sink(&self) -> &S {
        &self.sink
    }

    /// The sink, to take from it or reset it between iterations.
    pub fn sink_mut(&mut self) -> &mut S {
        &mut self.sink
    }

    /// Adds a listener, and reports the endpoint it bound.
    ///
    /// That endpoint is the one asked for, except for a TCP address whose
    /// port is `0`: the kernel picks the port, and what comes back is the
    /// address a peer must dial. An [`Endpoint::Unix`] socket file is created
    /// with mode `0777` less the umask bits and is unlinked when the service
    /// is closed; see [`ConnectionGroup::listen`].
    pub fn listen(&mut self, endpoint: Endpoint) -> io::Result<Endpoint> {
        self.group.listen(endpoint)
    }

    /// Adds a persistent outbound endpoint and starts connecting to it. The
    /// token it returns identifies the connection for its whole life,
    /// reconnects included.
    #[must_use = "the token identifies the outbound endpoint"]
    pub fn connect(&mut self, endpoint: Endpoint) -> Token {
        self.group.connect(endpoint)
    }

    /// Serialises one payload and sends it to a connected token, framed as
    /// the group frames. The closure is not called for a token that is
    /// unknown, disconnected or closing.
    pub fn send_with<F>(&mut self, token: Token, serialise: F) -> bool
    where
        F: FnOnce(&mut PayloadBuf<'_>),
    {
        self.group.send_with(token, serialise)
    }

    /// Serialises several payloads and sends them to a connected token as one
    /// batch: one socket write when the connection has no backlog, and under
    /// length-prefixed framing one frame per payload sharing one send
    /// timestamp. Payloads that are empty or exceed the frame size are
    /// skipped; see [`ConnectionGroup::send_many_with`].
    pub fn send_many_with<I, F>(&mut self, token: Token, items: I, serialise: F) -> bool
    where
        I: IntoIterator,
        F: FnMut(&mut PayloadBuf<'_>, I::Item),
    {
        self.group.send_many_with(token, items, serialise)
    }

    /// Serialises one payload and sends it to every connected token, reporting
    /// how many were attempted.
    pub fn broadcast_with<F>(&mut self, serialise: F) -> usize
    where
        F: FnOnce(&mut PayloadBuf<'_>),
    {
        self.group.broadcast_with(serialise)
    }

    /// Serialises several payloads once and sends the batch to every
    /// connected token, reporting how many were attempted; see
    /// [`ConnectionGroup::broadcast_many_with`].
    pub fn broadcast_many_with<I, F>(&mut self, items: I, serialise: F) -> usize
    where
        I: IntoIterator,
        F: FnMut(&mut PayloadBuf<'_>, I::Item),
    {
        self.group.broadcast_many_with(items, serialise)
    }

    /// Closes one connection now; its disconnect reaches the sink on the next
    /// tick.
    pub fn disconnect(&mut self, token: Token) -> bool {
        self.group.disconnect(token)
    }

    /// Closes one connection once its queued bytes have reached the peer.
    pub fn disconnect_when_drained(&mut self, token: Token) -> bool {
        self.group.disconnect_when_drained(token)
    }

    /// Shuts the write side of one connection once its queued bytes have
    /// reached the peer, leaving the read side open.
    pub fn shutdown_write_when_drained(&mut self, token: Token) -> bool {
        self.group.shutdown_write_when_drained(token)
    }

    /// Drops one connection and any outbound endpoint behind it, without a
    /// disconnect event.
    pub fn remove(&mut self, token: Token) -> bool {
        self.group.remove(token)
    }

    /// Connections this service refused because its group was at its
    /// connection cap.
    pub fn refused_connections(&self) -> u64 {
        self.group.refused_connections()
    }

    /// Closes the service: every connection and listener of its group goes,
    /// whatever the sink still holds goes with it, and the group's slot
    /// closes.
    ///
    /// This is the one operation that names the network, because ending a
    /// group's life is the network's bookkeeping rather than the service's.
    /// Dropping a service without closing it leaves an open slot with no
    /// service, which the next scheduling call reports.
    pub fn close(self, net: &mut StreamNetwork) {
        net.remove_group(self.group);
    }
}

impl<S: StreamSink> Service for StreamService<S> {
    fn group_id(&self) -> &ConnectionGroupId {
        self.group.group_id()
    }

    fn handle_event(&mut self, readiness: &Event) -> ReadinessOutcome {
        let Self { group, sink } = self;
        group.handle_event(readiness, &mut |event: StreamEvent<'_>| sink.on_event(event))
    }

    fn tick(&mut self, now: Instant) -> TickOutcome {
        let Self { group, sink } = self;
        let maintained = group.maintain(now, &mut |event: StreamEvent<'_>| sink.on_event(event));
        // What the sink holds for the tile is work exposed upward: it rides
        // the did-work report and never the deadline.
        maintained.or_worked(sink.has_pending())
    }

    fn next_deadline(&self, now: Instant) -> Deadline {
        self.group.next_deadline(now)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn message(token: usize, payload: &[u8]) -> StreamEvent<'_> {
        StreamEvent::Message { token: Token(token), payload, send_ts: Nanos(token as u64) }
    }

    fn pulled(sink: &mut Retained) -> Option<(Token, Vec<u8>)> {
        match sink.next_event()? {
            StreamEvent::Message { token, payload, send_ts } => {
                assert_eq!(send_ts, Nanos(token.0 as u64), "the wire timestamp rides the record");
                Some((token, payload.to_vec()))
            }
            StreamEvent::Accepted { token, .. } => Some((token, b"accepted".to_vec())),
            StreamEvent::Connected { token, .. } => Some((token, b"connected".to_vec())),
            StreamEvent::Disconnected { token, .. } => Some((token, b"disconnected".to_vec())),
        }
    }

    #[test]
    fn events_pull_in_arrival_order_across_kinds_and_connections() {
        let mut sink = Retained::default();
        sink.on_event(StreamEvent::Accepted { token: Token(1), peer: Peer::Unix });
        sink.on_event(message(1, b"first"));
        sink.on_event(StreamEvent::Connected { token: Token(2), peer: Peer::Unix });
        sink.on_event(message(2, b"second"));
        sink.on_event(message(1, b""));
        sink.on_event(StreamEvent::Disconnected { token: Token(1), peer: Peer::Unix });
        assert_eq!(sink.pending(), 6);
        assert!(sink.has_pending());

        let mut order = Vec::new();
        while let Some(event) = pulled(&mut sink) {
            order.push(event);
        }
        assert_eq!(order, vec![
            (Token(1), b"accepted".to_vec()),
            (Token(1), b"first".to_vec()),
            (Token(2), b"connected".to_vec()),
            (Token(2), b"second".to_vec()),
            (Token(1), Vec::new()),
            (Token(1), b"disconnected".to_vec()),
        ]);
        assert_eq!(sink.pending(), 0);
        assert!(!sink.has_pending());
    }

    #[test]
    fn a_bounded_pull_loses_nothing_across_iterations() {
        let mut sink = Retained::default();
        sink.on_event(message(1, b"a"));
        sink.on_event(message(1, b"b"));
        sink.on_event(message(1, b"c"));
        assert_eq!(pulled(&mut sink), Some((Token(1), b"a".to_vec())));
        // The next iteration records more before the rest has been pulled.
        sink.on_event(message(2, b"d"));
        sink.on_event(message(2, b"e"));
        assert_eq!(sink.pending(), 4);

        let mut rest = Vec::new();
        while let Some((_, payload)) = pulled(&mut sink) {
            rest.push(payload);
        }
        assert_eq!(rest, [b"b".to_vec(), b"c".to_vec(), b"d".to_vec(), b"e".to_vec()]);
    }

    #[test]
    fn both_stores_start_over_once_everything_is_pulled() {
        let mut sink = Retained::default();
        sink.on_event(message(1, &[1; 100]));
        sink.on_event(message(1, &[2; 100]));
        while pulled(&mut sink).is_some() {}
        assert_eq!((sink.records.len(), sink.arena.len(), sink.cursor), (2, 200, 2));

        let records = sink.records.capacity();
        let arena = sink.arena.capacity();
        sink.on_event(message(1, &[3; 50]));
        assert_eq!((sink.records.len(), sink.arena.len(), sink.cursor), (1, 50, 0));
        assert_eq!((sink.records.capacity(), sink.arena.capacity()), (records, arena));
        assert_eq!(pulled(&mut sink), Some((Token(1), vec![3; 50])));
    }

    #[test]
    fn undrained_stores_grow_behind_the_cursor() {
        let mut sink = Retained::default();
        for round in 0..10 {
            sink.on_event(message(1, &[round; 10]));
            sink.on_event(message(1, &[round; 10]));
            // One pulled per two recorded: the cursor never catches up.
            assert!(pulled(&mut sink).is_some());
        }
        assert_eq!((sink.records.len(), sink.arena.len(), sink.cursor), (20, 200, 10));
        assert_eq!(sink.pending(), 10);
    }

    #[test]
    fn the_warning_trips_at_the_threshold_and_not_below() {
        let mut sink = Retained::warn_at(Some(100));
        sink.on_event(message(1, &[0; 50]));
        assert!(sink.last_warning.is_none(), "below the threshold");
        sink.on_event(message(1, &[0; 60]));
        assert!(sink.last_warning.is_some(), "at or past it");

        let mut quiet = Retained::warn_at(None);
        quiet.on_event(message(1, &[0; 1000]));
        assert!(quiet.last_warning.is_none(), "None never warns");
    }

    #[test]
    fn one_warning_per_interval() {
        let mut sink = Retained::warn_at(Some(1));
        let first = Instant(1);
        assert!(sink.should_warn(first));
        assert!(!sink.should_warn(first + Duration::from_secs(9)), "quiet inside the interval");
        assert!(sink.should_warn(first + Duration::from_secs(10)), "due again after it");
    }

    #[test]
    fn the_default_threshold_is_the_transports() {
        assert_eq!(Retained::default().warn_bytes, Some(64 * 1024 * 1024));
    }
}
