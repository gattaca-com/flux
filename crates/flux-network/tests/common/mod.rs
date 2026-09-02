//! A downstream-style Service pair, written against nothing but the public
//! interface.
//!
//! These live in the test tree rather than behind a crate feature so flux
//! commits to no helper Service, and they are compiled as a separate crate
//! would compile them: whatever they need, a consuming crate can reach.
//!
//! [`RawService`] is a leaf — it owns a [`ConnectionGroup`] and turns
//! [`StreamEvent`] into its own pullable events through a generic callback and
//! an exhaustive match. [`RelayService`] is composed: it owns a `RawService`,
//! delegates every scheduling phase to it and consumes its lending events
//! through a bounded drain, which is the shape a Flux-provided Service must
//! support for a downstream one to be built on it.

#![allow(dead_code)]

use std::io;

use flux_network::{
    Token,
    stream::{
        ConnectionGroup, ConnectionGroupId, Deadline, Endpoint, Peer, ReadinessOutcome, Service,
        StreamEvent, TickOutcome,
    },
};
use flux_timing::Instant;
use mio::event::Event;

/// One lifecycle event a leaf Service recorded, in arrival order.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Record {
    Accepted { token: Token, peer: Peer },
    Connected { token: Token, peer: Peer },
    Disconnected { token: Token, peer: Peer },
}

/// One payload awaiting a drain, in the storage the Service owns: a message
/// lends its bytes only for its transport callback, so anything kept past that
/// callback is the protocol's to hold.
struct Inbound {
    token: Token,
    bytes: Vec<u8>,
}

/// What a drain lends its consumer: the payload borrowed from the leaf's own
/// storage, and a reply scoped to the connection it arrived on.
///
/// The two borrow disjoint fields of the same leaf, which is what lets a
/// containing Service answer without reborrowing the leaf it already holds.
pub enum RawEvent<'a> {
    Message { token: Token, payload: &'a [u8], reply: Reply<'a> },
}

/// A write path scoped to one connection, lent for the duration of one drain
/// callback.
pub struct Reply<'a> {
    group: &'a mut ConnectionGroup,
    token: Token,
}

impl Reply<'_> {
    /// Queues `bytes` on the connection this reply belongs to.
    pub fn send(self, bytes: &[u8]) -> bool {
        self.group.send_with(self.token, |out| out.extend_from_slice(bytes))
    }
}

/// A leaf Service: it owns one raw-framed group and its own protocol state.
pub struct RawService {
    group: ConnectionGroup,
    records: Vec<Record>,
    /// Payloads awaiting a drain, oldest first from `cursor`.
    inbox: Vec<Inbound>,
    cursor: usize,
    /// Storage handed back by full drains and reused, so a warm inbox
    /// allocates nothing.
    spare: Vec<Inbound>,
    /// A deadline the test asks the Service to report, to check that its
    /// group's deadline folds in beside it.
    deadline: Option<Instant>,
    ticks: usize,
}

impl RawService {
    #[must_use]
    pub fn new(group: ConnectionGroup) -> Self {
        Self {
            group,
            records: Vec::new(),
            inbox: Vec::new(),
            cursor: 0,
            spare: Vec::new(),
            deadline: None,
            ticks: 0,
        }
    }

    pub fn listen(&mut self, endpoint: Endpoint) -> io::Result<Endpoint> {
        self.group.listen(endpoint)
    }

    pub fn connect(&mut self, endpoint: Endpoint) -> Token {
        self.group.connect(endpoint)
    }

    pub fn send(&mut self, token: Token, bytes: &[u8]) -> bool {
        self.group.send_with(token, |out| out.extend_from_slice(bytes))
    }

    pub fn broadcast(&mut self, bytes: &[u8]) -> usize {
        self.group.broadcast_with(|out| out.extend_from_slice(bytes))
    }

    pub fn disconnect(&mut self, token: Token) -> bool {
        self.group.disconnect(token)
    }

    pub fn disconnect_when_drained(&mut self, token: Token) -> bool {
        self.group.disconnect_when_drained(token)
    }

    pub fn shutdown_write_when_drained(&mut self, token: Token) -> bool {
        self.group.shutdown_write_when_drained(token)
    }

    pub fn remove(&mut self, token: Token) -> bool {
        self.group.remove(token)
    }

    pub fn refused_connections(&self) -> u64 {
        self.group.refused_connections()
    }

    /// The group, for the one operation that ends its life.
    #[must_use]
    pub fn into_group(self) -> ConnectionGroup {
        self.group
    }

    pub fn records(&self) -> &[Record] {
        &self.records
    }

    pub fn take_records(&mut self) -> Vec<Record> {
        std::mem::take(&mut self.records)
    }

    /// The token of the first connection this Service accepted.
    pub fn accepted(&self) -> Option<Token> {
        self.records.iter().find_map(|record| match record {
            Record::Accepted { token, .. } => Some(*token),
            _ => None,
        })
    }

    pub fn ticks(&self) -> usize {
        self.ticks
    }

    /// Makes this Service report `deadline` as its protocol deadline.
    pub fn set_deadline(&mut self, deadline: Option<Instant>) {
        self.deadline = deadline;
    }

    /// Payloads still awaiting a drain.
    pub fn pending(&self) -> usize {
        self.inbox.len() - self.cursor
    }

    /// Puts one payload straight into the inbox: a composition test's
    /// deterministic stand-in for a transport read.
    pub fn push_inbound(&mut self, token: Token, bytes: &[u8]) {
        Self::store(&mut self.inbox, &mut self.spare, token, bytes);
    }

    /// Files one payload into the inbox, reusing storage a full drain handed
    /// back.
    fn store(inbox: &mut Vec<Inbound>, spare: &mut Vec<Inbound>, token: Token, payload: &[u8]) {
        let mut entry = spare.pop().unwrap_or(Inbound { token, bytes: Vec::new() });
        entry.token = token;
        entry.bytes.clear();
        entry.bytes.extend_from_slice(payload);
        inbox.push(entry);
    }

    /// Hands up to `max_events` payloads to `on_event`, oldest first, and
    /// reports whether events remain undrained.
    ///
    /// The bound belongs to the caller: one connection can yield unbounded
    /// messages, and what is left stays pullable for a later iteration. The
    /// report is about the leftovers, not the bound — a drain that consumes
    /// exactly its bound and empties the queue owes no extra iteration.
    #[must_use = "events remain undrained until a drain reports none left"]
    pub fn spin<F>(&mut self, max_events: usize, mut on_event: F) -> bool
    where
        F: FnMut(RawEvent<'_>),
    {
        let Self { group, inbox, cursor, spare, .. } = self;
        let mut drained = 0;
        while drained < max_events && *cursor < inbox.len() {
            let entry = &inbox[*cursor];
            on_event(RawEvent::Message {
                token: entry.token,
                payload: &entry.bytes,
                reply: Reply { group: &mut *group, token: entry.token },
            });
            *cursor += 1;
            drained += 1;
        }
        if *cursor == inbox.len() {
            spare.append(inbox);
            *cursor = 0;
        }
        *cursor < inbox.len()
    }

    /// Turns one transport event into protocol state. Both scheduling phases
    /// route through here, so the match lives in one place.
    fn on_stream_event(
        records: &mut Vec<Record>,
        inbox: &mut Vec<Inbound>,
        spare: &mut Vec<Inbound>,
        event: &StreamEvent<'_>,
    ) {
        match *event {
            StreamEvent::Accepted { token, peer } => {
                records.push(Record::Accepted { token, peer });
            }
            StreamEvent::Connected { token, peer } => {
                records.push(Record::Connected { token, peer });
            }
            // The bytes are lent for this callback only, so a payload kept for
            // a later drain is copied into storage this Service owns.
            StreamEvent::Message { token, payload, .. } => {
                Self::store(inbox, spare, token, payload);
            }
            StreamEvent::Disconnected { token, peer } => {
                records.push(Record::Disconnected { token, peer });
            }
        }
    }
}

impl Service for RawService {
    fn group_id(&self) -> &ConnectionGroupId {
        self.group.group_id()
    }

    fn handle_event(&mut self, readiness: &Event) -> ReadinessOutcome {
        let Self { group, records, inbox, spare, .. } = self;
        let mut on_event =
            |event: StreamEvent<'_>| Self::on_stream_event(records, inbox, spare, &event);
        group.handle_event(readiness, &mut on_event)
    }

    fn tick(&mut self, now: Instant) -> TickOutcome {
        let Self { group, records, inbox, spare, .. } = self;
        let maintained = {
            let mut on_event =
                |event: StreamEvent<'_>| Self::on_stream_event(records, inbox, spare, &event);
            group.maintain(now, &mut on_event)
        };
        self.ticks += 1;
        // Payloads awaiting a drain are work this Service exposes upward:
        // they ride the did-work report, never the deadline.
        maintained.or_worked(self.pending() > 0)
    }

    fn next_deadline(&self) -> Deadline {
        self.group.next_deadline().earliest(self.deadline)
    }
}

/// A composed Service: it owns a leaf and adds protocol state above it.
///
/// Only this one is scheduled. It delegates readiness, the tick and the
/// deadline to the leaf, then consumes the leaf's lending events through a
/// bounded drain — echoing each payload back through the scoped reply, which
/// is what proves an upper layer can answer without reborrowing the lower
/// Service it already holds.
pub struct RelayService {
    lower: RawService,
    /// How many payloads this relay has echoed.
    echoed: usize,
    /// Payloads it has yet to report to its caller.
    unpulled: usize,
    /// The drain bound this relay chooses per call.
    max_events: usize,
    /// The tick instant whose bounded drain left the leaf's events undrained,
    /// cleared by the tick whose drain reports none left. Leftovers are work
    /// the next tick can progress, so they fold into the deadline — at the
    /// instant the tick already holds, never a fresh clock read. Only ticks
    /// touch this: the mandatory post-readiness tick drains again, so a
    /// readiness-time drain's leftovers never need a deadline of their own.
    lower_work_due: Option<Instant>,
}

impl RelayService {
    #[must_use]
    pub fn new(lower: RawService, max_events: usize) -> Self {
        Self { lower, echoed: 0, unpulled: 0, max_events, lower_work_due: None }
    }

    pub fn lower(&self) -> &RawService {
        &self.lower
    }

    pub fn lower_mut(&mut self) -> &mut RawService {
        &mut self.lower
    }

    pub fn echoed(&self) -> usize {
        self.echoed
    }

    /// Takes what the relay has to report, as a caller pulls protocol events.
    pub fn take_unpulled(&mut self) -> usize {
        std::mem::take(&mut self.unpulled)
    }

    /// The lower service, for the one operation that ends its life: a
    /// composed service closes by delegating, down to the group's removal.
    #[must_use]
    pub fn into_lower(self) -> RawService {
        self.lower
    }

    /// Drains the leaf within this relay's bound and echoes every payload,
    /// reporting whether lower events remain undrained.
    fn relay(&mut self) -> bool {
        let bound = self.max_events;
        let mut seen = 0;
        let leftovers = self.lower.spin(bound, |event| match event {
            RawEvent::Message { payload, reply, .. } => {
                let echoed_ok = reply.send(payload);
                assert!(echoed_ok, "the scoped reply reached its connection");
                seen += 1;
            }
        });
        self.echoed += seen;
        self.unpulled += seen;
        leftovers
    }
}

impl Service for RelayService {
    fn group_id(&self) -> &ConnectionGroupId {
        self.lower.group_id()
    }

    fn handle_event(&mut self, readiness: &Event) -> ReadinessOutcome {
        let outcome = self.lower.handle_event(readiness);
        if !outcome.is_owned() {
            return outcome;
        }
        // The drain's leftover signal only preserves did-work here: the
        // mandatory post-readiness tick drains again, and only its leftovers
        // arm a deadline. The leaf's work is never withdrawn; this only adds
        // the relay's own.
        let leftovers = self.relay();
        outcome.or_worked(leftovers || self.unpulled > 0)
    }

    fn tick(&mut self, now: Instant) -> TickOutcome {
        let lower = self.lower.tick(now);
        let leftovers = self.relay();
        self.lower_work_due = leftovers.then_some(now);
        // Payloads this relay exposes upward ride the did-work report.
        lower.or_worked(leftovers || self.unpulled > 0)
    }

    fn next_deadline(&self) -> Deadline {
        // The deadline carries only work a tick of this Service can progress:
        // what the last tick's bounded drain left in the leaf, due at that
        // tick's own instant. Payloads exposed upward are the caller's to
        // pull, and ride the did-work report instead.
        self.lower.next_deadline().earliest(self.lower_work_due)
    }
}
