//! The scheduling contract a service implements. The three results only a
//! service's connection group can produce live with the group:
//! [`ReadinessOutcome`], [`TickOutcome`] and [`Deadline`].

use flux_timing::Instant;
use mio::event::Event;

use super::{ConnectionGroupId, Deadline, ReadinessOutcome, TickOutcome};

/// What [`StreamNetwork`](super::StreamNetwork) calls on each service it
/// schedules, in the order
/// [`StreamNetwork::drive`](super::StreamNetwork::drive) fixes.
///
/// A service owns exactly one [`ConnectionGroup`](super::ConnectionGroup),
/// either directly or through a service it contains. Only the outermost one is
/// passed to the driver; it reports the group's identity, offers readiness
/// along its chain, ticks it and folds its deadlines.
///
/// The driver is generic over this trait and never calls it through a trait
/// object.
///
/// # The contract: three laws
/// 1. **Tick is fresh.** Every tick reaches the group on that invocation: a
///    service owning the group directly calls
///    [`ConnectionGroup::maintain`](super::ConnectionGroup::maintain) at the
///    start of every [`Self::tick`] with the `now` it was given; a service
///    containing another ticks it with that same `now`.
/// 2. **Work is monotone.** A tick's work is the lower work or the service's
///    own; work reported below is never erased.
/// 3. **Deadline is fresh and monotone.** Every [`Self::next_deadline`]
///    consults the group, or the lower service, on that invocation with the
///    `now` it was given, and reports no later than what it was told. `None` is
///    infinity.
///
/// The return types carry the laws. A [`TickOutcome`], a [`Deadline`] and a
/// [`ReadinessOutcome`] each come from the group or not at all, so the
/// monotone halves and reaching the group are compile-time facts:
/// [`TickOutcome::or_worked`] only widens and [`Deadline::earliest`] only
/// brings forward. The fresh halves are stamps: the group marks each tick
/// outcome and deadline with the `now` it was asked with, and the driver
/// reads them for the `now` it handed out, so an answer kept from an earlier
/// invocation and returned for a later one panics under debug assertions at
/// the first read. What a service passes down is what it was handed, never a
/// fresh clock read; the driver mints one instant per delegation chain, one
/// for the fold and another after the poll wait for the tick.
///
/// The stamps are a debug-time contract detector, not a complete correctness
/// guarantee: they certify that delegation was fresh, not the service's own
/// bookkeeping. A stale timer of the service's own folded onto a fresh group
/// deadline, a wrong pending report, events dropped after `maintain`, or a
/// replayed [`ReadinessOutcome`] are ordinary defects for the service's own
/// tests. flux tests its own services; the author of a service outside flux
/// exercises it in debug-mode tests, and flux makes no promise about an
/// untested implementation in a release build.
///
/// # Testing a service
/// Freshness needs no sockets: two quiet iterations with distinct instants,
/// each reading the fold and the tick for the instant it asked with. Any
/// answer replayed from the first iteration carries the first instant and
/// fails the second read.
///
/// ```no_run
/// # use flux_network::stream::{ConnectionGroupConfig, Service, StreamNetwork, StreamService};
/// # use flux_timing::{Duration, Instant};
/// let mut net = StreamNetwork::default();
/// let mut service = StreamService::new(net.add_group(ConnectionGroupConfig::default()));
/// let base = Instant::now();
/// for step in 1..=2 {
///     let now = base + Duration::from_millis(step);
///     let _ = service.next_deadline(now).instant(now);
///     let _ = service.tick(now).worked(now);
/// }
/// ```
///
/// Replay can be gated on state — a cache that fills only after a handshake,
/// or only while no readiness arrives between tick and fold — so the two
/// iterations are run quiet, with no readiness between them, and repeated in
/// every protocol state the service can reach. The value half of the laws,
/// that an overdue reconnect is attempted and the deadline advances, is
/// covered by a test that adds an outbound endpoint nobody listens on and
/// ticks past the retry instant; the shipped services' own tests do both.
///
/// # Lifetime
/// A service must not be used after the
/// [`StreamNetwork`](super::StreamNetwork) that created its group is dropped:
/// the group's registrations then reach no poll, so nothing readiness-driven
/// happens again. Close a service first where its sockets matter; at process
/// teardown, dropping services and network together is harmless.
pub trait Service {
    /// The group this service owns, directly or through the service it
    /// contains.
    ///
    /// Side-effect-free and O(1), and stable for the service's whole life:
    /// every call reports the same identity until a consuming close ends the
    /// service. The scheduler calls this on every phase of every iteration.
    fn group_id(&self) -> &ConnectionGroupId;

    /// Offers one readiness event to this service.
    fn handle_event(&mut self, event: &Event) -> ReadinessOutcome;

    /// Runs the transport work and timers due now, once after readiness, from
    /// the owned group outward. The outcome reports whether anything happened,
    /// events left unpulled from an earlier iteration included.
    fn tick(&mut self, now: Instant) -> TickOutcome;

    /// The earliest transport or protocol deadline in this service, folded
    /// from its group outward with [`Deadline::earliest`], for the fold at
    /// `now`.
    ///
    /// `now` is handed down unchanged to the group or the lower service; the
    /// deadline it returns carries it. It is a consistency token, not a
    /// filter: an instant already in the past is still reported. Only work a
    /// tick of this service can progress becomes immediately due, and it
    /// reports the instant it *became* due, never a fresh clock read. Work
    /// exposed upward for the caller to pull is the caller's to schedule: it
    /// rides the did-work report and never alters the deadline.
    fn next_deadline(&self, now: Instant) -> Deadline;
}

/// Lets a slice of borrowed services go through the driver unchanged, without
/// giving the scheduler a trait object.
impl<S: Service> Service for &mut S {
    fn group_id(&self) -> &ConnectionGroupId {
        (**self).group_id()
    }

    fn handle_event(&mut self, event: &Event) -> ReadinessOutcome {
        (**self).handle_event(event)
    }

    fn tick(&mut self, now: Instant) -> TickOutcome {
        (**self).tick(now)
    }

    fn next_deadline(&self, now: Instant) -> Deadline {
        (**self).next_deadline(now)
    }
}
