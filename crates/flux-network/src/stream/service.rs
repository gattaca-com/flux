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
/// # Obligations
/// A service that owns the group directly calls
/// [`ConnectionGroup::maintain`](super::ConnectionGroup::maintain) at the start
/// of every [`Self::tick`] and folds
/// [`ConnectionGroup::next_deadline`](super::ConnectionGroup::next_deadline)
/// into every [`Self::next_deadline`]. The return types carry both: a
/// [`TickOutcome`], a [`Deadline`] and a [`ReadinessOutcome`] each come from
/// the group or not at all, and a containing service can only pass them
/// through, widen the work or bring the deadline forward.
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
    /// from its group outward with [`Deadline::earliest`].
    ///
    /// Only work a tick of this service can progress becomes immediately due,
    /// and it reports the instant it *became* due, never a fresh clock read.
    /// Work exposed upward for the caller to pull is the caller's to
    /// schedule: it rides the did-work report and never alters the deadline.
    fn next_deadline(&self) -> Deadline;
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

    fn next_deadline(&self) -> Deadline {
        (**self).next_deadline()
    }
}
