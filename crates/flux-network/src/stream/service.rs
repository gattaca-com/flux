//! The scheduling contract a service implements, and the readiness result only
//! its connection group can produce.

use flux_timing::Instant;
use mio::event::Event;

use super::ConnectionGroupId;

/// What offering one readiness event to a service produced.
///
/// Only [`ConnectionGroup::handle_event`](super::ConnectionGroup::handle_event)
/// builds one, and the value is neither `Copy` nor `Clone`, so a service
/// reaches a truthful outcome only by offering the event to the group it owns.
/// A service containing another can pass the inner outcome through or widen
/// its work with [`Self::or_worked`], and can do nothing else with it: the
/// obligation to delegate readiness is the type, not a runtime check.
#[must_use]
pub struct ReadinessOutcome {
    owned: bool,
    worked: bool,
}

impl ReadinessOutcome {
    /// The token is not one this group holds, so the scheduler tries the next
    /// service.
    pub(in crate::stream) fn not_owned() -> Self {
        Self { owned: false, worked: false }
    }

    /// The token is this group's, and `worked` records whether handling it
    /// emitted an event.
    pub(in crate::stream) fn owned(worked: bool) -> Self {
        Self { owned: true, worked }
    }

    /// Whether the event belonged to this service's group. The scheduler stops
    /// routing an owned event.
    pub fn is_owned(&self) -> bool {
        self.owned
    }

    /// Whether handling the event produced observable work. Always false for
    /// an event this service does not own.
    pub fn worked(&self) -> bool {
        self.owned && self.worked
    }

    /// Adds work a containing service found while consuming what the inner one
    /// emitted.
    ///
    /// An outcome that is not owned stays not owned and gains no work, and
    /// work already reported is never withdrawn.
    pub fn or_worked(mut self, worked: bool) -> Self {
        if self.owned {
            self.worked |= worked;
        }
        self
    }
}

/// What [`StreamNetwork`](super::StreamNetwork) calls on each service it
/// schedules, in the order [`StreamNetwork::drive`](super::StreamNetwork::drive)
/// fixes.
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
/// into every [`Self::next_deadline`]. The network verifies both and panics on
/// either omission. Readiness needs no check: a
/// [`ReadinessOutcome`] comes from the group or not at all.
pub trait Service {
    /// The group this service owns, directly or through the service it
    /// contains.
    fn group_id(&self) -> &ConnectionGroupId;

    /// Offers one readiness event to this service.
    fn handle_event(&mut self, event: &Event) -> ReadinessOutcome;

    /// Runs the transport work and timers due now, once after readiness, from
    /// the owned group outward. Reports whether anything happened, events left
    /// unpulled from an earlier iteration included.
    fn tick(&mut self, now: Instant) -> bool;

    /// The earliest transport or protocol deadline in this service, folded
    /// from its group outward. Immediately due while a tick already owes work.
    fn next_deadline(&self) -> Option<Instant>;
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

    fn tick(&mut self, now: Instant) -> bool {
        (**self).tick(now)
    }

    fn next_deadline(&self) -> Option<Instant> {
        (**self).next_deadline()
    }
}
