use std::ops::{Add, AddAssign, Sub, SubAssign};

use crate::{Duration, Instant};

#[derive(Clone, Copy, Debug, PartialEq, Eq, Default)]
pub struct Repeater {
    interval: Duration,
    last_acted: Instant,
}

impl Repeater {
    #[inline]
    pub fn every(interval: Duration) -> Self {
        Self { interval, last_acted: Instant::ZERO }
    }

    #[inline]
    pub fn maybe<F>(&mut self, mut f: F)
    where
        F: FnMut(Duration),
    {
        let el = self.last_acted.elapsed();
        if el >= self.interval {
            f(el);
            self.last_acted = Instant::now();
        }
    }

    #[inline]
    pub fn fired(&mut self) -> bool {
        self.fired_at(Instant::now())
    }

    #[inline]
    pub fn fired_at(&mut self, now: Instant) -> bool {
        let el = now.elapsed_since(self.last_acted);
        if el >= self.interval {
            self.last_acted = now;
            true
        } else {
            false
        }
    }

    /// The instant from which the next [`Self::fired_at`] fires, for a caller
    /// folding this repeater into a poll timeout.
    #[inline]
    pub fn next_fire(&self) -> Instant {
        self.last_acted + self.interval
    }

    #[inline]
    pub fn interval(&self) -> Duration {
        self.interval
    }

    #[inline]
    pub fn set_interval(&mut self, interval: Duration) {
        self.interval = interval;
    }

    pub fn reset(&mut self) {
        self.last_acted = Instant::now();
    }

    pub fn force_fire(&mut self) {
        self.last_acted = Instant::ZERO;
    }
}

impl Add<Duration> for Repeater {
    type Output = Self;
    fn add(self, rhs: Duration) -> Self::Output {
        Self { interval: self.interval.saturating_add(rhs), ..self }
    }
}

impl Sub<Duration> for Repeater {
    type Output = Self;
    fn sub(self, rhs: Duration) -> Self::Output {
        Self { interval: self.interval.saturating_sub(rhs), ..self }
    }
}

impl AddAssign<Duration> for Repeater {
    fn add_assign(&mut self, rhs: Duration) {
        self.interval = self.interval.saturating_add(rhs);
    }
}

impl SubAssign<Duration> for Repeater {
    fn sub_assign(&mut self, rhs: Duration) {
        self.interval = self.interval.saturating_sub(rhs);
    }
}

#[cfg(test)]
mod tests {
    use super::{Duration, Instant, Repeater};

    #[test]
    fn fired_at_uses_the_supplied_instant() {
        let mut repeater = Repeater::every(Duration(10));
        assert!(!repeater.fired_at(Instant(9)));
        assert!(repeater.fired_at(Instant(10)));
        assert!(!repeater.fired_at(Instant(19)));
        assert!(repeater.fired_at(Instant(20)));
    }

    #[test]
    fn next_fire_is_the_instant_the_next_fire_happens_at() {
        let mut repeater = Repeater::every(Duration(10));
        assert_eq!(repeater.next_fire(), Instant(10));
        assert!(repeater.fired_at(Instant(12)));
        assert_eq!(repeater.next_fire(), Instant(22));
        assert!(!repeater.fired_at(Instant(21)));
        assert!(repeater.fired_at(repeater.next_fire()));
    }
}
