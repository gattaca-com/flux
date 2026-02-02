use std::ops::{Add, AddAssign, Sub, SubAssign};

use crate::{Duration, Instant};

#[derive(Clone, Copy, Debug, PartialEq, Default)]
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
        let el = self.last_acted.elapsed();
        if el >= self.interval {
            self.last_acted = Instant::now();
            true
        } else {
            false
        }
    }

    #[inline]
    pub fn interval(&self) -> Duration {
        self.interval
    }

    #[inline]
    pub fn set_interval(&mut self, interval: Duration) {
        self.interval = interval
    }

    pub fn reset(&mut self) {
        self.last_acted = Instant::now()
    }

    pub fn force_fire(&mut self) {
        self.last_acted = Instant::ZERO
    }
}

impl Add<Duration> for Repeater {
    type Output = Repeater;
    fn add(self, rhs: Duration) -> Self::Output {
        Repeater { interval: self.interval.saturating_add(rhs), ..self }
    }
}

impl Sub<Duration> for Repeater {
    type Output = Repeater;
    fn sub(self, rhs: Duration) -> Self::Output {
        Repeater { interval: self.interval.saturating_sub(rhs), ..self }
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
