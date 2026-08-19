use std::{
    sync::{Arc, OnceLock},
    time::{SystemTime, UNIX_EPOCH},
};

use governor::{clock::Clock as GovernorClock, nanos::Nanos as GovernorNanos};
use quanta::Mock;

use crate::Nanos;

pub type Clock = quanta::Clock;

#[derive(Clone, Debug)]
pub enum OurClockForNanos {
    Clock(Clock),
    System,
}

impl OurClockForNanos {
    pub fn raw(&self) -> u64 {
        match self {
            Self::Clock(clock) => clock.raw(),
            Self::System => unsafe {
                SystemTime::now().duration_since(UNIX_EPOCH).unwrap_unchecked().as_nanos() as u64
            },
        }
    }

    #[inline]
    pub fn now(&self) -> Nanos {
        Nanos(self.raw())
    }
}

impl GovernorClock for OurClockForNanos {
    type Instant = GovernorNanos;

    fn now(&self) -> Self::Instant {
        GovernorNanos::new(self.raw())
    }
}

// might be mocked
static GLOBAL_CLOCK: OnceLock<OurClockForNanos> = OnceLock::new();
// never mocked
static GLOBAL_CLOCK_NON_MOCKED: OnceLock<Clock> = OnceLock::new();

#[inline]
pub fn init_global_with_mock() -> Arc<Mock> {
    let (mock, controller) = Clock::mock();
    let mock = GLOBAL_CLOCK.get_or_init(|| OurClockForNanos::Clock(mock));
    // this is in some effort to never not have 2 threads racing to initialize
    // different mocks and/or global clock before mock
    assert_eq!(mock.raw(), 0, "Do not initialize the global mock clock from 2 different threads");
    controller.increment(1);
    controller
}

#[inline]
pub fn global_clock() -> &'static OurClockForNanos {
    GLOBAL_CLOCK.get_or_init(|| OurClockForNanos::System)
}

#[inline]
pub fn global_clock_not_mocked() -> &'static Clock {
    GLOBAL_CLOCK_NON_MOCKED.get_or_init(Clock::new)
}

static NANOS_PER_TICK: OnceLock<u64> = OnceLock::new();
static TICKS_PER_NANO: OnceLock<u64> = OnceLock::new();
/// A tick is a fraction of a nanosecond - 0.2330078 of one at 4.3GHz - and an
/// integer cannot hold that, so the rate is kept multiplied by
/// `2^FRACTION_BITS` and shifted back down after each conversion. Whatever is
/// too small to fit in those bits is lost, and a lost fraction is a clock that
/// runs slow:
///
///   whole number (old `ticks_per_micro`: 3792 of a true 3792.87)  20 s/day
///   16 bits                                                       5.7 s/day
///   24 bits                                                       22 ms/day
///   32 bits                                                       86 us/day
const FRACTION_BITS: u32 = 32;
const ONE: u128 = 1 << FRACTION_BITS;

#[inline]
fn nanos_per_tick() -> u64 {
    *NANOS_PER_TICK.get_or_init(|| global_clock_not_mocked().delta_as_nanos(0, ONE as u64))
}

/// Rates are stored times `ONE`, so `nanos_per_tick()` holds `0.233 * ONE`.
/// The reverse rate has to come out stored the same way:
///
/// ```text
/// ONE * ONE / (0.233 * ONE) = 4.29 * ONE
/// ```
#[inline]
fn ticks_per_nano() -> u64 {
    *TICKS_PER_NANO.get_or_init(|| (ONE * ONE / u128::from(nanos_per_tick())) as u64)
}

#[inline]
fn scale(value: u64, rate: u64) -> u64 {
    ((u128::from(value) * u128::from(rate)) >> FRACTION_BITS) as u64
}

#[inline]
pub(super) fn ticks_to_nanos(ticks: u64) -> u64 {
    scale(ticks, nanos_per_tick())
}

/// A u64 of ticks runs out after ~136 years at 4GHz, and so does this.
#[inline]
pub(super) fn nanos_to_ticks(nanos: u64) -> u64 {
    scale(nanos, ticks_per_nano())
}
