use std::{
    sync::Arc,
    time::{SystemTime, UNIX_EPOCH},
};

use governor::{clock::Clock as GovernorClock, nanos::Nanos as GovernorNanos};
use once_cell::sync::OnceCell;
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
            OurClockForNanos::Clock(clock) => clock.raw(),
            OurClockForNanos::System => unsafe {
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

static GLOBAL_NANOS_FOR_100: OnceCell<u64> = OnceCell::new();
// might be mocked
static GLOBAL_CLOCK: OnceCell<OurClockForNanos> = OnceCell::new();
// never mocked
static GLOBAL_CLOCK_NON_MOCKED: OnceCell<Clock> = OnceCell::new();

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

#[inline]
pub(super) fn nanos_for_100() -> u64 {
    *GLOBAL_NANOS_FOR_100.get_or_init(|| global_clock_not_mocked().delta_as_nanos(0, 100))
}
