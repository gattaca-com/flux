use std::{fmt::Display, marker::PhantomData};

use flux::timing::{Duration, Nanos};
use serde::{Deserialize, Serialize};

pub trait Statisticable: Into<u64> + From<u64> + Display + Clone + Copy + PartialEq {
    fn to_plotpoint(&self) -> f64 {
        Into::<u64>::into(*self) as f64
    }
}

impl Statisticable for Duration {}

impl Statisticable for Nanos {}

/// Keep track of msg latencies
/// All in nanos
#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub struct DataPoint<T: Statisticable> {
    pub avg: u64,
    pub min: u64,
    pub max: u64,
    pub median: u64,
    pub tot: u64,
    pub n_samples: usize,
    pub vline: bool,
    pub rate: MsgPer10Sec,
    pub time: u64,
    pub _p: std::marker::PhantomData<T>,
}

impl<T: Statisticable> DataPoint<T> {
    fn empty(time: u64) -> Self {
        Self { time, ..Default::default() }
    }

    pub fn block_start(time: u64) -> DataPoint<T> {
        Self { vline: true, ..Self::empty(time) }
    }
}

impl<T: Statisticable> Default for DataPoint<T> {
    fn default() -> Self {
        Self {
            avg: 0,
            min: u64::MAX,
            max: 0,
            median: 0,
            n_samples: 0,
            vline: false,
            rate: Default::default(),
            _p: PhantomData {},
            tot: 0,
            time: 0,
        }
    }
}

#[derive(Copy, Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct MsgPer10Sec(pub u64);

impl Statisticable for MsgPer10Sec {}

impl Display for MsgPer10Sec {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:.1}", self.0 as f64 / 10.0)
    }
}

impl From<MsgPer10Sec> for u64 {
    fn from(value: MsgPer10Sec) -> Self {
        value.0
    }
}
impl From<u64> for MsgPer10Sec {
    fn from(value: u64) -> Self {
        Self(value)
    }
}
