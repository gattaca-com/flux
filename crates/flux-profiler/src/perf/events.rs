//! Event vocabulary: turn a portable name into the `perf_event_open`
//! `(type, config)` for the running CPU. **Edit [`METRICS`] to add a counter.**
//!
//! All of this runs once, at [`Schema`] init — never on the hot path. Names
//! resolve like `perf -e`: the curated table, then raw `rNNNN`, then a
//! `/sys/bus/event_source/devices/cpu/events/<name>` event.

use std::sync::OnceLock;

use tracing::warn;

use super::sample::MAX_EVENTS;

/// A resolved hardware event: the `perf_event_open` ABI numbers plus a label.
/// `type_`/`config` are the same ids perf derives from an event name, so a
/// postprocessor can relabel or merge across runs from them alone.
#[derive(Clone)]
pub struct EventSpec {
    #[cfg_attr(not(feature = "perf"), allow(dead_code))]
    pub type_: u32,
    #[cfg_attr(not(feature = "perf"), allow(dead_code))]
    pub config: u64,
    pub label: String,
}

const PERF_TYPE_HARDWARE: u32 = 0;
const PERF_TYPE_HW_CACHE: u32 = 3;
const PERF_TYPE_RAW: u32 = 4;

/// Events read every run unless `PERF_EVENTS` overrides — IPC inputs,
/// branch mispredicts, and a miss counter. Kept small (two general-purpose
/// counters) so it never multiplexes; widen the set per run via the env var.
const DEFAULT_EVENTS: &str = "instructions,cpu-cycles,branch-misses,cache-misses";

/// `PERF_TYPE_HW_CACHE` config = `cache_id | op<<8 | result<<16` (kernel ABI).
const fn cache(id: u64, op: u64, result: u64) -> u64 {
    id | (op << 8) | (result << 16)
}

/// CPU vendor, detected once from `/proc/cpuinfo`. Cache-miss encodings differ
/// by vendor — the generic `LL` `HW_CACHE` encoding works on Intel but reads 0
/// on AMD, where last-level traffic is a core event — so [`METRICS`] keys the
/// L2/L3 entries on it.
#[derive(Clone, Copy, PartialEq)]
enum Vendor {
    Amd,
    Intel,
    Other,
}

fn vendor() -> Vendor {
    static VENDOR: OnceLock<Vendor> = OnceLock::new();
    *VENDOR.get_or_init(|| {
        let info = std::fs::read_to_string("/proc/cpuinfo").unwrap_or_default();
        if info.contains("AuthenticAMD") {
            Vendor::Amd
        } else if info.contains("GenuineIntel") {
            Vendor::Intel
        } else {
            Vendor::Other
        }
    })
}

/// How a curated metric maps to `perf_event_open` ABI numbers on one vendor.
#[derive(Clone, Copy)]
enum Enc {
    /// `PERF_TYPE_HARDWARE` config.
    Hw(u64),
    /// `PERF_TYPE_HW_CACHE` (`cache_id`, op, result) — kernel maps per model.
    Cache(u64, u64, u64),
    /// `PERF_TYPE_RAW` config (event | umask<<8 | …); vendor/family specific.
    Raw(u64),
}

impl Enc {
    fn spec(self, label: &str) -> EventSpec {
        let (type_, config) = match self {
            Self::Hw(c) => (PERF_TYPE_HARDWARE, c),
            Self::Cache(id, op, r) => (PERF_TYPE_HW_CACHE, cache(id, op, r)),
            Self::Raw(c) => (PERF_TYPE_RAW, c),
        };
        EventSpec { type_, config, label: label.to_owned() }
    }
}

/// A metric's encoding: `Any` is portable across vendors (the kernel maps the
/// generic code per model); `PerVendor` needs a vendor-specific raw code.
#[derive(Clone, Copy)]
enum Spec {
    Any(Enc),
    PerVendor { amd: Enc, intel: Enc },
}

/// One curated metric — the single place to add a portable counter. Names
/// follow common perf/PAPI usage (`l1d-misses` ≈ `PAPI_L1_DCM`, `l3-misses` ≈
/// `PAPI_L3_TCM` / perf's `LLC-*-misses`). Raw `rNNNN` and `cpu/events/<name>`
/// (sysfs) remain escape hatches in [`resolve`].
struct Metric {
    name: &'static str,
    spec: Spec,
}

// cache ids: L1D=0 L1I=1 LL=2; op: READ=0 WRITE=1; result: ACCESS=0 MISS=1.
// AMD L2/L3 (verified on Zen5) both come from `ls_dmnd_fills_from_sys`
// (event 0x43): demand L1 fills broken down by source. The umask is every
// source bit except the levels at/above the miss:
//   l2-misses = not local L2            -> umask 0xfe -> 0xfe43
//   l3-misses = not local L2 / L3 (ccx) -> umask 0xfc -> 0xfc43
// One demand-only counter family, so l2 >= l3 holds by construction. The
// named `l2_cache_req_stat.ic_dc_miss_in_l2` (0x964) is NOT used: it
// under-reports demand data misses ~8x on the pointer-chasing state walk
// (7.9k vs 57k). Intel l2 `MEM_LOAD_RETIRED.L2_MISS` = 0x10d1 (Skylake+,
// verify per microarch); Intel l3 via the generic LL HW_CACHE below.
const METRICS: &[Metric] = &[
    Metric { name: "instructions", spec: Spec::Any(Enc::Hw(1)) },
    Metric { name: "cycles", spec: Spec::Any(Enc::Hw(0)) },
    Metric { name: "cpu-cycles", spec: Spec::Any(Enc::Hw(0)) },
    Metric { name: "branch-misses", spec: Spec::Any(Enc::Hw(5)) },
    Metric { name: "cache-misses", spec: Spec::Any(Enc::Hw(3)) },
    Metric { name: "l1d-misses", spec: Spec::Any(Enc::Cache(0, 0, 1)) },
    Metric { name: "l1i-misses", spec: Spec::Any(Enc::Cache(1, 0, 1)) },
    Metric {
        name: "l2-misses",
        spec: Spec::PerVendor { amd: Enc::Raw(0xfe43), intel: Enc::Raw(0x10d1) },
    },
    Metric {
        name: "l3-misses",
        spec: Spec::PerVendor { amd: Enc::Raw(0xfc43), intel: Enc::Cache(2, 0, 1) },
    },
];

/// Resolve a curated metric for the running CPU.
fn canonical(name: &str) -> Option<EventSpec> {
    let m = METRICS.iter().find(|m| m.name == name)?;
    let enc = match m.spec {
        Spec::Any(e) => e,
        Spec::PerVendor { amd, intel } => match vendor() {
            Vendor::Amd => amd,
            Vendor::Intel => intel,
            Vendor::Other => return None,
        },
    };
    Some(enc.spec(name))
}

/// Resolve a sysfs-exposed event, e.g. `cpu/events/cache-misses` →
/// `event=0x64,umask=0x09`, the way perf does for PMU-named events.
fn sysfs(name: &str) -> Option<EventSpec> {
    const BASE: &str = "/sys/bus/event_source/devices/cpu";
    let content = std::fs::read_to_string(format!("{BASE}/events/{name}")).ok()?;
    let mut config = 0u64;
    for kv in content.trim().split(',') {
        let (k, v) = kv.split_once('=')?;
        let n = u64::from_str_radix(v.trim().trim_start_matches("0x"), 16).ok()?;
        match k.trim() {
            "event" => config |= n,
            "umask" => config |= n << 8,
            "edge" => config |= n << 18,
            "inv" => config |= n << 23,
            "cmask" => config |= n << 24,
            _ => {}
        }
    }
    let type_ = std::fs::read_to_string(format!("{BASE}/type")).ok()?.trim().parse().ok()?;
    Some(EventSpec { type_, config, label: name.to_owned() })
}

fn resolve(name: &str) -> Option<EventSpec> {
    if let Some(spec) = canonical(name) {
        return Some(spec);
    }
    if let Some(hex) = name.strip_prefix('r') &&
        let Ok(config) = u64::from_str_radix(hex, 16)
    {
        return Some(EventSpec { type_: PERF_TYPE_RAW, config, label: name.to_owned() });
    }
    sysfs(name)
}

/// The perf event vocabulary a run measures: an ordered slot list that counter
/// samples are positional in.
#[derive(Clone)]
pub struct Schema(Vec<EventSpec>);

impl Schema {
    /// Resolve a comma-separated spec: unknown names are skipped with a
    /// warning, and the list is capped at [`MAX_EVENTS`].
    pub fn parse(spec: &str) -> Self {
        Self(
            spec.split(',')
                .map(str::trim)
                .filter(|s| !s.is_empty())
                .filter_map(|n| {
                    resolve(n).or_else(|| {
                        warn!("perf: unknown event '{n}', skipping");
                        None
                    })
                })
                .take(MAX_EVENTS)
                .collect(),
        )
    }

    pub const fn empty() -> Self {
        Self(Vec::new())
    }

    /// Slot index of the event labelled `label`.
    fn slot(&self, label: &str) -> Option<usize> {
        self.0.iter().position(|e| e.label == label)
    }

    /// Value of `label` in a positional sample's slots, or 0 if the run didn't
    /// measure it.
    fn value(&self, vals: &[u64], label: &str) -> u64 {
        self.slot(label).map_or(0, |i| vals[i])
    }

    /// CPU cycles under either spelling perf uses for the event.
    fn cycles(&self, vals: &[u64]) -> u64 {
        self.value(vals, "cpu-cycles").max(self.value(vals, "cycles"))
    }

    /// Instructions per cycle, or 0 when cycles weren't measured.
    pub fn ipc(&self, vals: &[u64]) -> f64 {
        let cycles = self.cycles(vals);
        if cycles == 0 { 0.0 } else { self.value(vals, "instructions") as f64 / cycles as f64 }
    }

    /// This process's own counter slots, resolved once from
    /// `PERF_EVENTS` (or [`DEFAULT_EVENTS`]) — the producer's
    /// vocabulary.
    pub fn local() -> &'static Self {
        static SCHEMA: OnceLock<Schema> = OnceLock::new();
        SCHEMA.get_or_init(|| {
            let spec = std::env::var("PERF_EVENTS").unwrap_or_else(|_| DEFAULT_EVENTS.to_owned());
            Self::parse(&spec)
        })
    }
}

impl std::ops::Deref for Schema {
    type Target = [EventSpec];
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}
