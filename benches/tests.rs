#![allow(dead_code, unused_imports, clippy::float_cmp)]

mod tiny_payloads {
    use std::{panic::AssertUnwindSafe, sync::atomic::AtomicUsize};

    use super::super::{CAPACITY, Credit, Message, Rx, payload, validate};

    struct Replay<const B: usize>(Vec<Message<B>>);

    impl<const B: usize> Rx<B> for Replay<B> {
        fn drain(&mut self, mut callback: impl FnMut(&Message<B>)) {
            for message in self.0.drain(..) {
                callback(&message);
            }
        }
    }

    fn check<const B: usize>() {
        let expected: Vec<_> = (CAPACITY..2 * CAPACITY).map(payload::<B>).collect();
        let credit = Credit(AtomicUsize::new(0));
        validate(&mut Replay(expected.clone()), &expected, &credit);

        let mut corrupted = expected.clone();
        corrupted[0].0[B / 2] ^= 1;
        let stale = (0..CAPACITY).map(payload::<B>).collect();
        for messages in [corrupted, stale] {
            assert!(
                std::panic::catch_unwind(AssertUnwindSafe(|| {
                    validate(&mut Replay(messages), &expected, &credit);
                }))
                .is_err()
            );
        }
    }

    #[test]
    fn detects_corruption_and_replayed_ring_lap() {
        check::<1>();
        check::<2>();
        check::<4>();
    }
}

mod workers {
    use std::sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
        mpsc,
    };

    use super::super::{Case, Message, Mode, Rx, Settings, Tx, VALIDATION, WARMUP, window_stats};

    impl<const B: usize> Tx<B> for mpsc::Sender<Message<B>> {
        fn send(&mut self, message: &Message<B>) {
            Self::send(self, *message).unwrap();
        }
    }

    struct Counted<const B: usize>(mpsc::Receiver<Message<B>>, Arc<AtomicUsize>);

    impl<const B: usize> Rx<B> for Counted<B> {
        fn drain(&mut self, mut callback: impl FnMut(&Message<B>)) {
            while let Ok(message) = self.0.try_recv() {
                callback(&message);
                self.1.fetch_add(1, Ordering::Relaxed);
            }
        }
    }

    #[test]
    fn checks_follow_each_completed_round() {
        let cpu = core_affinity::get_core_ids().unwrap()[0].id;
        let mut modes = vec![Mode::Verify, Mode::Throughput];
        if cfg!(target_arch = "x86_64") {
            modes.push(Mode::Latency);
        }
        for mode in modes {
            let settings = Settings {
                cpus: [cpu; 2],
                mode,
                messages: 2 * window_stats::WINDOW_MESSAGES + 1,
                runs: 1,
                size: None,
                queue: None,
                slot: super::super::SlotLayout::Natural,
                windows: true,
                window_samples: false,
                pauses: [0, 1],
            };
            let (sender, receiver) = mpsc::channel();
            let drained = Arc::new(AtomicUsize::new(0));
            let mut checked = Vec::new();
            let mut case = Case::<32>::new("test", &settings);
            // Asserting here would leave the workers waiting at a barrier.
            case.run(1, sender, Counted(receiver, drained.clone()), |count| {
                checked.push((count, drained.load(Ordering::Relaxed)));
            });
            case.summarize();
            let rounds: &[_] = match mode {
                Mode::Verify => &[VALIDATION],
                _ => &[VALIDATION, WARMUP, settings.messages],
            };
            let expected: Vec<_> = rounds
                .iter()
                .scan(0, |total, &count| {
                    *total += count;
                    Some((count, *total))
                })
                .collect();
            assert_eq!(checked, expected, "{mode:?}");
        }
    }
}

#[cfg(target_arch = "x86_64")]
mod latency {
    use flux_timing::{Duration, Instant};

    use super::super::latency::{Latencies, now, ns_per_tick, read_timestamp, write_timestamp};

    #[test]
    fn quantiles_retain_fractional_tick_conversion() {
        for ticks in [1, 3, 37] {
            let mut histogram = Latencies::new();
            histogram.record(Instant(100), Instant(100 + ticks));
            let expected = ticks as f64 * ns_per_tick();
            assert!((histogram.quantile_ns(0.50) - expected).abs() < 1e-9);
        }
    }

    #[test]
    fn timestamp_uses_first_eight_bytes_only() {
        let _ = Latencies::new();
        for size in [8, 32, 1024] {
            let mut message = vec![0xa5; size];
            let stamp = now();
            write_timestamp(&mut message, stamp);
            assert_eq!(read_timestamp(&message), stamp);
            assert!(message[8..].iter().all(|&byte| byte == 0xa5));
        }
    }

    #[test]
    fn histogram_counts_zero_and_merges_every_observation() {
        let mut first = Latencies::new();
        let mut second = Latencies::new();
        second.record(Instant(100), Instant(100));
        assert_eq!(second.len(), 1);
        assert_eq!(second.quantile_ns(0.50), 0.);
        let mut second = Latencies::new();
        for delta in [0, 10, 20] {
            first.record(Instant(100), Instant(100 + delta));
        }
        second.record(Instant(200), Instant(230));
        first.add(&second);
        assert_eq!(first.len(), 4);
        assert_eq!(first.quantile_ns(0.95), 30. * ns_per_tick());
        assert_eq!(first.quantile_ns(0.50), 10. * ns_per_tick());
        let (mean, sd) = first.mean_and_stdev_ns();
        let scale = Duration(1 << 32).as_nanos() / (1u64 << 32) as f64;
        assert!((mean / scale - 15.0).abs() < 1e-6);
        assert!((sd / scale - 125f64.sqrt()).abs() < 1e-6);
    }

    #[test]
    fn overlap_counts_send_start_before_previous_receive() {
        let mut histogram = Latencies::new();
        histogram.record(Instant(100), Instant(150));
        histogram.record(Instant(140), Instant(160));
        histogram.record(Instant(160), Instant(170));
        assert_eq!(histogram.overlapping_sends, 1);
    }

    #[test]
    fn reset_discards_warmup_samples_and_overlap_state() {
        let mut histogram = Latencies::new();
        histogram.record(Instant(100), Instant(200));
        histogram.record(Instant(150), Instant(300));
        assert_eq!(histogram.overlapping_sends, 1);
        histogram.reset();
        assert_eq!(histogram.len(), 0);
        assert_eq!(histogram.overlapping_sends, 0);
        histogram.record(Instant(10), Instant(20));
        assert_eq!(histogram.len(), 1);
        assert_eq!(histogram.overlapping_sends, 0);
        assert_eq!(histogram.quantile_ns(0.50), 10. * ns_per_tick());
    }

    #[test]
    #[should_panic(expected = "latency clock moved backwards")]
    fn reversed_clock_is_rejected() {
        Latencies::new().record(Instant(2), Instant(1));
    }

    #[test]
    #[should_panic(expected = "latency exceeds preallocated histogram range")]
    fn out_of_range_sample_is_rejected_instead_of_dropped() {
        Latencies::new().record(Instant(0), Instant(u64::MAX));
    }
}

mod windows {
    use std::time::Instant;

    use super::super::window_stats::{Moments, WINDOW_MESSAGES, WindowSamples};

    #[test]
    fn window_mean_is_not_message_weighted() {
        let start = Instant::now();
        let samples = WindowSamples {
            marks: vec![
                (0, start),
                (WINDOW_MESSAGES, start + std::time::Duration::from_nanos(WINDOW_MESSAGES as u64)),
                (
                    WINDOW_MESSAGES + 1,
                    start + std::time::Duration::from_nanos(WINDOW_MESSAGES as u64 + 101),
                ),
            ],
            used: 3,
        };
        let moments = samples.report(WINDOW_MESSAGES + 1, false);
        assert_eq!(moments.mean, 51.);
        let whole_run = (WINDOW_MESSAGES + 101) as f64 / (WINDOW_MESSAGES + 1) as f64;
        assert!((moments.mean - whole_run).abs() > 1.);
    }

    fn moments(values: &[f64]) -> Moments {
        let mut result = Moments::default();
        for &value in values {
            result.add(value);
        }
        result
    }

    #[test]
    fn centered_moments_preserve_small_variation_around_large_values() {
        let m = moments(&[1e12 - 1., 1e12, 1e12 + 1.]);
        assert_eq!(m.mean, 1e12);
        assert_eq!(m.m2, 2.);
        assert_eq!(m.sample_sd(), 1.);
        assert!(Moments::default().sample_sd().is_nan());
        assert!(moments(&[1.]).sample_sd().is_nan());
    }

    fn samples(counts: &[usize]) -> WindowSamples {
        let mut samples = WindowSamples::new(counts.len() * WINDOW_MESSAGES);
        samples.start();
        let start = Instant::now();
        for &count in &counts[..counts.len() - 1] {
            samples.marks[samples.used] =
                (count, start + std::time::Duration::from_nanos(3 * count as u64));
            samples.used += 1;
        }
        let count = *counts.last().unwrap();
        samples.finish(count, start, start + std::time::Duration::from_nanos(3 * count as u64));
        samples
    }

    #[test]
    fn windows_cover_partial_counts_and_drain_overshoot() {
        for counts in [
            vec![1],
            vec![WINDOW_MESSAGES],
            vec![WINDOW_MESSAGES + 1],
            vec![WINDOW_MESSAGES + 63, 2 * WINDOW_MESSAGES + 10, 3 * WINDOW_MESSAGES],
            vec![WINDOW_MESSAGES, WINDOW_MESSAGES + 1],
            (1..=1024).map(|i| i * WINDOW_MESSAGES).collect(),
        ] {
            let total = *counts.last().unwrap();
            let mut samples = samples(&counts);
            let m = samples.report(total, false);
            assert_eq!(m.n, counts.len());
            assert_eq!(m.mean, 3.);
            assert_eq!(m.m2, 0.);
            samples.start();
            let start = Instant::now();
            samples.finish(1, start, start + std::time::Duration::from_nanos(9));
            assert_eq!(samples.report(1, false).mean, 9.);
        }
    }

    #[test]
    #[should_panic(expected = "empty window")]
    fn duplicate_window_endpoint_is_rejected() {
        samples(&[WINDOW_MESSAGES, WINDOW_MESSAGES]).report(WINDOW_MESSAGES, false);
    }

    #[test]
    #[should_panic(expected = "missing window threshold")]
    fn missing_window_threshold_is_rejected() {
        samples(&[3 * WINDOW_MESSAGES]).report(3 * WINDOW_MESSAGES, false);
    }
}
