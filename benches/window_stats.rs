use std::time::Instant;

pub const WINDOW_MESSAGES: usize = 1 << 14;

#[derive(Clone, Copy, Debug, Default)]
pub struct Moments {
    pub n: usize,
    pub mean: f64,
    pub m2: f64,
    pub sum: f64,
    pub sum_squares: f64,
}

impl Moments {
    pub fn add(&mut self, value: f64) {
        assert!(value.is_finite());
        self.n += 1;
        let delta = value - self.mean;
        self.mean += delta / self.n as f64;
        self.m2 += delta * (value - self.mean);
        self.sum += value;
        self.sum_squares += value * value;
    }

    pub fn sample_sd(self) -> f64 {
        if self.n < 2 { f64::NAN } else { (self.m2 / (self.n - 1) as f64).sqrt() }
    }

    pub fn cv(self) -> f64 {
        self.sample_sd() / self.mean
    }
}

pub struct WindowSamples {
    pub(super) marks: Vec<(usize, Instant)>,
    pub(super) used: usize,
}

impl WindowSamples {
    pub fn new(max_messages: usize) -> Self {
        Self {
            marks: vec![(0, Instant::now()); max_messages.div_ceil(WINDOW_MESSAGES) + 1],
            used: 0,
        }
    }

    pub fn start(&mut self) {
        self.used = 1;
    }

    #[inline]
    pub fn record(&mut self, completed: usize) {
        self.marks[self.used] = (completed, Instant::now());
        self.used += 1;
    }

    pub fn finish(&mut self, completed: usize, started: Instant, finished: Instant) {
        self.marks[0] = (0, started);
        self.marks[self.used] = (completed, finished);
        self.used += 1;
    }

    pub fn report(&self, expected_messages: usize, print_samples: bool) -> Moments {
        let marks = &self.marks[..self.used];
        assert!(marks.len() >= 2);
        assert_eq!(marks[0].0, 0);
        assert_eq!(marks.last().unwrap().0, expected_messages);
        let mut moments = Moments::default();
        if print_samples {
            println!("# window,index,messages,elapsed_ns (post-drain; outer run endpoints)");
        }
        for (index, pair) in marks.windows(2).enumerate() {
            let count = pair[1].0.checked_sub(pair[0].0).unwrap();
            assert!(count > 0, "empty window");
            // Interior marks cross successive absolute thresholds. The final
            // drain may cross a threshold and finish together, giving one mark.
            if index + 2 < marks.len() {
                assert!(
                    ((index + 1) * WINDOW_MESSAGES..(index + 2) * WINDOW_MESSAGES)
                        .contains(&pair[1].0),
                    "window mark outside its threshold"
                );
            } else {
                assert!(
                    expected_messages <= (index + 2) * WINDOW_MESSAGES,
                    "missing window threshold"
                );
            }
            let elapsed = pair[1].1.checked_duration_since(pair[0].1).unwrap().as_nanos();
            assert!(elapsed > 0);
            moments.add(elapsed as f64 / count as f64);
            if print_samples {
                println!("window,{index},{count},{elapsed}");
            }
        }
        println!(
            "# windows n={} mean_ns_per_message={:.17e} sample_sd_ns_per_message={:.17e} cv={:.9} sum={:.17e} sum_squares={:.17e} m2={:.17e}",
            moments.n,
            moments.mean,
            moments.sample_sd(),
            moments.cv(),
            moments.sum,
            moments.sum_squares,
            moments.m2
        );
        moments
    }
}
