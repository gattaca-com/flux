use flux_timing::{Duration, Instant};

#[inline(always)]
pub fn vsync<F, R>(duration: Option<Duration>, f: F) -> R
where
    F: FnOnce() -> R,
{
    match duration {
        Some(duration) if duration != Duration(0) => {
            let start_t = Instant::now();
            let out = f();
            let el = start_t.elapsed();
            if el < duration {
                std::thread::sleep((duration - el).into())
            }
            out
        }
        _ => f(),
    }
}
