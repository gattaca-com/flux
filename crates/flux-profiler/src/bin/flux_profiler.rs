//! Attach to a running `#[timed]` producer and dump its retained marks as a
//! Fuchsia FXT trace (open at <https://magic-trace.org> or in Perfetto) when
//! stopped by Ctrl-C / SIGTERM or by the producer exiting.

use std::{
    path::PathBuf,
    process::ExitCode,
    sync::atomic::{AtomicBool, Ordering},
    thread,
    time::{Duration, Instant},
};

use clap::Parser;
use flux_profiler::{CrossProcessReader, EventsDrainer, live_apps, published_pid};
use flux_timing::Duration as TscDuration;
use rustc_hash::FxHashMap;

static STOP: AtomicBool = AtomicBool::new(false);

extern "C" fn on_signal(_: libc::c_int) {
    STOP.store(true, Ordering::Release);
}

#[derive(Parser)]
#[command(about = "Attach to a #[timed] producer and export its marks as an FXT trace")]
struct Args {
    /// Producer pid; needed only when multiple instrumented apps are live.
    #[arg(long)]
    pid: Option<u32>,
    /// Trace output path (default: `<app>-trace-<pid>.fxt`).
    #[arg(long)]
    out: Option<PathBuf>,
    /// Stop and export after this much capture time, e.g. `30s`, `5m`, `1h`.
    #[arg(long, value_parser = humantime::parse_duration)]
    duration: Option<Duration>,
    /// Stop and export once the reader's retained events exceed this, e.g.
    /// `512MB`, `2GB`. Guards against an unbounded capture.
    #[arg(long, default_value = "1GB")]
    max_mem: bytesize::ByteSize,
    /// Discard a completed top-level frame (its close empties the stack) when
    /// it spans less than this, e.g. `100ns`, `5us` — throws away idle polls
    /// so only traces of interest are retained.
    #[arg(long, value_parser = humantime::parse_duration)]
    filter_short_frames: Option<Duration>,
    /// Print inclusive per-frame timing quantiles after capture.
    #[arg(long)]
    summary: bool,
}

fn print_summary(events: &EventsDrainer) {
    let mut samples: FxHashMap<u64, Vec<u64>> = FxHashMap::default();
    for thread in events.threads() {
        let mut stack = Vec::new();
        for mark in thread.marks {
            if mark.is_open() {
                stack.push((mark.id, mark.ts));
                continue;
            }
            let Some((id, started)) = stack.pop() else { continue };
            if id != mark.id {
                stack.clear();
                continue;
            }
            let elapsed_ns = TscDuration(mark.ts.saturating_sub(started)).as_nanos() as u64;
            samples.entry(id).or_default().push(elapsed_ns);
        }
    }

    let mut rows: Vec<_> = samples
        .into_iter()
        .filter_map(|(id, mut values)| {
            if values.is_empty() {
                return None;
            }
            values.sort_unstable();
            let total: u128 = values.iter().map(|&value| u128::from(value)).sum();
            let percentile = |numerator: usize, denominator: usize| {
                let index = (values.len() - 1) * numerator / denominator;
                values[index]
            };
            Some((
                total,
                events.meta().names.get(&id).cloned().unwrap_or_else(|| format!("unknown_{id}")),
                values.len(),
                total as f64 / values.len() as f64,
                percentile(1, 2),
                percentile(99, 100),
                percentile(999, 1000),
                *values.last().expect("non-empty"),
            ))
        })
        .collect();
    rows.sort_unstable_by(|left, right| right.0.cmp(&left.0));

    println!("PROFILE_SUMMARY count mean_us p50_us p99_us p999_us max_us total_ms frame");
    for (total, name, count, mean, p50, p99, p999, max) in rows {
        println!(
            "PROFILE_FRAME {count} {:.3} {:.3} {:.3} {:.3} {:.3} {:.3} {name}",
            mean / 1_000.0,
            p50 as f64 / 1_000.0,
            p99 as f64 / 1_000.0,
            p999 as f64 / 1_000.0,
            max as f64 / 1_000.0,
            total as f64 / 1_000_000.0,
        );
    }
}

/// The producer to attach to: the one matching `--pid`, or the sole live one
/// when it's unambiguous.
fn resolve(pid: Option<u32>) -> Result<(String, u32), String> {
    let live = live_apps();
    match pid {
        Some(pid) => live.into_iter().find(|(_, p)| *p == pid).ok_or_else(|| {
            format!("pid {pid} has not published `#[timed]` rings (is it live and enabled?)")
        }),
        None => match live.len() {
            0 => Err("no live `#[timed]` producer found".to_owned()),
            1 => Ok(live.into_iter().next().expect("len == 1")),
            _ => {
                let list: Vec<_> =
                    live.iter().map(|(app, pid)| format!("  {app} (pid {pid})")).collect();
                Err(format!("multiple live producers; pass --pid <pid>:\n{}", list.join("\n")))
            }
        },
    }
}

fn main() -> ExitCode {
    tracing_subscriber::fmt()
        .with_writer(std::io::stderr)
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("warn")),
        )
        .init();

    let args = Args::parse();

    let (app, pid) = match resolve(args.pid) {
        Ok(target) => target,
        Err(e) => {
            eprintln!("{e}");
            return ExitCode::FAILURE;
        }
    };
    let Some(mut reader) = CrossProcessReader::attach(&app) else {
        eprintln!("no live producer published under app '{app}'");
        return ExitCode::FAILURE;
    };
    if let Some(min) = args.filter_short_frames {
        reader.filter_short_frames(min);
    }

    unsafe {
        libc::signal(libc::SIGINT, on_signal as libc::sighandler_t);
        libc::signal(libc::SIGTERM, on_signal as libc::sighandler_t);
    }
    eprintln!("attached to '{app}' (pid {pid}); Ctrl-C to stop and export");

    // Observe stop before polling so the final poll flushes the ring tails;
    // the rings and pid file outlive the producer, so this also holds when it
    // exits between iterations.
    let start = Instant::now();
    let mut iterations = 0u32;
    loop {
        let mut stopping = STOP.load(Ordering::Acquire);
        reader.poll();
        if let Some(limit) = args.duration {
            if start.elapsed() >= limit {
                eprintln!("reached --duration limit");
                stopping = true;
            }
        }
        if reader.events().retained_bytes() as u64 >= args.max_mem.as_u64() {
            eprintln!("reached --max-mem limit ({})", args.max_mem);
            stopping = true;
        }
        if stopping {
            break;
        }
        iterations += 1;
        if iterations.is_multiple_of(1000) && published_pid(&app) != Some(pid) {
            eprintln!("producer exited");
            break;
        }
        thread::sleep(Duration::from_millis(1));
    }

    let events = reader.events();
    if args.summary {
        print_summary(events);
    }
    let out = args.out.unwrap_or_else(|| PathBuf::from(format!("{app}-trace-{pid}.fxt")));
    if let Err(e) = std::fs::write(&out, reader.events().fxt_trace()) {
        eprintln!("failed to write {}: {e}", out.display());
        return ExitCode::FAILURE;
    }
    println!("exported {} threads → {}", events.threads().count(), out.display());
    if events.threads().any(|t| t.loss.is_lossy()) {
        eprintln!("warning: events were lost (producer outran the reader); the trace has holes");
    }
    ExitCode::SUCCESS
}
