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
use flux_profiler::{CrossProcessReader, live_apps, published_pid};

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

    let out = args.out.unwrap_or_else(|| PathBuf::from(format!("{app}-trace-{pid}.fxt")));
    if let Err(e) = std::fs::write(&out, reader.events().fxt_trace()) {
        eprintln!("failed to write {}: {e}", out.display());
        return ExitCode::FAILURE;
    }
    let events = reader.events();
    println!("exported {} threads → {}", events.threads().count(), out.display());
    if events.threads().any(|t| t.loss.is_lossy()) {
        eprintln!("warning: events were lost (producer outran the reader); the trace has holes");
    }
    ExitCode::SUCCESS
}
