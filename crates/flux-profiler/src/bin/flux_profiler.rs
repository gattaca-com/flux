//! Attach to a running `#[timed]` producer and dump its retained marks as a
//! Fuchsia FXT trace (open at <https://magic-trace.org> or in Perfetto) when
//! stopped by Ctrl-C / SIGTERM or by the producer exiting.

use std::{
    path::PathBuf,
    process::ExitCode,
    sync::atomic::{AtomicBool, Ordering},
    thread,
    time::Duration,
};

use flux_profiler::{CrossProcessReader, live_apps, published_pid};

const USAGE: &str = "usage: flux-profiler [--pid <pid>] [--out <path.fxt>]";

static STOP: AtomicBool = AtomicBool::new(false);

extern "C" fn on_signal(_: libc::c_int) {
    STOP.store(true, Ordering::Release);
}

#[derive(Default)]
struct Args {
    pid: Option<u32>,
    out: Option<PathBuf>,
}

impl Args {
    /// Accepts both `--flag value` and `--flag=value`.
    fn parse() -> Result<Self, String> {
        let mut parsed = Self::default();
        let mut args = std::env::args().skip(1);
        while let Some(arg) = args.next() {
            let (flag, inline) = match arg.split_once('=') {
                Some((flag, value)) => (flag.to_owned(), Some(value.to_owned())),
                None => (arg, None),
            };
            let mut value = || {
                inline
                    .clone()
                    .or_else(|| args.next())
                    .ok_or_else(|| format!("{flag} needs a value"))
            };
            match flag.as_str() {
                "--pid" => {
                    parsed.pid = Some(value()?.parse().map_err(|e| format!("--pid: {e}"))?);
                }
                "--out" => parsed.out = Some(value()?.into()),
                _ => return Err(format!("unknown argument: {flag}")),
            }
        }
        Ok(parsed)
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
    let args = match Args::parse() {
        Ok(args) => args,
        Err(e) => {
            eprintln!("{e}\n{USAGE}");
            return ExitCode::FAILURE;
        }
    };

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
    let mut iterations = 0u32;
    loop {
        let stopping = STOP.load(Ordering::Acquire);
        reader.poll();
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
