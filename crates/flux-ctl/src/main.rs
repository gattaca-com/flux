use std::{
    fs::File,
    io::IsTerminal,
    path::PathBuf,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};

use clap::{Parser, Subcommand};
use flux_ctl::{discovery, record, tui};
use flux_timing::Nanos;
use tracing_subscriber::EnvFilter;

#[derive(Parser)]
#[command(name = "flux-ctl", about = "Manage and observe flux shared memory")]
struct Cli {
    /// Base directory (default: ~/.local/share)
    #[arg(long, global = true)]
    base_dir: Option<PathBuf>,

    /// Clean up all dead/stale shmem segments and exit
    #[arg(long)]
    clean: bool,

    #[command(subcommand)]
    command: Option<Commands>,
}

#[derive(Subcommand)]
enum Commands {
    /// List all shmem segments across all apps
    List {
        #[arg(short, long)]
        verbose: bool,
        /// Output as JSON
        #[arg(long)]
        json: bool,
        /// Filter by app name
        #[arg(short, long)]
        app: Option<String>,
    },
    /// Inspect a specific segment
    Inspect {
        /// App name filter
        app: Option<String>,
        /// Segment name filter
        segment: Option<String>,
    },
    /// Live TUI monitor (default)
    Watch {
        /// App name filter
        app: Option<String>,
    },
    /// Clean stale segments (dead PIDs)
    Clean {
        /// Actually remove (default: dry-run)
        #[arg(long)]
        force: bool,
        /// App name filter
        app: Option<String>,
    },
    /// Record an app's timer and tile-metric samples to Parquet
    Record {
        /// App name
        app: String,
        /// Directory for `timers-<n>.parquet` and `tiles-<n>.parquet`
        /// (default: `<app>-<UTC time>` in the current directory)
        #[arg(long)]
        out: Option<PathBuf>,
        /// Seconds; 0 records until ^C, SIGTERM or SIGHUP, which also end a
        /// timed recording early
        #[arg(long, default_value_t = 60)]
        duration: u64,
        /// Seconds per file; 0 writes one file per table
        #[arg(long, default_value_t = 300)]
        rotate_secs: u64,
        /// Only queues whose name contains this
        #[arg(long = "match")]
        filter: Option<String>,
    },
    /// Per-queue statistics of a `record` directory, as JSON
    Summarize {
        /// A `record --out` directory
        dir: PathBuf,
        /// Window start, in nanoseconds since the Unix epoch (`date +%s%N`)
        #[arg(long)]
        from: Option<u64>,
        /// Window end, exclusive
        #[arg(long)]
        to: Option<u64>,
    },
    /// Show summary statistics for all registered segments
    Stats {
        /// Filter by app name
        #[arg(short, long)]
        app: Option<String>,
        /// Run `health_check` diagnostics and print results
        #[arg(short, long)]
        verbose: bool,
    },
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let cli = Cli::parse();
    let base_dir = cli.base_dir.unwrap_or_else(flux_utils::directories::local_share_dir);

    let tui = !cli.clean && matches!(cli.command, None | Some(Commands::Watch { .. }));
    init_logging(tui)?;
    if cli.clean {
        return discovery::clean(&base_dir, None, true);
    }

    match cli.command.unwrap_or(Commands::Watch { app: None }) {
        Commands::List { verbose, json, app } => {
            if json {
                discovery::list_json(&base_dir, app.as_deref())
            } else {
                discovery::list_all(&base_dir, verbose, app.as_deref())
            }
        }
        Commands::Inspect { app, segment } => {
            discovery::inspect(&base_dir, app.as_deref(), segment.as_deref())
        }
        Commands::Watch { app } => tui::run(&base_dir, app.as_deref()),
        Commands::Clean { force, app } => discovery::clean(&base_dir, app.as_deref(), force),
        Commands::Stats { app, verbose } => discovery::stats(&base_dir, app.as_deref(), verbose),
        Commands::Summarize { dir, from, to } => {
            let report = record::summarize(&dir, from, to)?;
            println!("{}", serde_json::to_string_pretty(&report)?);
            Ok(())
        }
        Commands::Record { app, out, duration, rotate_secs, filter } => {
            let secs = |s| (s > 0).then(|| Duration::from_secs(s));
            let out = out.unwrap_or_else(|| {
                PathBuf::from(format!("{app}-{}", Nanos::now().with_fmt_utc("%Y%m%dT%H%M%SZ")))
            });
            let recorder = record::Recorder::attach(
                &base_dir,
                &app,
                &out,
                secs(rotate_secs),
                filter.as_deref(),
            )?;
            let stop = Arc::new(AtomicBool::new(false));
            let on_signal = Arc::clone(&stop);
            ctrlc::set_handler(move || on_signal.store(true, Ordering::Relaxed))?;

            let until = record::Until { duration: secs(duration) };
            print!("{}", recorder.run(until, &stop)?);
            Ok(())
        }
    }
}

/// Sends the `tracing` events of flux-ctl and the flux crates to stderr, or,
/// for the TUI, which draws on the terminal, to
/// `$XDG_STATE_HOME/flux-ctl/flux-ctl.log` (`~/.local/state` without it).
/// `RUST_LOG` overrides the default filter, `warn,flux_ctl=info`.
fn init_logging(tui: bool) -> Result<(), Box<dyn std::error::Error>> {
    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| "warn,flux_ctl=info".into());
    let logs = tracing_subscriber::fmt().with_env_filter(filter);
    if tui {
        let state = std::env::var_os("XDG_STATE_HOME").map(PathBuf::from).or_else(|| {
            std::env::var_os("HOME").map(|home| PathBuf::from(home).join(".local/state"))
        });
        let dir = state.ok_or("neither XDG_STATE_HOME nor HOME is set")?.join("flux-ctl");
        std::fs::create_dir_all(&dir)?;
        let file = File::options().create(true).append(true).open(dir.join("flux-ctl.log"))?;
        logs.with_writer(Mutex::new(file)).with_ansi(false).init();
    } else {
        logs.with_writer(std::io::stderr).with_ansi(std::io::stderr().is_terminal()).init();
    }
    Ok(())
}
