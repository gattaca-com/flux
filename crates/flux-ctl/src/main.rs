use std::path::PathBuf;

use clap::{Parser, Subcommand};
use flux_ctl::{discovery, tui};

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
    /// Scan filesystem for pre-existing shmem, register missing entries,
    /// and remove stale flinks
    Scan,
    /// Clean stale segments (dead PIDs)
    Clean {
        /// Actually remove (default: dry-run)
        #[arg(long)]
        force: bool,
        /// App name filter
        app: Option<String>,
    },
    /// Show summary statistics for all registered segments
    Stats {
        /// Filter by app name
        #[arg(short, long)]
        app: Option<String>,
        /// Run health_check diagnostics and print results
        #[arg(short, long)]
        verbose: bool,
    },
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let cli = Cli::parse();
    let base_dir = cli.base_dir.unwrap_or_else(flux_utils::directories::local_share_dir);

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
        Commands::Scan => discovery::scan(&base_dir),
        Commands::Clean { force, app } => discovery::clean(&base_dir, app.as_deref(), force),
        Commands::Stats { app, verbose } => discovery::stats(&base_dir, app.as_deref(), verbose),
    }
}
