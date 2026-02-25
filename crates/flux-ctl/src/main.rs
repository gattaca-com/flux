use std::path::PathBuf;

use clap::{Parser, Subcommand};
use flux_ctl::{discovery, tui};

#[derive(Parser)]
#[command(name = "flux-ctl", about = "Manage and observe flux shared memory")]
struct Cli {
    /// Base directory (default: ~/.local/share)
    #[arg(long, global = true)]
    base_dir: Option<PathBuf>,

    #[command(subcommand)]
    command: Option<Commands>,
}

#[derive(Subcommand)]
enum Commands {
    /// List all shmem segments across all apps
    List {
        #[arg(short, long)]
        verbose: bool,
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
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let cli = Cli::parse();
    let base_dir = cli.base_dir.unwrap_or_else(flux_utils::directories::local_share_dir);

    match cli.command.unwrap_or(Commands::Watch { app: None }) {
        Commands::List { verbose } => discovery::list_all(&base_dir, verbose),
        Commands::Inspect { app, segment } => {
            discovery::inspect(&base_dir, app.as_deref(), segment.as_deref())
        }
        Commands::Watch { app } => tui::run(&base_dir, app.as_deref()),
        Commands::Clean { force, app } => discovery::clean(&base_dir, app.as_deref(), force),
    }
}
