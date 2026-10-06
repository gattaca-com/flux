use std::path::{Path, PathBuf};

use clap::{Parser, Subcommand};

use super::MigrationSet;

#[derive(Parser)]
#[command(name = "chmig", about = "ClickHouse migration sync")]
struct Cli {
    #[arg(short, long, default_value = "config.example.toml")]
    config: PathBuf,
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    Sync,
    Rollback { target_version: u32 },
    Validate,
    Create { name: String },
}

#[derive(serde::Deserialize)]
struct ConfigFile {
    clickhouse: ClickhouseSection,
}

#[derive(serde::Deserialize)]
struct ClickhouseSection {
    url: String,
    database: String,
    user: String,
    #[serde(default)]
    password: Option<String>,
}

impl ClickhouseSection {
    fn client(&self) -> clickhouse::Client {
        let mut client = clickhouse::Client::default()
            .with_url(&self.url)
            .with_database(&self.database)
            .with_user(&self.user);
        if let Some(password) = &self.password {
            client = client.with_password(password);
        }
        client
    }
}

/// Run `chmig` commands against embedded SQL. `create` writes to
/// `migrations_dir`. Call from a binary entry point before initializing tracing
/// or an async runtime.
pub fn run(migrations: &MigrationSet<'_>, migrations_dir: &Path) -> eyre::Result<()> {
    tracing_subscriber::fmt::init();
    let cli = Cli::parse();

    match cli.command {
        Command::Sync => {
            let config = load_config(&cli.config)?;
            runtime()?.block_on(migrations.sync(&config.client()))?;
        }
        Command::Rollback { target_version } => {
            let config = load_config(&cli.config)?;
            runtime()?.block_on(migrations.rollback(&config.client(), target_version))?;
        }
        Command::Validate => migrations.validate()?,
        Command::Create { name } => create_migration(migrations_dir, &name)?,
    }

    Ok(())
}

fn runtime() -> eyre::Result<tokio::runtime::Runtime> {
    Ok(tokio::runtime::Builder::new_multi_thread().enable_all().build()?)
}

fn load_config(path: &Path) -> eyre::Result<ClickhouseSection> {
    let text = std::fs::read_to_string(path)
        .map_err(|error| eyre::eyre!("failed to read {}: {error}", path.display()))?;
    let config: ConfigFile = toml::from_str(&text)
        .map_err(|error| eyre::eyre!("failed to parse {}: {error}", path.display()))?;
    Ok(config.clickhouse)
}

fn create_migration(migrations_dir: &Path, name: &str) -> eyre::Result<()> {
    validate_migration_name(name)?;
    eyre::ensure!(
        migrations_dir.is_dir(),
        "migrations directory not found at {}",
        migrations_dir.display(),
    );

    let mut max_version = 0u32;
    for entry in std::fs::read_dir(migrations_dir)? {
        let entry = entry?;
        if !entry.file_type()?.is_dir() {
            continue;
        }
        let directory_name = entry.file_name();
        let Some(directory_name) = directory_name.to_str() else {
            continue;
        };
        if let Some((version, _)) = directory_name.split_once('_') &&
            let Ok(version) = version.parse::<u32>()
        {
            max_version = max_version.max(version);
        }
    }

    let version =
        max_version.checked_add(1).ok_or_else(|| eyre::eyre!("migration version overflow"))?;
    let directory = migrations_dir.join(format!("{version:04}_{name}"));
    std::fs::create_dir(&directory)?;
    std::fs::write(directory.join("up.sql"), "")?;
    std::fs::write(directory.join("down.sql"), "")?;
    println!("created {}", directory.display());
    Ok(())
}

fn validate_migration_name(name: &str) -> eyre::Result<()> {
    eyre::ensure!(!name.is_empty(), "migration name must not be empty");
    eyre::ensure!(
        !name.starts_with('_') && !name.ends_with('_'),
        "migration name must not start or end with '_'",
    );
    eyre::ensure!(!name.contains("__"), "migration name must not contain consecutive '_'");

    let first = name.chars().next().expect("checked non-empty");
    eyre::ensure!(first.is_ascii_lowercase(), "migration name must start with a lowercase letter",);
    for character in name.chars() {
        eyre::ensure!(
            character.is_ascii_lowercase() || character.is_ascii_digit() || character == '_',
            "migration name must be snake_case ASCII ([a-z0-9_])",
        );
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    #[test]
    fn create_in_caller_directory() {
        let directory = tempfile::tempdir().unwrap();
        super::create_migration(directory.path(), "initial").unwrap();
        let first = directory.path().join("0001_initial/up.sql");
        std::fs::write(&first, "SELECT 1").unwrap();
        super::create_migration(directory.path(), "next").unwrap();
        assert_eq!(std::fs::read_to_string(first).unwrap(), "SELECT 1");
        assert_eq!(std::fs::read(directory.path().join("0002_next/up.sql")).unwrap(), b"");
        assert_eq!(std::fs::read(directory.path().join("0002_next/down.sql")).unwrap(), b"");
        assert!(super::create_migration(directory.path(), "../escape").is_err());
        assert_eq!(std::fs::read_dir(directory.path()).unwrap().count(), 2);
    }
}
