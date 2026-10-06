//! Embedded migrations over the HTTP client.
//!
//! SQL files use
//! `NNNN_name/{up,down}.sql` and separate statements with `;\n`. Each
//! application owns its migration directory. Sync only moves forward; rollback
//! uses the down SQL stored in `_migrations`.

use std::cmp::Reverse;

use eyre::ensure;
use include_dir::Dir;
use sha2::{Digest, Sha256};
use tracing::info;

#[cfg(feature = "cli")]
mod cli;
#[cfg(feature = "cli")]
pub use cli::run as run_cli;

/// Embedded SQL and the last version exempt from compatibility linting.
pub struct MigrationSet<'a> {
    migrations_dir: Dir<'a>,
    compatibility_lint_baseline: u32,
}

impl<'a> MigrationSet<'a> {
    pub const fn new(migrations_dir: Dir<'a>, compatibility_lint_baseline: u32) -> Self {
        Self { migrations_dir, compatibility_lint_baseline }
    }

    /// Apply missing versions and check existing checksums. Newer database
    /// versions are retained.
    pub async fn sync(&self, client: &clickhouse::Client) -> eyre::Result<()> {
        sync_migrations(self, client).await
    }

    pub async fn rollback(
        &self,
        client: &clickhouse::Client,
        target_version: u32,
    ) -> eyre::Result<()> {
        rollback(client, target_version).await
    }

    /// Check version continuity and backward compatibility without connecting
    /// to a database.
    pub fn validate(&self) -> eyre::Result<()> {
        let migrations = parse_migrations(&self.migrations_dir)?;
        lint_backward_compatibility(&migrations, self.compatibility_lint_baseline)
    }
}

const CREATE_TRACKING_TABLE: &str = "
CREATE TABLE IF NOT EXISTS _migrations (
    version    UInt32,
    name       String,
    checksum   String,
    down_sql   String,
    applied_at DateTime64(3, 'UTC') DEFAULT now()
) ENGINE = ReplacingMergeTree(applied_at)
ORDER BY version
";

struct Migration {
    version: u32,
    name: String,
    up_sql: String,
    down_sql: String,
    checksum: String,
}

#[derive(clickhouse::Row, serde::Deserialize, serde::Serialize)]
struct MigrationRow {
    version: u32,
    name: String,
    checksum: String,
    down_sql: String,
}

fn parse_migrations(migrations_dir: &Dir<'_>) -> eyre::Result<Vec<Migration>> {
    let mut migrations = Vec::new();

    for entry in migrations_dir.dirs() {
        let dir_name = entry
            .path()
            .file_name()
            .and_then(|name| name.to_str())
            .ok_or_else(|| eyre::eyre!("invalid migration directory name"))?;
        let (version_str, name) = dir_name
            .split_once('_')
            .ok_or_else(|| eyre::eyre!("migration dir must be NNNN_name: {dir_name}"))?;
        let version = version_str
            .parse::<u32>()
            .map_err(|_| eyre::eyre!("invalid version number in {dir_name}"))?;
        let up_sql = entry
            .get_file(entry.path().join("up.sql"))
            .and_then(|file| file.contents_utf8())
            .ok_or_else(|| eyre::eyre!("missing up.sql in {dir_name}"))?
            .to_owned();
        let down_sql = entry
            .get_file(entry.path().join("down.sql"))
            .and_then(|file| file.contents_utf8())
            .ok_or_else(|| eyre::eyre!("missing down.sql in {dir_name}"))?
            .to_owned();
        let checksum = sha256_hex(up_sql.as_bytes());
        migrations.push(Migration { version, name: name.to_owned(), up_sql, down_sql, checksum });
    }

    migrations.sort_by_key(|migration| migration.version);
    validate_migration_sequence(&migrations)?;
    Ok(migrations)
}

fn validate_migration_sequence(migrations: &[Migration]) -> eyre::Result<()> {
    if migrations.is_empty() {
        return Ok(());
    }

    ensure!(
        migrations[0].version == 1,
        "migration sequence must start at version 1, found {} ({})",
        migrations[0].version,
        migrations[0].name,
    );

    for window in migrations.windows(2) {
        let previous = &window[0];
        let current = &window[1];
        ensure!(
            current.version != previous.version,
            "duplicate migration version {} ({} and {})",
            current.version,
            previous.name,
            current.name,
        );
        ensure!(
            current.version == previous.version + 1,
            "non-contiguous migration versions: expected {} after {} ({}), found {} ({})",
            previous.version + 1,
            previous.version,
            previous.name,
            current.version,
            current.name,
        );
    }

    Ok(())
}

fn sha256_hex(data: &[u8]) -> String {
    Sha256::digest(data).iter().fold(String::with_capacity(64), |mut output, byte| {
        use std::fmt::Write;
        let _ = write!(output, "{byte:02x}");
        output
    })
}

fn split_statements(sql: &str) -> Vec<&str> {
    sql.split(";\n").map(str::trim).filter(|statement| !statement.is_empty()).collect()
}

async fn exec_statements(client: &clickhouse::Client, sql: &str) -> eyre::Result<()> {
    for statement in split_statements(sql) {
        client.query(statement).execute().await?;
    }
    Ok(())
}

async fn get_applied(client: &clickhouse::Client) -> eyre::Result<Vec<MigrationRow>> {
    Ok(client
        .query("SELECT version, name, checksum, down_sql FROM _migrations FINAL ORDER BY version")
        .fetch_all()
        .await?)
}

async fn record_applied(client: &clickhouse::Client, migration: &Migration) -> eyre::Result<()> {
    let mut insert = client.insert::<MigrationRow>("_migrations").await?;
    insert
        .write(&MigrationRow {
            version: migration.version,
            name: migration.name.clone(),
            checksum: migration.checksum.clone(),
            down_sql: migration.down_sql.clone(),
        })
        .await?;
    insert.end().await?;
    Ok(())
}

async fn sync_migrations(
    migration_set: &MigrationSet<'_>,
    client: &clickhouse::Client,
) -> eyre::Result<()> {
    client.query(CREATE_TRACKING_TABLE).execute().await?;

    let embedded = parse_migrations(&migration_set.migrations_dir)?;
    let applied = get_applied(client).await?;
    let applied_versions: rustc_hash::FxHashSet<u32> =
        applied.iter().map(|migration| migration.version).collect();

    for migration in &embedded {
        if let Some(database) = applied.iter().find(|row| row.version == migration.version) {
            ensure!(
                migration.checksum == database.checksum,
                "checksum mismatch for migration {} ({}): fs={} db={}",
                migration.version,
                migration.name,
                migration.checksum,
                database.checksum,
            );
        }
    }

    let mut applied_count = 0u32;
    for migration in &embedded {
        if applied_versions.contains(&migration.version) {
            continue;
        }
        exec_statements(client, &migration.up_sql).await?;
        record_applied(client, migration).await?;
        info!(version = migration.version, name = migration.name, "applied");
        applied_count += 1;
    }

    if applied_count == 0 {
        let binary_version = embedded.last().map_or(0, |migration| migration.version);
        let database_version = applied.last().map_or(0, |migration| migration.version);
        info!(binary_version, database_version, "migrations up to date");
    }

    Ok(())
}

/// Revert migrations above `target_version` using the stored down SQL.
pub async fn rollback(client: &clickhouse::Client, target_version: u32) -> eyre::Result<()> {
    client.query(CREATE_TRACKING_TABLE).execute().await?;

    let applied = get_applied(client).await?;
    let database_version = applied.last().map_or(0, |migration| migration.version);
    ensure!(
        target_version <= database_version,
        "target version {target_version} is not below current DB version {database_version}",
    );

    let mut to_revert: Vec<&MigrationRow> =
        applied.iter().filter(|migration| migration.version > target_version).collect();
    to_revert.sort_by_key(|migration| Reverse(migration.version));

    if to_revert.is_empty() {
        info!(version = database_version, "nothing to rollback");
        return Ok(());
    }

    for migration in &to_revert {
        ensure!(
            !migration.down_sql.is_empty(),
            "cannot rollback migration {} ({}): no down_sql stored in DB",
            migration.version,
            migration.name,
        );
    }

    for migration in to_revert {
        exec_statements(client, &migration.down_sql).await?;
        client
            .query(&format!(
                "ALTER TABLE _migrations DELETE WHERE version = {} SETTINGS mutations_sync = 1",
                migration.version,
            ))
            .execute()
            .await?;
        info!(version = migration.version, name = migration.name, "reverted");
    }

    Ok(())
}

const ALLOW_DESTRUCTIVE: &str = "-- chmig:allow-destructive";
const DESTRUCTIVE_PATTERNS: &[&str] =
    &["DROP COLUMN", "DROP TABLE", "RENAME COLUMN", "RENAME TABLE", "MODIFY COLUMN"];

fn lint_backward_compatibility(
    migrations: &[Migration],
    compatibility_lint_baseline: u32,
) -> eyre::Result<()> {
    let mut errors = Vec::new();

    for migration in
        migrations.iter().filter(|migration| migration.version > compatibility_lint_baseline)
    {
        let lines: Vec<&str> = migration.up_sql.lines().collect();
        let upper = migration.up_sql.to_ascii_uppercase();
        let upper_lines: Vec<&str> = upper.lines().collect();

        for (line_index, line) in lines.iter().enumerate() {
            if line.contains(ALLOW_DESTRUCTIVE) ||
                line_index > 0 && lines[line_index - 1].contains(ALLOW_DESTRUCTIVE)
            {
                continue;
            }
            let upper_line = upper_lines.get(line_index).copied().unwrap_or_default();
            let collapsed = upper_line.split_whitespace().collect::<Vec<_>>().join(" ");
            for pattern in DESTRUCTIVE_PATTERNS {
                if collapsed.contains(pattern) {
                    errors.push(format!(
                        "  migration {} ({}) line {}: found `{pattern}`",
                        migration.version,
                        migration.name,
                        line_index + 1,
                    ));
                }
            }
        }

        for statement in split_statements(&migration.up_sql) {
            if statement.contains(ALLOW_DESTRUCTIVE) {
                continue;
            }
            let upper_statement =
                statement.to_ascii_uppercase().split_whitespace().collect::<Vec<_>>().join(" ");
            if upper_statement.contains("ADD COLUMN") && !upper_statement.contains("DEFAULT") {
                let offset = statement.as_ptr() as usize - migration.up_sql.as_ptr() as usize;
                let line = migration.up_sql[..offset].matches('\n').count() + 1;
                errors.push(format!(
                    "  migration {} ({}) line {}: `ADD COLUMN` without `DEFAULT`",
                    migration.version, migration.name, line,
                ));
            }
        }
    }

    if !errors.is_empty() {
        eyre::bail!(
            "backward-compatibility lint failed:\n{}\n\nAdd `{ALLOW_DESTRUCTIVE}` to an intentional statement.",
            errors.join("\n"),
        );
    }
    Ok(())
}
