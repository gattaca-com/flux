# flux-clickhouse

The native client and `BufferedTable` use the caller's network poll loop.
Optional features add HTTP helpers, embedded migrations, and a migration CLI.

| Feature | API |
| --- | --- |
| Default | `ClickHouse`, `BufferedTable`, `buffered_batch!`, `rowbinary` |
| `http` | `http::insert_rows` using `clickhouse::Client` |
| `migrations` | `migrations::MigrationSet`, `migrations::rollback`; enables `http` |
| `cli` | `migrations::run_cli`; enables `migrations` |

## Embedded migrations

The application owns SQL files, its lint baseline, and its binary entry point:

```rust,ignore
use flux_clickhouse::migrations::MigrationSet;
use include_dir::include_dir;

static MIGRATIONS: MigrationSet<'static> =
    MigrationSet::new(include_dir!("$CARGO_MANIFEST_DIR/migrations"), 0);

fn main() -> eyre::Result<()> {
    flux_clickhouse::migrations::run_cli(&MIGRATIONS, "migrations".as_ref())
}
```

Enable `cli` for this binary. Add `include_dir` and `eyre` to the application.
Its build script must emit `cargo:rerun-if-changed=migrations` so SQL edits rebuild
embedded files. Startup code can call `MIGRATIONS.sync(&client).await` directly.

Directories use `NNNN_name/up.sql` and `down.sql`, starting at version 1 without
gaps. Separate statements with `;\n`. The client selects an existing database;
table names should not contain a database prefix.

The CLI supports `sync`, `rollback <version>`, `validate`, and `create <name>`.
`--config` defaults to `config.example.toml`, with a `[clickhouse]` section
containing `url`, `database`, `user`, and an optional literal `password`.
`validate` and `create` do not require a config file or a running server.
`create` writes to the directory passed to `run_cli`; other commands use embedded SQL.

`sync` preserves newer database versions and rejects changed checksums. It records
rollback SQL in `_migrations`. `rollback` uses that stored SQL even when this binary
does not embed the newer migration. Neither operation provides a transaction or
an exclusive migration lock; coordinate concurrent schema changes externally.

`validate` checks backward compatibility after the supplied baseline. Destructive
statements and added columns without `DEFAULT` require the existing
`-- chmig:allow-destructive` annotation. Sync does not run this lint automatically.

## Buffered native inserts

```rust,ignore
let mut rows = flux_clickhouse::BufferedTable::new(
    "INSERT INTO events (`results.cost_units`) SETTINGS async_insert = 0 VALUES",
)
.with_max_rows_per_batch(1_000);
rows.push(row);
rows.flush(&mut client)?;
client.drive(&mut network, |id, result| {
    if rows.on_result(id, &result) {
        // Handle errors here using the application's logging policy.
    }
});
```

Use one client for the table's lifetime. Call `flush` on each poll iteration.
The buffer keeps rows until success, with one batch in flight. The default limit
is 10,000 rows per batch; `with_max_rows_per_batch` accepts any nonzero limit.
Queue refusal and request failure delay retries for two seconds.
Encoding failure drops only the invalid row and returns the error to the caller.
Retries can duplicate committed rows after a lost response. Pending rows have no
size limit; the application owns ingestion limits and shutdown draining.

`buffered_batch!` groups different row types without changing each table's settings:

```rust,ignore
flux_clickhouse::buffered_batch! {
    pub struct TelemetryBatch {
        pub events: EventRow,
        pub health: HealthRow,
    }
}

let mut batch = TelemetryBatch {
    events: flux_clickhouse::BufferedTable::new(events_sql).with_max_rows_per_batch(1_000),
    health: flux_clickhouse::BufferedTable::new(health_sql).with_max_rows_per_batch(100),
};
batch.events.push(event);
batch.flush(&mut client, |field, error| eprintln!("{field}: {error}"));
client.drive(&mut network, |id, result| {
    if let Some(field) = batch.on_result(id, &result) {
        if let Err(error) = result {
            eprintln!("{field}: {error:?}");
        }
    }
});
```

`flush` attempts every table and reports encoding errors through the callback.
`on_result` returns the matching field name. `is_empty` includes rows still in flight.
Use the same client for all tables in a batch.

`http::insert_rows(&client, table, &rows).await` writes owned rows without copying
them. Empty batches make no request. HTTP timeouts, retry, and discard policies
remain with the caller.

## Focused checks

```sh
cargo test -p flux-clickhouse --all-features --lib migrations::cli::tests::create_in_caller_directory
```

The database checks use a disposable local ClickHouse server with user and password
`flux_test`. Each check creates and deletes its own database. The buffer check uses
the native TCP endpoint and repairs a missing table after an insert fails:

```sh
FLUX_CLICKHOUSE_TEST_URL=http://127.0.0.1:8123 cargo test -p flux-clickhouse --features migrations --test migrations migration_history_and_http_inserts -- --ignored
FLUX_CLICKHOUSE_TEST_URL=http://127.0.0.1:8123 FLUX_CLICKHOUSE_TEST_ADDR=127.0.0.1:9000 cargo test -p flux-clickhouse --features http --test clickhouse buffered_rows_survive_refusal_and_retry -- --ignored
```
