#![cfg(feature = "migrations")]

use flux_clickhouse::{http::insert_rows, migrations::MigrationSet};
use include_dir::{Dir, DirEntry, File};

const ENTRIES: &[DirEntry<'static>] = &[
    DirEntry::Dir(Dir::new("0001_initial", &[
        DirEntry::File(File::new(
            "0001_initial/up.sql",
            b"CREATE TABLE rows (id UInt64) ENGINE = MergeTree ORDER BY id;\n",
        )),
        DirEntry::File(File::new("0001_initial/down.sql", b"DROP TABLE rows;\n")),
    ])),
    DirEntry::Dir(Dir::new("0002_label", &[
        DirEntry::File(File::new(
            "0002_label/up.sql",
            b"ALTER TABLE rows ADD COLUMN label String DEFAULT '';\n",
        )),
        DirEntry::File(File::new("0002_label/down.sql", b"ALTER TABLE rows DROP COLUMN label;\n")),
    ])),
];

#[tokio::test]
#[ignore = "requires FLUX_CLICKHOUSE_TEST_URL pointing to a disposable local server"]
async fn migration_history_and_http_inserts() -> eyre::Result<()> {
    #[derive(clickhouse::Row, serde::Serialize)]
    struct Row {
        id: u64,
    }
    const CHANGED: MigrationSet<'static> = MigrationSet::new(
        Dir::new("", &[DirEntry::Dir(Dir::new("0001_initial", &[
            DirEntry::File(File::new("0001_initial/up.sql", b"SELECT 1;\n")),
            DirEntry::File(File::new("0001_initial/down.sql", b"SELECT 1;\n")),
        ]))]),
        0,
    );
    let client = clickhouse::Client::default()
        .with_url(std::env::var("FLUX_CLICKHOUSE_TEST_URL")?)
        .with_user("flux_test")
        .with_password("flux_test");
    let database = format!("flux_migrations_test_{}", std::process::id());
    client.query(&format!("CREATE DATABASE {database}")).execute().await?;
    let db = client.clone().with_database(&database);
    let result: eyre::Result<()> = async {
        let migrations = MigrationSet::new(Dir::new("", ENTRIES), 0);
        migrations.sync(&db).await?;
        migrations.sync(&db).await?;
        let older = MigrationSet::new(Dir::new("", &ENTRIES[..1]), 0);
        older.sync(&db).await?;
        assert_eq!(db.query("SELECT count() FROM _migrations FINAL").fetch_one::<u64>().await?, 2);

        insert_rows::<Row>(&client.clone().with_url("http://127.0.0.1:1"), "rows", &[]).await?;
        insert_rows(&db, "rows", &[Row { id: 7 }]).await?;
        assert_eq!(db.query("SELECT id, label FROM rows").fetch_one::<(u64, String)>().await?, (7, String::new()));
        assert!(insert_rows(&db, "missing_table", &[Row { id: 8 }]).await.is_err());

        assert!(CHANGED.sync(&db).await.unwrap_err().to_string().contains("checksum mismatch"));
        // The older binary must roll back SQL that it does not embed.
        older.rollback(&db, 1).await?;
        assert_eq!(db.query("SELECT count() FROM system.columns WHERE database = currentDatabase() AND table = 'rows'").fetch_one::<u64>().await?, 1);
        assert_eq!(db.query("SELECT count() FROM _migrations FINAL").fetch_one::<u64>().await?, 1);
        older.rollback(&db, 0).await?;
        assert_eq!(db.query("EXISTS TABLE rows").fetch_one::<u8>().await?, 0);
        Ok(())
    }.await;
    client.query(&format!("DROP DATABASE {database}")).execute().await?;
    result
}
