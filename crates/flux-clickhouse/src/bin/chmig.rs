fn main() -> eyre::Result<()> {
    flux_clickhouse::migrations::run_directory_cli()
}
