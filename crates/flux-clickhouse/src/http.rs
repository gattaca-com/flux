//! Small helpers for the async HTTP client. Callers own timeout and retry
//! policies.

use clickhouse::{Client, RowOwned, RowWrite, error::Result};

/// Insert an owned-row batch. Empty batches make no request; failures leave
/// rows with the caller.
pub async fn insert_rows<T: RowOwned + RowWrite + Sync>(
    client: &Client,
    table: &str,
    rows: &[T],
) -> Result<()> {
    if rows.is_empty() {
        return Ok(());
    }
    let mut insert = client.insert::<T>(table).await?;
    for row in rows {
        insert.write(row).await?;
    }
    insert.end().await
}
