use std::collections::VecDeque;

use flux_timing::{Instant, Nanos};
use serde::Serialize;

use crate::{ClickHouse, Error, Output, QueryId, rowbinary};

const DEFAULT_MAX_ROWS: usize = 10_000;
const RETRY_INTERVAL: Nanos = Nanos::from_secs(2);

/// Retains native insert rows until success, with one batch in flight per
/// table.
///
/// Call `flush` from the poll loop and pass client outcomes to
/// `on_result`. Retries can duplicate rows if the server committed before a
/// connection failed. Pending rows are unbounded; callers must limit ingestion
/// during an outage.
pub struct BufferedTable<T> {
    sql: String,
    max_rows_per_batch: usize,
    pending: VecDeque<T>,
    in_flight: Option<(QueryId, usize)>,
    retry_after: Instant,
}

impl<T: Serialize> BufferedTable<T> {
    /// Supply an `INSERT INTO ... (columns) [SETTINGS ...] VALUES` statement.
    /// Quote nested column names, for example `` `results.cost_units` ``.
    /// The SQL is trusted application configuration, not unescaped user input.
    pub fn new(sql: impl Into<String>) -> Self {
        Self {
            sql: sql.into(),
            max_rows_per_batch: DEFAULT_MAX_ROWS,
            pending: VecDeque::new(),
            in_flight: None,
            retry_after: Instant::ZERO,
        }
    }

    /// Set the maximum rows per insert. Defaults to 10,000; panics if zero.
    pub fn with_max_rows_per_batch(mut self, max_rows_per_batch: usize) -> Self {
        assert!(max_rows_per_batch > 0, "max_rows_per_batch must be nonzero");
        self.max_rows_per_batch = max_rows_per_batch;
        self
    }

    pub fn push(&mut self, row: T) {
        self.pending.push_back(row);
    }

    /// Includes rows in the in-flight batch.
    pub fn len(&self) -> usize {
        self.pending.len()
    }

    pub fn is_empty(&self) -> bool {
        self.pending.is_empty()
    }

    /// Queue up to the configured row limit. A full queue or failed request
    /// backs off for two seconds. An encoding error drops only the invalid
    /// row and returns its error.
    pub fn flush(&mut self, client: &mut ClickHouse) -> Result<(), rowbinary::Error> {
        if self.in_flight.is_some() || self.pending.is_empty() || Instant::now() < self.retry_after
        {
            return Ok(());
        }
        let count = self.pending.len().min(self.max_rows_per_batch);
        let mut body = Vec::new();
        for (index, row) in self.pending.iter().take(count).enumerate() {
            if let Err(error) = rowbinary::encode(&mut body, row) {
                self.pending.remove(index);
                return Err(error);
            }
        }
        match client.insert(&self.sql, body) {
            Ok(id) => self.in_flight = Some((id, count)),
            Err(_) => self.retry_after = Instant::now() + RETRY_INTERVAL,
        }
        Ok(())
    }

    /// Return whether this outcome belongs to the table. Callers can log
    /// matching errors. Use the same client for `flush` and outcomes; query
    /// IDs are local to that client.
    pub fn on_result(&mut self, id: QueryId, result: &Result<Output, Error>) -> bool {
        let Some((request, count)) = self.in_flight else { return false };
        if request != id {
            return false;
        }
        self.in_flight = None;
        match result {
            Ok(_) => {
                self.pending.drain(..count);
            }
            Err(_) => self.retry_after = Instant::now() + RETRY_INTERVAL,
        }
        true
    }
}
