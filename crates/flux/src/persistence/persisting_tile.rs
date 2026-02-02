use flux_timing::{InternalMessage, Nanos};

use crate::{
    persistence::Persistable,
    spine::{FluxSpine, SpineAdapter, SpineConsumer},
    tile::{Tile, TileName},
};

pub const PERSIST_INTERVAL: Nanos = Nanos::from_mins(1);
pub const TIMESTAMP_FORMAT_UTC: &str = "%Y-%m-%d_%H:%M_utc";

pub struct PersistingQueueTile<T: Persistable> {
    pending_data: Vec<InternalMessage<T>>,
    last_persist_time: Nanos,
}

impl<T: Persistable> PersistingQueueTile<T> {
    pub fn new() -> Self {
        Self { pending_data: Vec::new(), last_persist_time: Nanos::now() }
    }

    fn handle_persisting(&mut self, now: Nanos, app_name: &'static str) {
        if !self.pending_data.is_empty() {
            let current_minute =
                self.pending_data[0].publish_t().round_to_interval(PERSIST_INTERVAL);
            InternalMessage::<T>::persist(
                app_name,
                &self.pending_data,
                Some(9),
                Some(current_minute.with_fmt_utc(TIMESTAMP_FORMAT_UTC)),
            );
        }

        self.last_persist_time = now;
        self.pending_data.clear();
    }

    fn push(&mut self, d: InternalMessage<T>, app_name: &'static str) {
        if self.last_persist_time.round_to_interval(PERSIST_INTERVAL) <
            d.publish_t().round_to_interval(PERSIST_INTERVAL)
        {
            self.handle_persisting(d.publish_t(), app_name);
        }

        self.pending_data.push(d);

        let new_time = self.pending_data.last().unwrap().publish_t();
        let mut i = self.pending_data.len() - 1;

        // Sort elements by publish_t to store them in the correct order
        // Amount of swaps in practice will be small as reordering happens only when
        // multiple producers push data at the same time
        while i > 0 && self.pending_data[i - 1].publish_t() > new_time {
            self.pending_data.swap(i - 1, i);
            i -= 1;
        }
    }
}

impl<S, T> Tile<S> for PersistingQueueTile<T>
where
    S: FluxSpine,
    S::Consumers: AsMut<SpineConsumer<T>>,
    T: 'static + Copy + Default + Send + Sync + Persistable,
{
    fn name(&self) -> TileName {
        TileName::from_str_truncate("Persistence")
    }

    fn loop_body(&mut self, adapter: &mut SpineAdapter<S>) {
        adapter.consume_internal_message(|event, _| {
            self.push(*event, S::app_name());
        });
    }

    fn teardown(mut self, _adapter: &mut SpineAdapter<S>) {
        self.handle_persisting(Nanos::now(), S::app_name());
    }
}

impl<T: Persistable> Default for PersistingQueueTile<T> {
    fn default() -> Self {
        Self::new()
    }
}
