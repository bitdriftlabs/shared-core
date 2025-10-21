#[cfg(test)]
#[path = "./reconnect_test.rs"]
mod tests;

use bd_key_value::{Key, Storable};
use bd_runtime::runtime::DurationWatch;
use bd_time::TimeProvider;
use std::sync::Arc;
use time::OffsetDateTime;

const LAST_CONNECTED_AT_KEY: Key<LastConnectedAt> = Key::new("api:reconnect:last_connected_at");

#[derive(serde::Serialize, serde::Deserialize)]
struct LastConnectedAt {
  last_connected_at: OffsetDateTime,
}

impl Storable for LastConnectedAt {}

pub struct ReconnectState {
  store: Arc<bd_key_value::Store>,
  last_connected_at: Option<OffsetDateTime>,
  min_reconnect_interval: DurationWatch<bd_runtime::runtime::api::MinReconnectInterval>,
  time_provider: Arc<dyn TimeProvider>,
}

impl ReconnectState {
  pub fn new(
    store: Arc<bd_key_value::Store>,
    min_reconnect_interval: DurationWatch<bd_runtime::runtime::api::MinReconnectInterval>,
    time_provider: Arc<dyn TimeProvider>,
  ) -> Self {
    let last_connected_at = store
      .get(&LAST_CONNECTED_AT_KEY)
      .map(|v| v.last_connected_at);

    Self {
      store,
      last_connected_at,
      min_reconnect_interval,
      time_provider,
    }
  }

  pub fn next_reconnect_delay(&self) -> Option<std::time::Duration> {
    let last_connected_at = self.last_connected_at?;

    let next_reconnect_time = last_connected_at + *self.min_reconnect_interval.read();
    let delay = next_reconnect_time - self.time_provider.now();
    log::trace!("last reconnect was at {last_connected_at}, waiting {delay:?} before reconnecting",);

    (delay > std::time::Duration::ZERO).then_some(delay.unsigned_abs())
  }

  /// Updates the reconnection state with a connectivity event. The last seen connectivity event is
  /// used as the baseline for calculating the next reconnect time.
  ///
  /// In practice, this should be called whenever we've completed a handshake with the server or
  /// we're sending data over a connected stream.
  pub fn record_connectivity_event(&mut self) {
    let last_connected_at = self.time_provider.now();
    self.last_connected_at = Some(last_connected_at);
    let () = self.store.set(
      &LAST_CONNECTED_AT_KEY,
      &LastConnectedAt { last_connected_at },
    );
  }
}
