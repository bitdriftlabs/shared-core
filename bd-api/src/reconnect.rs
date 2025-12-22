// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

#[cfg(test)]
#[path = "./reconnect_test.rs"]
mod tests;

use bd_key_value::{Key, Storage};
use bd_time::{OffsetDateTimeExt, TimeProvider, TimestampExt};
use protobuf::well_known_types::timestamp::Timestamp;
use std::sync::Arc;
use time::{Duration, OffsetDateTime};

const LAST_CONNECTED_AT_KEY: Key<Timestamp> = Key::new("api:reconnect:last_connected_at");

pub struct ReconnectState<T> {
  store: Arc<bd_key_value::Store<T>>,
  last_connected_at: Option<OffsetDateTime>,
  time_provider: Arc<dyn TimeProvider>,
}

impl<S: Storage> ReconnectState<S> {
  pub fn new(store: Arc<bd_key_value::Store<S>>, time_provider: Arc<dyn TimeProvider>) -> Self {
    let last_connected_at = store
      .get(&LAST_CONNECTED_AT_KEY)
      .map(|v| v.to_offset_date_time());

    Self {
      store,
      last_connected_at,
      time_provider,
    }
  }

  pub fn next_reconnect_delay(
    &self,
    min_reconnect_interval: Duration,
  ) -> Option<std::time::Duration> {
    let Some(last_connected_at) = self.last_connected_at else {
      log::trace!("no prior connectivity event, reconnect immediately");
      return None;
    };

    let next_reconnect_time = last_connected_at + min_reconnect_interval;
    let delay = next_reconnect_time - self.time_provider.now();

    // If delay is negative or zero, return None (no delay needed)
    if delay <= std::time::Duration::ZERO {
      log::trace!("last reconnect was at {last_connected_at}, no delay needed");
      return None;
    }

    // Cap the delay at the minimum reconnect interval to prevent excessively long waits
    let capped_delay = std::cmp::min(delay.unsigned_abs(), min_reconnect_interval.unsigned_abs());
    log::trace!(
      "last reconnect was at {last_connected_at}, waiting {capped_delay:?} before reconnecting"
    );

    Some(capped_delay)
  }

  /// Updates the reconnection state with a connectivity event. The last seen connectivity event is
  /// used as the baseline for calculating the next reconnect time.
  ///
  /// In practice, this should be called whenever we've completed a handshake with the server or
  /// we're sending data over a connected stream.
  pub fn record_connectivity_event(&mut self) {
    log::trace!("recording connectivity event for reconnect state");

    let last_connected_at = self.time_provider.now();
    self.last_connected_at = Some(last_connected_at);
    let () = self
      .store
      .set(&LAST_CONNECTED_AT_KEY, &last_connected_at.into_proto());
  }
}
