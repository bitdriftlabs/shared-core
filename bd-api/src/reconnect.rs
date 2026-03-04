// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

#[cfg(test)]
#[path = "./reconnect_test.rs"]
mod tests;

use bd_key_value::{Key, Store};
use bd_time::{OffsetDateTimeExt, TimeProvider, TimestampExt};
use protobuf::well_known_types::timestamp::Timestamp;
use std::sync::Arc;
use time::{Duration, OffsetDateTime};

const LAST_CONNECTED_AT_KEY: Key<Timestamp> = Key::new("api:reconnect:last_connected_at");
const NEXT_TRY_NOT_BEFORE_KEY: Key<Timestamp> = Key::new("api:reconnect:next_try_not_before");

pub struct ReconnectState {
  store: Arc<Store>,
  last_connected_at: Option<OffsetDateTime>,
  next_try_not_before: Option<OffsetDateTime>,
  time_provider: Arc<dyn TimeProvider>,
}

impl ReconnectState {
  pub fn new(store: Arc<Store>, time_provider: Arc<dyn TimeProvider>) -> Self {
    let last_connected_at = store
      .get(&LAST_CONNECTED_AT_KEY)
      .map(|v| v.to_offset_date_time());
    let next_try_not_before = store
      .get(&NEXT_TRY_NOT_BEFORE_KEY)
      .map(|v| v.to_offset_date_time());

    Self {
      store,
      last_connected_at,
      next_try_not_before,
      time_provider,
    }
  }

  pub fn next_reconnect_delay(
    &self,
    min_reconnect_interval: Duration,
  ) -> Option<std::time::Duration> {
    let now = self.time_provider.now();
    let next_reconnect_from_connectivity = self
      .last_connected_at
      .map(|last_connected_at| last_connected_at + min_reconnect_interval);

    let next_reconnect_time = match (next_reconnect_from_connectivity, self.next_try_not_before) {
      (Some(a), Some(b)) => Some(std::cmp::max(a, b)),
      (Some(a), None) => Some(a),
      (None, Some(b)) => Some(b),
      (None, None) => None,
    };

    let Some(next_reconnect_time) = next_reconnect_time else {
      log::trace!("no prior connectivity event or retry schedule, reconnect immediately");
      return None;
    };

    let delay = next_reconnect_time - now;

    // If delay is negative or zero, return None (no delay needed)
    if delay <= Duration::ZERO {
      log::trace!("next reconnect time already elapsed ({next_reconnect_time})");
      return None;
    }

    // Cap the delay from last connectivity at the minimum reconnect interval to prevent
    // excessively long waits due to clock skew. If explicit next_try_not_before is later, keep it.
    let capped_delay = if let Some(next_reconnect_from_connectivity) = next_reconnect_from_connectivity
      && next_reconnect_time == next_reconnect_from_connectivity
    {
      std::cmp::min(delay.unsigned_abs(), min_reconnect_interval.unsigned_abs())
    } else {
      delay.unsigned_abs()
    };

    log::trace!(
      "next reconnect at {next_reconnect_time}, waiting {capped_delay:?} before reconnecting"
    );

    Some(capped_delay)
  }

  pub fn record_next_try_after(&mut self, delay: Duration) {
    let bounded_delay = if delay.is_negative() {
      Duration::ZERO
    } else {
      delay
    };
    let next_try_not_before = self.time_provider.now() + bounded_delay;

    log::trace!(
      "recording next reconnect try-not-before at {next_try_not_before} (delay: {bounded_delay:?})"
    );

    self.next_try_not_before = Some(next_try_not_before);
    let () = self
      .store
      .set(&NEXT_TRY_NOT_BEFORE_KEY, &next_try_not_before.into_proto());
  }

  pub fn clear_next_try_not_before(&mut self) {
    self.next_try_not_before = None;

    let now = self.time_provider.now();
    let () = self.store.set(&NEXT_TRY_NOT_BEFORE_KEY, &now.into_proto());
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
