// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

use crate::reconnect::ReconnectState;
use bd_runtime::runtime::DurationWatch;
use bd_test_helpers::session::in_memory_store;
use bd_time::TestTimeProvider;
use std::sync::Arc;
use time::ext::NumericalDuration;
use time::macros::datetime;

#[test]
fn store_persisted_timeout() {
  let store = in_memory_store();
  let time = Arc::new(TestTimeProvider::new(datetime!(2024-01-01 00:00:00 UTC)));

  let mut state = ReconnectState::new(
    store.clone(),
    DurationWatch::new_for_testing(time::Duration::seconds(30)),
    time.clone(),
  );

  assert_eq!(state.next_reconnect_delay(), None);

  state.record_connectivity_event();

  assert_eq!(
    state.next_reconnect_delay(),
    Some(std::time::Duration::from_secs(30))
  );

  let new_state = ReconnectState::new(
    store,
    DurationWatch::new_for_testing(time::Duration::seconds(30)),
    time.clone(),
  );

  assert_eq!(
    new_state.next_reconnect_delay(),
    Some(std::time::Duration::from_secs(30))
  );

  time.advance(5.seconds());

  assert_eq!(
    new_state.next_reconnect_delay(),
    Some(std::time::Duration::from_secs(25))
  );
  assert_eq!(
    state.next_reconnect_delay(),
    Some(std::time::Duration::from_secs(25))
  );
}

#[test]
fn negative_delay_returns_none() {
  let store = in_memory_store();
  let time = Arc::new(TestTimeProvider::new(datetime!(2024-01-01 00:00:00 UTC)));

  let mut state = ReconnectState::new(
    store,
    DurationWatch::new_for_testing(time::Duration::seconds(30)),
    time.clone(),
  );

  state.record_connectivity_event();

  // Advance time beyond the reconnect interval
  time.advance(35.seconds());

  // Since we've passed the interval, no delay should be needed
  assert_eq!(state.next_reconnect_delay(), None);
}

#[test]
fn excessive_delay_gets_capped() {
  let store = in_memory_store();
  // Set the current time
  let time = Arc::new(TestTimeProvider::new(datetime!(2024-01-01 00:00:00 UTC)));

  // Create a custom state with a manipulated last_connected_at that would cause excessive delay
  let mut state = ReconnectState::new(
    store,
    DurationWatch::new_for_testing(time::Duration::seconds(30)),
    time.clone(),
  );

  // Record a connectivity event
  state.record_connectivity_event();

  // Simulate a time jump backward (which could happen due to system clock adjustment)
  // This creates a situation where the next_reconnect_time would be far in the future
  time.set_time(datetime!(2023-12-31 00:00:00 UTC)); // Set time to 1 day earlier

  // Even though the calculation would give a 24-hour delay, it should be capped at 30 seconds
  assert_eq!(
    state.next_reconnect_delay(),
    Some(std::time::Duration::from_secs(30)) // Capped at min_reconnect_interval
  );
}
