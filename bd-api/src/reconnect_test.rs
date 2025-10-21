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
