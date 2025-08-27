// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

use crate::global_state::{Reader, Tracker, UpdateResult};
use bd_device::Store;
use bd_runtime::runtime::Watch;
use bd_test_helpers::session::InMemoryStorage;
use std::sync::Arc;
use time::ext::{NumericalDuration, NumericalStdDuration};

#[test]
fn global_state_update() {
  let store = Arc::new(Store::new(Box::<InMemoryStorage>::default()));
  let reader = Reader::new(store.clone());
  let mut state_tracker = Tracker::new(store, Watch::new_for_testing(0.seconds()));

  assert!(state_tracker.current_global_state.0.is_empty());
  assert!(reader.global_state_fields().is_empty());

  let fields = [
    ("key2".into(), "value2".into()),
    ("key".into(), "value".into()),
  ]
  .into();
  assert_eq!(
    UpdateResult::Updated,
    state_tracker.maybe_update_global_state(&fields)
  );
  assert_eq!(
    UpdateResult::NoChange,
    state_tracker.maybe_update_global_state(&fields)
  );

  assert_eq!(reader.global_state_fields(), fields);
  assert_eq!(state_tracker.current_global_state.0, fields);

  let updated_fields = [("key".into(), "value".into())].into();

  assert_eq!(
    UpdateResult::Updated,
    state_tracker.maybe_update_global_state(&updated_fields)
  );
  assert_eq!(
    UpdateResult::NoChange,
    state_tracker.maybe_update_global_state(&updated_fields)
  );

  assert_eq!(reader.global_state_fields(), updated_fields);
  assert_eq!(state_tracker.current_global_state.0, updated_fields);

  let updated_fields = [("key".into(), b"value".to_vec().into())].into();

  assert_eq!(
    UpdateResult::Updated,
    state_tracker.maybe_update_global_state(&updated_fields)
  );
  assert_eq!(
    UpdateResult::NoChange,
    state_tracker.maybe_update_global_state(&updated_fields)
  );

  assert_eq!(reader.global_state_fields(), updated_fields);
}

#[tokio::test]
async fn write_is_scheduled_and_deferred() {
  tokio::time::pause();
  let store = Arc::new(Store::new(Box::<InMemoryStorage>::default()));
  let mut tracker = Tracker::new(store.clone(), Watch::new_for_testing(10.seconds()));
  let initial = [("x".into(), "y".into())].into();
  assert_eq!(
    UpdateResult::Updated,
    tracker.maybe_update_global_state(&initial)
  );
  let changed = [("a".into(), "b".into())].into();

  // Should defer since we are within the coalesce window.
  assert_eq!(
    UpdateResult::Deferred,
    tracker.maybe_update_global_state(&changed)
  );
  // Make sure we really didn't write the update.
  let reader = Reader::new(store);
  assert_eq!(reader.global_state_fields(), initial);

  // Advance less than coalesce window
  tokio::time::advance(5.std_seconds()).await;
  assert_eq!(
    UpdateResult::Deferred,
    tracker.maybe_update_global_state(&changed)
  );
  assert_eq!(reader.global_state_fields(), initial);

  // Advance past coalesce window
  tokio::time::advance(6.std_seconds()).await;
  assert_eq!(
    UpdateResult::Updated,
    tracker.maybe_update_global_state(&changed)
  );
  assert_eq!(reader.global_state_fields(), changed);
}

#[tokio::test]
async fn no_scheduled_write_on_same_data() {
  tokio::time::pause();
  let store = Arc::new(Store::new(Box::<InMemoryStorage>::default()));
  let mut tracker = Tracker::new(store.clone(), Watch::new_for_testing(6.seconds()));
  let original = [("m".into(), "n".into())].into();
  assert_eq!(
    UpdateResult::Updated,
    tracker.maybe_update_global_state(&original)
  );
  assert_eq!(
    UpdateResult::NoChange,
    tracker.maybe_update_global_state(&original)
  );
}

#[tokio::test]
async fn scheduled_write_no_change_after_window() {
  tokio::time::pause();
  let store = Arc::new(Store::new(Box::<InMemoryStorage>::default()));
  let mut tracker = Tracker::new(store.clone(), Watch::new_for_testing(6.seconds()));
  let original = [("m".into(), "n".into())].into();
  assert_eq!(
    UpdateResult::Updated,
    tracker.maybe_update_global_state(&original)
  );
  let new = [("x".into(), "y".into())].into();
  assert_eq!(
    UpdateResult::Deferred,
    tracker.maybe_update_global_state(&new)
  );

  tokio::time::advance(7.std_seconds()).await;

  assert_eq!(
    UpdateResult::NoChange,
    tracker.maybe_update_global_state(&original)
  );
}
