// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

use crate::global_state::Tracker;
use bd_device::Store;
use bd_test_helpers::session::InMemoryStorage;
use std::sync::Arc;

#[test]
fn global_state_update() {
  let mut state_tracker = Tracker::new(Arc::new(Store::new(Box::<InMemoryStorage>::default())));

  assert!(state_tracker.global_state_fields().is_empty());

  let fields = [
    ("key2".into(), "value2".into()),
    ("key".into(), "value".into()),
  ]
  .into();
  assert!(state_tracker.maybe_update_global_state(&fields));
  assert!(!state_tracker.maybe_update_global_state(&fields));

  assert_eq!(state_tracker.global_state_fields(), fields);
  assert_eq!(state_tracker.current_global_state.0, fields);

  let updated_fields = [("key".into(), "value".into())].into();

  assert!(state_tracker.maybe_update_global_state(&updated_fields));
  assert!(!state_tracker.maybe_update_global_state(&updated_fields));

  assert_eq!(state_tracker.global_state_fields(), updated_fields);
  assert_eq!(state_tracker.current_global_state.0, updated_fields);

  let updated_fields = [("key".into(), b"value".to_vec().into())].into();

  assert!(state_tracker.maybe_update_global_state(&updated_fields));
  assert!(!state_tracker.maybe_update_global_state(&updated_fields));

  assert_eq!(state_tracker.global_state_fields(), updated_fields);
}
