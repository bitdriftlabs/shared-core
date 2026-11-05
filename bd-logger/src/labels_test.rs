// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE.polyform file or at:
// https://polyformproject.org/licenses/strict/1.0.0

use super::{InitialLabels, Key, LABELS_KEY, LabelsState, LabelsUpdate};
use bd_metadata::labels::Labels;
use bd_test_helpers::session::in_memory_store;

#[test]
fn initialization_restores_replaces_and_explicitly_clears_persisted_labels() {
  let store = in_memory_store();
  let read = |initial| {
    LabelsState::new(initial, store.clone())
      .unwrap()
      .updates
      .borrow()
      .clone()
  };
  assert!(read(InitialLabels::UseExisting).is_empty());
  let labels = Labels::from([("channel".to_string(), "qa".to_string())]);
  assert_eq!(read(InitialLabels::Set(labels.clone())), labels);
  assert_eq!(read(InitialLabels::UseExisting), labels);
  assert!(read(InitialLabels::None).is_empty());
  assert!(read(InitialLabels::UseExisting).is_empty());
}

#[test]
fn runtime_update_is_restored_from_persistence() {
  let store = in_memory_store();
  let state = LabelsState::new(InitialLabels::None, store.clone()).unwrap();
  let next = Labels::from([
    ("channel".to_string(), "caf\u{e9}".to_string()),
    ("empty".to_string(), String::new()),
  ]);
  state.update(|labels| *labels = next.clone()).unwrap();
  assert_eq!(
    store
      .get_internal(&LABELS_KEY)
      .unwrap()
      .unwrap()
      .labels
      .into_iter()
      .collect::<Labels>(),
    next
  );
  assert_eq!(
    *LabelsState::new(InitialLabels::UseExisting, store)
      .unwrap()
      .updates
      .borrow(),
    next
  );
}

#[test]
fn corrupt_persistence_is_explicit_and_clear_can_recover_it() {
  let store = in_memory_store();
  store
    .set_internal_string(&Key::new(LABELS_KEY.key()), "broken")
    .unwrap();
  assert!(LabelsState::new(InitialLabels::UseExisting, store.clone()).is_err());
  assert!(
    LabelsState::new(InitialLabels::None, store)
      .unwrap()
      .updates
      .borrow()
      .is_empty()
  );
}

#[test]
fn invalid_persisted_label_map_is_rejected() {
  let store = in_memory_store();
  store
    .set_internal(
      &LABELS_KEY,
      &LabelsUpdate {
        labels: [("__bd_env".to_string(), "production".to_string())].into(),
        ..Default::default()
      },
    )
    .unwrap();
  assert!(LabelsState::new(InitialLabels::UseExisting, store).is_err());
}
