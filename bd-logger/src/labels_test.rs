// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE.polyform file or at:
// https://polyformproject.org/licenses/strict/1.0.0

use super::{InitialLabels, LABELS_KEY, LabelsState};
use bd_client_stats_store::Collector;
use bd_metadata::labels::Labels;
use bd_test_helpers::session::in_memory_store;
use std::collections::HashMap;

#[test]
fn initialization_restores_replaces_and_explicitly_clears_persisted_labels() {
  let store = in_memory_store();
  let scope = Collector::default().scope("");
  let make =
    |initial| LabelsState::new(initial, store.clone(), scope.clone(), HashMap::new()).unwrap();
  assert!(make(InitialLabels::UseExisting).initial.is_empty());
  let labels = Labels::from([("channel".to_string(), "qa".to_string())]);
  assert_eq!(make(InitialLabels::Set(labels.clone())).initial, labels);
  assert_eq!(make(InitialLabels::UseExisting).initial, labels);
  assert!(make(InitialLabels::None).initial.is_empty());
  assert!(make(InitialLabels::UseExisting).initial.is_empty());
}

#[test]
fn ordered_application_does_not_overwrite_newer_durable_or_wire_intent() {
  let store = in_memory_store();
  let scope = Collector::default().scope("");
  let state = LabelsState::new(
    InitialLabels::None,
    store.clone(),
    scope.clone(),
    HashMap::new(),
  )
  .unwrap();
  let updates = state.updates.subscribe();
  let next = Labels::from([("channel".to_string(), "qa".to_string())]);
  state.persist(&next).unwrap();
  state.apply(&state.initial).unwrap();
  assert!(!updates.has_changed().unwrap());
  assert_eq!(
    LabelsState::new(InitialLabels::UseExisting, store, scope, HashMap::new())
      .unwrap()
      .initial,
    next
  );
  state.apply(&next).unwrap();
  assert!(!updates.has_changed().unwrap());
  assert_eq!(state.origin.borrow()["__bd_labels"], r#"{"channel":"qa"}"#);
}

#[test]
fn corrupt_persistence_is_explicit_and_clear_can_recover_it() {
  let store = in_memory_store();
  store.set_internal_string(&LABELS_KEY, "broken").unwrap();
  let scope = Collector::default().scope("");
  assert!(
    LabelsState::new(
      InitialLabels::UseExisting,
      store.clone(),
      scope.clone(),
      HashMap::new()
    )
    .is_err()
  );
  assert!(
    LabelsState::new(InitialLabels::None, store, scope, HashMap::new())
      .unwrap()
      .initial
      .is_empty()
  );
}
