// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

use crate::{Key, Storable, Storage, Store};
use pretty_assertions::assert_eq;
use std::collections::HashMap;

static TEST_KEY_1: Key<MockState> = Key::new("test");
static TEST_KEY_2: Key<String> = Key::new("test");

#[derive(serde::Serialize, serde::Deserialize, Debug, Default, Eq, PartialEq)]
struct MockState {}

impl Storable for MockState {}

//
// MockStorage
//

#[derive(Default)]
struct MockStorage {
  state: parking_lot::Mutex<HashMap<String, String>>,
}

impl Storage for MockStorage {
  fn set_string(&self, key: &str, value: &str) -> anyhow::Result<()> {
    let mut guard = self.state.lock();
    let mut state = guard.clone();
    state.insert(key.to_string(), value.to_string());
    *guard = state;

    Ok(())
  }

  fn get_string(&self, key: &str) -> anyhow::Result<Option<String>> {
    Ok(self.state.lock().get(key).cloned())
  }

  fn delete(&self, key: &str) -> anyhow::Result<()> {
    self.state.lock().remove(key);
    Ok(())
  }
}

#[test]
fn returns_stored_value() {
  let storage = Box::<MockStorage>::default();
  let store = Store::new(storage);

  let value = MockState::default();
  store.set(&TEST_KEY_1, &value);

  assert_eq!(value, store.get(&TEST_KEY_1).unwrap());
}

#[test]
fn returns_none_if_underlying_data_malformed() {
  let storage = Box::<MockStorage>::default();
  let store = Store::new(storage);

  let value = MockState::default();
  store.set(&TEST_KEY_1, &value);

  // The data is invalid, we should receive None and the underlying value in storage should be
  // cleared.
  assert!(store.get(&TEST_KEY_2).is_none());
  // The underlying value in storage should be cleared.
  assert!(store.get(&TEST_KEY_1).is_none());
}
