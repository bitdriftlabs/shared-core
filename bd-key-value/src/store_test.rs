// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

use crate::{Key, Storage, Store};
use base64::Engine as _;
use pretty_assertions::assert_eq;
use protobuf::well_known_types::wrappers::BoolValue;
use std::collections::HashMap;
use std::sync::Arc;

static STRING_TEST_KEY: Key<String> = Key::new("test");
static PROTO_TEST_KEY: Key<BoolValue> = Key::new("test");

//
// MockStorage
//

#[derive(Default)]
struct MockStorage {
  state: Arc<parking_lot::Mutex<HashMap<String, String>>>,
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

  store.set_string(&STRING_TEST_KEY, "foo");

  assert_eq!("foo", store.get_string(&STRING_TEST_KEY).unwrap());
}

#[test]
fn returns_none_if_underlying_data_malformed() {
  let storage = Box::<MockStorage>::default();
  let store = Store::new(storage);

  let value = "foo".to_string();
  store.set_string(&STRING_TEST_KEY, &value);

  // The data is invalid, we should receive None and the underlying value in storage should be
  // cleared.
  assert!(store.get(&PROTO_TEST_KEY).is_none());
  // The underlying value in storage should be cleared.
  assert!(store.get_string(&STRING_TEST_KEY).is_none());
}

#[test]
fn bincode_string_compatibility() {
  let storage = Box::<MockStorage>::default();
  let storage_values = storage.state.clone();
  let store = Store::new(storage);

  let value = "test-string".to_string();
  store.set_string(&STRING_TEST_KEY, &value);
  let bincode_encoded = bincode::encode_to_vec(value, bincode::config::legacy()).unwrap();

  assert_eq!(
    base64::engine::general_purpose::STANDARD.encode(&bincode_encoded),
    *storage_values.lock().get(STRING_TEST_KEY.key()).unwrap()
  );
}

#[test]
fn bincode_string_compatibility_long_string() {
  let storage = Box::<MockStorage>::default();
  let storage_values = storage.state.clone();
  let store = Store::new(storage);

  // Test a large string so that we ensure length prefixes are using the correct endianness.
  let value = "test-string".repeat(1000);
  store.set_string(&STRING_TEST_KEY, &value);

  let bincode_encoded = bincode::encode_to_vec(value, bincode::config::legacy()).unwrap();
  assert_eq!(
    base64::engine::general_purpose::STANDARD.encode(&bincode_encoded),
    *storage_values.lock().get(STRING_TEST_KEY.key()).unwrap()
  );
}

#[test]
fn returns_stored_proto_value() {
  let storage = Box::<MockStorage>::default();
  let store = Store::new(storage);

  let mut value = BoolValue::new();
  value.value = true;

  store.set_internal(&PROTO_TEST_KEY, &value).unwrap();

  assert!(store.get(&PROTO_TEST_KEY).unwrap().value);
}
