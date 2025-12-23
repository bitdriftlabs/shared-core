// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

use crate::{DEVICE_ID_KEY, Device};
use bd_key_value::{Storage, Store};
use bd_test_helpers::session::InMemoryStorage;
use pretty_assertions::assert_eq;
use std::sync::Arc;

//
// MockStorage
//

/// Allows for a shared access to Storage.
struct MockStorage {
  storage: Arc<InMemoryStorage>,
}

impl Storage for MockStorage {
  fn set_string(&self, key: &str, value: &str) -> anyhow::Result<()> {
    self.storage.set_string(key, value)
  }

  fn get_string(&self, key: &str) -> anyhow::Result<Option<String>> {
    self.storage.get_string(key)
  }

  fn delete(&self, key: &str) -> anyhow::Result<()> {
    self.storage.delete(key)
  }
}

#[test]
fn device_id_is_stored_once_and_does_not_change() {
  let storage = Arc::new(InMemoryStorage::default());
  let store = Arc::new(Store::new(Box::new(MockStorage {
    storage: storage.clone(),
  })));
  let device = Device::new(store.clone());

  let device_id_1 = device.id();
  assert_eq!(store.get_string(&DEVICE_ID_KEY).unwrap(), device_id_1);

  let device_id_2 = device.id();
  assert_eq!(device_id_1, device_id_2);
  assert_eq!(1, storage.writes_count(DEVICE_ID_KEY.key()));
}
