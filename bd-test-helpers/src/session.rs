// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

use bd_key_value::Storage;
use std::collections::HashMap;

//
// InMemoryStorage
//

#[derive(Default)]
pub struct InMemoryStorage {
  state: parking_lot::Mutex<HashMap<String, (String, i32)>>,
}

impl InMemoryStorage {
  pub fn writes_count(&self, key: &str) -> i32 {
    self.state.lock().get(key).map_or(0, |v| v.1)
  }
}

impl Storage for InMemoryStorage {
  fn set_string(&self, key: &str, value: &str) -> anyhow::Result<()> {
    let mut guard = self.state.lock();
    let mut state = guard.clone();

    if let Some((_, writes_count)) = state.get(key) {
      state.insert(key.to_string(), (value.to_string(), writes_count + 1));
    } else {
      state.insert(key.to_string(), (value.to_string(), 1));
    }

    *guard = state;

    Ok(())
  }

  fn get_string(&self, key: &str) -> anyhow::Result<Option<String>> {
    Ok(self.state.lock().get(key).map(|v| v.0.clone()))
  }

  fn delete(&self, key: &str) -> anyhow::Result<()> {
    self.state.lock().remove(key);
    Ok(())
  }
}
