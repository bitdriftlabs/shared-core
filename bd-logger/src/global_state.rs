// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

#[cfg(test)]
#[path = "./global_state_test.rs"]
mod tests;

use bd_device::Store;
use bd_key_value::{Key, Storable};
use bd_log_primitives::LogFields;
use std::sync::Arc;

const KEY: Key<State> = Key::new("global_state");

#[derive(Debug, serde::Serialize, serde::Deserialize, Default, PartialEq, Eq)]
struct State(LogFields);

impl Storable for State {}

//
// GlobalStateTracker
//

pub struct Tracker {
  store: Arc<Store>,
  current_global_state: State,
}

impl Tracker {
  pub fn new(store: Arc<Store>) -> Self {
    let global_state = store.get(&KEY).unwrap_or_default();

    Self {
      store,
      current_global_state: global_state,
    }
  }

  pub fn maybe_update_global_state(&mut self, new_global_state: &LogFields) -> bool {
    if self.current_global_state.0 == *new_global_state {
      return false;
    }

    self.current_global_state = State(new_global_state.clone());
    self.store.set(&KEY, &self.current_global_state);

    true
  }

  pub fn global_state_fields(&self) -> LogFields {
    self.current_global_state.0.clone()
  }
}
