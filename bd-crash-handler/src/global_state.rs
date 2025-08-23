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
use bd_runtime::runtime::DurationWatch;
use std::sync::Arc;
use std::time::Instant;

const KEY: Key<State> = Key::new("global_state");

#[derive(Debug, serde::Serialize, serde::Deserialize, Default, PartialEq, Eq)]
struct State(LogFields);

impl Storable for State {}

//
// Tracker
//

pub struct Tracker {
  store: Arc<Store>,
  current_global_state: State,
  last_write: Option<Instant>,
  next_write: Option<Instant>,
  coalesce_window: DurationWatch<bd_runtime::runtime::global_state::CoalesceWindow>,
}

impl Tracker {
  #[must_use]
  pub fn new(
    store: Arc<Store>,
    coalesce_window: DurationWatch<bd_runtime::runtime::global_state::CoalesceWindow>,
  ) -> Self {
    let global_state = store.get(&KEY).unwrap_or_default();

    Self {
      store,
      current_global_state: global_state,
      last_write: None,
      next_write: None,
      coalesce_window,
    }
  }

  pub fn maybe_update_global_state(&mut self, new_global_state: &LogFields) -> bool {
    // In order to avoid writing too frequently, we coalesce writes that happen within a short time
    // window. The first write happens immediately, and subsequent writes within the coalesce window
    // are delayed until the window has passed. Given the ootb frequency of logs we should expect
    // to see the persisted value be fairly up to date.

    // TODO(snowp): If we ever need to support a situation in which logs are being written more
    // slowly we'd likely need to revisit this logic to avoid global state getting out of date.

    // If we have a pending write scheduled, check if it's time to do it.
    if let Some(next_write) = self.next_write {
      if next_write <= Instant::now() {
        // Check again at this point to see if the state has changed since we last checked. If it's
        // the same we don't have to do anything.
        if self.current_global_state.0 != *new_global_state {
          self.current_global_state = State(new_global_state.clone());
          self.write_global_state();
        }

        self.next_write = None;
      } else {
        // We have a pending write scheduled, but it's not time yet. Just return.
        return false;
      }
    }

    // No write is scheduled and there has been no change, no need to do anything.
    if self.current_global_state.0 == *new_global_state {
      return false;
    }

    match (self.last_write, self.next_write) {
      // We have never written before, write immediately.
      (None, _) => {
        self.current_global_state = State(new_global_state.clone());
        self.write_global_state();
        true
      },
      // We have written before, but there is no pending write. Schedule one for 30 seconds from
      // now.
      (Some(last_write), None) => {
        let coalesce_window = *self.coalesce_window.read();
        if last_write.elapsed() < coalesce_window {
          self.next_write = Some(self.last_write.unwrap() + coalesce_window);
        }
        false
      },
      // We have a pending write scheduled, but it's not time yet. Just return.
      (Some(_), Some(_)) => false,
    }
  }

  fn write_global_state(&mut self) {
    self.store.set(&KEY, &self.current_global_state);
    self.last_write = Some(Instant::now());
    self.next_write = self
      .last_write
      .map(|t| t + std::time::Duration::from_secs(30));
  }
}

//
// Reader
//

pub struct Reader {
  store: Arc<Store>,
}

impl Reader {
  #[must_use]
  pub fn new(store: Arc<Store>) -> Self {
    Self { store }
  }

  #[must_use]
  pub fn global_state_fields(&self) -> LogFields {
    self.store.get(&KEY).unwrap_or_default().0
  }
}
