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
use tokio::time::Instant;

const KEY: Key<State> = Key::new("global_state");

#[derive(Debug, serde::Serialize, serde::Deserialize, Default, PartialEq, Eq)]
struct State(LogFields);

impl Storable for State {}

/// Results of attempting to update the global state. This is exposed for testing purposes..
#[derive(Debug, PartialEq, Eq)]
pub enum UpdateResult {
  NoChange,
  Updated,
  Deferred,
}

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

  pub fn maybe_update_global_state(&mut self, new_global_state: &LogFields) -> UpdateResult {
    // In order to avoid writing too frequently, we coalesce writes that happen within a short time
    // window. The first write happens immediately, and subsequent writes within the coalesce window
    // are delayed until the window has passed. Given the ootb frequency of logs we should expect
    // to see the persisted value be fairly up to date.

    // TODO(snowp): If we ever need to support a situation in which logs are being written more
    // slowly we'd likely need to revisit this logic to avoid global state getting out of date.

    // TODO(snowp): All of this is likely going to be replaced by the crash safe k-v map, but
    // this will require a bit more work to integrate with so that the reports can be constructed
    // with the map directly.

    // If we have a pending write scheduled, check if it's time to do it.
    let now = Instant::now();
    if let Some(next_write) = self.next_write {
      if next_write <= now {
        self.next_write = None;
        // Check again at this point to see if the state has changed since we last checked. If it's
        // the same we don't have to do anything.
        if self.current_global_state.0 == *new_global_state {
          log::trace!(
            "No change to global state at coalesced write time, not writing but clearing timer"
          );
          return UpdateResult::NoChange;
        }
        log::trace!("Writing coalesced global state");
        self.current_global_state = State(new_global_state.clone());
        self.write_global_state();
        return UpdateResult::Updated;
      }
      // We have a pending write scheduled, but it's not time yet. Just return.
      log::trace!("Deferring global state write for {:?}", next_write - now);
      return UpdateResult::Deferred;
    }

    // No write is scheduled and there has been no change, no need to do anything.
    if self.current_global_state.0 == *new_global_state {
      log::trace!("No change to global state, not writing");
      return UpdateResult::NoChange;
    }

    match (self.last_write, self.next_write) {
      // We have never written before, write immediately.
      (None, _) => {
        log::trace!("Writing initial global state");
        self.current_global_state = State(new_global_state.clone());
        self.write_global_state();
        UpdateResult::Updated
      },
      // We have written before, but there is no pending write. Schedule one based on the runtime
      // flag value.
      (Some(last_write), None) => {
        let coalesce_window = *self.coalesce_window.read();
        log::trace!("Scheduling global state write with coalesce window of {coalesce_window:?}");
        if last_write.elapsed() < coalesce_window {
          self.next_write = Some(Instant::from_std(last_write.into_std() + coalesce_window));
          log::trace!("Scheduling global state write at {:?}", self.next_write);
          UpdateResult::Deferred
        } else {
          log::trace!("Writing global state immediately");
          self.current_global_state = State(new_global_state.clone());
          self.write_global_state();
          UpdateResult::Updated
        }
      },
      // We have a pending write scheduled, but it's not time yet. Just return.
      (Some(_), Some(_)) => UpdateResult::Deferred,
    }
  }

  fn write_global_state(&mut self) {
    self.store.set(&KEY, &self.current_global_state);
    self.last_write = Some(Instant::now());
  }
}

//
// Reader
//

#[derive(Clone)]
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
