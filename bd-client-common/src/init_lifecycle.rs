// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

use parking_lot::RwLock;
use std::sync::Arc;

#[repr(u8)]
#[derive(Debug, PartialEq, Eq, Clone, Copy, PartialOrd, Ord)]
pub enum InitLifecycle {
  NotStarted,
  RuntimeLoaded,
  LogProcessingStarted,
}

#[derive(Clone)]
pub struct InitLifecycleState {
  #[cfg(debug_assertions)]
  state: Arc<RwLock<InitLifecycle>>,
}

impl Default for InitLifecycleState {
  fn default() -> Self {
    Self::new()
  }
}

impl InitLifecycleState {
  #[must_use]
  pub fn new() -> Self {
    Self {
      #[cfg(debug_assertions)]
      state: Arc::new(RwLock::new(InitLifecycle::NotStarted)),
    }
  }

  #[cfg(debug_assertions)]
  #[must_use]
  pub fn get(&self) -> InitLifecycle {
    *self.state.read()
  }

  pub fn set(&self, new_state: InitLifecycle) {
    #[cfg(debug_assertions)]
    {
      let mut state = self.state.write();
      debug_assert!(
        new_state >= *state,
        "Cannot move lifecycle state backwards from {:?} to {:?}",
        *state,
        new_state
      );

      *state = new_state;
    }
  }
}
