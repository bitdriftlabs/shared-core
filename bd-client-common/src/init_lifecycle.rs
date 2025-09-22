// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

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
  state: std::sync::Arc<parking_lot::RwLock<InitLifecycle>>,
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
      state: std::sync::Arc::new(parking_lot::RwLock::new(InitLifecycle::NotStarted)),
    }
  }

  #[cfg(debug_assertions)]
  #[must_use]
  pub fn get(&self) -> InitLifecycle {
    *self.state.read()
  }

  #[cfg(not(debug_assertions))]
  pub fn get(&self) -> InitLifecycle {
    InitLifecycle::NotStarted
  }

  #[cfg(debug_assertions)]
  pub fn set(&self, new_state: InitLifecycle) {
    let mut state = self.state.write();
    debug_assert!(
      new_state >= *state,
      "Cannot move lifecycle state backwards from {:?} to {:?}",
      *state,
      new_state
    );

    *state = new_state;
  }

  #[cfg(not(debug_assertions))]
  pub fn set(&self, _new_state: InitLifecycle) {}
}

/// Verifies that the current lifecycle state is less than the expected state.
/// This macro only has an effect in debug builds; in release builds it does nothing.
#[macro_export]
macro_rules! debug_check_lifecycle_less_than {
  ($lifecycle:expr, $expected_state:expr, $text:expr) => {
    #[cfg(debug_assertions)]
    {
      let current_state = $lifecycle.get();
      debug_assert!(current_state < $expected_state, $text);
    }

    // In release builds, we do nothing with the lifecycle state to avoid unused variable warnings.
    #[cfg(not(debug_assertions))]
    {
      let _ = $lifecycle;
      let _ = $expected_state;
    }
  };
}
