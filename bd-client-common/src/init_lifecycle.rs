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
      state: Arc::new(RwLock::new(InitLifecycle::NotStarted)),
    }
  }

  #[must_use]
  pub fn is_not_at_or_later(&self, other: InitLifecycle) -> bool {
    let current = *self.state.read();
    current < other
  }

  #[must_use]
  pub fn get(&self) -> InitLifecycle {
    *self.state.read()
  }

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
}
