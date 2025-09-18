use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Hash)]
pub struct WorkflowDebugStateKey {
  pub state_id: String,
  pub transition_type: WorkflowDebugTransitionType,
}

impl WorkflowDebugStateKey {
  #[must_use]
  pub fn new(state_id: String, transition_type: WorkflowDebugTransitionType) -> Self {
    Self {
      state_id,
      transition_type,
    }
  }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Hash)]
pub enum WorkflowDebugTransitionType {
  // Normal transition including the index.
  Normal(usize),
  // Timeout transition.
  Timeout,
}
