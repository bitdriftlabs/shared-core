// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

#[derive(PartialEq, Eq, Hash, Debug)]
pub struct WorkflowDebugKey {
  pub workflow_id: String,
  pub state_key: WorkflowDebugStateKey,
}

impl WorkflowDebugKey {
  #[must_use]
  pub fn new(workflow_id: String, state_key: WorkflowDebugStateKey) -> Self {
    Self {
      workflow_id,
      state_key,
    }
  }
}

#[bd_macros::proto_serializable]
#[derive(Debug, Clone, PartialEq, Eq, Hash, Default)]
pub enum WorkflowDebugStateKey {
  #[field(id = 1)]
  StateTransition {
    #[field(id = 1)]
    state_id: String,
    #[field(id = 2)]
    transition_type: WorkflowDebugTransitionType,
  },
  #[field(id = 2)]
  #[default]
  StartOrReset,
}

impl WorkflowDebugStateKey {
  #[must_use]
  pub fn new_state_transition(
    state_id: String,
    transition_type: WorkflowDebugTransitionType,
  ) -> Self {
    Self::StateTransition {
      state_id,
      transition_type,
    }
  }
}

#[bd_macros::proto_serializable]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum WorkflowDebugTransitionType {
  // Normal transition including the index.
  #[field(id = 1)]
  Normal(u64),
  // Timeout transition.
  #[field(id = 2)]
  Timeout,
}

impl Default for WorkflowDebugTransitionType {
  fn default() -> Self {
    Self::Normal(0)
  }
}
