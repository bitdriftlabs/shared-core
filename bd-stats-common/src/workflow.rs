// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

use serde::{Deserialize, Serialize};

#[derive(PartialEq, Eq, Hash)]
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

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Hash)]
pub enum WorkflowDebugStateKey {
  StateTransition {
    state_id: String,
    transition_type: WorkflowDebugTransitionType,
  },
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

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Hash)]
pub enum WorkflowDebugTransitionType {
  // Normal transition including the index.
  Normal(usize),
  // Timeout transition.
  Timeout,
}
