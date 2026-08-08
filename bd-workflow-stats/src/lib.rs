// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE.polyform file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

pub mod workflow;

use crate::workflow::WorkflowDebugKey;
use std::collections::BTreeMap;

//
// StatsCollector
//

pub trait StatsCollector: Send + Sync {
  type Counter;
  type Histogram;

  fn record_dynamic_counter(&self, tags: BTreeMap<String, String>, id: &str, value: u64);

  fn record_workflow_debug_state(&self, state: Vec<WorkflowDebugKey>);

  fn workflow_dynamic_counter(
    &self,
    tags: BTreeMap<String, String>,
    id: &str,
  ) -> Option<Self::Counter>;

  fn workflow_dynamic_histogram(
    &self,
    tags: BTreeMap<String, String>,
    id: &str,
  ) -> Option<Self::Histogram>;
}
