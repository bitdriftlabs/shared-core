// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE.polyform file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

#[cfg(test)]
#[path = "./routing_test.rs"]
mod routing_test;

use bd_log_matcher::matcher::LogTypeSet;
use bd_proto::protos::logging::payload::LogType;
use itertools::Itertools;
use protobuf::Enum;

const LOG_TYPE_BUCKET_COUNT: usize = LogType::VALUES.len();

//
// WorkflowLogRoute
//

// TODO(snowp): We should be able to apply this routing logic to other event types to dramatically
// reduce the number of workflows attempted for state events. For now state events are rare so we
// limit this to logs.

/// Describes which log types can require a workflow to process a log.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum WorkflowLogRoute {
  /// No active traversal can react to a log. This can happen if the workflow is matching on a state
  /// change.
  #[default]
  None,
  /// The workflow must inspect every log.
  Fallback,
  /// The workflow only needs logs whose type is included in the set.
  Types(LogTypeSet),
}

//
// WorkflowLogRouter
//

/// Maintains sorted workflow indices for efficient, allocation-free log routing.
///
/// This is a derived view over the engine's primary workflow vector: every stored index refers to
/// the workflow at that same position. The caller must rebuild it after adding, removing, or
/// reordering workflows; between those boundaries, the router updates only workflows which
/// processed the current log.
///
/// Non-matching logs are expected to be common, so selection and evaluation do not allocate.
/// Rebuilds may reserve storage after configuration or workflow-state changes; that infrequent
/// cost keeps this additional routing layer allocation-free during the common case of log
/// processing.
#[derive(Debug, Default)]
pub struct WorkflowLogRouter {
  routes: Vec<WorkflowLogRoute>,
  fallback_workflow_indices: Vec<usize>,
  type_buckets: [Vec<usize>; LOG_TYPE_BUCKET_COUNT],
  candidate_indices: Vec<usize>,
}

impl WorkflowLogRouter {
  /// Clears the derived routes before repopulating them in primary workflow-vector order.
  pub(crate) fn prepare(&mut self, workflow_count: usize) {
    self.routes.clear();
    self.routes.reserve(workflow_count);
    self.fallback_workflow_indices.clear();
    reserve_to(&mut self.fallback_workflow_indices, workflow_count);
    self.candidate_indices.clear();
    reserve_to(&mut self.candidate_indices, workflow_count);

    for bucket in &mut self.type_buckets {
      bucket.clear();
      reserve_to(bucket, workflow_count);
    }
  }

  pub(crate) fn append_workflow_route(&mut self, workflow_index: usize, route: WorkflowLogRoute) {
    debug_assert_eq!(workflow_index, self.routes.len());
    self.routes.push(route);
    self.add_route_to_buckets(workflow_index, route);
  }

  /// Selects the workflows which need to evaluate a log with this type.
  ///
  /// The candidate set is the ordered union of type-specific workflows and fallback workflows,
  /// which must inspect every log. Keeping the result in workflow-index order preserves the
  /// original engine evaluation order.
  ///
  /// `prepare` reserves enough space for every workflow, so clearing and merging this scratch
  /// vector reuses its capacity without allocating while processing a log.
  ///
  /// The returned indices remain valid until the next selection or route refresh.
  pub(crate) fn select_candidates_for_log_type(&mut self, log_type: LogType) -> &[usize] {
    self.candidate_indices.clear();
    let type_indices = &self.type_buckets[log_type as usize];

    self.candidate_indices.extend(
      self
        .fallback_workflow_indices
        .iter()
        .copied()
        .merge(type_indices.iter().copied()),
    );
    &self.candidate_indices
  }

  /// Updates routes for the workflows selected by the preceding log selection.
  pub(crate) fn refresh_selected_routes<F>(&mut self, mut route_for_index: F)
  where
    F: FnMut(usize) -> WorkflowLogRoute,
  {
    for candidate_position in 0 .. self.candidate_indices.len() {
      let Some(workflow_index) = self.candidate_indices.get(candidate_position).copied() else {
        continue;
      };
      let route = route_for_index(workflow_index);
      self.replace(workflow_index, route);
    }
  }

  fn replace(&mut self, workflow_index: usize, route: WorkflowLogRoute) {
    let Some(previous_route) = self.routes.get(workflow_index).copied() else {
      return;
    };

    if previous_route == route {
      return;
    }

    // An active workflow frequently advances from one type-constrained state to another. Keep
    // membership in the shared buckets and update only the types that changed.
    if let (WorkflowLogRoute::Types(previous_types), WorkflowLogRoute::Types(next_types)) =
      (previous_route, route)
    {
      for log_type in previous_types.difference(next_types).iter() {
        remove_sorted(&mut self.type_buckets[log_type as usize], workflow_index);
      }
      for log_type in next_types.difference(previous_types).iter() {
        insert_sorted(&mut self.type_buckets[log_type as usize], workflow_index);
      }
    } else {
      self.remove_route_from_buckets(workflow_index, previous_route);
      self.add_route_to_buckets(workflow_index, route);
    }

    if let Some(previous_route) = self.routes.get_mut(workflow_index) {
      *previous_route = route;
    }
  }

  fn add_route_to_buckets(&mut self, workflow_index: usize, route: WorkflowLogRoute) {
    match route {
      WorkflowLogRoute::None => {},
      WorkflowLogRoute::Fallback => {
        insert_sorted(&mut self.fallback_workflow_indices, workflow_index);
      },
      WorkflowLogRoute::Types(log_types) => {
        for log_type in log_types.iter() {
          insert_sorted(&mut self.type_buckets[log_type as usize], workflow_index);
        }
      },
    }
  }

  fn remove_route_from_buckets(&mut self, workflow_index: usize, route: WorkflowLogRoute) {
    match route {
      WorkflowLogRoute::None => {},
      WorkflowLogRoute::Fallback => {
        remove_sorted(&mut self.fallback_workflow_indices, workflow_index);
      },
      WorkflowLogRoute::Types(log_types) => {
        for log_type in log_types.iter() {
          remove_sorted(&mut self.type_buckets[log_type as usize], workflow_index);
        }
      },
    }
  }
}

fn reserve_to(values: &mut Vec<usize>, capacity: usize) {
  if values.capacity() < capacity {
    values.reserve(capacity - values.capacity());
  }
}

fn insert_sorted(values: &mut Vec<usize>, value: usize) {
  match values.binary_search(&value) {
    Ok(_) => {},
    Err(position) => values.insert(position, value),
  }
}

fn remove_sorted(values: &mut Vec<usize>, value: usize) {
  if let Ok(position) = values.binary_search(&value) {
    values.remove(position);
  }
}
