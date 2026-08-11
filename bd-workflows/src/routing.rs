// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

#[cfg(test)]
#[path = "./routing_test.rs"]
mod routing_test;

use bd_log_matcher::matcher::LogTypeSet;
use bd_proto::protos::logging::payload::LogType;

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
// LogTypeBucket
//

#[derive(Debug)]
struct LogTypeBucket {
  log_type: LogType,
  workflow_indices: Vec<usize>,
}

//
// WorkflowLogRouter
//

/// Maintains sorted workflow indices for efficient, allocation-free log routing.
///
/// Workflow indices remain stable between configuration updates. The router is rebuilt at those
/// boundaries and otherwise only updates the workflows which processed the current log.
#[derive(Debug, Default)]
pub struct WorkflowLogRouter {
  routes: Vec<WorkflowLogRoute>,
  fallback_workflow_indices: Vec<usize>,
  type_buckets: Vec<LogTypeBucket>,
  candidate_indices: Vec<usize>,
}

impl WorkflowLogRouter {
  pub(crate) fn prepare(&mut self, workflow_count: usize, known_log_types: LogTypeSet) {
    self.routes.clear();
    self.routes.reserve(workflow_count);
    self.fallback_workflow_indices.clear();
    reserve_to(&mut self.fallback_workflow_indices, workflow_count);
    self.candidate_indices.clear();
    reserve_to(&mut self.candidate_indices, workflow_count);

    for log_type in known_log_types.iter() {
      if self
        .type_buckets
        .iter()
        .all(|bucket| bucket.log_type != log_type)
      {
        self.type_buckets.push(LogTypeBucket {
          log_type,
          workflow_indices: Vec::with_capacity(workflow_count),
        });
      }
    }

    for bucket in &mut self.type_buckets {
      bucket.workflow_indices.clear();
      reserve_to(&mut bucket.workflow_indices, workflow_count);
    }
  }

  pub(crate) fn insert(&mut self, workflow_index: usize, route: WorkflowLogRoute) {
    debug_assert_eq!(workflow_index, self.routes.len());
    self.routes.push(route);
    self.insert_route(workflow_index, route);
  }

  /// Selects the workflows which need to evaluate a log with this type.
  ///
  /// The returned indices remain valid until the next selection or route refresh.
  pub(crate) fn select_candidates_for_log_type(&mut self, log_type: LogType) -> &[usize] {
    self.candidate_indices.clear();
    let type_indices = self
      .type_buckets
      .iter()
      .find(|bucket| bucket.log_type == log_type)
      .map_or(&[][..], |bucket| bucket.workflow_indices.as_slice());

    merge_sorted_indices(
      &self.fallback_workflow_indices,
      type_indices,
      &mut self.candidate_indices,
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

    self.remove_route(workflow_index, previous_route);
    if let Some(previous_route) = self.routes.get_mut(workflow_index) {
      *previous_route = route;
    }
    self.insert_route(workflow_index, route);
  }

  fn insert_route(&mut self, workflow_index: usize, route: WorkflowLogRoute) {
    match route {
      WorkflowLogRoute::None => {},
      WorkflowLogRoute::Fallback => {
        insert_sorted(&mut self.fallback_workflow_indices, workflow_index);
      },
      WorkflowLogRoute::Types(log_types) => {
        for log_type in log_types.iter() {
          if let Some(bucket) = self
            .type_buckets
            .iter_mut()
            .find(|bucket| bucket.log_type == log_type)
          {
            insert_sorted(&mut bucket.workflow_indices, workflow_index);
          }
        }
      },
    }
  }

  fn remove_route(&mut self, workflow_index: usize, route: WorkflowLogRoute) {
    match route {
      WorkflowLogRoute::None => {},
      WorkflowLogRoute::Fallback => {
        remove_sorted(&mut self.fallback_workflow_indices, workflow_index);
      },
      WorkflowLogRoute::Types(log_types) => {
        for log_type in log_types.iter() {
          if let Some(bucket) = self
            .type_buckets
            .iter_mut()
            .find(|bucket| bucket.log_type == log_type)
          {
            remove_sorted(&mut bucket.workflow_indices, workflow_index);
          }
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

fn merge_sorted_indices(first: &[usize], second: &[usize], output: &mut Vec<usize>) {
  let mut first = first.iter().copied().peekable();
  let mut second = second.iter().copied().peekable();

  while let (Some(first_index), Some(second_index)) = (first.peek(), second.peek()) {
    if first_index < second_index {
      if let Some(index) = first.next() {
        output.push(index);
      }
    } else if let Some(index) = second.next() {
      output.push(index);
    }
  }

  output.extend(first);
  output.extend(second);
}
