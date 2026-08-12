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
const EVENT_BUCKET_COUNT: usize = 2;

//
// WorkflowEventKind
//

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum WorkflowEventKind {
  StateChange,
  SessionStart,
}

impl WorkflowEventKind {
  const ALL: [Self; EVENT_BUCKET_COUNT] = [Self::StateChange, Self::SessionStart];

  const fn index(self) -> usize {
    match self {
      Self::StateChange => 0,
      Self::SessionStart => 1,
    }
  }
}

//
// WorkflowEventRoute
//

/// Describes which events can require a workflow to process an event.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct WorkflowEventRoute {
  log: WorkflowLogRoute,
  event_mask: u8,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum WorkflowLogRoute {
  #[default]
  None,
  Fallback,
  Types(LogTypeSet),
}

impl WorkflowEventRoute {
  #[must_use]
  pub(crate) const fn fallback() -> Self {
    Self {
      log: WorkflowLogRoute::Fallback,
      event_mask: Self::all_event_mask(),
    }
  }

  #[cfg(test)]
  #[must_use]
  pub(crate) const fn from_log_types(log_types: LogTypeSet) -> Self {
    Self {
      log: WorkflowLogRoute::Types(log_types),
      event_mask: 0,
    }
  }

  pub(crate) fn add_event(&mut self, event: WorkflowEventKind) {
    self.event_mask |= Self::event_mask(event);
  }

  pub(crate) fn set_log_route(&mut self, log: WorkflowLogRoute) {
    debug_assert!(
      matches!(self.log, WorkflowLogRoute::None),
      "workflow event route log routing must be finalized exactly once"
    );
    self.log = log;
  }

  const fn needs_event(self, event: WorkflowEventKind) -> bool {
    self.event_mask & Self::event_mask(event) != 0
  }

  const fn event_mask(event: WorkflowEventKind) -> u8 {
    1 << event.index()
  }

  const fn all_event_mask() -> u8 {
    (1 << EVENT_BUCKET_COUNT) - 1
  }
}

//
// WorkflowLogRouter
//

/// Maintains sorted workflow indices for efficient, allocation-free log routing.
///
/// This is a derived view over the engine's primary workflow vector: every stored index refers to
/// the workflow at that same position. The caller must rebuild it after adding, removing, or
/// reordering workflows; between those boundaries, it refreshes only the routes for workflows
/// selected to process the current event.
///
/// Non-matching logs are expected to be common, so selection and evaluation do not allocate.
/// Rebuilds may reserve storage after configuration or workflow-state changes; that infrequent
/// cost keeps this additional routing layer allocation-free during the common case of log
/// processing.
#[derive(Debug, Default)]
pub struct WorkflowLogRouter {
  routes: Vec<WorkflowEventRoute>,
  fallback_workflow_indices: Vec<usize>,
  type_buckets: [Vec<usize>; LOG_TYPE_BUCKET_COUNT],
  event_buckets: [Vec<usize>; EVENT_BUCKET_COUNT],
  candidate_indices: Vec<usize>,
}

//
// SelectedWorkflows
//

/// A selected candidate set that must be finalized before processing the next event.
///
/// Holding this guard prevents the router from being updated while the engine is evaluating the
/// selected workflows. Its debug-only drop assertion catches future early returns that would leave
/// the derived routes stale.
#[must_use = "selected workflow candidates must be finalized after processing"]
pub struct SelectedWorkflows<'a> {
  router: &'a mut WorkflowLogRouter,
  finalized: bool,
}

impl SelectedWorkflows<'_> {
  #[must_use]
  pub(crate) fn indices(&self) -> &[usize] {
    &self.router.candidate_indices
  }

  /// Finalizes a selection without recalculating any routes when processing left them unchanged.
  #[cfg(test)]
  pub(crate) fn finish_without_route_refresh(mut self) {
    self.finalized = true;
  }

  /// Recalculates routes for exactly the workflows selected for the preceding event.
  pub(crate) fn refresh_routes<F>(mut self, mut route_for_index: F)
  where
    F: FnMut(usize) -> WorkflowEventRoute,
  {
    for candidate_position in 0 .. self.router.candidate_indices.len() {
      let Some(workflow_index) = self
        .router
        .candidate_indices
        .get(candidate_position)
        .copied()
      else {
        continue;
      };
      let route = route_for_index(workflow_index);
      self.router.replace(workflow_index, route);
    }
    self.finalized = true;
  }
}

impl Drop for SelectedWorkflows<'_> {
  fn drop(&mut self) {
    debug_assert!(
      self.finalized,
      "selected workflow candidates were not finalized"
    );
  }
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
    for bucket in &mut self.event_buckets {
      bucket.clear();
      reserve_to(bucket, workflow_count);
    }
  }

  pub(crate) fn append_workflow_route(&mut self, workflow_index: usize, route: WorkflowEventRoute) {
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
  pub(crate) fn select_candidates_for_log_type(
    &mut self,
    log_type: LogType,
  ) -> SelectedWorkflows<'_> {
    self.candidate_indices.clear();
    let type_indices = self
      .type_buckets
      .get(log_type as usize)
      .map_or(&[] as &[usize], Vec::as_slice);

    self.candidate_indices.extend(
      self
        .fallback_workflow_indices
        .iter()
        .copied()
        .merge(type_indices.iter().copied()),
    );
    SelectedWorkflows {
      router: self,
      finalized: false,
    }
  }

  /// Selects only the workflows which need to evaluate this non-log event.
  pub(crate) fn select_event_candidates(
    &mut self,
    event: WorkflowEventKind,
  ) -> SelectedWorkflows<'_> {
    self.candidate_indices.clear();
    if let Some(indices) = self.event_buckets.get(event.index()) {
      self.candidate_indices.extend(indices);
    }
    SelectedWorkflows {
      router: self,
      finalized: false,
    }
  }

  fn replace(&mut self, workflow_index: usize, route: WorkflowEventRoute) {
    let Some(previous_route) = self.routes.get(workflow_index).copied() else {
      return;
    };

    if previous_route == route {
      return;
    }

    self.replace_log_route(workflow_index, previous_route.log, route.log);
    for event in WorkflowEventKind::ALL {
      match (previous_route.needs_event(event), route.needs_event(event)) {
        (false, true) => {
          if let Some(bucket) = self.event_buckets.get_mut(event.index()) {
            insert_sorted(bucket, workflow_index);
          }
        },
        (true, false) => {
          if let Some(bucket) = self.event_buckets.get_mut(event.index()) {
            remove_sorted(bucket, workflow_index);
          }
        },
        (false, false) | (true, true) => {},
      }
    }

    if let Some(previous_route) = self.routes.get_mut(workflow_index) {
      *previous_route = route;
    }
  }

  fn add_route_to_buckets(&mut self, workflow_index: usize, route: WorkflowEventRoute) {
    self.add_log_route_to_buckets(workflow_index, route.log);
    for event in WorkflowEventKind::ALL {
      if route.needs_event(event)
        && let Some(bucket) = self.event_buckets.get_mut(event.index())
      {
        insert_sorted(bucket, workflow_index);
      }
    }
  }

  fn replace_log_route(
    &mut self,
    workflow_index: usize,
    previous_route: WorkflowLogRoute,
    route: WorkflowLogRoute,
  ) {
    if previous_route == route {
      return;
    }

    // An active workflow frequently advances from one type-constrained state to another. Keep
    // membership in shared log-type buckets and update only the types that changed.
    if let (WorkflowLogRoute::Types(previous_types), WorkflowLogRoute::Types(next_types)) =
      (previous_route, route)
    {
      self.remove_types_from_buckets(workflow_index, previous_types.difference(next_types));
      self.add_types_to_buckets(workflow_index, next_types.difference(previous_types));
    } else {
      self.remove_log_route_from_buckets(workflow_index, previous_route);
      self.add_log_route_to_buckets(workflow_index, route);
    }
  }

  fn add_log_route_to_buckets(&mut self, workflow_index: usize, route: WorkflowLogRoute) {
    match route {
      WorkflowLogRoute::None => {},
      WorkflowLogRoute::Fallback => {
        insert_sorted(&mut self.fallback_workflow_indices, workflow_index);
      },
      WorkflowLogRoute::Types(log_types) => {
        self.add_types_to_buckets(workflow_index, log_types);
      },
    }
  }

  fn remove_log_route_from_buckets(&mut self, workflow_index: usize, route: WorkflowLogRoute) {
    match route {
      WorkflowLogRoute::None => {},
      WorkflowLogRoute::Fallback => {
        remove_sorted(&mut self.fallback_workflow_indices, workflow_index);
      },
      WorkflowLogRoute::Types(log_types) => {
        self.remove_types_from_buckets(workflow_index, log_types);
      },
    }
  }

  fn add_types_to_buckets(&mut self, workflow_index: usize, log_types: LogTypeSet) {
    for log_type in log_types.iter() {
      if let Some(bucket) = self.type_buckets.get_mut(log_type as usize) {
        insert_sorted(bucket, workflow_index);
      }
    }
  }

  fn remove_types_from_buckets(&mut self, workflow_index: usize, log_types: LogTypeSet) {
    for log_type in log_types.iter() {
      if let Some(bucket) = self.type_buckets.get_mut(log_type as usize) {
        remove_sorted(bucket, workflow_index);
      }
    }
  }
}

fn reserve_to(values: &mut Vec<usize>, capacity: usize) {
  if values.capacity() < capacity {
    values.reserve(capacity - values.len());
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
