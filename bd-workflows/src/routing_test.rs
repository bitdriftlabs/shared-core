// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE.polyform file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

use super::{WorkflowEventKind, WorkflowEventRoute, WorkflowLogRouter};
use bd_log_matcher::matcher::LogTypeSet;
use bd_proto::protos::logging::payload::LogType;
use pretty_assertions::assert_eq;
use protobuf::Enum;

fn log_types(types: &[LogType]) -> LogTypeSet {
  let mut log_types = LogTypeSet::default();
  for log_type in types {
    log_types.union(LogTypeSet::from_log_type(*log_type));
  }
  log_types
}

fn log_route(log_types: LogTypeSet) -> WorkflowEventRoute {
  WorkflowEventRoute::from_log_types(log_types)
}

fn no_route() -> WorkflowEventRoute {
  WorkflowEventRoute::default()
}

#[test]
fn log_type_values_are_dense_bucket_indices() {
  for (index, log_type) in LogType::VALUES.iter().enumerate() {
    assert_eq!(index, *log_type as usize);
  }
}

#[test]
fn selects_only_matching_and_fallback_workflows_in_index_order() {
  let lifecycle = log_types(&[LogType::LIFECYCLE]);
  let lifecycle_or_resource = log_types(&[LogType::LIFECYCLE, LogType::RESOURCE]);
  let mut router = WorkflowLogRouter::default();
  router.prepare(4);
  router.append_workflow_route(0, log_route(lifecycle));
  router.append_workflow_route(1, WorkflowEventRoute::fallback());
  router.append_workflow_route(2, log_route(lifecycle_or_resource));
  router.append_workflow_route(3, no_route());

  let selected = router.select_candidates_for_log_type(LogType::LIFECYCLE);
  assert_eq!(&[0, 1, 2], selected.indices());
  selected.finish_without_route_refresh();

  let selected = router.select_candidates_for_log_type(LogType::RESOURCE);
  assert_eq!(&[1, 2], selected.indices());
  selected.finish_without_route_refresh();

  let selected = router.select_candidates_for_log_type(LogType::NORMAL);
  assert_eq!(&[1], selected.indices());
  selected.finish_without_route_refresh();
}

#[test]
fn refreshes_only_the_workflows_selected_for_a_log() {
  let lifecycle_or_resource = log_types(&[LogType::LIFECYCLE, LogType::RESOURCE]);
  let resource_or_normal = log_types(&[LogType::RESOURCE, LogType::NORMAL]);
  let mut router = WorkflowLogRouter::default();
  router.prepare(3);
  router.append_workflow_route(0, log_route(lifecycle_or_resource));
  router.append_workflow_route(1, WorkflowEventRoute::fallback());
  router.append_workflow_route(2, log_route(resource_or_normal));

  let selected = router.select_candidates_for_log_type(LogType::LIFECYCLE);
  assert_eq!(&[0, 1], selected.indices());
  let refreshed_routes = [log_route(resource_or_normal), no_route()];
  selected.refresh_routes(|index| refreshed_routes[index]);

  let selected = router.select_candidates_for_log_type(LogType::LIFECYCLE);
  assert_eq!(&[] as &[usize], selected.indices());
  selected.finish_without_route_refresh();

  let selected = router.select_candidates_for_log_type(LogType::RESOURCE);
  assert_eq!(&[0, 2], selected.indices());
  selected.finish_without_route_refresh();

  let selected = router.select_candidates_for_log_type(LogType::NORMAL);
  assert_eq!(&[0, 2], selected.indices());
  selected.finish_without_route_refresh();
}

#[test]
fn selects_only_event_candidates() {
  let mut state_change = no_route();
  state_change.add_event(WorkflowEventKind::StateChange);
  let mut session_start = no_route();
  session_start.add_event(WorkflowEventKind::SessionStart);
  let mut router = WorkflowLogRouter::default();
  router.prepare(3);
  router.append_workflow_route(0, state_change);
  router.append_workflow_route(1, session_start);
  router.append_workflow_route(2, WorkflowEventRoute::fallback());

  let selected = router.select_event_candidates(WorkflowEventKind::StateChange);
  assert_eq!(&[0, 2], selected.indices());
  selected.finish_without_route_refresh();

  let selected = router.select_event_candidates(WorkflowEventKind::SessionStart);
  assert_eq!(&[1, 2], selected.indices());
  selected.finish_without_route_refresh();
}

#[test]
fn refreshing_an_event_route_moves_it_between_event_buckets() {
  let mut state_change = no_route();
  state_change.add_event(WorkflowEventKind::StateChange);
  let mut session_start = no_route();
  session_start.add_event(WorkflowEventKind::SessionStart);
  let mut router = WorkflowLogRouter::default();
  router.prepare(1);
  router.append_workflow_route(0, state_change);

  let selected = router.select_event_candidates(WorkflowEventKind::StateChange);
  assert_eq!(&[0], selected.indices());
  selected.refresh_routes(|_| session_start);

  let selected = router.select_event_candidates(WorkflowEventKind::StateChange);
  assert_eq!(&[] as &[usize], selected.indices());
  selected.finish_without_route_refresh();

  let selected = router.select_event_candidates(WorkflowEventKind::SessionStart);
  assert_eq!(&[0], selected.indices());
  selected.finish_without_route_refresh();
}

#[test]
fn finish_without_route_refresh_leaves_routes_unchanged() {
  let lifecycle = log_types(&[LogType::LIFECYCLE]);
  let mut router = WorkflowLogRouter::default();
  router.prepare(1);
  router.append_workflow_route(0, log_route(lifecycle));

  let selected = router.select_candidates_for_log_type(LogType::LIFECYCLE);
  selected.finish_without_route_refresh();

  let selected = router.select_candidates_for_log_type(LogType::LIFECYCLE);
  assert_eq!(&[0], selected.indices());
  selected.finish_without_route_refresh();
}

#[test]
fn reserve_to_reaches_the_requested_capacity_after_clear() {
  let mut values = Vec::with_capacity(4);
  values.extend([0, 1, 2, 3]);
  values.clear();
  let requested_capacity = values.capacity() + 1;

  super::reserve_to(&mut values, requested_capacity);

  assert!(values.capacity() >= requested_capacity);
}
