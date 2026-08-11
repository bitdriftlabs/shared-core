// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE.polyform file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

use super::{WorkflowLogRoute, WorkflowLogRouter};
use bd_log_matcher::matcher::LogTypeSet;
use bd_proto::protos::logging::payload::LogType;
use pretty_assertions::assert_eq;

fn log_types(types: &[LogType]) -> LogTypeSet {
  let mut log_types = LogTypeSet::default();
  for log_type in types {
    log_types.union(LogTypeSet::from_log_type(*log_type));
  }
  log_types
}

#[test]
fn selects_only_matching_and_fallback_workflows_in_index_order() {
  let lifecycle = log_types(&[LogType::LIFECYCLE]);
  let lifecycle_or_resource = log_types(&[LogType::LIFECYCLE, LogType::RESOURCE]);
  let mut router = WorkflowLogRouter::default();
  router.prepare(4);
  router.insert(0, WorkflowLogRoute::Types(lifecycle));
  router.insert(1, WorkflowLogRoute::Fallback);
  router.insert(2, WorkflowLogRoute::Types(lifecycle_or_resource));
  router.insert(3, WorkflowLogRoute::None);

  assert_eq!(
    &[0, 1, 2],
    router.select_candidates_for_log_type(LogType::LIFECYCLE)
  );
  assert_eq!(
    &[1, 2],
    router.select_candidates_for_log_type(LogType::RESOURCE)
  );
  assert_eq!(&[1], router.select_candidates_for_log_type(LogType::NORMAL));
}

#[test]
fn refreshes_only_the_workflows_selected_for_a_log() {
  let lifecycle = log_types(&[LogType::LIFECYCLE]);
  let resource = log_types(&[LogType::RESOURCE]);
  let mut router = WorkflowLogRouter::default();
  router.prepare(3);
  router.insert(0, WorkflowLogRoute::Types(lifecycle));
  router.insert(1, WorkflowLogRoute::Fallback);
  router.insert(2, WorkflowLogRoute::Types(resource));

  assert_eq!(
    &[0, 1],
    router.select_candidates_for_log_type(LogType::LIFECYCLE)
  );
  let refreshed_routes = [WorkflowLogRoute::Types(resource), WorkflowLogRoute::None];
  router.refresh_selected_routes(|index| refreshed_routes[index]);

  assert_eq!(
    &[] as &[usize],
    router.select_candidates_for_log_type(LogType::LIFECYCLE)
  );
  assert_eq!(
    &[0, 2],
    router.select_candidates_for_log_type(LogType::RESOURCE)
  );
}
