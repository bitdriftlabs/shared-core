// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

use crate::config::{ActionEmitMetric, ActionFlushBuffers, Config, FlushBufferId, ValueIncrement};
use crate::workflow::{Run, TriggeredAction, Workflow, WorkflowResult, WorkflowResultStats};
use bd_log_primitives::{FieldsRef, LogFields, LogMessage, log_level};
use bd_proto::flatbuffers::buffer_log::bitdrift_public::fbs::logging::v_1::LogType;
use bd_stats_common::{MetricType, labels};
use bd_test_helpers::metric_value;
use bd_test_helpers::workflow::macros::{
  action,
  all,
  any,
  limit,
  log_matches,
  rule,
  state,
  workflow_proto,
};
use pretty_assertions::assert_eq;
use std::collections::{BTreeMap, BTreeSet};
use std::vec;

#[ctor::ctor]
fn test_global_init() {
  bd_test_helpers::test_global_init();
}

/// A macro that creates a workflow config using provided states.
/// See `workflow_proto` macro for more details.
/// A similar workflow! macro is available in `test_helpers::workflows`
/// but due to issues with referencing types defined in `workflows` crate
/// its usage from within `workflows` (current) crate results in a compilation error.
/// For this reason, we define a workflow! macro below.
macro_rules! workflow {
  ($($x:tt)*) => {
    $crate::config::Config::new(
      bd_test_helpers::workflow::macros::workflow_proto!($($x)*)
    ).unwrap()
  }
}

/// A macro that makes a given workflow process a log specified by
/// a message and/or tags.
#[macro_export]
#[allow(clippy::module_name_repetitions)]
macro_rules! workflow_process_log {
  ($annotated_workflow:expr; $message:expr) => {
    $annotated_workflow.workflow.process_log(
      &$annotated_workflow.config,
      &bd_log_primitives::LogRef {
        log_type: LogType::Normal,
        log_level: log_level::DEBUG,
        message: &LogMessage::String($message.to_string()),
        fields: &FieldsRef::new(&LogFields::new(), &LogFields::new()),
        session_id: "foo",
        occurred_at: time::OffsetDateTime::now_utc(),
        capture_session: None,
      },
      &mut 0,
      1000,
    )
  };
  ($annotated_workflow:expr; $message:expr,with $tags:expr) => {
    $annotated_workflow.workflow.process_log(
      &$annotated_workflow.config,
      &bd_log_primitives::LogRef {
        log_type: LogType::Normal,
        log_level: log_level::DEBUG,
        message: &LogMessage::String($message.to_string()),
        fields: &FieldsRef::new(
          &bd_test_helpers::workflow::make_tags($tags),
          &LogFields::new(),
        ),
        session_id: "foo",
        occurred_at: time::OffsetDateTime::now_utc(),
        capture_session: None,
      },
      &mut 0,
      1000,
    )
  };
}

//
// AnnotatedWorkflow
//

// A convenience wrapper that's supposed to reduce the amount of
// boilerplate code needed to tests workflows. Combines configuration and
// state of a given workflow into one entity for easier passing to method/macro
// calls.
pub struct AnnotatedWorkflow {
  config: Config,
  workflow: Workflow,
}

impl AnnotatedWorkflow {
  pub fn new(config: Config) -> Self {
    Self {
      workflow: Workflow::new(config.id().to_string()),
      config,
    }
  }

  pub fn runs(&self) -> &[Run] {
    self.workflow.runs()
  }

  pub fn is_in_initial_state(&self) -> bool {
    self.workflow.is_in_initial_state()
  }

  pub fn runs_state_list(&self) -> Vec<String> {
    self.workflow.runs_states(&self.config)
  }

  pub fn traversals_state_list(&self, run_index: usize) -> Vec<String> {
    self.workflow.traversals_states(&self.config, run_index)
  }
}

/// Asserts that the states of workflow' runs are equal to expected
/// list of states.
/// The macro assumes that each run has only one traversals.
/// For macros that don't meet this condition `assert_active_run_traversals`
/// macro should be used instead.
#[macro_export]
macro_rules! assert_active_runs {
    ($workflow_annotated:expr; $($state_id:expr),+) => {
      let expected_states = [$($state_id,)+];

      pretty_assertions::assert_eq!(
        expected_states.len(),
        $workflow_annotated.workflow.runs().len(),
        "workflow runs' states list ({:?}) and expected states list ({:?}) have different lengths",
        $workflow_annotated.runs_state_list(),
        expected_states
      );

      for (index, id) in expected_states.iter().enumerate() {
        let expected_state_index = $workflow_annotated.config.states().iter()
          .position(|state| state.id() == *id);
        assert!(
          expected_state_index.is_some(),
          "failed to find state with \"{}\" ID", *id
        );
        pretty_assertions::assert_eq!(
          1,
          $workflow_annotated.workflow.runs()[index].traversals.len(),
          "run has more than 1 traversal, use `assert_active_run_traversals` macro instead"
        );
        pretty_assertions::assert_eq!(
          expected_state_index.unwrap(),
          $workflow_annotated.workflow.runs()[index].traversals[0].state_index,
          "workflow runs' states list ({:?}) doesn't match expected states list ({:?}) length",
          $workflow_annotated.runs_state_list(),
          expected_states
        );
      }

      for run in &$workflow_annotated.workflow.runs {
        pretty_assertions::assert_eq!(1, run.traversals.len());
      }
    };
  }

/// Asserts that the states of given workflow run's traversals are equal
/// to expected list of states.
#[macro_export]
macro_rules! assert_active_run_traversals {
    ($annotated_workflow:expr; $run_index:expr; $($state_id:expr),+) => {
      #[allow(unused_comparisons)]
      let run_exists = $annotated_workflow.workflow.runs.len() >= $run_index;
      assert!(
        run_exists,
        "run with index ({}) doesn't exist, workflow has only {} runs: {:?}",
        $run_index,
        $annotated_workflow.workflow.runs().len(),
        $annotated_workflow.runs_state_list()
      );

      let expected_states = [$($state_id,)+];
      pretty_assertions::assert_eq!(
        expected_states.len(),
        $annotated_workflow.workflow.runs()[$run_index].traversals.len(),
        "workflow run traversals' states list ({:?}) \
         and expected states list ({:?}) have different lengths",
        $annotated_workflow.traversals_state_list($run_index),
        expected_states
      );

      for (index, id) in expected_states.iter().enumerate() {
        let expected_state_index = $annotated_workflow.config.states().iter()
        .position(|state| state.id() == *id);
        assert!(
          expected_state_index.is_some(),
          "failed to find state with \"{}\" ID", *id
        );
        pretty_assertions::assert_eq!(
          expected_state_index.unwrap(),
          $annotated_workflow.workflow.runs()[$run_index].traversals[index].state_index,
          "workflow runs traversals' states list ({:?}) doesn't match expected states list ({:?})",
          $annotated_workflow.traversals_state_list($run_index),
          expected_states
        );
      }
    };
  }

#[test]
fn one_state_workflow() {
  let a = state("A");

  // test exclusive workflow
  let config = workflow!(exclusive with a);
  let mut workflow = AnnotatedWorkflow::new(config);
  workflow_process_log!(workflow; "foo");
}

#[test]
fn unknown_state_reference_workflow() {
  let mut a = state("A");
  let b = state("B");

  a = a.declare_transition_with_actions(
    &b,
    rule!(log_matches!(message == "foo")),
    &[action!(flush_buffers &["foo_buffer_id"]; id "foo")],
  );

  let config = workflow_proto!(exclusive with a);
  assert_eq!(
    Config::new(config).err().unwrap().to_string(),
    "invalid workflow state configuration: reference to an unexisting state"
  );
}

#[test]
#[allow(clippy::many_single_char_names)]
fn multiple_start_nodes_initial_fork() {
  let mut a = state("A");
  let mut b = state("B");
  let mut c = state("C");
  let d = state("D");
  let e = state("E");

  a = a.declare_transition(&b, rule!(log_matches!(message == "foo")));

  b = b.declare_transition(&d, rule!(log_matches!(message == "D")));

  a = a.declare_transition(&c, rule!(log_matches!(message == "foo")));

  c = c.declare_transition_with_actions(
    &e,
    rule!(log_matches!(message == "E")),
    &[action!(flush_buffers &["foo_buffer_id"]; id "foo")],
  );

  let config = workflow!(exclusive with a, b, c, d, e);
  let mut workflow = AnnotatedWorkflow::new(config);
  assert!(workflow.runs().is_empty());

  // The first log causes a fork since it matches the first two transitions.
  let result = workflow_process_log!(workflow; "foo");
  assert_eq!(
    result,
    WorkflowResult {
      triggered_actions: vec![],
      logs_to_inject: BTreeMap::new(),
      stats: WorkflowResultStats {
        reset_exclusive_workflows_count: 0,
        created_runs_count: 1,
        advanced_runs_count: 1,
        completed_runs_count: 0,
        stopped_runs_count: 0,
        created_traversals_count: 2,
        advanced_traversals_count: 1,
        completed_traversals_count: 0,
        stopped_traversals_count: 0,
        traversals_count_limit_hit_count: 0,
        matched_logs_count: 2,
      },
    }
  );

  // Seeing the entry condition again resets all the traversals.
  let result = workflow_process_log!(workflow; "foo");
  assert_eq!(
    result,
    WorkflowResult {
      triggered_actions: vec![],
      logs_to_inject: BTreeMap::new(),
      stats: WorkflowResultStats {
        reset_exclusive_workflows_count: 1,
        created_runs_count: 1,
        advanced_runs_count: 1,
        completed_runs_count: 0,
        stopped_runs_count: 1,
        created_traversals_count: 2,
        advanced_traversals_count: 1,
        completed_traversals_count: 0,
        stopped_traversals_count: 2,
        traversals_count_limit_hit_count: 0,
        matched_logs_count: 2,
      },
    }
  );

  // Finalize one of the forks. Only one of the traversals/forks is completed.
  let result = workflow_process_log!(workflow; "E");
  assert_eq!(
    result,
    WorkflowResult {
      triggered_actions: vec![TriggeredAction::FlushBuffers(&ActionFlushBuffers {
        id: FlushBufferId::WorkflowActionId("foo".to_string()),
        buffer_ids: BTreeSet::from(["foo_buffer_id".to_string()]),
        streaming: None,
      })],
      logs_to_inject: BTreeMap::new(),
      stats: WorkflowResultStats {
        reset_exclusive_workflows_count: 0,
        created_runs_count: 1,
        advanced_runs_count: 1,
        completed_runs_count: 0,
        stopped_runs_count: 0,
        created_traversals_count: 1,
        advanced_traversals_count: 1,
        completed_traversals_count: 1,
        stopped_traversals_count: 0,
        traversals_count_limit_hit_count: 0,
        matched_logs_count: 1,
      },
    }
  );
}

#[test]
#[allow(clippy::many_single_char_names)]
fn multiple_start_nodes_initial_branching() {
  let mut a = state("A");
  let mut b = state("B");
  let mut c = state("C");
  let d = state("D");
  let e = state("E");

  a = a.declare_transition(&b, rule!(log_matches!(message == "foo")));

  b = b.declare_transition(&d, rule!(log_matches!(message == "D")));

  a = a.declare_transition(&c, rule!(log_matches!(message == "bar")));

  c = c.declare_transition_with_actions(
    &e,
    rule!(log_matches!(message == "E")),
    &[action!(flush_buffers &["foo_buffer_id"]; id "foo")],
  );

  let config = workflow!(exclusive with a, b, c, d, e);
  let mut workflow = AnnotatedWorkflow::new(config);
  assert!(workflow.runs().is_empty());

  // The first log progresses the workflow towards the "B" state.
  let result = workflow_process_log!(workflow; "foo");
  assert_eq!(
    result,
    WorkflowResult {
      triggered_actions: vec![],
      logs_to_inject: BTreeMap::new(),
      stats: WorkflowResultStats {
        reset_exclusive_workflows_count: 0,
        created_runs_count: 1,
        advanced_runs_count: 1,
        completed_runs_count: 0,
        stopped_runs_count: 0,
        created_traversals_count: 1,
        advanced_traversals_count: 1,
        completed_traversals_count: 0,
        stopped_traversals_count: 0,
        traversals_count_limit_hit_count: 0,
        matched_logs_count: 1,
      },
    }
  );

  // Seeing the initial log resets the workflow, even if the initial condition is not the same as
  // the one that initiated the workflow run.
  let result = workflow_process_log!(workflow; "bar");
  assert_eq!(
    result,
    WorkflowResult {
      triggered_actions: vec![],
      logs_to_inject: BTreeMap::new(),
      stats: WorkflowResultStats {
        reset_exclusive_workflows_count: 1,
        created_runs_count: 1,
        advanced_runs_count: 1,
        completed_runs_count: 0,
        stopped_runs_count: 1,
        created_traversals_count: 1,
        advanced_traversals_count: 1,
        completed_traversals_count: 0,
        stopped_traversals_count: 1,
        traversals_count_limit_hit_count: 0,
        matched_logs_count: 1,
      },
    }
  );

  // Finalize the workflow.
  let result = workflow_process_log!(workflow; "E");
  assert_eq!(
    result,
    WorkflowResult {
      triggered_actions: vec![TriggeredAction::FlushBuffers(&ActionFlushBuffers {
        id: FlushBufferId::WorkflowActionId("foo".to_string()),
        buffer_ids: BTreeSet::from(["foo_buffer_id".to_string()]),
        streaming: None,
      })],
      logs_to_inject: BTreeMap::new(),
      stats: WorkflowResultStats {
        reset_exclusive_workflows_count: 0,
        created_runs_count: 1,
        advanced_runs_count: 1,
        completed_runs_count: 1,
        stopped_runs_count: 0,
        created_traversals_count: 1,
        advanced_traversals_count: 1,
        completed_traversals_count: 1,
        stopped_traversals_count: 0,
        traversals_count_limit_hit_count: 0,
        matched_logs_count: 1,
      },
    }
  );
}

#[test]
#[allow(clippy::cognitive_complexity)]
fn basic_exclusive_workflow() {
  let mut a = state("A");
  let mut b = state("B");
  let c = state("C");

  a = a.declare_transition_with_actions(
    &b,
    rule!(all!(
      log_matches!(message == "foo"),
      log_matches!(tag("key") == "value"),
    )),
    &[
      action!(flush_buffers &["foo_buffer_id"]; id "foo"),
      action!(emit_counter "foo_metric"; value metric_value!(123)),
    ],
  );
  b = b.declare_transition_with_actions(
    &c,
    rule!(log_matches!(message == "bar")),
    &[action!(flush_buffers &["bar_buffer_id"]; id "bar")],
  );

  let config = workflow!(exclusive with a, b, c);
  let mut workflow = AnnotatedWorkflow::new(config);
  assert!(workflow.runs().is_empty());

  // * A new run is created to ensure that workflow has a run in initial state.
  // * The first run moves from "A" to non-final "B" state.
  let result = workflow_process_log!(workflow; "foo", with labels! { "key" => "value" });
  assert_eq!(
    result,
    WorkflowResult {
      triggered_actions: vec![
        TriggeredAction::FlushBuffers(&ActionFlushBuffers {
          id: FlushBufferId::WorkflowActionId("foo".to_string()),
          buffer_ids: BTreeSet::from(["foo_buffer_id".to_string()]),
          streaming: None,
        }),
        TriggeredAction::EmitMetric(&ActionEmitMetric {
          id: "foo_metric".to_string(),
          tags: BTreeMap::new(),
          increment: ValueIncrement::Fixed(123),
          metric_type: MetricType::Counter,
        })
      ],
      logs_to_inject: BTreeMap::new(),
      stats: WorkflowResultStats {
        reset_exclusive_workflows_count: 0,
        created_runs_count: 1,
        advanced_runs_count: 1,
        completed_runs_count: 0,
        stopped_runs_count: 0,
        created_traversals_count: 1,
        advanced_traversals_count: 1,
        completed_traversals_count: 0,
        stopped_traversals_count: 0,
        traversals_count_limit_hit_count: 0,
        matched_logs_count: 1,
      },
    }
  );
  assert_active_runs!(workflow; "B");
  assert!(!workflow.is_in_initial_state());

  // * The first run moves from "B" to final "C" state.
  // * The first run completes.
  let result = workflow_process_log!(workflow; "bar");
  assert_eq!(
    result,
    WorkflowResult {
      triggered_actions: vec![TriggeredAction::FlushBuffers(&ActionFlushBuffers {
        id: FlushBufferId::WorkflowActionId("bar".to_string()),
        buffer_ids: BTreeSet::from(["bar_buffer_id".to_string()]),
        streaming: None,
      })],
      logs_to_inject: BTreeMap::new(),
      stats: WorkflowResultStats {
        reset_exclusive_workflows_count: 0,
        created_runs_count: 1,
        advanced_runs_count: 1,
        completed_runs_count: 1,
        stopped_runs_count: 0,
        created_traversals_count: 1,
        advanced_traversals_count: 1,
        completed_traversals_count: 1,
        stopped_traversals_count: 0,
        traversals_count_limit_hit_count: 0,
        matched_logs_count: 1,
      },
    }
  );
  assert_active_runs!(workflow; "A");
  assert!(workflow.is_in_initial_state());

  // A new run is created to ensure that a workflow has a run in an initial state.
  let result = workflow_process_log!(workflow; "not matching");
  assert_eq!(result, WorkflowResult::default());

  assert_active_runs!(workflow; "A");
  assert!(workflow.is_in_initial_state());
}

#[test]
#[allow(clippy::cognitive_complexity)]
#[allow(clippy::many_single_char_names)]
fn exclusive_workflow_matched_logs_count_limit() {
  let mut a = state("A");
  let mut b = state("B");
  let c = state("C");
  let mut d = state("D");
  let e = state("E");

  a = a.declare_transition(&b, rule!(log_matches!(message == "foo")));
  b = b.declare_transition(&c, rule!(log_matches!(message == "bar"); times 3));
  a = a.declare_transition(&d, rule!(log_matches!(message == "foo")));
  d = d.declare_transition(&e, rule!(log_matches!(message == "zar")));

  let config: Config =
    workflow!(exclusive with a, b, c, d, e; matches limit!(count 3); duration limit!(seconds 10));
  let mut workflow = AnnotatedWorkflow::new(config);
  assert!(workflow.runs().is_empty());

  // * The first run is created.
  // * The first run transitions from state "A" into states "B" and "D" as two of its outgoing
  //   transitions are matched.
  // * The first run has 2 traversals.
  let result = workflow_process_log!(workflow; "foo");
  assert_eq!(
    result,
    WorkflowResult {
      triggered_actions: vec![],
      logs_to_inject: BTreeMap::new(),
      stats: WorkflowResultStats {
        reset_exclusive_workflows_count: 0,
        created_runs_count: 1,
        advanced_runs_count: 1,
        completed_runs_count: 0,
        stopped_runs_count: 0,
        created_traversals_count: 2,
        advanced_traversals_count: 1,
        completed_traversals_count: 0,
        stopped_traversals_count: 0,
        traversals_count_limit_hit_count: 0,
        matched_logs_count: 2,
      },
    }
  );
  assert_active_run_traversals!(workflow; 0; "B", "D");
  assert!(!workflow.is_in_initial_state());

  // * A new run is created and added to the beginning of runs list so that the workflow has a run
  //   in an initial state.
  // * The first traversal of the first run matches but doesn't advance.
  // * The second traversal of the first run does not match.
  let result = workflow_process_log!(workflow; "bar");
  assert_eq!(
    result,
    WorkflowResult {
      triggered_actions: vec![],
      logs_to_inject: BTreeMap::new(),
      stats: WorkflowResultStats {
        reset_exclusive_workflows_count: 0,
        created_runs_count: 1,
        advanced_runs_count: 0,
        completed_runs_count: 0,
        stopped_runs_count: 0,
        created_traversals_count: 1,
        advanced_traversals_count: 0,
        completed_traversals_count: 0,
        stopped_traversals_count: 0,
        traversals_count_limit_hit_count: 0,
        matched_logs_count: 1,
      },
    }
  );
  assert_active_run_traversals!(workflow; 0; "A");
  assert_active_run_traversals!(workflow; 1; "B", "D");
  assert!(!workflow.is_in_initial_state());

  // * The first traversal of the second run matches.
  // * The match causes the run to exceed the limit of allowed matches.
  // * The second run is removed.
  let result = workflow_process_log!(workflow; "bar");
  assert!(result.triggered_actions.is_empty());
  assert_eq!(
    result,
    WorkflowResult {
      triggered_actions: vec![],
      logs_to_inject: BTreeMap::new(),
      stats: WorkflowResultStats {
        reset_exclusive_workflows_count: 0,
        created_runs_count: 0,
        advanced_runs_count: 0,
        completed_runs_count: 0,
        stopped_runs_count: 1,
        created_traversals_count: 0,
        advanced_traversals_count: 0,
        completed_traversals_count: 0,
        stopped_traversals_count: 2,
        traversals_count_limit_hit_count: 0,
        matched_logs_count: 1,
      },
    }
  );
  assert_active_runs!(workflow; "A");
  assert!(workflow.is_in_initial_state());
}

#[test]
#[allow(clippy::cognitive_complexity)]
fn exclusive_workflow_log_rule_count() {
  let mut a = state("A");
  let mut b = state("B");
  let c = state("C");

  a = a.declare_transition_with_actions(
    &b,
    rule!(log_matches!(message == "foo"); times 2),
    &[action!(flush_buffers &["foo_buffer_id"]; id "foo")],
  );
  b = b.declare_transition_with_actions(
    &c,
    rule!(log_matches!(message == "bar")),
    &[action!(flush_buffers &["bar_buffer_id"]; id "bar")],
  );

  let config = workflow!(exclusive with a, b, c);
  let mut workflow = AnnotatedWorkflow::new(config);
  assert!(workflow.runs().is_empty());

  // * The first run is created.
  // * The first run does not advance.
  // * Log is matched but no run or traversal advances as the log matching rule has `count` set to
  //   2.
  let result = workflow_process_log!(workflow; "foo");
  assert_eq!(
    result,
    WorkflowResult {
      triggered_actions: vec![],
      logs_to_inject: BTreeMap::new(),
      stats: WorkflowResultStats {
        reset_exclusive_workflows_count: 0,
        created_runs_count: 1,
        advanced_runs_count: 0,
        completed_runs_count: 0,
        stopped_runs_count: 0,
        created_traversals_count: 1,
        advanced_traversals_count: 0,
        completed_traversals_count: 0,
        stopped_traversals_count: 0,
        traversals_count_limit_hit_count: 0,
        matched_logs_count: 1,
      },
    }
  );
  assert_active_runs!(workflow; "A");
  assert!(!workflow.is_in_initial_state());

  // A new run in an initial state is created.
  // The first run moves from "A" to "B" state.
  let result = workflow_process_log!(workflow; "foo");
  assert_eq!(
    result,
    WorkflowResult {
      triggered_actions: vec![TriggeredAction::FlushBuffers(&ActionFlushBuffers {
        id: FlushBufferId::WorkflowActionId("foo".to_string()),
        buffer_ids: BTreeSet::from(["foo_buffer_id".to_string()]),
        streaming: None,
      })],
      logs_to_inject: BTreeMap::new(),
      stats: WorkflowResultStats {
        reset_exclusive_workflows_count: 0,
        created_runs_count: 1,
        advanced_runs_count: 1,
        completed_runs_count: 0,
        stopped_runs_count: 0,
        created_traversals_count: 1,
        advanced_traversals_count: 1,
        completed_traversals_count: 0,
        stopped_traversals_count: 0,
        traversals_count_limit_hit_count: 0,
        matched_logs_count: 1,
      },
    }
  );
  assert_active_runs!(workflow; "A", "B");
  assert!(!workflow.is_in_initial_state());

  // None of the runs advance as they do not match the log.
  let result = workflow_process_log!(workflow; "not matching");
  assert!(result.triggered_actions.is_empty());
  assert_eq!(WorkflowResultStats::default(), result.stats);
  assert_active_runs!(workflow; "A", "B");

  // * The run that was created as first moves from "B" to "C" state.
  // * The run that was created as first completes.
  let result = workflow_process_log!(workflow; "bar");
  assert_eq!(
    result,
    WorkflowResult {
      triggered_actions: vec![TriggeredAction::FlushBuffers(&ActionFlushBuffers {
        id: FlushBufferId::WorkflowActionId("bar".to_string()),
        buffer_ids: BTreeSet::from(["bar_buffer_id".to_string()]),
        streaming: None,
      })],
      logs_to_inject: BTreeMap::new(),
      stats: WorkflowResultStats {
        reset_exclusive_workflows_count: 0,
        created_runs_count: 0,
        advanced_runs_count: 1,
        completed_runs_count: 1,
        stopped_runs_count: 0,
        created_traversals_count: 0,
        advanced_traversals_count: 1,
        completed_traversals_count: 1,
        stopped_traversals_count: 0,
        traversals_count_limit_hit_count: 0,
        matched_logs_count: 1,
      },
    }
  );
  assert_active_runs!(workflow; "A");
  assert!(workflow.is_in_initial_state());
}

#[test]
#[allow(clippy::cognitive_complexity)]
#[allow(clippy::many_single_char_names)]
fn branching_exclusive_workflow() {
  let mut a = state("A");
  let mut b = state("B");
  let mut c = state("C");
  let d = state("D");
  let e = state("E");

  a = a.declare_transition_with_actions(
    &b,
    rule!(any!(
      log_matches!(message == "foo"),
      log_matches!(tag("key") == "value"),
    )),
    &[action!(flush_buffers &["foo_buffer_id"]; id "foo")],
  );
  b = b.declare_transition_with_actions(
    &d,
    rule!(log_matches!(message == "zoo")),
    &[action!(flush_buffers &["zoo_buffer_id"]; id "zoo")],
  );
  a = a.declare_transition_with_actions(
    &c,
    rule!(log_matches!(message == "bar")),
    &[action!(flush_buffers &["bar_buffer_id"]; id "bar")],
  );
  c = c.declare_transition_with_actions(
    &e,
    rule!(log_matches!(message == "barbar")),
    &[action!(flush_buffers &["bar_buffer_id"]; id "barbar")],
  );

  let config = workflow!(exclusive with a, b, c, d, e);
  let mut workflow = AnnotatedWorkflow::new(config);
  assert!(workflow.runs().is_empty());

  // The first and only run is moved from "A" to non-final "B" state.
  let result = workflow_process_log!(workflow; "foo");
  assert_eq!(
    result,
    WorkflowResult {
      triggered_actions: vec![TriggeredAction::FlushBuffers(&ActionFlushBuffers {
        id: FlushBufferId::WorkflowActionId("foo".to_string()),
        buffer_ids: BTreeSet::from(["foo_buffer_id".to_string()]),
        streaming: None,
      })],
      logs_to_inject: BTreeMap::new(),
      stats: WorkflowResultStats {
        reset_exclusive_workflows_count: 0,
        created_runs_count: 1,
        advanced_runs_count: 1,
        completed_runs_count: 0,
        stopped_runs_count: 0,
        created_traversals_count: 1,
        advanced_traversals_count: 1,
        completed_traversals_count: 0,
        stopped_traversals_count: 0,
        traversals_count_limit_hit_count: 0,
        matched_logs_count: 1,
      },
    }
  );
  assert_active_runs!(workflow; "B");
  assert!(!workflow.is_in_initial_state());

  // 1. A new run is created and added to the beginning of runs list so that the workflow has a run
  //    in an
  // initial state.
  // 2. None of the run match the log.
  let result = workflow_process_log!(workflow; "fooo");
  assert!(result.triggered_actions.is_empty());
  assert_eq!(
    WorkflowResultStats {
      reset_exclusive_workflows_count: 0,
      created_runs_count: 1,
      advanced_runs_count: 0,
      completed_runs_count: 0,
      stopped_runs_count: 0,
      created_traversals_count: 1,
      advanced_traversals_count: 0,
      completed_traversals_count: 0,
      stopped_traversals_count: 0,
      traversals_count_limit_hit_count: 0,
      matched_logs_count: 0,
    },
    result.stats
  );
  assert_active_runs!(workflow; "A", "B");

  // * The second run is moved from "B" to final "D" state.
  // * The second run completes.
  let result = workflow_process_log!(workflow; "zoo");
  assert_eq!(
    result,
    WorkflowResult {
      triggered_actions: vec![TriggeredAction::FlushBuffers(&ActionFlushBuffers {
        id: FlushBufferId::WorkflowActionId("zoo".to_string()),
        buffer_ids: BTreeSet::from(["zoo_buffer_id".to_string()]),
        streaming: None,
      })],
      logs_to_inject: BTreeMap::new(),
      stats: WorkflowResultStats {
        reset_exclusive_workflows_count: 0,
        created_runs_count: 0,
        advanced_runs_count: 1,
        completed_runs_count: 1,
        stopped_runs_count: 0,
        created_traversals_count: 0,
        advanced_traversals_count: 1,
        completed_traversals_count: 1,
        stopped_traversals_count: 0,
        traversals_count_limit_hit_count: 0,
        matched_logs_count: 1,
      },
    }
  );
  assert_active_runs!(workflow; "A");
  assert!(workflow.is_in_initial_state());

  // * The first and only run ends up with two traversals.
  // * First of the traversals move to state "B", second one to state "C".
  let result = workflow_process_log!(workflow; "bar", with labels! { "key" => "value" });
  assert_eq!(
    result,
    WorkflowResult {
      triggered_actions: vec![
        TriggeredAction::FlushBuffers(&ActionFlushBuffers {
          id: FlushBufferId::WorkflowActionId("foo".to_string()),
          buffer_ids: BTreeSet::from(["foo_buffer_id".to_string()]),
          streaming: None,
        }),
        TriggeredAction::FlushBuffers(&ActionFlushBuffers {
          id: FlushBufferId::WorkflowActionId("bar".to_string()),
          buffer_ids: BTreeSet::from(["bar_buffer_id".to_string()]),
          streaming: None,
        }),
      ],
      logs_to_inject: BTreeMap::new(),
      stats: WorkflowResultStats {
        reset_exclusive_workflows_count: 0,
        created_runs_count: 0,
        advanced_runs_count: 1,
        completed_runs_count: 0,
        stopped_runs_count: 0,
        created_traversals_count: 1,
        advanced_traversals_count: 1,
        completed_traversals_count: 0,
        stopped_traversals_count: 0,
        traversals_count_limit_hit_count: 0,
        matched_logs_count: 2,
      },
    }
  );
  assert_eq!(2, workflow.runs()[0].traversals.len());
  assert_active_run_traversals!(workflow; 0; "B", "C");
  assert!(!workflow.is_in_initial_state());
}
