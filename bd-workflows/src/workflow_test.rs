// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

use crate::config::{
  ActionEmitMetric,
  ActionFlushBuffers,
  Config,
  FlushBufferId,
  ValueIncrement,
  WorkflowDebugMode,
};
use crate::test::{MakeConfig, TestLog};
use crate::workflow::{Run, TriggeredAction, Workflow, WorkflowResult, WorkflowResultStats};
use bd_log_primitives::{FieldsRef, LogFields, LogMessage, log_level};
use bd_proto::flatbuffers::buffer_log::bitdrift_public::fbs::logging::v_1::LogType;
use bd_stats_common::workflow::{WorkflowDebugStateKey, WorkflowDebugTransitionType};
use bd_stats_common::{MetricType, labels};
use bd_test_helpers::workflow::macros::{all, any, log_matches, rule};
use bd_test_helpers::workflow::{
  WorkflowBuilder,
  make_emit_counter_action,
  make_flush_buffers_action,
  metric_value,
  state,
};
use pretty_assertions::assert_eq;
use std::collections::{BTreeMap, BTreeSet};
use std::vec;
use time::ext::NumericalDuration;
use time::macros::datetime;

#[ctor::ctor]
fn test_global_init() {
  bd_test_helpers::test_global_init();
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
      workflow: Workflow::new(config.inner().id().to_string(), true),
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

  fn process_log(&mut self, log: TestLog) -> WorkflowResult<'_> {
    self.workflow.process_log(
      &self.config,
      &bd_log_primitives::LogRef {
        log_type: LogType::Normal,
        log_level: log_level::DEBUG,
        message: &LogMessage::String(log.message),
        session_id: log.session.as_ref().map_or("foo", |s| &**s),
        occurred_at: log.occurred_at,
        fields: FieldsRef::new(
          &bd_test_helpers::workflow::make_tags(log.tags),
          &LogFields::new(),
        ),
        capture_session: None,
      },
      log.now,
    )
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
        let expected_state_index = $workflow_annotated.config.inner().states().iter()
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
        let expected_state_index = $annotated_workflow.config.inner().states().iter()
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

  let config = WorkflowBuilder::new("1", &[&a]).build();
  assert_eq!(
    Config::new(config, WorkflowDebugMode::None)
      .err()
      .unwrap()
      .to_string(),
    "invalid workflow configuration: initial state must have at least one transition"
  );
}

#[test]
fn unknown_state_reference_workflow() {
  let mut a = state("A");
  let b = state("B");

  a = a.declare_transition_with_actions(
    &b,
    rule!(log_matches!(message == "foo")),
    &[make_flush_buffers_action(&["foo_buffer_id"], None, "foo")],
  );

  let config = WorkflowBuilder::new("1", &[&a]).build();
  assert_eq!(
    Config::new(config, WorkflowDebugMode::None)
      .err()
      .unwrap()
      .to_string(),
    "invalid workflow state configuration: reference to an unexisting state"
  );
}

#[test]
fn timeout_no_parallel_match() {
  let mut a = state("A");
  let mut b = state("B");
  let c = state("C");

  a = a.declare_transition_with_actions(
    &b,
    rule!(log_matches!(message == "foo")),
    &[make_emit_counter_action(
      "foo_metric",
      metric_value(1),
      vec![],
    )],
  );
  b = b.with_timeout(
    &c,
    1.seconds(),
    &[make_emit_counter_action(
      "timeout_metric",
      metric_value(1),
      vec![],
    )],
  );

  let config = WorkflowBuilder::new("1", &[&a, &b, &c]).make_config();
  let mut workflow = AnnotatedWorkflow::new(config);

  // Advance to the timeout.
  let result = workflow.process_log(TestLog::new("foo").with_now(datetime!(2023-01-01 00:00 UTC)));
  assert_eq!(
    result,
    WorkflowResult {
      triggered_actions: vec![TriggeredAction::EmitMetric(&ActionEmitMetric {
        id: "foo_metric".to_string(),
        tags: BTreeMap::new(),
        increment: ValueIncrement::Fixed(1),
        metric_type: MetricType::Counter,
      })],
      logs_to_inject: BTreeMap::new(),
      stats: WorkflowResultStats {
        matched_logs_count: 1,
        processed_timeout: true,
      },
      cumulative_workflow_debug_state: None,
      incremental_workflow_debug_state: vec![
        WorkflowDebugStateKey::new_state_transition(
          "A".to_string(),
          WorkflowDebugTransitionType::Normal(0)
        ),
        WorkflowDebugStateKey::StartOrReset
      ],
    }
  );
  assert_active_runs!(workflow; "B");

  // Some other random log should timeout and leave the initial state run.
  let result =
    workflow.process_log(TestLog::new("bar").with_now(datetime!(2023-01-01 00:00:01 UTC)));
  assert_eq!(
    result,
    WorkflowResult {
      triggered_actions: vec![TriggeredAction::EmitMetric(&ActionEmitMetric {
        id: "timeout_metric".to_string(),
        tags: BTreeMap::new(),
        increment: ValueIncrement::Fixed(1),
        metric_type: MetricType::Counter,
      })],
      logs_to_inject: BTreeMap::new(),
      stats: WorkflowResultStats {
        matched_logs_count: 0,
        processed_timeout: true,
      },
      cumulative_workflow_debug_state: None,
      incremental_workflow_debug_state: vec![WorkflowDebugStateKey::new_state_transition(
        "B".to_string(),
        WorkflowDebugTransitionType::Timeout
      )],
    }
  );
  assert_active_runs!(workflow; "A");

  // Start the timeout again.
  let result =
    workflow.process_log(TestLog::new("foo").with_now(datetime!(2023-01-01 00:00:02 UTC)));
  assert_eq!(
    result,
    WorkflowResult {
      triggered_actions: vec![TriggeredAction::EmitMetric(&ActionEmitMetric {
        id: "foo_metric".to_string(),
        tags: BTreeMap::new(),
        increment: ValueIncrement::Fixed(1),
        metric_type: MetricType::Counter,
      })],
      logs_to_inject: BTreeMap::new(),
      stats: WorkflowResultStats {
        matched_logs_count: 1,
        processed_timeout: true,
      },
      cumulative_workflow_debug_state: None,
      incremental_workflow_debug_state: vec![WorkflowDebugStateKey::new_state_transition(
        "A".to_string(),
        WorkflowDebugTransitionType::Normal(0)
      )],
    }
  );
  assert_active_runs!(workflow; "B");

  // Reset the workflow which will advance the initial state run and remove the other run.
  let result =
    workflow.process_log(TestLog::new("foo").with_now(datetime!(2023-01-01 00:00:02 UTC)));
  assert_eq!(
    result,
    WorkflowResult {
      triggered_actions: vec![TriggeredAction::EmitMetric(&ActionEmitMetric {
        id: "foo_metric".to_string(),
        tags: BTreeMap::new(),
        increment: ValueIncrement::Fixed(1),
        metric_type: MetricType::Counter,
      })],
      logs_to_inject: BTreeMap::new(),
      stats: WorkflowResultStats {
        matched_logs_count: 1,
        processed_timeout: true,
      },
      cumulative_workflow_debug_state: None,
      incremental_workflow_debug_state: vec![WorkflowDebugStateKey::new_state_transition(
        "A".to_string(),
        WorkflowDebugTransitionType::Normal(0)
      )],
    }
  );
  assert_active_runs!(workflow; "B");
}

#[test]
fn timeout_not_start() {
  let mut a = state("A");
  let mut b = state("B");
  let c = state("C");
  let d = state("D");

  a = a.declare_transition(&b, rule!(log_matches!(message == "foo")));
  b = b
    .declare_transition_with_actions(
      &c,
      rule!(log_matches!(message == "bar")),
      &[make_flush_buffers_action(&["bar_buffer_id"], None, "bar")],
    )
    .with_timeout(
      &d,
      1.seconds(),
      &[make_emit_counter_action(
        "bar_metric",
        metric_value(1),
        vec![],
      )],
    );

  let config = WorkflowBuilder::new("1", &[&a, &b, &c, &d]).make_config();
  let mut workflow = AnnotatedWorkflow::new(config);

  // Does not match, no transition and no timeout.
  workflow.process_log(TestLog::new("something else"));
  assert_active_runs!(workflow; "A");

  // Transitions to "B", starts the timeout.
  workflow.process_log(TestLog::new("foo").with_now(datetime!(2023-01-01 00:00 UTC)));
  assert_active_runs!(workflow; "B");

  // Does not match, does not progress timeout and does not reset.
  workflow
    .process_log(TestLog::new("something else").with_now(datetime!(2023-01-01 00:00:00.1 UTC)));
  assert_active_runs!(workflow; "A", "B");

  // Now hit the timeout, leaving the initial state run.
  let result = workflow
    .process_log(TestLog::new("something else").with_now(datetime!(2023-01-01 00:00:01 UTC)));
  assert_eq!(
    result,
    WorkflowResult {
      triggered_actions: vec![TriggeredAction::EmitMetric(&ActionEmitMetric {
        id: "bar_metric".to_string(),
        tags: BTreeMap::new(),
        increment: ValueIncrement::Fixed(1),
        metric_type: MetricType::Counter,
      })],
      logs_to_inject: BTreeMap::new(),
      stats: WorkflowResultStats {
        matched_logs_count: 0,
        processed_timeout: true,
      },
      cumulative_workflow_debug_state: None,
      incremental_workflow_debug_state: vec![WorkflowDebugStateKey::new_state_transition(
        "B".to_string(),
        WorkflowDebugTransitionType::Timeout
      )],
    }
  );
  assert_active_runs!(workflow; "A");

  // Progress through to make sure not timing out works as expected.
  workflow.process_log(TestLog::new("foo").with_now(datetime!(2023-01-01 00:00:02 UTC)));
  assert_active_runs!(workflow; "B");
  let result =
    workflow.process_log(TestLog::new("bar").with_now(datetime!(2023-01-01 00:00:02.1 UTC)));
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
        matched_logs_count: 1,
        processed_timeout: false,
      },
      cumulative_workflow_debug_state: None,
      incremental_workflow_debug_state: vec![WorkflowDebugStateKey::new_state_transition(
        "B".to_string(),
        WorkflowDebugTransitionType::Normal(0)
      )],
    }
  );
  assert_active_runs!(workflow; "A");
}

#[test]
fn initial_state_run_does_not_have_timeout() {
  let mut a = state("A");
  let mut b = state("B");
  let c = state("C");
  let d = state("D");

  a = a
    .declare_transition(&b, rule!(log_matches!(message == "foo")))
    .with_timeout(
      &c,
      1.seconds(),
      &[make_emit_counter_action(
        "foo_metric",
        metric_value(1),
        vec![],
      )],
    );
  b = b.declare_transition_with_actions(
    &d,
    rule!(log_matches!(message == "bar")),
    &[make_emit_counter_action(
      "bar_metric",
      metric_value(1),
      vec![],
    )],
  );

  let config = WorkflowBuilder::new("1", &[&a, &b, &c, &d]).make_config();
  let mut workflow = AnnotatedWorkflow::new(config);

  // Progress which will move past the initial state timeout.
  workflow.process_log(TestLog::new("foo").with_now(datetime!(2023-01-01 00:00:02 UTC)));
  assert_active_runs!(workflow; "B");

  // This log does not match. It will create an initial state run, however we do not expect this
  // run to have a timeout as there is an active run already.
  workflow
    .process_log(TestLog::new("something else").with_now(datetime!(2023-01-01 00:00:02.1 UTC)));
  assert_active_runs!(workflow; "A", "B");
  assert!(workflow.runs()[0].traversals()[0].timeout_unix_ms.is_none());

  // This log will complete the active run. At this point we should initialize the timeout on
  // the initial state run.
  workflow.process_log(TestLog::new("bar").with_now(datetime!(2023-01-01 00:00:02.2 UTC)));
  assert_active_runs!(workflow; "A");
  assert!(workflow.runs()[0].traversals()[0].timeout_unix_ms.is_some());
}

#[test]
fn timeout_from_start() {
  let mut a = state("A");
  let b = state("B");
  let c = state("C");

  a = a
    .declare_transition_with_actions(
      &b,
      rule!(log_matches!(message == "foo")),
      &[make_flush_buffers_action(&["foo_buffer_id"], None, "foo")],
    )
    .with_timeout(
      &c,
      1.seconds(),
      &[make_emit_counter_action(
        "foo_metric",
        metric_value(1),
        vec![],
      )],
    );

  let config = WorkflowBuilder::new("1", &[&a, &b, &c]).make_config();
  let mut workflow = AnnotatedWorkflow::new(config);

  // The first log will create the run and set the timeout.
  workflow.process_log(TestLog::new("something else").with_now(datetime!(2023-01-01 00:00 UTC)));
  assert_active_runs!(workflow; "A");

  // Some other log will not match, but not be long enough in the future to trigger the timeout.
  // This will make another initial state run without a timeout.
  workflow.process_log(TestLog::new("blah").with_now(datetime!(2023-01-01 00:00:00.5 UTC)));
  assert_active_runs!(workflow; "A", "A");
  assert!(workflow.runs()[0].traversals()[0].timeout_unix_ms.is_none());
  assert!(workflow.runs()[1].traversals()[0].timeout_unix_ms.is_some());

  // This will not match but will be far enough in the future to trigger the timeout. This will
  // complete the workflow.
  let result =
    workflow.process_log(TestLog::new("blah2").with_now(datetime!(2023-01-01 00:00:01.5 UTC)));
  assert_eq!(
    result,
    WorkflowResult {
      triggered_actions: vec![TriggeredAction::EmitMetric(&ActionEmitMetric {
        id: "foo_metric".to_string(),
        tags: BTreeMap::new(),
        increment: ValueIncrement::Fixed(1),
        metric_type: MetricType::Counter,
      })],
      logs_to_inject: BTreeMap::new(),
      stats: WorkflowResultStats {
        matched_logs_count: 0,
        processed_timeout: true,
      },
      cumulative_workflow_debug_state: None,
      incremental_workflow_debug_state: vec![WorkflowDebugStateKey::new_state_transition(
        "A".to_string(),
        WorkflowDebugTransitionType::Timeout
      )],
    }
  );
  assert_active_runs!(workflow; "A");
  assert!(workflow.runs()[0].traversals()[0].timeout_unix_ms.is_some());
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
    &[make_flush_buffers_action(&["foo_buffer_id"], None, "foo")],
  );

  let config = WorkflowBuilder::new("1", &[&a, &b, &c, &d, &e]).make_config();
  let mut workflow = AnnotatedWorkflow::new(config);
  assert!(workflow.runs().is_empty());

  // The first log causes a fork since it matches the first two transitions.
  let result = workflow.process_log(TestLog::new("foo"));
  assert_eq!(
    result,
    WorkflowResult {
      triggered_actions: vec![],
      logs_to_inject: BTreeMap::new(),
      stats: WorkflowResultStats {
        matched_logs_count: 2,
        processed_timeout: false,
      },
      cumulative_workflow_debug_state: None,
      incremental_workflow_debug_state: vec![
        WorkflowDebugStateKey::new_state_transition(
          "A".to_string(),
          WorkflowDebugTransitionType::Normal(0)
        ),
        WorkflowDebugStateKey::new_state_transition(
          "A".to_string(),
          WorkflowDebugTransitionType::Normal(1)
        ),
        WorkflowDebugStateKey::StartOrReset
      ],
    }
  );

  // Seeing the entry condition again resets all the traversals.
  let result = workflow.process_log(TestLog::new("foo"));
  assert_eq!(
    result,
    WorkflowResult {
      triggered_actions: vec![],
      logs_to_inject: BTreeMap::new(),
      stats: WorkflowResultStats {
        matched_logs_count: 2,
        processed_timeout: false,
      },
      cumulative_workflow_debug_state: None,
      incremental_workflow_debug_state: vec![
        WorkflowDebugStateKey::new_state_transition(
          "A".to_string(),
          WorkflowDebugTransitionType::Normal(0)
        ),
        WorkflowDebugStateKey::new_state_transition(
          "A".to_string(),
          WorkflowDebugTransitionType::Normal(1)
        )
      ],
    }
  );

  // Finalize one of the forks. Only one of the traversals/forks is completed.
  let result = workflow.process_log(TestLog::new("E"));
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
        matched_logs_count: 1,
        processed_timeout: false,
      },
      cumulative_workflow_debug_state: None,
      incremental_workflow_debug_state: vec![WorkflowDebugStateKey::new_state_transition(
        "C".to_string(),
        WorkflowDebugTransitionType::Normal(0)
      )],
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
    &[make_flush_buffers_action(&["foo_buffer_id"], None, "foo")],
  );

  let config = WorkflowBuilder::new("1", &[&a, &b, &c, &d, &e]).make_config();
  let mut workflow = AnnotatedWorkflow::new(config);
  assert!(workflow.runs().is_empty());

  // The first log progresses the workflow towards the "B" state.
  let result = workflow.process_log(TestLog::new("foo"));
  assert_eq!(
    result,
    WorkflowResult {
      triggered_actions: vec![],
      logs_to_inject: BTreeMap::new(),
      stats: WorkflowResultStats {
        matched_logs_count: 1,
        processed_timeout: false,
      },
      cumulative_workflow_debug_state: None,
      incremental_workflow_debug_state: vec![
        WorkflowDebugStateKey::new_state_transition(
          "A".to_string(),
          WorkflowDebugTransitionType::Normal(0)
        ),
        WorkflowDebugStateKey::StartOrReset
      ],
    }
  );

  // Seeing the initial log resets the workflow, even if the initial condition is not the same as
  // the one that initiated the workflow run.
  let result = workflow.process_log(TestLog::new("bar"));
  assert_eq!(
    result,
    WorkflowResult {
      triggered_actions: vec![],
      logs_to_inject: BTreeMap::new(),
      stats: WorkflowResultStats {
        matched_logs_count: 1,
        processed_timeout: false,
      },
      cumulative_workflow_debug_state: None,
      incremental_workflow_debug_state: vec![WorkflowDebugStateKey::new_state_transition(
        "A".to_string(),
        WorkflowDebugTransitionType::Normal(1)
      )],
    }
  );

  // Finalize the workflow.
  let result = workflow.process_log(TestLog::new("E"));
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
        matched_logs_count: 1,
        processed_timeout: false,
      },
      cumulative_workflow_debug_state: None,
      incremental_workflow_debug_state: vec![WorkflowDebugStateKey::new_state_transition(
        "C".to_string(),
        WorkflowDebugTransitionType::Normal(0)
      )],
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
      make_flush_buffers_action(&["foo_buffer_id"], None, "foo"),
      make_emit_counter_action("foo_metric", metric_value(123), vec![]),
    ],
  );
  b = b.declare_transition_with_actions(
    &c,
    rule!(log_matches!(message == "bar")),
    &[make_flush_buffers_action(&["bar_buffer_id"], None, "bar")],
  );

  let config = WorkflowBuilder::new("1", &[&a, &b, &c]).make_config();
  let mut workflow = AnnotatedWorkflow::new(config);
  assert!(workflow.runs().is_empty());

  // * A new run is created to ensure that workflow has a run in initial state.
  // * The first run moves from "A" to non-final "B" state.
  let result = workflow.process_log(TestLog::new("foo").with_tags(labels! { "key" => "value" }));
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
        matched_logs_count: 1,
        processed_timeout: false,
      },
      cumulative_workflow_debug_state: None,
      incremental_workflow_debug_state: vec![
        WorkflowDebugStateKey::new_state_transition(
          "A".to_string(),
          WorkflowDebugTransitionType::Normal(0)
        ),
        WorkflowDebugStateKey::StartOrReset
      ],
    }
  );
  assert_active_runs!(workflow; "B");
  assert!(!workflow.is_in_initial_state());

  // * The first run moves from "B" to final "C" state.
  // * The first run completes.
  let result = workflow.process_log(TestLog::new("bar"));
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
        matched_logs_count: 1,
        processed_timeout: false,
      },
      cumulative_workflow_debug_state: None,
      incremental_workflow_debug_state: vec![WorkflowDebugStateKey::new_state_transition(
        "B".to_string(),
        WorkflowDebugTransitionType::Normal(0)
      )],
    }
  );
  assert_active_runs!(workflow; "A");
  assert!(workflow.is_in_initial_state());

  // A new run is created to ensure that a workflow has a run in an initial state.
  let result = workflow.process_log(TestLog::new("not matching"));
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

  let config: Config = WorkflowBuilder::new("1", &[&a, &b, &c, &d, &e])
    .with_log_limit(3)
    .with_duration_limit(10.seconds())
    .make_config();
  let mut workflow = AnnotatedWorkflow::new(config);
  assert!(workflow.runs().is_empty());

  // * The first run is created.
  // * The first run transitions from state "A" into states "B" and "D" as two of its outgoing
  //   transitions are matched.
  // * The first run has 2 traversals.
  let result = workflow.process_log(TestLog::new("foo"));
  assert_eq!(
    result,
    WorkflowResult {
      triggered_actions: vec![],
      logs_to_inject: BTreeMap::new(),
      stats: WorkflowResultStats {
        matched_logs_count: 2,
        processed_timeout: false,
      },
      cumulative_workflow_debug_state: None,
      incremental_workflow_debug_state: vec![
        WorkflowDebugStateKey::new_state_transition(
          "A".to_string(),
          WorkflowDebugTransitionType::Normal(0)
        ),
        WorkflowDebugStateKey::new_state_transition(
          "A".to_string(),
          WorkflowDebugTransitionType::Normal(1)
        ),
        WorkflowDebugStateKey::StartOrReset
      ],
    }
  );
  assert_active_run_traversals!(workflow; 0; "B", "D");
  assert!(!workflow.is_in_initial_state());

  // * A new run is created and added to the beginning of runs list so that the workflow has a run
  //   in an initial state.
  // * The first traversal of the first run matches but doesn't advance.
  // * The second traversal of the first run does not match.
  let result = workflow.process_log(TestLog::new("bar"));
  assert_eq!(
    result,
    WorkflowResult {
      triggered_actions: vec![],
      logs_to_inject: BTreeMap::new(),
      stats: WorkflowResultStats {
        matched_logs_count: 1,
        processed_timeout: false,
      },
      cumulative_workflow_debug_state: None,
      incremental_workflow_debug_state: vec![],
    }
  );
  assert_active_run_traversals!(workflow; 0; "A");
  assert_active_run_traversals!(workflow; 1; "B", "D");
  assert!(!workflow.is_in_initial_state());

  // * The first traversal of the second run matches.
  // * The match causes the run to exceed the limit of allowed matches.
  // * The second run is removed.
  let result = workflow.process_log(TestLog::new("bar"));
  assert!(result.triggered_actions.is_empty());
  assert_eq!(
    result,
    WorkflowResult {
      triggered_actions: vec![],
      logs_to_inject: BTreeMap::new(),
      stats: WorkflowResultStats {
        matched_logs_count: 1,
        processed_timeout: false,
      },
      cumulative_workflow_debug_state: None,
      incremental_workflow_debug_state: vec![],
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
    &[make_flush_buffers_action(&["foo_buffer_id"], None, "foo")],
  );
  b = b.declare_transition_with_actions(
    &c,
    rule!(log_matches!(message == "bar")),
    &[make_flush_buffers_action(&["bar_buffer_id"], None, "bar")],
  );

  let config = WorkflowBuilder::new("1", &[&a, &b, &c]).make_config();
  let mut workflow = AnnotatedWorkflow::new(config);
  assert!(workflow.runs().is_empty());

  // * The first run is created.
  // * The first run does not advance.
  // * Log is matched but no run or traversal advances as the log matching rule has `count` set to
  //   2.
  let result = workflow.process_log(TestLog::new("foo"));
  assert_eq!(
    result,
    WorkflowResult {
      triggered_actions: vec![],
      logs_to_inject: BTreeMap::new(),
      stats: WorkflowResultStats {
        matched_logs_count: 1,
        processed_timeout: false,
      },
      cumulative_workflow_debug_state: None,
      incremental_workflow_debug_state: vec![WorkflowDebugStateKey::StartOrReset],
    }
  );
  assert_active_runs!(workflow; "A");
  assert!(!workflow.is_in_initial_state());

  // A new run in an initial state is created.
  // The first run moves from "A" to "B" state.
  let result = workflow.process_log(TestLog::new("foo"));
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
        matched_logs_count: 1,
        processed_timeout: false,
      },
      cumulative_workflow_debug_state: None,
      incremental_workflow_debug_state: vec![WorkflowDebugStateKey::new_state_transition(
        "A".to_string(),
        WorkflowDebugTransitionType::Normal(0)
      )],
    }
  );
  assert_active_runs!(workflow; "A", "B");
  assert!(!workflow.is_in_initial_state());

  // None of the runs advance as they do not match the log.
  let result = workflow.process_log(TestLog::new("not matching"));
  assert!(result.triggered_actions.is_empty());
  assert_eq!(WorkflowResultStats::default(), result.stats);
  assert_active_runs!(workflow; "A", "B");

  // * The run that was created as first moves from "B" to "C" state.
  // * The run that was created as first completes.
  let result = workflow.process_log(TestLog::new("bar"));
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
        matched_logs_count: 1,
        processed_timeout: false,
      },
      cumulative_workflow_debug_state: None,
      incremental_workflow_debug_state: vec![WorkflowDebugStateKey::new_state_transition(
        "B".to_string(),
        WorkflowDebugTransitionType::Normal(0)
      )],
    }
  );
  assert_active_runs!(workflow; "A");
  assert!(workflow.is_in_initial_state());
}

#[test]
fn debug_with_fork() {
  let mut a = state("A");
  let mut b = state("B");
  let mut c = state("C");
  let d = state("D");

  a = a
    .declare_transition(&b, rule!(log_matches!(message == "foo")))
    .declare_transition(&c, rule!(log_matches!(message == "foo")));
  let cloned_b = b.clone();
  b = b.declare_transition_with_actions(
    &cloned_b,
    rule!(log_matches!(message == "bar")),
    &[make_emit_counter_action(
      "bar_metric",
      metric_value(1),
      vec![],
    )],
  );
  c = c.declare_transition_with_actions(
    &d,
    rule!(log_matches!(message == "baz")),
    &[make_emit_counter_action(
      "baz_metric",
      metric_value(1),
      vec![],
    )],
  );

  let config = WorkflowBuilder::new("1", &[&a, &b, &c, &d]).make_config();
  let mut workflow = AnnotatedWorkflow::new(config);
  assert!(workflow.runs().is_empty());

  let result = workflow.process_log(TestLog::new("foo"));
  assert_eq!(
    result,
    WorkflowResult {
      triggered_actions: vec![],
      logs_to_inject: BTreeMap::new(),
      stats: WorkflowResultStats {
        matched_logs_count: 2,
        processed_timeout: false,
      },
      cumulative_workflow_debug_state: None,
      incremental_workflow_debug_state: vec![
        WorkflowDebugStateKey::new_state_transition(
          "A".to_string(),
          WorkflowDebugTransitionType::Normal(0)
        ),
        WorkflowDebugStateKey::new_state_transition(
          "A".to_string(),
          WorkflowDebugTransitionType::Normal(1)
        ),
        WorkflowDebugStateKey::StartOrReset
      ],
    }
  );
  assert_active_run_traversals!(workflow; 0; "B", "C");
  assert!(!workflow.is_in_initial_state());

  let result = workflow.process_log(TestLog::new("bar"));
  assert_eq!(
    result,
    WorkflowResult {
      triggered_actions: vec![TriggeredAction::EmitMetric(&ActionEmitMetric {
        id: "bar_metric".to_string(),
        tags: BTreeMap::new(),
        increment: ValueIncrement::Fixed(1),
        metric_type: MetricType::Counter,
      })],
      logs_to_inject: BTreeMap::new(),
      stats: WorkflowResultStats {
        matched_logs_count: 1,
        processed_timeout: false,
      },
      cumulative_workflow_debug_state: None,
      incremental_workflow_debug_state: vec![WorkflowDebugStateKey::new_state_transition(
        "B".to_string(),
        WorkflowDebugTransitionType::Normal(0)
      ),],
    }
  );
  assert_active_run_traversals!(workflow; 1; "B", "C");
  assert!(!workflow.is_in_initial_state());

  let result = workflow.process_log(TestLog::new("baz"));
  assert_eq!(
    result,
    WorkflowResult {
      triggered_actions: vec![TriggeredAction::EmitMetric(&ActionEmitMetric {
        id: "baz_metric".to_string(),
        tags: BTreeMap::new(),
        increment: ValueIncrement::Fixed(1),
        metric_type: MetricType::Counter,
      })],
      logs_to_inject: BTreeMap::new(),
      stats: WorkflowResultStats {
        matched_logs_count: 1,
        processed_timeout: false,
      },
      cumulative_workflow_debug_state: None,
      incremental_workflow_debug_state: vec![WorkflowDebugStateKey::new_state_transition(
        "C".to_string(),
        WorkflowDebugTransitionType::Normal(0)
      ),],
    }
  );
  assert_active_run_traversals!(workflow; 1; "B");
  assert!(!workflow.is_in_initial_state());

  let result = workflow.process_log(TestLog::new("bar"));
  assert_eq!(
    result,
    WorkflowResult {
      triggered_actions: vec![TriggeredAction::EmitMetric(&ActionEmitMetric {
        id: "bar_metric".to_string(),
        tags: BTreeMap::new(),
        increment: ValueIncrement::Fixed(1),
        metric_type: MetricType::Counter,
      })],
      logs_to_inject: BTreeMap::new(),
      stats: WorkflowResultStats {
        matched_logs_count: 1,
        processed_timeout: false,
      },
      cumulative_workflow_debug_state: None,
      incremental_workflow_debug_state: vec![WorkflowDebugStateKey::new_state_transition(
        "B".to_string(),
        WorkflowDebugTransitionType::Normal(0)
      ),],
    }
  );
  assert_active_run_traversals!(workflow; 1; "B");
  assert!(!workflow.is_in_initial_state());
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
    &[make_flush_buffers_action(&["foo_buffer_id"], None, "foo")],
  );
  b = b.declare_transition_with_actions(
    &d,
    rule!(log_matches!(message == "zoo")),
    &[make_flush_buffers_action(&["zoo_buffer_id"], None, "zoo")],
  );
  a = a.declare_transition_with_actions(
    &c,
    rule!(log_matches!(message == "bar")),
    &[make_flush_buffers_action(&["bar_buffer_id"], None, "bar")],
  );
  c = c.declare_transition_with_actions(
    &e,
    rule!(log_matches!(message == "barbar")),
    &[make_flush_buffers_action(
      &["bar_buffer_id"],
      None,
      "barbar",
    )],
  );

  let config = WorkflowBuilder::new("1", &[&a, &b, &c, &d, &e]).make_config();
  let mut workflow = AnnotatedWorkflow::new(config);
  assert!(workflow.runs().is_empty());

  // The first and only run is moved from "A" to non-final "B" state.
  let result = workflow.process_log(TestLog::new("foo"));
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
        matched_logs_count: 1,
        processed_timeout: false,
      },
      cumulative_workflow_debug_state: None,
      incremental_workflow_debug_state: vec![
        WorkflowDebugStateKey::new_state_transition(
          "A".to_string(),
          WorkflowDebugTransitionType::Normal(0)
        ),
        WorkflowDebugStateKey::StartOrReset
      ],
    }
  );
  assert_active_runs!(workflow; "B");
  assert!(!workflow.is_in_initial_state());

  // 1. A new run is created and added to the beginning of runs list so that the workflow has a run
  //    in an
  // initial state.
  // 2. None of the run match the log.
  let result = workflow.process_log(TestLog::new("fooo"));
  assert!(result.triggered_actions.is_empty());
  assert_eq!(
    WorkflowResultStats {
      matched_logs_count: 0,
      processed_timeout: false,
    },
    result.stats
  );
  assert_active_runs!(workflow; "A", "B");

  // * The second run is moved from "B" to final "D" state.
  // * The second run completes.
  let result = workflow.process_log(TestLog::new("zoo"));
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
        matched_logs_count: 1,
        processed_timeout: false,
      },
      cumulative_workflow_debug_state: None,
      incremental_workflow_debug_state: vec![WorkflowDebugStateKey::new_state_transition(
        "B".to_string(),
        WorkflowDebugTransitionType::Normal(0)
      )],
    }
  );
  assert_active_runs!(workflow; "A");
  assert!(workflow.is_in_initial_state());

  // * The first and only run ends up with two traversals.
  // * First of the traversals move to state "B", second one to state "C".
  let result = workflow.process_log(TestLog::new("bar").with_tags(labels! { "key" => "value" }));
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
        matched_logs_count: 2,
        processed_timeout: false,
      },
      cumulative_workflow_debug_state: None,
      incremental_workflow_debug_state: vec![
        WorkflowDebugStateKey::new_state_transition(
          "A".to_string(),
          WorkflowDebugTransitionType::Normal(0)
        ),
        WorkflowDebugStateKey::new_state_transition(
          "A".to_string(),
          WorkflowDebugTransitionType::Normal(1)
        )
      ],
    }
  );
  assert_eq!(2, workflow.runs()[0].traversals.len());
  assert_active_run_traversals!(workflow; 0; "B", "C");
  assert!(!workflow.is_in_initial_state());
}
