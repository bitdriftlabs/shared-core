// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

#![allow(clippy::unwrap_used)]

use super::engine_test_helpers::{
  Setup,
  make_is_set_match,
  make_state_change_rule,
  make_string_match,
};
use crate::engine::WorkflowsEngineConfig;
use crate::engine_assert_active_runs;
use crate::test::{MakeConfig, TestLog};
use bd_client_stats_store::test::StatsHelper;
use bd_log_matcher::builder::message_equals;
use bd_proto::protos::logging::payload::LogType;
use bd_stats_common::labels;
use bd_test_helpers::workflow::macros::rule;
use bd_test_helpers::workflow::{
  TestFieldRef,
  TestFieldType,
  WorkflowBuilder,
  make_emit_counter_action,
  make_emit_histogram_action,
  make_generate_log_action_proto,
  make_save_timestamp_extraction,
  metric_tag,
  metric_value,
  state,
};
use time::OffsetDateTime;
use time::ext::NumericalDuration;
use time::macros::datetime;

// Tests that state changes can trigger workflow transitions using string value matching.
#[tokio::test]
async fn state_change_string_match_triggers_transition() {
  let b = state("B");

  // State A needs a self-loop to create an initial run, then can transition to B on state change
  let a = state("A")
    .declare_transition(&state("A"), rule!(message_equals("start")))
    .declare_transition_with_actions(
      &b,
      make_state_change_rule(
        bd_state::Scope::FeatureFlag,
        "feature_flag",
        make_string_match("enabled"),
      ),
      &[make_emit_counter_action(
        "string_match_metric",
        metric_value(1),
        vec![],
      )],
    );

  let workflow = WorkflowBuilder::new("state_change_workflow", &[&a, &b]).make_config();
  let setup = Setup::new();

  let mut engine = setup
    .make_workflows_engine(WorkflowsEngineConfig::new_with_workflow_configurations(
      vec![workflow],
    ))
    .await;

  // Create initial run in state A
  engine.process_log(TestLog::new("start"));
  engine_assert_active_runs!(engine; 0; "A");

  // Process a state change that matches
  let state_change = bd_state::StateChange {
    scope: bd_state::Scope::FeatureFlag,
    key: "feature_flag".to_string(),
    change_type: bd_state::StateChangeType::Inserted {
      value: "enabled".to_string(),
    },
    timestamp: OffsetDateTime::now_utc(),
  };

  engine.process_state_change(&state_change);

  // Verify transition occurred by checking the metric was emitted
  setup
    .collector
    .assert_workflow_counter_eq(1, "string_match_metric", labels! {});
}

// Tests that IsSet matcher works for state changes (matching any value).
#[tokio::test]
async fn state_change_is_set_match() {
  let b = state("B");
  let a = state("A")
    .declare_transition(&state("A"), rule!(message_equals("start")))
    .declare_transition_with_actions(
      &b,
      make_state_change_rule(
        bd_state::Scope::FeatureFlag,
        "any_value",
        make_is_set_match(),
      ),
      &[make_emit_counter_action(
        "is_set_match_metric",
        metric_value(1),
        vec![],
      )],
    );

  let workflow = WorkflowBuilder::new("is_set_test", &[&a, &b]).make_config();
  let setup = Setup::new();

  let mut engine = setup
    .make_workflows_engine(WorkflowsEngineConfig::new_with_workflow_configurations(
      vec![workflow],
    ))
    .await;

  // Create initial run
  engine.process_log(TestLog::new("start"));
  engine_assert_active_runs!(engine; 0; "A");

  // Test with string value
  let state_change = bd_state::StateChange {
    scope: bd_state::Scope::FeatureFlag,
    key: "any_value".to_string(),
    change_type: bd_state::StateChangeType::Inserted {
      value: "hello".to_string(),
    },
    timestamp: OffsetDateTime::now_utc(),
  };

  engine.process_state_change(&state_change);

  // Verify transition occurred by checking the metric was emitted
  setup
    .collector
    .assert_workflow_counter_eq(1, "is_set_match_metric", labels! {});
}

// Tests that state changes that don't match predicates don't trigger transitions.
#[tokio::test]
async fn state_change_no_match_no_transition() {
  let b = state("B");
  let a = state("A")
    .declare_transition(&state("A"), rule!(message_equals("start")))
    .declare_transition_with_actions(
      &b,
      make_state_change_rule(
        bd_state::Scope::FeatureFlag,
        "flag",
        make_string_match("enabled"),
      ),
      &[make_emit_counter_action(
        "no_match_metric",
        metric_value(1),
        vec![],
      )],
    );

  let workflow = WorkflowBuilder::new("no_match_test", &[&a, &b]).make_config();
  let setup = Setup::new();

  let mut engine = setup
    .make_workflows_engine(WorkflowsEngineConfig::new_with_workflow_configurations(
      vec![workflow],
    ))
    .await;

  // Create initial run
  engine.process_log(TestLog::new("start"));
  engine_assert_active_runs!(engine; 0; "A");

  // Process state change with wrong key
  let state_change = bd_state::StateChange {
    scope: bd_state::Scope::FeatureFlag,
    key: "different_key".to_string(),
    change_type: bd_state::StateChangeType::Inserted {
      value: "enabled".to_string(),
    },
    timestamp: OffsetDateTime::now_utc(),
  };

  engine.process_state_change(&state_change);
  // Verify no transition occurred - metric should not be emitted
  assert!(
    setup
      .collector
      .find_counter(
        &bd_stats_common::NameType::Global("no_match_metric".to_string()),
        &labels! {}
      )
      .is_none()
  );

  // Process state change with wrong value
  let state_change = bd_state::StateChange {
    scope: bd_state::Scope::FeatureFlag,
    key: "flag".to_string(),
    change_type: bd_state::StateChangeType::Inserted {
      value: "disabled".to_string(),
    },
    timestamp: OffsetDateTime::now_utc(),
  };

  engine.process_state_change(&state_change);
  // Verify still no transition - metric should still not be emitted
  assert!(
    setup
      .collector
      .find_counter(
        &bd_stats_common::NameType::Global("no_match_metric".to_string()),
        &labels! {}
      )
      .is_none()
  );
}

// Tests that state changes support timestamp extractions.
#[tokio::test]
async fn state_change_with_timestamp_extraction() {
  let c = state("C");
  let b = state("B").declare_transition_with_all(
    &c,
    rule!(message_equals("check_timestamp")),
    &[make_generate_log_action_proto(
      "message_with_timestamp",
      &[(
        "extracted_timestamp",
        TestFieldType::Single(TestFieldRef::SavedTimestampId("state_change_time")),
      )],
      "generated_log_id",
      LogType::NORMAL,
    )],
    &[],
  );

  let a = state("A")
    .declare_transition(&state("A"), rule!(message_equals("start")))
    .declare_transition_with_extractions(
      &b,
      make_state_change_rule(
        bd_state::Scope::FeatureFlag,
        "test_flag",
        make_is_set_match(),
      ),
      &[make_save_timestamp_extraction("state_change_time")],
    );

  let workflow = WorkflowBuilder::new("extraction_test", &[&a, &b, &c]).make_config();
  let setup = Setup::new();

  let mut engine = setup
    .make_workflows_engine(WorkflowsEngineConfig::new_with_workflow_configurations(
      vec![workflow],
    ))
    .await;

  // Create initial run
  engine.process_log(TestLog::new("start"));
  engine_assert_active_runs!(engine; 0; "A");

  // Process state change at a specific timestamp
  let state_change_time = datetime!(2024-01-15 10:30:45 UTC);
  let state_change = bd_state::StateChange {
    scope: bd_state::Scope::FeatureFlag,
    key: "test_flag".to_string(),
    change_type: bd_state::StateChangeType::Inserted {
      value: "enabled".to_string(),
    },
    timestamp: state_change_time,
  };

  let result = engine.process_state_change(&state_change);
  assert!(result.logs_to_inject.is_empty());
  engine_assert_active_runs!(engine; 0; "B");

  // Trigger log generation to use the extracted timestamp
  let result = engine.process_log(TestLog::new("check_timestamp"));

  // Check that a log was generated with the extracted timestamp
  let generated_logs: Vec<_> = result.logs_to_inject.into_values().collect();
  assert_eq!(generated_logs.len(), 1);

  let generated_log = &generated_logs[0];
  assert_eq!(
    generated_log.message,
    bd_log_primitives::LogMessage::String("message_with_timestamp".to_string())
  );

  // Verify the extracted timestamp field matches the state change timestamp
  let extracted_timestamp = generated_log
    .field_value("extracted_timestamp")
    .expect("extracted_timestamp field should exist");

  // The timestamp should be stored as fractional milliseconds since epoch
  // 2024-01-15T10:30:45.000000000Z = 1705314645000 milliseconds
  assert_eq!(extracted_timestamp, "1705314645000");
}

// Tests that state changes DO trigger timeout checks.
// Both logs and state changes can trigger timeouts since timeout transitions
// use default extractions and don't depend on the triggering event's fields.
#[tokio::test]
async fn state_change_does_trigger_timeout() {
  // Create a workflow with a timeout from state A to state C
  let c = state("C").declare_transition(&state("C"), rule!(message_equals("keep_alive")));
  let b = state("B").declare_transition(&state("B"), rule!(message_equals("keep_alive")));
  let a = state("A")
    .declare_transition(
      &b,
      make_state_change_rule(
        bd_state::Scope::FeatureFlag,
        "wrong_flag",
        make_is_set_match(),
      ),
    )
    .with_timeout(
      &c,
      1.seconds(),
      &[make_emit_counter_action(
        "timeout_metric",
        metric_value(1),
        vec![],
      )],
    );

  let workflow = WorkflowBuilder::new("timeout_test", &[&a, &b, &c]).make_config();
  let setup = Setup::new();

  let mut engine = setup
    .make_workflows_engine(WorkflowsEngineConfig::new_with_workflow_configurations(
      vec![workflow],
    ))
    .await;

  // Create initial run at T=0 by processing a log (this initializes the timeout)
  engine.process_log(TestLog::new("init").with_now(datetime!(2024-01-01 00:00:00 UTC)));
  engine_assert_active_runs!(engine; 0; "A");

  // Process a state change at T=2 seconds (after the timeout would have expired)
  // This SHOULD trigger the timeout transition to C (not match the state change rule)
  let state_change = bd_state::StateChange {
    scope: bd_state::Scope::FeatureFlag,
    key: "different_flag".to_string(),
    change_type: bd_state::StateChangeType::Inserted {
      value: "enabled".to_string(),
    },
    timestamp: datetime!(2024-01-01 00:00:02 UTC),
  };

  engine.process_state_change(&state_change);

  // Verify that we transitioned to C (via timeout), not B (via state change match)
  // The state change DID trigger the timeout check
  assert!(
    engine.state.workflows[0]
      .runs()
      .iter()
      .any(|r| r.traversals().first().is_some_and(|t| t.state_index == 2))
  ); // State C is index 2

  // Verify the timeout metric was emitted
  setup
    .collector
    .assert_workflow_counter_eq(1, "timeout_metric", labels! {});
}

// Tests that state changes can trigger transitions with counter metrics.
#[tokio::test]
async fn state_change_triggers_counter_metric() {
  let b = state("B");
  let a = state("A").declare_transition_with_actions(
    &b,
    make_state_change_rule(
      bd_state::Scope::FeatureFlag,
      "test_flag",
      make_is_set_match(),
    ),
    &[make_emit_counter_action(
      "state_change_counter",
      metric_value(5),
      vec![metric_tag("fixed_tag", "tag_value")],
    )],
  );

  let workflow = WorkflowBuilder::new("state_change_metric_test", &[&a, &b]).make_config();
  let setup = Setup::new();

  let mut engine = setup
    .make_workflows_engine(WorkflowsEngineConfig::new_with_workflow_configurations(
      vec![workflow],
    ))
    .await;

  // Create initial run
  engine.process_log(TestLog::new("init"));
  engine_assert_active_runs!(engine; 0; "A");

  // Process a state change that matches the rule
  let state_change = bd_state::StateChange {
    scope: bd_state::Scope::FeatureFlag,
    key: "test_flag".to_string(),
    change_type: bd_state::StateChangeType::Inserted {
      value: "enabled".to_string(),
    },
    timestamp: time::OffsetDateTime::now_utc(),
  };

  engine.process_state_change(&state_change);

  // Verify the counter metric was emitted with the correct value and tag
  setup.collector.assert_workflow_counter_eq(
    5,
    "state_change_counter",
    labels! {
      "fixed_tag" => "tag_value",
    },
  );
}

// Tests that state changes can trigger transitions with histogram metrics.
#[tokio::test]
async fn state_change_triggers_histogram_metric() {
  let b = state("B");
  let a = state("A").declare_transition_with_actions(
    &b,
    make_state_change_rule(
      bd_state::Scope::FeatureFlag,
      "test_flag",
      make_is_set_match(),
    ),
    &[make_emit_histogram_action(
      "state_change_histogram",
      metric_value(42),
      vec![],
    )],
  );

  let workflow = WorkflowBuilder::new("state_change_histogram_test", &[&a, &b]).make_config();
  let setup = Setup::new();

  let mut engine = setup
    .make_workflows_engine(WorkflowsEngineConfig::new_with_workflow_configurations(
      vec![workflow],
    ))
    .await;

  // Create initial run
  engine.process_log(TestLog::new("init"));
  engine_assert_active_runs!(engine; 0; "A");

  // Process a state change that matches the rule
  let state_change = bd_state::StateChange {
    scope: bd_state::Scope::FeatureFlag,
    key: "test_flag".to_string(),
    change_type: bd_state::StateChangeType::Inserted {
      value: "enabled".to_string(),
    },
    timestamp: time::OffsetDateTime::now_utc(),
  };

  engine.process_state_change(&state_change);

  // Verify the histogram metric was emitted
  setup
    .collector
    .assert_workflow_histogram_observed(42.0, "state_change_histogram", labels! {});
}

// Tests that timeout transitions triggered by state changes emit metrics correctly.
#[tokio::test]
async fn state_change_timeout_emits_metrics() {
  let c = state("C").declare_transition(&state("C"), rule!(message_equals("keep_alive")));
  let b = state("B");
  let a = state("A")
    .declare_transition(
      &b,
      make_state_change_rule(
        bd_state::Scope::FeatureFlag,
        "wrong_flag",
        make_is_set_match(),
      ),
    )
    .with_timeout(
      &c,
      1.seconds(),
      &[
        make_emit_counter_action("timeout_counter", metric_value(10), vec![]),
        make_emit_histogram_action("timeout_histogram", metric_value(99), vec![]),
      ],
    );

  let workflow = WorkflowBuilder::new("state_change_timeout_metrics", &[&a, &b, &c]).make_config();
  let setup = Setup::new();

  let mut engine = setup
    .make_workflows_engine(WorkflowsEngineConfig::new_with_workflow_configurations(
      vec![workflow],
    ))
    .await;

  // Create initial run at T=0
  engine.process_log(TestLog::new("init").with_now(datetime!(2024-01-01 00:00:00 UTC)));
  engine_assert_active_runs!(engine; 0; "A");

  // Process a state change at T=2 seconds (after timeout)
  let state_change = bd_state::StateChange {
    scope: bd_state::Scope::FeatureFlag,
    key: "different_flag".to_string(),
    change_type: bd_state::StateChangeType::Inserted {
      value: "enabled".to_string(),
    },
    timestamp: datetime!(2024-01-01 00:00:02 UTC),
  };

  engine.process_state_change(&state_change);

  // Verify timeout transition to C occurred
  assert!(
    engine.state.workflows[0]
      .runs()
      .iter()
      .any(|r| r.traversals().first().is_some_and(|t| t.state_index == 2))
  );

  // Verify both metrics were emitted
  setup
    .collector
    .assert_workflow_counter_eq(10, "timeout_counter", labels! {});
  setup
    .collector
    .assert_workflow_histogram_observed(99.0, "timeout_histogram", labels! {});
}
