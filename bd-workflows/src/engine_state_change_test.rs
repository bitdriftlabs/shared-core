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
use bd_log_matcher::builder::{feature_flag_equals, message_equals, state_equals_saved_field};
use bd_proto::protos::logging::payload::LogType;
use bd_proto::protos::state::scope::StateScope;
use bd_state::StateChange;
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
  make_state_change_rule_with_extra_matcher,
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
  let a = state("A").declare_transition_with_actions(
    &b,
    make_state_change_rule(
      bd_state::Scope::FeatureFlagExposure,
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

  // Process a state change that matches
  let state_change = StateChange::inserted(
    bd_state::Scope::FeatureFlagExposure,
    "feature_flag",
    "enabled",
    OffsetDateTime::now_utc(),
  );

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
  let a = state("A").declare_transition_with_actions(
    &b,
    make_state_change_rule(
      bd_state::Scope::FeatureFlagExposure,
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

  // Test with string value
  let state_change = StateChange::inserted(
    bd_state::Scope::FeatureFlagExposure,
    "any_value",
    "hello",
    OffsetDateTime::now_utc(),
  );

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
  let a = state("A").declare_transition_with_actions(
    &b,
    make_state_change_rule(
      bd_state::Scope::FeatureFlagExposure,
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

  // Process state change with wrong key
  let state_change = StateChange::inserted(
    bd_state::Scope::FeatureFlagExposure,
    "different_key",
    "enabled",
    OffsetDateTime::now_utc(),
  );

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
  let state_change = StateChange::inserted(
    bd_state::Scope::FeatureFlagExposure,
    "flag",
    "disabled",
    OffsetDateTime::now_utc(),
  );

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
        bd_state::Scope::FeatureFlagExposure,
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
  let state_change = StateChange::inserted(
    bd_state::Scope::FeatureFlagExposure,
    "test_flag",
    "enabled",
    state_change_time,
  );

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
        bd_state::Scope::FeatureFlagExposure,
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
  let state_change = StateChange::inserted(
    bd_state::Scope::FeatureFlagExposure,
    "different_flag",
    "enabled",
    datetime!(2024-01-01 00:00:02 UTC),
  );

  engine.process_state_change(&state_change);

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
      bd_state::Scope::FeatureFlagExposure,
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
  let state_change = StateChange::inserted(
    bd_state::Scope::FeatureFlagExposure,
    "test_flag",
    "enabled",
    OffsetDateTime::now_utc(),
  );

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
      bd_state::Scope::FeatureFlagExposure,
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
  let state_change = StateChange::inserted(
    bd_state::Scope::FeatureFlagExposure,
    "test_flag",
    "enabled",
    OffsetDateTime::now_utc(),
  );

  engine.process_state_change(&state_change);

  // Verify the histogram metric was emitted
  setup
    .collector
    .assert_workflow_histogram_observed(42.0, "state_change_histogram", labels! {});
}

// Tests that state changes can extract state values from the state reader.
// During a state change transition, we can extract values from the state reader (like feature
// flags) and use them in metric tags. This is useful for adding context about other state
// when a state change occurs.
#[tokio::test]
async fn state_change_with_feature_flag_extraction() {
  use bd_test_helpers::workflow::extract_feature_flag_tag;

  let b = state("B");

  // Transition from A to B on state change, extracting a feature flag value into a metric tag
  let a = state("A").declare_transition_with_actions(
    &b,
    make_state_change_rule(
      bd_state::Scope::FeatureFlagExposure,
      "trigger_flag",
      make_is_set_match(),
    ),
    &[make_emit_counter_action(
      "state_change_with_extraction",
      metric_value(1),
      vec![extract_feature_flag_tag("extracted_flag", "flag_value")],
    )],
  );

  let workflow = WorkflowBuilder::new("feature_flag_extraction_test", &[&a, &b]).make_config();
  let setup = Setup::new();

  let mut engine = setup
    .make_workflows_engine(WorkflowsEngineConfig::new_with_workflow_configurations(
      vec![workflow],
    ))
    .await;

  // Set up a state reader with a state value (feature flag) to extract
  let mut state_reader = bd_state::test::TestStateReader::new();
  state_reader.insert(
    bd_state::Scope::FeatureFlagExposure,
    "extracted_flag",
    "extracted_value",
  );

  // Process state change that triggers the transition, with our custom state reader
  // This will create the initial run and transition to B
  let state_change = StateChange::inserted(
    bd_state::Scope::FeatureFlagExposure,
    "trigger_flag",
    "enabled",
    OffsetDateTime::now_utc(),
  );

  engine.process_state_change_with_reader(&state_change, &state_reader);

  // State B is a final state (no outgoing transitions), so the run completes and is removed
  // We can verify the transition occurred by checking the metric was emitted

  // Verify the metric was emitted with the extracted state value in the tag
  setup.collector.assert_workflow_counter_eq(
    1,
    "state_change_with_extraction",
    labels! {
      "flag_value" => "extracted_value",
    },
  );
}

// Tests that state changes can include additional log matchers that check other state values.
// This allows state change transitions to require multiple conditions: the state change itself
// AND additional conditions like other state values being set to specific values.
#[tokio::test]
async fn state_change_with_extra_state_matcher() {
  let b = state("B");

  // Create a state change rule that also checks another state value using log matcher
  let state_change_rule_with_matcher = make_state_change_rule_with_extra_matcher(
    StateScope::FEATURE_FLAG,
    "trigger_flag",
    None, // Any value triggers
    Some(feature_flag_equals("required_flag", "enabled")),
  );

  let a = state("A").declare_transition_with_actions(
    &b,
    state_change_rule_with_matcher,
    &[make_emit_counter_action(
      "state_change_with_extra_match",
      metric_value(1),
      vec![],
    )],
  );

  let workflow = WorkflowBuilder::new("extra_matcher_test", &[&a, &b]).make_config();
  let setup = Setup::new();

  let mut engine = setup
    .make_workflows_engine(WorkflowsEngineConfig::new_with_workflow_configurations(
      vec![workflow],
    ))
    .await;

  // Set up a state reader with the required state value
  let mut state_reader = bd_state::test::TestStateReader::new();
  state_reader.insert(
    bd_state::Scope::FeatureFlagExposure,
    "required_flag",
    "enabled",
  );

  // Process state change with the state reader - this SHOULD match
  let state_change = StateChange::inserted(
    bd_state::Scope::FeatureFlagExposure,
    "trigger_flag",
    "any_value",
    OffsetDateTime::now_utc(),
  );

  engine.process_state_change_with_reader(&state_change, &state_reader);

  // Verify the transition occurred
  setup
    .collector
    .assert_workflow_counter_eq(1, "state_change_with_extra_match", labels! {});
}

// Tests that state change transitions fail when the extra matcher doesn't match.
#[tokio::test]
async fn state_change_with_extra_state_matcher_no_match() {
  let b = state("B");

  // Create a state change rule that requires another state value to be "enabled"
  let state_change_rule_with_matcher = make_state_change_rule_with_extra_matcher(
    StateScope::FEATURE_FLAG,
    "trigger_flag",
    None, // Any value triggers
    Some(feature_flag_equals("required_flag", "enabled")),
  );

  let a = state("A").declare_transition_with_actions(
    &b,
    state_change_rule_with_matcher,
    &[make_emit_counter_action(
      "should_not_emit",
      metric_value(1),
      vec![],
    )],
  );

  let workflow = WorkflowBuilder::new("extra_matcher_no_match_test", &[&a, &b]).make_config();
  let setup = Setup::new();

  let mut engine = setup
    .make_workflows_engine(WorkflowsEngineConfig::new_with_workflow_configurations(
      vec![workflow],
    ))
    .await;

  // Set up a state reader with the WRONG value for required_flag
  let mut state_reader = bd_state::test::TestStateReader::new();
  state_reader.insert(
    bd_state::Scope::FeatureFlagExposure,
    "required_flag",
    "disabled", // Wrong value!
  );

  // Process state change - this should NOT match due to the extra matcher
  let state_change = StateChange::inserted(
    bd_state::Scope::FeatureFlagExposure,
    "trigger_flag",
    "any_value",
    OffsetDateTime::now_utc(),
  );

  engine.process_state_change_with_reader(&state_change, &state_reader);

  // Verify the transition did NOT occur
  assert!(
    setup
      .collector
      .find_counter(
        &bd_stats_common::NameType::Global("should_not_emit".to_string()),
        &labels! {}
      )
      .is_none()
  );
}

// Tests that state change transitions can check multiple state values simultaneously.
// This allows complex conditions like "trigger when flag A changes AND flag B equals X".
#[tokio::test]
async fn state_change_with_multiple_state_conditions() {
  let b = state("B");

  // Create a state change rule that checks TWO different state values:
  // 1. The trigger_flag must change (primary condition)
  // 2. The secondary_flag must equal "ready" (extra condition via log_matcher)
  let state_change_rule_with_multiple_conditions = make_state_change_rule_with_extra_matcher(
    StateScope::FEATURE_FLAG,
    "trigger_flag",
    None, // Any value triggers
    Some(feature_flag_equals("secondary_flag", "ready")),
  );

  let a = state("A").declare_transition_with_actions(
    &b,
    state_change_rule_with_multiple_conditions,
    &[make_emit_counter_action(
      "multi_condition_metric",
      metric_value(1),
      vec![],
    )],
  );

  let workflow = WorkflowBuilder::new("multi_condition_test", &[&a, &b]).make_config();
  let setup = Setup::new();

  let mut engine = setup
    .make_workflows_engine(WorkflowsEngineConfig::new_with_workflow_configurations(
      vec![workflow],
    ))
    .await;

  // Set up state reader with secondary_flag = "ready"
  let mut state_reader = bd_state::test::TestStateReader::new();
  state_reader.insert(
    bd_state::Scope::FeatureFlagExposure,
    "secondary_flag",
    "ready",
  );

  // Process state change - should match because both conditions are met
  let state_change = StateChange::inserted(
    bd_state::Scope::FeatureFlagExposure,
    "trigger_flag",
    "enabled",
    OffsetDateTime::now_utc(),
  );

  engine.process_state_change_with_reader(&state_change, &state_reader);

  // Verify the transition occurred
  setup
    .collector
    .assert_workflow_counter_eq(1, "multi_condition_metric", labels! {});
}

// Tests that we can match against extracted fields during state change by comparing
// a state value against an extracted field value using SaveFieldId.
#[tokio::test]
async fn state_change_compares_state_to_extracted_field() {
  use bd_test_helpers::workflow::make_save_field_extraction;

  let c = state("C").declare_transition(&state("C"), rule!(message_equals("keep_alive")));

  // Matcher that compares state value "requirement" to extracted field "saved_tier"
  let matcher = state_equals_saved_field(StateScope::FEATURE_FLAG, "requirement", "saved_tier");

  // B -> C transition triggered by state change, with extra matcher comparing state to extracted
  // field
  let state_change_rule = make_state_change_rule_with_extra_matcher(
    StateScope::FEATURE_FLAG,
    "trigger",
    None, // Any value triggers
    Some(matcher),
  );

  let b = state("B").declare_transition_with_actions(
    &c,
    state_change_rule,
    &[make_emit_counter_action("matched", metric_value(1), vec![])],
  );

  // A -> B transition triggered by log, with field extraction
  let a = state("A")
    .declare_transition(&state("A"), rule!(message_equals("init")))
    .declare_transition_with_extractions(
      &b,
      rule!(message_equals("extract")),
      &[make_save_field_extraction("saved_tier", "tier")],
    );

  let workflow = WorkflowBuilder::new("test", &[&a, &b, &c]).make_config();
  let setup = Setup::new();

  let mut engine = setup
    .make_workflows_engine(WorkflowsEngineConfig::new_with_workflow_configurations(
      vec![workflow],
    ))
    .await;

  // Step 0: Start workflow in state A
  engine.process_log(TestLog::new("init"));
  engine_assert_active_runs!(engine; 0; "A");

  // Step 1: A -> B via log match with field extraction (tier="premium" extracted as saved_tier)
  engine.process_log(TestLog::new("extract").with_tags(labels! { "tier" => "premium" }));
  engine_assert_active_runs!(engine; 0; "B");

  // Step 2: B -> C via state change, where the extra matcher compares state to extracted field
  // Set state requirement="premium" (should match saved_tier="premium")
  let mut state_reader = bd_state::test::TestStateReader::new();
  state_reader.insert(
    bd_state::Scope::FeatureFlagExposure,
    "requirement",
    "premium",
  );

  let state_change = StateChange::inserted(
    bd_state::Scope::FeatureFlagExposure,
    "trigger",
    "on",
    OffsetDateTime::now_utc(),
  );

  engine.process_state_change_with_reader(&state_change, &state_reader);

  // Should transition to C because requirement state ("premium") equals saved_tier field
  // ("premium") The counter action should have been triggered, proving the extra matcher worked
  setup
    .collector
    .assert_workflow_counter_eq(1, "matched", labels! {});
}

// Tests that state changes can match on global metadata fields passed in via FieldsRef.
// This verifies that fields collected from global metadata (like device info, app version, etc.)
// can be used in log matchers during state change transitions.
#[tokio::test]
async fn state_change_matches_on_global_metadata_fields() {
  use bd_log_matcher::builder::field_equals;

  let b = state("B");

  // Create a state change rule that requires a specific device_id field value
  let state_change_rule = {
    use bd_proto::protos::workflow::workflow::workflow::rule::Rule_type;
    use bd_proto::protos::workflow::workflow::workflow::{Rule, RuleStateChangeMatch};

    Rule {
      rule_type: Some(Rule_type::RuleStateChangeMatch(RuleStateChangeMatch {
        scope: bd_proto::protos::state::scope::StateScope::FEATURE_FLAG.into(),
        key: "trigger_flag".to_string(),
        previous_value: protobuf::MessageField::none(),
        new_value: protobuf::MessageField::some(
          bd_proto::protos::state::matcher::StateValueMatch {
            value_match: Some(make_is_set_match()),
            ..Default::default()
          },
        ),
        // Add a log matcher that checks device_id field
        log_matcher: protobuf::MessageField::some(field_equals("device_id", "test_device_123")),
        ..Default::default()
      })),
      ..Default::default()
    }
  };

  let a = state("A").declare_transition_with_actions(
    &b,
    state_change_rule,
    &[make_emit_counter_action(
      "matched_on_field",
      metric_value(1),
      vec![],
    )],
  );

  let workflow = WorkflowBuilder::new("field_match_test", &[&a, &b]).make_config();
  let setup = Setup::new();

  let mut engine = setup
    .make_workflows_engine(WorkflowsEngineConfig::new_with_workflow_configurations(
      vec![workflow],
    ))
    .await;

  // Create initial run
  engine.process_log(TestLog::new("init"));
  engine_assert_active_runs!(engine; 0; "A");

  // Process state change WITH global metadata fields
  let state_change = StateChange::inserted(
    bd_state::Scope::FeatureFlagExposure,
    "trigger_flag",
    "enabled",
    OffsetDateTime::now_utc(),
  );

  // Create fields that would come from global metadata
  let fields = bd_test_helpers::workflow::make_tags(labels! { "device_id" => "test_device_123" });
  let matching_fields = bd_log_primitives::LogFields::default();

  // Process state change with fields
  let fields_ref = bd_log_primitives::FieldsRef::new(&fields, &matching_fields);
  engine.engine.process_event(
    crate::workflow::WorkflowEvent::StateChange(&state_change, fields_ref),
    &super::engine_test_helpers::EMPTY_BUFFER_IDS,
    &bd_state::test::TestStateReader::default(),
    state_change.timestamp,
  );

  // Verify the transition occurred because the field matched
  setup
    .collector
    .assert_workflow_counter_eq(1, "matched_on_field", labels! {});
}

// Tests that state changes can extract values from global metadata fields.
// This verifies that fields passed in via FieldsRef (like device info, app version)
// can be extracted during the state change transition and used in actions.
#[tokio::test]
async fn state_change_extracts_global_metadata_fields() {
  let b = state("B");

  // A -> B transition triggered by state change, extracting app_version field from fields
  // and using it directly in a metric tag
  let a = state("A").declare_transition_with_actions(
    &b,
    make_state_change_rule(
      bd_state::Scope::FeatureFlagExposure,
      "trigger_flag",
      make_is_set_match(),
    ),
    &[make_emit_counter_action(
      "extraction_metric",
      metric_value(1),
      vec![bd_test_helpers::workflow::extract_metric_tag(
        "app_version",
        "app_version_tag",
      )],
    )],
  );

  let workflow = WorkflowBuilder::new("field_extraction_test", &[&a, &b]).make_config();
  let setup = Setup::new();

  let mut engine = setup
    .make_workflows_engine(WorkflowsEngineConfig::new_with_workflow_configurations(
      vec![workflow],
    ))
    .await;

  // Create initial run
  engine.process_log(TestLog::new("init"));
  engine_assert_active_runs!(engine; 0; "A");

  // Process state change with global metadata fields
  let state_change = StateChange::inserted(
    bd_state::Scope::FeatureFlagExposure,
    "trigger_flag",
    "enabled",
    OffsetDateTime::now_utc(),
  );

  // Create fields that would come from global metadata
  let fields = bd_test_helpers::workflow::make_tags(labels! { "app_version" => "1.2.3" });
  let matching_fields = bd_log_primitives::LogFields::default();

  // Process state change with fields
  let fields_ref = bd_log_primitives::FieldsRef::new(&fields, &matching_fields);
  engine.engine.process_event(
    crate::workflow::WorkflowEvent::StateChange(&state_change, fields_ref),
    &super::engine_test_helpers::EMPTY_BUFFER_IDS,
    &bd_state::test::TestStateReader::default(),
    state_change.timestamp,
  );

  // Verify the metric was emitted with the extracted app_version field
  setup.collector.assert_workflow_counter_eq(
    1,
    "extraction_metric",
    labels! {
      "app_version_tag" => "1.2.3",
    },
  );
}

// Tests that state changes can include global metadata fields in generated logs.
// This verifies that when a state change triggers a log generation action,
// the fields from global metadata are available and can be included in the generated log.
#[tokio::test]
async fn state_change_includes_fields_in_generated_log() {
  use bd_test_helpers::workflow::TestFieldRef;

  let b = state("B");

  // A -> B transition triggered by state change, generating a log with a field reference
  let a = state("A").declare_transition_with_actions(
    &b,
    make_state_change_rule(
      bd_state::Scope::FeatureFlagExposure,
      "trigger_flag",
      make_is_set_match(),
    ),
    &[make_generate_log_action_proto(
      "state_change_occurred",
      &[
        (
          "device",
          TestFieldType::Single(TestFieldRef::FieldFromCurrentLog("device_id")),
        ),
        (
          "version",
          TestFieldType::Single(TestFieldRef::FieldFromCurrentLog("app_version")),
        ),
      ],
      "generated_log_id",
      LogType::NORMAL,
    )],
  );

  let workflow = WorkflowBuilder::new("generated_log_test", &[&a, &b]).make_config();
  let setup = Setup::new();

  let mut engine = setup
    .make_workflows_engine(WorkflowsEngineConfig::new_with_workflow_configurations(
      vec![workflow],
    ))
    .await;

  // Create initial run
  engine.process_log(TestLog::new("init"));
  engine_assert_active_runs!(engine; 0; "A");

  // Process state change with global metadata fields
  let state_change = StateChange::inserted(
    bd_state::Scope::FeatureFlagExposure,
    "trigger_flag",
    "enabled",
    OffsetDateTime::now_utc(),
  );

  // Create fields that would come from global metadata
  let fields = bd_test_helpers::workflow::make_tags(labels! {
    "device_id" => "device_abc",
    "app_version" => "2.0.0",
  });
  let matching_fields = bd_log_primitives::LogFields::default();

  // Process state change with fields
  let fields_ref = bd_log_primitives::FieldsRef::new(&fields, &matching_fields);
  let result = engine.engine.process_event(
    crate::workflow::WorkflowEvent::StateChange(&state_change, fields_ref),
    &super::engine_test_helpers::EMPTY_BUFFER_IDS,
    &bd_state::test::TestStateReader::default(),
    state_change.timestamp,
  );

  // Verify a log was generated
  let generated_logs: Vec<_> = result.logs_to_inject.into_values().collect();
  assert_eq!(generated_logs.len(), 1);

  let generated_log = &generated_logs[0];
  assert_eq!(
    generated_log.message,
    bd_log_primitives::LogMessage::String("state_change_occurred".to_string())
  );

  // Verify the generated log includes the fields from global metadata
  let device_field = generated_log
    .field_value("device")
    .expect("device field should exist");
  assert_eq!(device_field, "device_abc");

  let version_field = generated_log
    .field_value("version")
    .expect("version field should exist");
  assert_eq!(version_field, "2.0.0");
}
