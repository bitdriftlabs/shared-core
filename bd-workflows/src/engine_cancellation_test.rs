// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE.polyform file or at:
// https://polyformproject.org/licenses/strict/1.0.0

use super::engine_test::{timed_command_log, workflow_command_rule};
use super::engine_test_helpers::{Setup, make_state_change_rule, make_string_match};
use super::{WorkflowCommandLog, WorkflowsEngineConfig};
use crate::engine_assert_active_runs;
use crate::test::MakeConfig;
use crate::workflow::{
  CommandCancellationReason,
  WorkflowCommandCompletionError,
  WorkflowCommandOutcome,
  WorkflowCommandRequest,
  WorkflowEvent,
};
use bd_client_stats_store::test::StatsHelper;
use bd_log_matcher::builder::{field_equals, message_equals};
use bd_log_primitives::tiny_set::TinySet;
use bd_log_primitives::{
  COMMAND_OUTCOME_MESSAGE,
  DataValue,
  LOG_FIELD_NAME_COMMAND_CANCELLATION_REASON,
  LOG_FIELD_NAME_COMMAND_DETAILS,
  LOG_FIELD_NAME_DURATION_MS,
  LOG_FIELD_NAME_RESULT,
  LOG_FIELD_NAME_SPAN_ID,
  LogFields,
  SPAN_RESULT_CANCELED,
  SPAN_RESULT_SUCCESS,
};
use bd_proto::protos::workflow::workflow::workflow::rule::Rule_type;
use bd_state::{Scope, StateChange, string_value};
use bd_stats_common::{NameType, labels};
use bd_test_helpers::workflow::macros::rule;
use bd_test_helpers::workflow::{
  WorkflowBuilder,
  extract_log_body_tag,
  extract_metric_tag,
  make_emit_counter_action,
  make_save_field_extraction,
  make_start_tracing_action,
  metric_value,
  state,
};
use time::OffsetDateTime;
use time::ext::NumericalDuration;
use time::macros::datetime;

fn assert_cancelled(
  logs: &[WorkflowCommandLog],
  request: &WorkflowCommandRequest,
  reason: CommandCancellationReason,
  now: OffsetDateTime,
) {
  assert_eq!(logs.len(), 1);
  let log = &logs[0];
  assert_eq!(log.token, request.completion_token());
  assert_eq!(log.log.session_id.as_ref(), request.session_id);
  assert_eq!(log.log.occurred_at, now);
  assert_eq!(
    log.log.fields.get(LOG_FIELD_NAME_COMMAND_DETAILS),
    request
      .start_log()
      .fields
      .get(LOG_FIELD_NAME_COMMAND_DETAILS),
  );
  assert_eq!(
    log.log.fields.get(LOG_FIELD_NAME_RESULT).unwrap().as_str(),
    Some(SPAN_RESULT_CANCELED),
  );
  assert_eq!(
    log.log.fields.get(LOG_FIELD_NAME_SPAN_ID),
    request.start_log().fields.get(LOG_FIELD_NAME_SPAN_ID),
  );
  assert_eq!(
    log
      .log
      .fields
      .get(LOG_FIELD_NAME_COMMAND_CANCELLATION_REASON)
      .unwrap()
      .as_str(),
    Some(reason.as_str()),
  );
}

#[tokio::test]
async fn workflow_command_actions_require_accepted_matching_completion() {
  for succeeded in [true, false] {
    let terminal = state("terminal");
    let mut command_rule = workflow_command_rule();
    let Some(Rule_type::MatchRunCommand(command_config)) = &mut command_rule.rule_type else {
      panic!("expected command matcher");
    };
    command_config
      .command_selector
      .as_mut()
      .unwrap()
      .arguments
      .insert("input".to_string(), DataValue::U64(7).into_proto());
    command_config.outcome_log_matcher =
      Some(field_equals(LOG_FIELD_NAME_RESULT, SPAN_RESULT_SUCCESS)).into();
    let command = state("command").declare_transition_with_actions(
      &terminal,
      command_rule,
      &[make_emit_counter_action(
        "own_action",
        metric_value(1),
        vec![],
      )],
    );
    let start = state("start").declare_transition(&command, rule!(message_equals("start")));
    let setup = Setup::new();
    let mut engine = setup
      .make_workflows_engine(WorkflowsEngineConfig::new_with_workflow_configurations(
        vec![WorkflowBuilder::new("workflow", &[&start, &command, &terminal]).make_config()],
      ))
      .await;
    let now = datetime!(2026-01-01 00:00 UTC);
    engine.process_log(timed_command_log("start", now));
    let request = engine
      .process_log(timed_command_log("execute", now))
      .workflow_commands_to_start
      .pop()
      .unwrap();
    assert!(
      setup
        .collector
        .find_counter(&NameType::ActionId("own_action".into()), &labels! {})
        .is_none()
    );
    assert_eq!(
      request
        .start_log()
        .fields
        .get(LOG_FIELD_NAME_COMMAND_DETAILS)
        .unwrap()
        .as_str(),
      Some(r#"{"key":"command","args":{"input":7}}"#),
    );
    let fields = [(LOG_FIELD_NAME_COMMAND_DETAILS.into(), "spoofed".into())].into();
    let outcome = if succeeded {
      WorkflowCommandOutcome::Succeeded {
        message: None,
        fields,
      }
    } else {
      WorkflowCommandOutcome::Failed {
        message: None,
        fields,
      }
    };
    let log = engine
      .engine
      .workflow_command_outcome_log(&request.completion_token(), outcome, now)
      .unwrap();
    assert_eq!(
      log.fields.get(LOG_FIELD_NAME_COMMAND_DETAILS),
      request
        .start_log()
        .fields
        .get(LOG_FIELD_NAME_COMMAND_DETAILS),
    );
    let token = request.completion_token();
    engine.engine.process_event(
      WorkflowEvent::CommandCompletion {
        log: &log,
        token: &token,
      },
      &TinySet::default(),
      &bd_state::InMemoryStateReader::default(),
      now,
    );
    assert!(
      setup
        .collector
        .find_counter(&NameType::ActionId("own_action".into()), &labels! {})
        .is_none()
    );
    engine
      .engine
      .accept_workflow_command_completion(&token)
      .unwrap();
    engine.engine.process_event(
      WorkflowEvent::CommandCompletion {
        log: &log,
        token: &token,
      },
      &TinySet::default(),
      &bd_state::InMemoryStateReader::default(),
      now,
    );
    if succeeded {
      setup
        .collector
        .assert_workflow_counter_eq(1, "own_action", labels! {});
    } else {
      assert!(
        setup
          .collector
          .find_counter(&NameType::ActionId("own_action".into()), &labels! {})
          .is_none()
      );
    }
    assert!(
      engine
        .engine
        .take_cancelled_workflow_commands(now)
        .is_empty()
    );
  }
}

#[tokio::test]
async fn workflow_command_cancellation_observes_logs_without_actions_or_command_authority() {
  let terminal = state("terminal");
  let command = state("command").declare_transition_with_actions(
    &terminal,
    workflow_command_rule(),
    &[
      make_emit_counter_action("own_action", metric_value(1), vec![]),
      make_start_tracing_action(),
    ],
  );
  let start = state("start").declare_transition(&command, rule!(message_equals("start")));
  let observed = state("observed").declare_transition(&terminal, rule!(message_equals("never")));
  let observer = state("observer").declare_transition_with_actions(
    &observed,
    rule!(message_equals(COMMAND_OUTCOME_MESSAGE)),
    &[make_emit_counter_action(
      "cancellation_observed",
      metric_value(1),
      vec![
        extract_metric_tag(LOG_FIELD_NAME_COMMAND_CANCELLATION_REASON, "reason"),
        extract_log_body_tag(),
      ],
    )],
  );
  let command_observer =
    state("command_observer").declare_transition(&terminal, workflow_command_rule());
  let setup = Setup::new();
  let mut engine = setup
    .make_workflows_engine(WorkflowsEngineConfig::new_with_workflow_configurations(
      vec![
        WorkflowBuilder::new("owner", &[&start, &command, &terminal]).make_config(),
        WorkflowBuilder::new("observer", &[&observer, &observed, &terminal]).make_config(),
      ],
    ))
    .await;
  let now = datetime!(2026-01-01 00:00 UTC);
  engine.process_log(timed_command_log("start", now));
  let request = engine
    .process_log(timed_command_log("execute", now))
    .workflow_commands_to_start
    .pop()
    .unwrap();
  let mut configs = engine.engine.configs.clone();
  configs
    .push(WorkflowBuilder::new("command_observer", &[&command_observer, &terminal]).make_config());
  engine
    .engine
    .update(WorkflowsEngineConfig::new_with_workflow_configurations(
      configs,
    ));

  // Merely observing a cancellation cannot resume even a still-owned pending command.
  let log = request.cancelled_log(CommandCancellationReason::SessionChanged, now);
  let buffer_ids = TinySet::default();
  let result = engine.engine.process_event(
    WorkflowEvent::CommandCancelled(&log),
    &buffer_ids,
    &bd_state::InMemoryStateReader::default(),
    now,
  );
  assert_eq!(
    result.workflow_commands_to_start,
    [] as [WorkflowCommandRequest; 0]
  );
  assert!(result.triggered_flush_buffers_action_ids.is_empty());
  assert!(!result.is_tracing_active);
  assert!(
    engine
      .engine
      .pending_workflow_command_index
      .contains_key(&request.completion_token().0)
  );
  assert!(
    setup
      .collector
      .find_counter(&NameType::ActionId("own_action".into()), &labels! {})
      .is_none()
  );
  setup.collector.assert_workflow_counter_eq(
    1,
    "cancellation_observed",
    labels! {
      "reason" => "session_changed", "__log_body__" => COMMAND_OUTCOME_MESSAGE,
    },
  );
  engine
    .engine
    .cancel_workflow_command(
      &request.completion_token(),
      CommandCancellationReason::SessionChanged,
      now,
    )
    .unwrap();
  assert!(engine.engine.pending_workflow_command_index.is_empty());
  assert!(
    engine
      .engine
      .take_cancelled_workflow_commands(now)
      .is_empty()
  );
  assert_eq!(
    engine
      .engine
      .accept_workflow_command_completion(&request.completion_token()),
    Err(WorkflowCommandCompletionError::UnknownToken),
  );
}

#[tokio::test]
async fn workflow_command_session_change_captures_old_identity_and_historical_events_are_safe() {
  let terminal = state("terminal");
  let command = state("command").declare_transition(&terminal, workflow_command_rule());
  let start = state("start").declare_transition(&command, rule!(message_equals("start")));
  let setup = Setup::new();
  let mut engine = setup
    .make_workflows_engine(WorkflowsEngineConfig::new_with_workflow_configurations(
      vec![WorkflowBuilder::new("workflow", &[&start, &command, &terminal]).make_config()],
    ))
    .await;
  let now = datetime!(2026-01-01 00:00 UTC);
  engine.process_log(timed_command_log("start", now));
  let request = engine
    .process_log(timed_command_log("execute", now))
    .workflow_commands_to_start
    .pop()
    .unwrap();
  engine.process_log(timed_command_log("start", now + 1.seconds()).with_session("new-session"));
  let logs = engine
    .engine
    .take_cancelled_workflow_commands(now + 1.seconds());
  assert_cancelled(
    &logs,
    &request,
    CommandCancellationReason::SessionChanged,
    now + 1.seconds(),
  );
  assert_eq!(
    logs[0]
      .log
      .fields
      .get(LOG_FIELD_NAME_DURATION_MS)
      .unwrap()
      .as_str(),
    Some("1000"),
  );
  let start_log = request.start_log();
  for event in [
    WorkflowEvent::CommandCancelled(&logs[0].log),
    WorkflowEvent::CommandLog(&start_log),
  ] {
    engine.engine.process_event(
      event,
      &TinySet::default(),
      &bd_state::InMemoryStateReader::default(),
      now + 60.seconds(),
    );
    assert_eq!(engine.engine.state.session_id, "new-session");
  }
  // Direct events retain ordinary command-start semantics, but the request belongs to the current
  // engine session, not the historical log's session.
  assert_eq!(engine.engine.pending_workflow_command_index.len(), 1);
  assert!(
    engine
      .engine
      .take_cancelled_workflow_commands(now)
      .is_empty()
  );
}

#[tokio::test]
async fn workflow_command_config_removal_replacement_and_noop_update() {
  for replace in [false, true] {
    let terminal = state("terminal");
    let command = state("command").declare_transition(&terminal, workflow_command_rule());
    let config = WorkflowBuilder::new("workflow", &[&command, &terminal]).make_config();
    let setup = Setup::new();
    let mut engine = setup
      .make_workflows_engine(WorkflowsEngineConfig::new_with_workflow_configurations(
        vec![config.clone()],
      ))
      .await;
    let now = datetime!(2026-01-01 00:00 UTC);
    let request = engine
      .process_log(timed_command_log("execute", now))
      .workflow_commands_to_start
      .pop()
      .unwrap();
    engine
      .engine
      .update(WorkflowsEngineConfig::new_with_workflow_configurations(
        vec![config],
      ));
    assert!(
      engine
        .engine
        .take_cancelled_workflow_commands(now)
        .is_empty()
    );
    let replacement = state("replacement").declare_transition(&terminal, workflow_command_rule());
    let configs = if replace {
      vec![WorkflowBuilder::new("workflow", &[&replacement, &terminal]).make_config()]
    } else {
      vec![]
    };
    engine
      .engine
      .update(WorkflowsEngineConfig::new_with_workflow_configurations(
        configs,
      ));
    let logs = engine.engine.take_cancelled_workflow_commands(now);
    assert_cancelled(
      &logs,
      &request,
      CommandCancellationReason::WorkflowReset,
      now,
    );
    assert!(
      engine
        .engine
        .take_cancelled_workflow_commands(now)
        .is_empty()
    );
    assert_eq!(
      engine
        .engine
        .accept_workflow_command_completion(&request.completion_token()),
      Err(WorkflowCommandCompletionError::UnknownToken),
    );
  }
}

#[tokio::test]
async fn workflow_command_exclusive_reset_and_parallel_signature_replacement() {
  for parallel in [false, true] {
    let terminal = state("terminal");
    let command = state("command").declare_transition(&terminal, workflow_command_rule());
    let start = state("start").declare_transition_with_all(
      &command,
      rule!(message_equals("start")),
      &[make_start_tracing_action()],
      &[make_save_field_extraction("saved_user", "user")],
    );
    let builder = WorkflowBuilder::new("workflow", &[&start, &command, &terminal]);
    let builder = if parallel {
      builder.with_parallel_execution(Some(3))
    } else {
      builder
    };
    let setup = Setup::new();
    let mut engine = setup
      .make_workflows_engine(WorkflowsEngineConfig::new_with_workflow_configurations(
        vec![builder.make_config()],
      ))
      .await;
    let now = datetime!(2026-01-01 00:00 UTC);
    engine.process_log(timed_command_log("start", now).with_tags(labels! { "user" => "same" }));
    let request = engine
      .process_log(timed_command_log("execute", now))
      .workflow_commands_to_start
      .pop()
      .unwrap();
    engine.process_log(timed_command_log("start", now).with_tags(labels! { "user" => "same" }));
    assert_cancelled(
      &engine.engine.take_cancelled_workflow_commands(now),
      &request,
      CommandCancellationReason::WorkflowReset,
      now,
    );
    assert_eq!(engine.engine.state.active_run_tracing_count, 1);
  }
}

#[tokio::test]
async fn workflow_command_parallel_eviction_and_run_duration_stop() {
  for duration_stop in [false, true] {
    let terminal = state("terminal");
    let command = state("command").declare_transition(&terminal, workflow_command_rule());
    let start = state("start").declare_transition_with_actions(
      &command,
      rule!(message_equals("start")),
      &[make_start_tracing_action()],
    );
    let builder = WorkflowBuilder::new("workflow", &[&start, &command, &terminal]);
    let builder = if duration_stop {
      builder.with_duration_limit(1.seconds())
    } else {
      builder.with_parallel_execution(Some(2))
    };
    let setup = Setup::new();
    let mut engine = setup
      .make_workflows_engine(WorkflowsEngineConfig::new_with_workflow_configurations(
        vec![builder.make_config()],
      ))
      .await;
    let now = datetime!(2026-01-01 00:00 UTC);
    engine.process_log(timed_command_log("start", now));
    let request = engine
      .process_log(timed_command_log("execute", now))
      .workflow_commands_to_start
      .pop()
      .unwrap();
    if !duration_stop {
      engine.process_log(timed_command_log("start", now));
    }
    engine.process_log(timed_command_log("unmatched", now + 2.seconds()));
    assert_cancelled(
      &engine
        .engine
        .take_cancelled_workflow_commands(now + 2.seconds()),
      &request,
      CommandCancellationReason::WorkflowReset,
      now + 2.seconds(),
    );
    assert_eq!(
      engine.engine.state.active_run_tracing_count,
      u32::from(!duration_stop)
    );
    assert!(engine.engine.needs_state_persistence);
  }
}

#[tokio::test]
async fn workflow_command_same_event_parallel_eviction_only_dispatches_surviving_owners() {
  for (max_runs, replace_signature) in [(1, false), (2, false), (3, true)] {
    let terminal = state("terminal");
    let command = state("command").declare_transition(&terminal, workflow_command_rule());
    let start = state("start").declare_transition_with_extractions(
      &command,
      rule!(message_equals("start")),
      &[make_save_field_extraction("saved_user", "user")],
    );
    let setup = Setup::new();
    let mut engine = setup
      .make_workflows_engine(WorkflowsEngineConfig::new_with_workflow_configurations(
        vec![
          WorkflowBuilder::new("workflow", &[&start, &command, &terminal])
            .with_parallel_execution(Some(max_runs))
            .make_config(),
        ],
      ))
      .await;
    let now = datetime!(2026-01-01 00:00 UTC);
    engine.process_log(timed_command_log("start", now).with_tags(labels! { "user" => "first" }));
    // The old run first parks a command; later in this same event, the new initial run can evict
    // or replace it. No worker or span start may escape for a token with no surviving owner.
    let requests = engine
      .process_log(timed_command_log("start", now).with_tags(labels! {
        "user" => if replace_signature { "first" } else { "second" },
      }))
      .workflow_commands_to_start;
    let expected = usize::from(max_runs == 2);
    assert_eq!(requests.len(), expected);
    assert_eq!(engine.engine.pending_workflow_command_index.len(), expected);
    for request in requests {
      assert!(
        engine
          .engine
          .pending_workflow_command_index
          .contains_key(&request.completion_token().0)
      );
      engine
        .engine
        .accept_workflow_command_completion(&request.completion_token())
        .unwrap();
    }
    assert!(
      engine
        .engine
        .take_cancelled_workflow_commands(now)
        .is_empty()
    );
  }
}

#[tokio::test]
async fn workflow_command_session_start_carryover_reconciles_final_owners() {
  for scenario in ["reset", "eviction", "survives"] {
    let terminal = state("terminal");
    let command = state("command").declare_transition(&terminal, workflow_command_rule());
    let start = state("start")
      .declare_transition(
        &command,
        make_state_change_rule(
          Scope::FeatureFlagExposure,
          "ready",
          make_string_match("yes"),
        ),
      )
      .declare_transition(&command, rule!(message_equals("reset")));
    let builder = WorkflowBuilder::new("carryover", &[&start, &command, &terminal]);
    let builder = if scenario == "eviction" {
      builder.with_parallel_execution(Some(1))
    } else {
      builder
    };
    // This command is already returned for dispatch by the state change. If reset, its
    // cancellation must survive suppression of the never-dispatched carryover command.
    let existing = state("existing")
      .declare_transition(&terminal, workflow_command_rule())
      .declare_transition(&command, rule!(message_equals("reset")));
    let setup = Setup::new();
    let mut engine = setup
      .make_workflows_engine(WorkflowsEngineConfig::new_with_workflow_configurations(
        vec![
          builder.make_config(),
          WorkflowBuilder::new("existing", &[&existing, &command, &terminal]).make_config(),
        ],
      ))
      .await;
    let now = datetime!(2026-01-01 00:00 UTC);
    let previous = engine
      .process_state_change(&StateChange::inserted(
        Scope::FeatureFlagExposure,
        "ready",
        string_value("yes"),
        now,
      ))
      .workflow_commands_to_start;
    assert_eq!(previous.len(), 1);
    let previous_token = previous[0].completion_token();
    let requests = engine
      .process_log(timed_command_log(
        if scenario == "reset" { "reset" } else { "keep" },
        now,
      ))
      .workflow_commands_to_start;
    assert_eq!(requests.len(), usize::from(scenario == "survives"));
    for request in requests {
      engine
        .engine
        .accept_workflow_command_completion(&request.completion_token())
        .unwrap();
    }
    let cancellations = engine.engine.take_cancelled_workflow_commands(now);
    if scenario == "reset" {
      assert_cancelled(
        &cancellations,
        &previous[0],
        CommandCancellationReason::WorkflowReset,
        now,
      );
    } else {
      assert!(cancellations.is_empty());
      assert!(
        engine
          .engine
          .pending_workflow_command_index
          .contains_key(&previous_token.0)
      );
    }
  }
}

#[tokio::test]
async fn workflow_command_cancellation_promotes_only_dormant_exclusive_initial_timeout() {
  for (parallel, sibling, recovered) in [
    (false, false, false),
    (false, true, false),
    (true, false, false),
    (false, false, true),
    (false, true, true),
    (true, false, true),
  ] {
    let terminal = state("terminal");
    let timeout = state("timeout");
    let sibling_state =
      state("sibling").declare_transition(&terminal, rule!(message_equals("finish")));
    let command = state("command").declare_transition(&terminal, workflow_command_rule());
    let mut start = state("start")
      .declare_transition(&command, rule!(message_equals("start")))
      .with_timeout(
        &timeout,
        1.seconds(),
        &[make_emit_counter_action(
          "initial_timeout",
          metric_value(1),
          vec![],
        )],
      );
    if sibling {
      start = start.declare_transition(&sibling_state, rule!(message_equals("start")));
    }
    let builder = WorkflowBuilder::new(
      "workflow",
      &[&start, &command, &sibling_state, &terminal, &timeout],
    );
    let builder = if parallel {
      builder.with_parallel_execution(Some(3))
    } else {
      builder
    };
    let setup = Setup::new();
    let mut engine = setup
      .make_workflows_engine(WorkflowsEngineConfig::new_with_workflow_configurations(
        vec![builder.make_config()],
      ))
      .await;
    let now = datetime!(2026-01-01 00:00 UTC);
    engine.process_log(timed_command_log("start", now));
    let request = engine
      .process_log(timed_command_log("execute", now + 100.milliseconds()))
      .workflow_commands_to_start
      .pop()
      .unwrap();
    let cancelled_at = now + 200.milliseconds();
    if recovered {
      assert_cancelled(
        &engine.engine.fail_recovered_workflow_commands(cancelled_at),
        &request,
        CommandCancellationReason::SdkRestart,
        cancelled_at,
      );
    } else {
      engine
        .engine
        .cancel_workflow_command(
          &request.completion_token(),
          CommandCancellationReason::WorkflowReset,
          cancelled_at,
        )
        .unwrap();
    }
    engine.process_log(timed_command_log("probe", now + 1_100.milliseconds()));
    if parallel {
      // Parallel timers were already armed when the run was created, not at cancellation.
      setup
        .collector
        .assert_workflow_counter_eq(1, "initial_timeout", labels! {});
    } else {
      assert!(
        setup
          .collector
          .find_counter(&NameType::ActionId("initial_timeout".into()), &labels! {})
          .is_none()
      );
    }
    engine.process_log(timed_command_log("probe", now + 1_200.milliseconds()));
    if sibling {
      assert!(
        setup
          .collector
          .find_counter(&NameType::ActionId("initial_timeout".into()), &labels! {})
          .is_none()
      );
    } else {
      setup
        .collector
        .assert_workflow_counter_eq(1, "initial_timeout", labels! {});
    }
  }
}

#[tokio::test]
async fn workflow_command_log_count_stop_cancels_pending_traversals() {
  let terminal = state("terminal");
  let sibling = state("sibling").declare_transition(&terminal, rule!(message_equals("finish")));
  let command = state("command").declare_transition(&terminal, workflow_command_rule());
  let start = state("start")
    .declare_transition(&command, rule!(message_equals("start")))
    .declare_transition(&sibling, rule!(message_equals("start")));
  let setup = Setup::new();
  let mut engine = setup
    .make_workflows_engine(WorkflowsEngineConfig::new_with_workflow_configurations(
      vec![
        WorkflowBuilder::new("workflow", &[&start, &command, &sibling, &terminal])
          .with_log_limit(2)
          .make_config(),
      ],
    ))
    .await;
  let now = datetime!(2026-01-01 00:00 UTC);
  engine.process_log(timed_command_log("start", now));
  let request = engine
    .process_log(timed_command_log("execute", now))
    .workflow_commands_to_start
    .pop()
    .unwrap();
  engine.process_log(timed_command_log("finish", now));
  assert_cancelled(
    &engine.engine.take_cancelled_workflow_commands(now),
    &request,
    CommandCancellationReason::WorkflowReset,
    now,
  );
}

#[tokio::test]
async fn workflow_command_cancellation_and_recovery_prune_only_pending_siblings() {
  for recovered in [false, true] {
    let terminal = state("terminal");
    let sibling = state("sibling").declare_transition(&terminal, rule!(message_equals("finish")));
    let command = state("command").declare_transition(&terminal, workflow_command_rule());
    let start = state("start")
      .declare_transition_with_actions(
        &command,
        rule!(message_equals("start")),
        &[make_start_tracing_action()],
      )
      .declare_transition(&sibling, rule!(message_equals("start")));
    let config =
      WorkflowBuilder::new("workflow", &[&start, &command, &sibling, &terminal]).make_config();
    let setup = Setup::new();
    let mut engine = setup
      .make_workflows_engine(WorkflowsEngineConfig::new_with_workflow_configurations(
        vec![config],
      ))
      .await;
    let now = datetime!(2026-01-01 00:00 UTC);
    engine.process_log(timed_command_log("start", now));
    let request = engine
      .process_log(timed_command_log("execute", now))
      .workflow_commands_to_start
      .pop()
      .unwrap();
    assert!(engine.engine.is_tracing_active());
    let cooldown = engine.engine.state.command_last_started_at_ns.clone();
    let logs = if recovered {
      // Legacy snapshots predate recorded span start times.
      let location = engine
        .engine
        .pending_workflow_command_index
        .get(&request.completion_token().0)
        .unwrap()
        .clone();
      engine.engine.state.workflows[location.workflow_index].runs[location.run_index].traversals
        [location.traversal_index]
        .pending_command
        .as_mut()
        .unwrap()
        .started_at = None;
      engine.engine.rebuild_pending_workflow_command_index();
      engine.engine.fail_recovered_workflow_commands(now)
    } else {
      vec![
        engine
          .engine
          .complete_workflow_command(
            &request.completion_token(),
            WorkflowCommandOutcome::Cancelled {
              reason: CommandCancellationReason::SessionChanged,
            },
            now,
          )
          .unwrap(),
      ]
    };
    assert_eq!(logs.len(), 1);
    assert_eq!(engine.engine.state.command_last_started_at_ns, cooldown);
    assert!(engine.engine.is_tracing_active());
    assert!(engine.engine.pending_workflow_command_index.is_empty());
    assert!(engine.engine.needs_state_persistence);
    if recovered {
      assert!(!logs[0].log.fields.contains_key(LOG_FIELD_NAME_DURATION_MS));
    }
    engine_assert_active_runs!(engine; 0; "start", "sibling");
    engine.process_log(timed_command_log("finish", now));
    assert!(!engine.engine.is_tracing_active());
    assert!(
      engine
        .engine
        .take_cancelled_workflow_commands(now)
        .is_empty()
    );
  }
}

#[tokio::test]
async fn workflow_command_recovery_skips_received_terminal_ends() {
  let terminal = state("terminal");
  let command = state("command").declare_transition(&terminal, workflow_command_rule());
  let setup = Setup::new();
  let mut engine = setup
    .make_workflows_engine(WorkflowsEngineConfig::new_with_workflow_configurations(
      vec![WorkflowBuilder::new("workflow", &[&command, &terminal]).make_config()],
    ))
    .await;
  let now = datetime!(2026-01-01 00:00 UTC);
  let request = engine
    .process_log(timed_command_log("execute", now))
    .workflow_commands_to_start
    .pop()
    .unwrap();
  engine
    .engine
    .accept_workflow_command_completion(&request.completion_token())
    .unwrap();
  assert!(matches!(
    engine.engine.cancel_workflow_command(
      &request.completion_token(),
      CommandCancellationReason::SessionChanged,
      now,
    ),
    Err(WorkflowCommandCompletionError::AlreadyCompleted),
  ));
  assert!(
    engine
      .engine
      .fail_recovered_workflow_commands(now)
      .is_empty()
  );
  assert!(engine.engine.pending_workflow_command_index.is_empty());
  assert!(
    engine
      .engine
      .take_cancelled_workflow_commands(now)
      .is_empty()
  );
}

#[tokio::test]
async fn workflow_command_cancellation_reindexes_other_pending_siblings() {
  let terminal = state("terminal");
  let first = state("first").declare_transition(&terminal, workflow_command_rule());
  let second = state("second").declare_transition(&terminal, workflow_command_rule());
  let start = state("start")
    .declare_transition(&first, rule!(message_equals("start")))
    .declare_transition(&second, rule!(message_equals("start")));
  let setup = Setup::new();
  let mut engine = setup
    .make_workflows_engine(WorkflowsEngineConfig::new_with_workflow_configurations(
      vec![WorkflowBuilder::new("workflow", &[&start, &first, &second, &terminal]).make_config()],
    ))
    .await;
  let now = datetime!(2026-01-01 00:00 UTC);
  engine.process_log(timed_command_log("start", now));
  let mut requests = engine
    .process_log(timed_command_log("execute", now))
    .workflow_commands_to_start;
  assert_eq!(requests.len(), 2);
  let cancelled = requests.pop().unwrap();
  let remaining = requests.pop().unwrap();
  engine
    .engine
    .cancel_workflow_command(
      &cancelled.completion_token(),
      CommandCancellationReason::WorkflowReset,
      now,
    )
    .unwrap();
  assert_eq!(engine.engine.pending_workflow_command_index.len(), 1);
  let end = engine
    .engine
    .complete_workflow_command(
      &remaining.completion_token(),
      WorkflowCommandOutcome::Succeeded {
        message: None,
        fields: LogFields::default(),
      },
      now,
    )
    .unwrap();
  engine.engine.process_event(
    WorkflowEvent::CommandCompletion {
      log: &end.log,
      token: &end.token,
    },
    &TinySet::default(),
    &bd_state::InMemoryStateReader::default(),
    now,
  );
  assert!(engine.engine.pending_workflow_command_index.is_empty());
  assert!(
    engine
      .engine
      .take_cancelled_workflow_commands(now)
      .is_empty()
  );
}

#[tokio::test]
async fn workflow_command_recovery_persists_pending_pruning_but_keeps_sibling_progress() {
  let terminal = state("terminal");
  let sibling = state("sibling").declare_transition(&terminal, rule!(message_equals("finish")));
  let command = state("command").declare_transition(&terminal, workflow_command_rule());
  let start = state("start")
    .declare_transition_with_actions(
      &command,
      rule!(message_equals("start")),
      &[make_start_tracing_action()],
    )
    .declare_transition(&sibling, rule!(message_equals("start")));
  let configs =
    vec![WorkflowBuilder::new("workflow", &[&start, &command, &sibling, &terminal]).make_config()];
  let setup = Setup::new();
  let mut engine = setup
    .make_workflows_engine(WorkflowsEngineConfig::new_with_workflow_configurations(
      configs.clone(),
    ))
    .await;
  let now = datetime!(2026-01-01 00:00 UTC);
  engine.process_log(timed_command_log("start", now));
  let request = engine
    .process_log(timed_command_log("execute", now))
    .workflow_commands_to_start
    .pop()
    .unwrap();
  engine.maybe_persist(true).await;
  drop(engine);
  let setup = Setup::new_with_sdk_directory(&setup.sdk_directory);
  let mut engine = setup
    .make_workflows_engine(WorkflowsEngineConfig::new_with_workflow_configurations(
      configs.clone(),
    ))
    .await;
  assert_cancelled(
    &engine.engine.fail_recovered_workflow_commands(now),
    &request,
    CommandCancellationReason::SdkRestart,
    now,
  );
  engine_assert_active_runs!(engine; 0; "sibling");
  assert!(engine.engine.is_tracing_active());
  engine.maybe_persist(true).await;
  drop(engine);
  let setup = Setup::new_with_sdk_directory(&setup.sdk_directory);
  let mut engine = setup
    .make_workflows_engine(WorkflowsEngineConfig::new_with_workflow_configurations(
      configs,
    ))
    .await;
  assert!(
    engine
      .engine
      .fail_recovered_workflow_commands(now)
      .is_empty()
  );
  engine_assert_active_runs!(engine; 0; "sibling");
}

#[tokio::test]
async fn workflow_command_removed_recovered_config_still_delivers_cancellation() {
  let terminal = state("terminal");
  let mut command_rule = workflow_command_rule();
  let Some(Rule_type::MatchRunCommand(command_config)) = &mut command_rule.rule_type else {
    panic!("expected command matcher");
  };
  command_config
    .command_selector
    .as_mut()
    .unwrap()
    .arguments
    .insert("input".to_string(), DataValue::U64(7).into_proto());
  let command = state("command").declare_transition(&terminal, command_rule);
  let setup = Setup::new();
  let mut engine = setup
    .make_workflows_engine(WorkflowsEngineConfig::new_with_workflow_configurations(
      vec![WorkflowBuilder::new("workflow", &[&command, &terminal]).make_config()],
    ))
    .await;
  let now = datetime!(2026-01-01 00:00 UTC);
  let request = engine
    .process_log(timed_command_log("execute", now))
    .workflow_commands_to_start
    .pop()
    .unwrap();
  engine.maybe_persist(true).await;
  drop(engine);
  let setup = Setup::new_with_sdk_directory(&setup.sdk_directory);
  let mut engine = setup
    .make_workflows_engine(WorkflowsEngineConfig::new_with_workflow_configurations(
      vec![],
    ))
    .await;
  assert!(
    engine
      .engine
      .fail_recovered_workflow_commands(now)
      .is_empty()
  );
  assert_cancelled(
    &engine.engine.take_cancelled_workflow_commands(now),
    &request,
    CommandCancellationReason::SdkRestart,
    now,
  );
  assert!(engine.engine.needs_state_persistence);
}

#[tokio::test]
async fn workflow_command_accepted_terminal_is_not_cancelled_by_invalidation() {
  for session_change in [false, true] {
    let terminal = state("terminal");
    let command = state("command").declare_transition(&terminal, workflow_command_rule());
    let setup = Setup::new();
    let mut engine = setup
      .make_workflows_engine(WorkflowsEngineConfig::new_with_workflow_configurations(
        vec![WorkflowBuilder::new("workflow", &[&command, &terminal]).make_config()],
      ))
      .await;
    let now = datetime!(2026-01-01 00:00 UTC);
    let request = engine
      .process_log(timed_command_log("execute", now))
      .workflow_commands_to_start
      .pop()
      .unwrap();
    engine
      .engine
      .accept_workflow_command_completion(&request.completion_token())
      .unwrap();
    if session_change {
      engine.process_log(timed_command_log("new", now).with_session("new-session"));
    } else {
      engine
        .engine
        .update(WorkflowsEngineConfig::new_with_workflow_configurations(
          vec![],
        ));
    }
    assert!(
      engine
        .engine
        .take_cancelled_workflow_commands(now)
        .is_empty()
    );
  }
}
