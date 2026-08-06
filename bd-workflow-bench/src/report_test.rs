// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.

#![allow(clippy::indexing_slicing, clippy::unwrap_used)]

use crate::report::{
  EngineOutcome,
  EngineOutcomeTotals,
  SlowestLogRecord,
  SlowestLogs,
  TimingSummary,
  truncate_message,
};

#[test]
fn calculates_nearest_rank_percentiles() {
  let summary = TimingSummary::from_durations(&[10, 20, 30, 40]);

  assert_eq!(10, summary.min_ns);
  assert_eq!(25, summary.mean_ns);
  assert_eq!(20, summary.p50_ns);
  assert_eq!(40, summary.p90_ns);
  assert_eq!(40, summary.p999_ns);
}

#[test]
fn retains_slowest_records() {
  let mut slowest = SlowestLogs::new(2);
  for record in [
    SlowestLogRecord {
      replay_iteration: 1,
      source_line: 1,
      log_id: "one".to_string(),
      evaluation_ns: 10,
      log_type: "NORMAL".to_string(),
      message: "one".to_string(),
      field_count: 1,
      engine_outcome: EngineOutcome::default(),
    },
    SlowestLogRecord {
      replay_iteration: 1,
      source_line: 2,
      log_id: "two".to_string(),
      evaluation_ns: 30,
      log_type: "SPAN".to_string(),
      message: "two".to_string(),
      field_count: 2,
      engine_outcome: EngineOutcome::default(),
    },
    SlowestLogRecord {
      replay_iteration: 1,
      source_line: 3,
      log_id: "three".to_string(),
      evaluation_ns: 20,
      log_type: "NORMAL".to_string(),
      message: "three".to_string(),
      field_count: 3,
      engine_outcome: EngineOutcome::default(),
    },
  ] {
    slowest.insert(record);
  }
  let slowest = slowest.records();

  assert_eq!(2, slowest.len());
  assert_eq!(30, slowest[0].evaluation_ns);
  assert_eq!(20, slowest[1].evaluation_ns);
  assert_eq!("SPAN", slowest[0].log_type);
  assert_eq!("two", slowest[0].message);
  assert_eq!(2, slowest[0].field_count);
}

#[test]
fn truncates_messages_at_one_hundred_unicode_characters() {
  let message = format!("{}界", "a".repeat(100));
  let truncated = truncate_message(&message);

  assert_eq!(100, truncated.chars().count());
  assert!(truncated.ends_with('…'));
}

#[test]
fn accumulates_engine_outcomes() {
  let outcome = EngineOutcome {
    started_new_session: true,
    tracing_active: true,
    tracing_state_changed: true,
    triggered_flush_action_count: 2,
    triggered_flush_buffer_count: 3,
    capture_screenshot: true,
    injected_log_count: 4,
    workflow_debug_state_count: 5,
    has_debug_workflows: true,
  };
  let mut totals = EngineOutcomeTotals::default();

  totals.observe(&outcome);

  assert_eq!(1, totals.session_start_count);
  assert_eq!(1, totals.tracing_state_change_count);
  assert_eq!(2, totals.triggered_flush_action_count);
  assert_eq!(3, totals.triggered_flush_buffer_count);
  assert_eq!(1, totals.screenshot_request_count);
  assert_eq!(4, totals.injected_log_count);
  assert_eq!(5, totals.workflow_debug_state_count);
  assert_eq!(1, totals.debug_workflow_evaluation_count);
}
