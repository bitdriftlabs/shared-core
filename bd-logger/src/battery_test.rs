// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

#![allow(clippy::unwrap_used)]

use crate::battery::BatteryDrainTracker;
use crate::network::TimeProvider;
use bd_log_primitives::{
  AnnotatedLogField,
  AnnotatedLogFields,
  LogInterceptor,
  LogMessage,
  log_level,
};
use bd_proto::protos::logging::payload::LogType;
use parking_lot::Mutex;
use std::sync::Arc;
use std::time::{Duration, Instant};
use time::ext::NumericalDuration;

struct MockTimeProvider {
  now: Mutex<Instant>,
}

impl MockTimeProvider {
  fn new() -> Self {
    Self {
      now: Mutex::new(Instant::now()),
    }
  }

  fn advance(&self, duration: Duration) {
    let mut now = self.now.lock();
    *now += duration;
  }
}

impl TimeProvider for MockTimeProvider {
  fn now(&self) -> Instant {
    *self.now.lock()
  }
}

fn create_resource_log_fields(battery_level: i32) -> AnnotatedLogFields {
  let mut fields = AnnotatedLogFields::new();
  fields.insert(
    "_battery_level".into(),
    AnnotatedLogField::new_ootb(battery_level.to_string()),
  );
  fields
}

fn process_resource_log(tracker: &BatteryDrainTracker, fields: &mut AnnotatedLogFields) {
  tracker.process(
    log_level::INFO,
    LogType::RESOURCE,
    &LogMessage::String(String::new()),
    fields,
    &mut AnnotatedLogFields::new(),
  );
}

fn get_drain_rate(fields: &AnnotatedLogFields) -> Option<f64> {
  fields
    .get("_battery_level_change_per_min")
    .and_then(|f| match &f.value {
      bd_log_primitives::DataValue::String(s) => s.parse().ok(),
      _ => None,
    })
}

#[test]
fn does_not_report_before_window_is_full() {
  let time_provider = Arc::new(MockTimeProvider::new());
  let tracker = BatteryDrainTracker::new_with_window(time_provider.clone(), 1.minutes());

  assert!(process_battery_level(&tracker, 100).is_none());

  // 30s later — still under 1 minute.
  assert!(
    advance_and_process_battery_level(&time_provider, &tracker, Duration::from_secs(30), 99)
      .is_none()
  );

  // 59s total — still under 1 minute.
  assert!(
    advance_and_process_battery_level(&time_provider, &tracker, Duration::from_secs(29), 98)
      .is_none()
  );
}

#[test]
fn reports_once_window_is_full() {
  let time_provider = Arc::new(MockTimeProvider::new());
  let tracker = BatteryDrainTracker::new_with_window(time_provider.clone(), 1.minutes());

  process_battery_level(&tracker, 100);

  advance_and_process_battery_level(&time_provider, &tracker, Duration::from_secs(30), 99);

  // Cross the 1-minute boundary — first sample gets pruned.
  let drain =
    advance_and_process_battery_level(&time_provider, &tracker, Duration::from_secs(31), 98)
      .unwrap();

  // Oldest remaining is 99, newest is 98 → delta = 1.
  assert!((drain - 1.0).abs() < 0.01);
}

#[test]
fn does_not_report_before_two_minute_window_is_full() {
  let time_provider = Arc::new(MockTimeProvider::new());
  let tracker = BatteryDrainTracker::new_with_window(time_provider.clone(), 2.minutes());

  assert!(process_battery_level(&tracker, 100).is_none());

  assert!(
    advance_and_process_battery_level(&time_provider, &tracker, Duration::from_secs(90), 97)
      .is_none()
  );

  assert!(
    advance_and_process_battery_level(&time_provider, &tracker, Duration::from_secs(29), 95)
      .is_none()
  );
}

#[test]
fn reports_once_two_minute_window_is_full() {
  let time_provider = Arc::new(MockTimeProvider::new());
  let tracker = BatteryDrainTracker::new_with_window(time_provider.clone(), 2.minutes());

  process_battery_level(&tracker, 100);

  assert!(
    advance_and_process_battery_level(&time_provider, &tracker, Duration::from_secs(70), 96)
      .is_none()
  );

  let drain =
    advance_and_process_battery_level(&time_provider, &tracker, Duration::from_secs(51), 92)
      .unwrap();
  assert!((drain - 4.0).abs() < 0.01);
}

#[test]
fn five_minute_window_keeps_older_samples_longer() {
  let time_provider = Arc::new(MockTimeProvider::new());
  let tracker = BatteryDrainTracker::new_with_window(time_provider.clone(), 5.minutes());

  process_battery_level(&tracker, 100);

  assert!(
    advance_and_process_battery_level(&time_provider, &tracker, Duration::from_secs(150), 94)
      .is_none()
  );

  let drain =
    advance_and_process_battery_level(&time_provider, &tracker, Duration::from_secs(151), 90)
      .unwrap();
  assert!((drain - 4.0).abs() < 0.01);
}

#[test]
fn reports_negative_change_when_charging() {
  let time_provider = Arc::new(MockTimeProvider::new());
  let tracker = BatteryDrainTracker::new_with_window(time_provider.clone(), 1.minutes());

  process_battery_level(&tracker, 50);

  advance_and_process_battery_level(&time_provider, &tracker, Duration::from_secs(30), 51);

  let drain =
    advance_and_process_battery_level(&time_provider, &tracker, Duration::from_secs(31), 52)
      .unwrap();

  // Oldest remaining is 51, newest is 52 → delta = -1.
  assert!((drain - (-1.0)).abs() < 0.01);
}

#[test]
fn large_jump_settles_after_window_rolls_over() {
  let time_provider = Arc::new(MockTimeProvider::new());
  let tracker = BatteryDrainTracker::new_with_window(time_provider.clone(), 1.minutes());

  // Build up samples at 100 for >1 minute so window becomes full.
  // First sample at t=6s, last at t=66s (11 samples).
  for _ in 0 .. 11 {
    time_provider.advance(Duration::from_secs(6));
    process_battery_level(&tracker, 100);
  }

  // t=72s — first sample (t=6s) is now 66s old and gets pruned.
  let drain =
    advance_and_process_battery_level(&time_provider, &tracker, Duration::from_secs(6), 100)
      .unwrap();
  assert!(drain.abs() < 0.01);

  // Jump to 20 on next tick (t=78s).
  let drain =
    advance_and_process_battery_level(&time_provider, &tracker, Duration::from_secs(6), 20)
      .unwrap();
  // Oldest remaining is 100, newest is 20 → delta = 80.
  assert!((drain - 80.0).abs() < 0.01);

  // After the window fully rolls over with samples at 20, rate settles to 0.
  for _ in 0 .. 11 {
    time_provider.advance(Duration::from_secs(6));
    process_battery_level(&tracker, 20);
  }

  let drain =
    advance_and_process_battery_level(&time_provider, &tracker, Duration::from_secs(6), 20)
      .unwrap();
  assert!(drain.abs() < 0.01, "expected ~0, got {drain:.4}");
}

#[test]
fn ignores_non_resource_logs() {
  let time_provider = Arc::new(MockTimeProvider::new());
  let tracker = BatteryDrainTracker::new_with_window(time_provider, 1.minutes());

  let mut fields = create_resource_log_fields(75);

  tracker.process(
    log_level::INFO,
    LogType::NORMAL,
    &LogMessage::String("some message".to_string()),
    &mut fields,
    &mut AnnotatedLogFields::new(),
  );

  assert!(get_drain_rate(&fields).is_none());
}

#[test]
fn handles_missing_battery_fields() {
  let time_provider = Arc::new(MockTimeProvider::new());
  let tracker = BatteryDrainTracker::new_with_window(time_provider.clone(), 1.minutes());

  let mut fields = AnnotatedLogFields::new();
  process_resource_log(&tracker, &mut fields);
  assert!(get_drain_rate(&fields).is_none());

  time_provider.advance(Duration::from_secs(61));

  let mut fields = AnnotatedLogFields::new();
  process_resource_log(&tracker, &mut fields);
  assert!(get_drain_rate(&fields).is_none());
}

#[test]
fn steady_drain_reports_correct_rate() {
  let time_provider = Arc::new(MockTimeProvider::new());
  let tracker = BatteryDrainTracker::new_with_window(time_provider.clone(), 1.minutes());

  // 1 point every 6 seconds for 66 seconds (11 ticks).
  for i in 0 .. 11 {
    time_provider.advance(Duration::from_secs(6));
    process_battery_level(&tracker, 100 - i);
  }

  // Cross the 1-minute mark.
  let drain =
    advance_and_process_battery_level(&time_provider, &tracker, Duration::from_secs(6), 89)
      .unwrap();

  // Window spans ~60s of data, first sample (100) was pruned.
  // Oldest remaining is 99, newest is 89 → delta = 10.
  assert!((drain - 10.0).abs() < 0.5);
}

fn process_battery_level(tracker: &BatteryDrainTracker, battery_level: i32) -> Option<f64> {
  let mut fields = create_resource_log_fields(battery_level);
  process_resource_log(tracker, &mut fields);
  get_drain_rate(&fields)
}

fn advance_and_process_battery_level(
  time_provider: &MockTimeProvider,
  tracker: &BatteryDrainTracker,
  duration: Duration,
  battery_level: i32,
) -> Option<f64> {
  time_provider.advance(duration);
  process_battery_level(tracker, battery_level)
}
