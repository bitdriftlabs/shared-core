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
fn returns_none_with_single_sample() {
  let time_provider = Arc::new(MockTimeProvider::new());
  let tracker = BatteryDrainTracker::new(time_provider);

  let mut fields = create_resource_log_fields(75);
  process_resource_log(&tracker, &mut fields);

  assert!(get_drain_rate(&fields).is_none());
}

#[test]
fn calculates_drain_with_two_samples() {
  let time_provider = Arc::new(MockTimeProvider::new());
  let tracker = BatteryDrainTracker::new(time_provider.clone());

  let mut fields1 = create_resource_log_fields(75);
  process_resource_log(&tracker, &mut fields1);

  time_provider.advance(Duration::from_secs(30));

  let mut fields2 = create_resource_log_fields(74);
  process_resource_log(&tracker, &mut fields2);

  let drain = get_drain_rate(&fields2).unwrap();
  // Battery dropped 1% over 30s = 2% per min.
  assert!((drain - 2.0).abs() < 0.01);
}

#[test]
fn reports_negative_change_when_battery_increases() {
  let time_provider = Arc::new(MockTimeProvider::new());
  let tracker = BatteryDrainTracker::new(time_provider.clone());

  let mut fields1 = create_resource_log_fields(50);
  process_resource_log(&tracker, &mut fields1);

  time_provider.advance(Duration::from_secs(60));

  let mut fields2 = create_resource_log_fields(54);
  process_resource_log(&tracker, &mut fields2);

  let change = get_drain_rate(&fields2).unwrap();
  // Battery increased 4% over 60s = -4% per min.
  assert!((change - (-4.0)).abs() < 0.01);
}

#[test]
fn prunes_old_samples() {
  let time_provider = Arc::new(MockTimeProvider::new());
  let tracker = BatteryDrainTracker::new(time_provider.clone());

  let mut fields1 = create_resource_log_fields(80);
  process_resource_log(&tracker, &mut fields1);

  time_provider.advance(Duration::from_secs(30));

  let mut fields2 = create_resource_log_fields(79);
  process_resource_log(&tracker, &mut fields2);

  time_provider.advance(Duration::from_secs(40));

  let mut fields3 = create_resource_log_fields(78);
  process_resource_log(&tracker, &mut fields3);

  // Total time is 70 seconds, first sample should be pruned.
  // Remaining: fields2 (79) to fields3 (78) = 1% over 40s = 1.5% per min.
  let drain = get_drain_rate(&fields3).unwrap();
  assert!((drain - 1.5).abs() < 0.01);
}

#[test]
fn ignores_non_resource_logs() {
  let time_provider = Arc::new(MockTimeProvider::new());
  let tracker = BatteryDrainTracker::new(time_provider);

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
  let tracker = BatteryDrainTracker::new(time_provider.clone());

  let mut fields = AnnotatedLogFields::new();
  process_resource_log(&tracker, &mut fields);

  assert!(get_drain_rate(&fields).is_none());

  time_provider.advance(Duration::from_secs(30));

  let mut fields2 = AnnotatedLogFields::new();
  process_resource_log(&tracker, &mut fields2);

  assert!(get_drain_rate(&fields2).is_none());
}
