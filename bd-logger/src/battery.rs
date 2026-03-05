// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

#[cfg(test)]
#[path = "./battery_test.rs"]
mod battery_test;

use crate::network::TimeProvider;
use bd_log_primitives::{
  AnnotatedLogField,
  AnnotatedLogFields,
  DataValue,
  LogInterceptor,
  LogLevel,
  LogMessage,
};
use bd_proto::protos::logging::payload::LogType;
use std::collections::VecDeque;
use std::sync::Arc;
use time::ext::NumericalDuration;

//
// BatteryDrainTracker
//

/// Tracks battery level changes over time by monitoring battery level changes in RESOURCE logs.
/// Calculates the percentage point change per minute and adds it to resource logs.
pub struct BatteryDrainTracker {
  container: parking_lot::Mutex<BatteryMetricsContainer>,
}

impl BatteryDrainTracker {
  pub fn new(time_provider: Arc<dyn TimeProvider>) -> Self {
    Self {
      container: parking_lot::Mutex::new(BatteryMetricsContainer::new(time_provider)),
    }
  }
}

impl BatteryDrainTracker {
  fn process_resource_log(&self, fields: &mut AnnotatedLogFields) {
    let battery_level = get_field_as_i32(fields, "_battery_level");

    let mut guard = self.container.lock();

    if let Some(battery_level) = battery_level {
      guard.add_sample(battery_level);
    }

    if let Some(drain_rate) = guard.get_level_change_per_minute() {
      fields.insert(
        "_battery_level_change_per_min".into(),
        AnnotatedLogField::new_ootb(format!("{drain_rate:.4}")),
      );
    }
  }
}

impl LogInterceptor for BatteryDrainTracker {
  fn process(
    &self,
    _log_level: LogLevel,
    log_type: LogType,
    msg: &LogMessage,
    fields: &mut AnnotatedLogFields,
    _matching_fields: &mut AnnotatedLogFields,
  ) {
    let LogMessage::String(msg) = msg else { return };

    if log_type == LogType::RESOURCE && msg.is_empty() {
      self.process_resource_log(fields);
    }
  }
}

//
// BatteryMetricsContainer
//

struct BatteryMetricsContainer {
  samples: VecDeque<BatterySample>,
  time_provider: Arc<dyn TimeProvider>,
}

impl BatteryMetricsContainer {
  fn new(time_provider: Arc<dyn TimeProvider>) -> Self {
    Self {
      samples: VecDeque::new(),
      time_provider,
    }
  }

  fn add_sample(&mut self, battery_level: i32) {
    let now = self.time_provider.now();

    self.samples.push_back(BatterySample {
      timestamp: now,
      battery_level,
    });
  }

  fn get_level_change_per_minute(&mut self) -> Option<f64> {
    let now = self.time_provider.now();
    let count_before = self.samples.len();

    while self
      .samples
      .front()
      .is_some_and(|sample| now.duration_since(sample.timestamp) > 1.minutes())
    {
      self.samples.pop_front();
    }

    if self.samples.len() >= count_before {
      return None;
    }

    let oldest = self.samples.front()?;
    let newest = self.samples.back()?;

    // Positive means draining, negative means charging.
    Some(f64::from(oldest.battery_level - newest.battery_level))
  }
}

//
// BatterySample
//

struct BatterySample {
  timestamp: std::time::Instant,
  battery_level: i32,
}

fn get_field_as_i32(fields: &AnnotatedLogFields, field_key: &str) -> Option<i32> {
  let value = fields.get(field_key)?;
  let string_value = match &value.value {
    DataValue::String(value) => value,
    DataValue::SharedString(value) => value.as_ref(),
    DataValue::StaticString(value) => value,
    DataValue::Bytes(_)
    | DataValue::Boolean(_)
    | DataValue::U64(_)
    | DataValue::I64(_)
    | DataValue::Double(_)
    | DataValue::Map(_)
    | DataValue::Array(_) => return None,
  };

  string_value.parse().ok()
}
