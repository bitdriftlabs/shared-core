// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

use bd_log_metadata::AnnotatedLogFields;
use bd_log_primitives::{AnnotatedLogField, LogInterceptor, LogLevel, LogMessage, LogType};
use bd_runtime::runtime::{debugging, BoolWatch, ConfigLoader};

//
// Reporter
//

pub struct Reporter {
  is_enabled_flag: BoolWatch<debugging::PeriodicInternalLoggingFlag>,

  state: parking_lot::Mutex<State>,
}

impl Reporter {
  pub fn new(runtime: &ConfigLoader) -> Self {
    Self {
      is_enabled_flag: runtime.register_watch().unwrap(),

      state: parking_lot::Mutex::new(State::default()),
    }
  }
}

impl LogInterceptor for Reporter {
  fn process(
    &self,
    _log_level: LogLevel,
    log_type: LogType,
    msg: &LogMessage,
    fields: &mut AnnotatedLogFields,
    _matching_fields: &mut AnnotatedLogFields,
  ) {
    let mut guard = self.state.lock();

    guard.logs_count += 1;
    guard.logs_total_count += 1;

    match log_type {
      LogType::Device => {
        guard.device_logs_count += 1;
        guard.device_logs_total_count += 1;
      },
      LogType::InternalSDK => {
        guard.internal_logs_count += 1;
        guard.internal_logs_total_count += 1;
      },
      LogType::Lifecycle => {
        guard.lifecycle_logs_count += 1;
        guard.lifecycle_logs_total_count += 1;
      },
      LogType::Span => {
        guard.network_logs_count += 1;
        guard.network_logs_total_count += 1;
      },
      LogType::Normal => {
        guard.normal_logs_count += 1;
        guard.normal_logs_total_count += 1;
      },
      LogType::Replay => {
        guard.replay_logs_count += 1;
        guard.replay_logs_total_count += 1;
      },
      LogType::Resource => {
        guard.resource_logs_count += 1;
        guard.resource_logs_total_count += 1;
      },
      _ => {},
    }

    let LogMessage::String(msg) = msg else { return };
    if !(log_type == LogType::Resource && msg.is_empty() && *self.is_enabled_flag.read()) {
      return;
    }

    fields.insert(
      "_logs_count".into(),
      create_field(guard.logs_count.to_string()),
    );
    fields.insert(
      "_logs_total_count".into(),
      create_field(guard.logs_total_count.to_string()),
    );
    fields.insert(
      "_device_logs_count".into(),
      create_field(guard.device_logs_count.to_string()),
    );
    fields.insert(
      "_device_logs_total_count".into(),
      create_field(guard.device_logs_total_count.to_string()),
    );
    fields.insert(
      "_internal_logs_count".into(),
      create_field(guard.internal_logs_count.to_string()),
    );
    fields.insert(
      "_internal_logs_total_count".into(),
      create_field(guard.internal_logs_total_count.to_string()),
    );
    fields.insert(
      "_network_logs_count".into(),
      create_field(guard.network_logs_count.to_string()),
    );
    fields.insert(
      "_network_logs_total_count".into(),
      create_field(guard.network_logs_total_count.to_string()),
    );
    fields.insert(
      "_normal_logs_count".into(),
      create_field(guard.normal_logs_count.to_string()),
    );
    fields.insert(
      "_normal_logs_total_count".into(),
      create_field(guard.normal_logs_total_count.to_string()),
    );
    fields.insert(
      "_lifecycle_logs_count".into(),
      create_field(guard.lifecycle_logs_count.to_string()),
    );
    fields.insert(
      "_lifecycle_logs_total_count".into(),
      create_field(guard.lifecycle_logs_total_count.to_string()),
    );
    fields.insert(
      "_replay_logs_count".into(),
      create_field(guard.replay_logs_count.to_string()),
    );
    fields.insert(
      "_replay_logs_total_count".into(),
      create_field(guard.replay_logs_total_count.to_string()),
    );
    fields.insert(
      "_resource_logs_count".into(),
      create_field(guard.resource_logs_count.to_string()),
    );
    fields.insert(
      "_resource_logs_total_count".into(),
      create_field(guard.resource_logs_total_count.to_string()),
    );

    guard.clear();
  }
}

fn create_field(value: String) -> AnnotatedLogField {
  AnnotatedLogField::new_ootb(value.into())
}

//
// State
//

#[allow(clippy::struct_field_names)]
#[derive(Default)]
struct State {
  logs_count: u32,
  logs_total_count: u32,

  device_logs_count: u32,
  device_logs_total_count: u32,

  internal_logs_count: u32,
  internal_logs_total_count: u32,

  network_logs_count: u32,
  network_logs_total_count: u32,

  normal_logs_count: u32,
  normal_logs_total_count: u32,

  lifecycle_logs_count: u32,
  lifecycle_logs_total_count: u32,

  replay_logs_count: u32,
  replay_logs_total_count: u32,

  resource_logs_count: u32,
  resource_logs_total_count: u32,
}

impl State {
  fn clear(&mut self) {
    self.device_logs_count = 0;
    self.internal_logs_count = 0;
    self.lifecycle_logs_count = 0;
    self.logs_count = 0;
    self.network_logs_count = 0;
    self.normal_logs_count = 0;
    self.replay_logs_count = 0;
    self.resource_logs_count = 0;
  }
}
