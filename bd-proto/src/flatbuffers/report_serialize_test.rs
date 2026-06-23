// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

use crate::flatbuffers::report::bitdrift_public::fbs::issue_reporting::v_1::*;
use serde::Serialize;
use serde_json::json;

// Quick and dirty means to determining whether the structures have changed
// since the last time the serialization logic was updated to avoid breakage
macro_rules! assert_struct_size {
  ($name:ident, $value:expr) => {
    assert_eq!(
      core::mem::size_of::<$name>(),
      $value,
      concat!(
        "src/flatbuffers/report_serialize.rs needs to be updated for ",
        stringify!($name),
      )
    )
  };
}

fn serialization_loop<T: Sized + Serialize>(object: &T) -> serde_json::Value {
  let serialized = serde_json::to_string(&object).unwrap();
  serde_json::from_str(&serialized).unwrap()
}

#[test]
fn serialize_report_type() {
  let object = serialization_loop(&ReportType::JVMCrash);
  assert_eq!(json!(3), object);
}

#[test]
fn serialize_platform() {
  let object = serialization_loop(&Platform::Android);
  assert_eq!(json!(1), object);
}

#[test]
fn serialize_arch() {
  let object = serialization_loop(&Architecture::arm64);
  assert_eq!(json!(2), object);
}

#[test]
fn serialize_frame_type() {
  let object = serialization_loop(&FrameType::AndroidNative);
  assert_eq!(json!(3), object);
}

#[test]
fn serialize_error_relation() {
  let object = serialization_loop(&ErrorRelation::CausedBy);
  assert_eq!(json!(1), object);
}

#[test]
fn serialize_power_state() {
  let object = serialization_loop(&PowerState::PluggedInCharged);
  assert_eq!(json!(4), object);
}

#[test]
fn serialize_network_state() {
  let object = serialization_loop(&NetworkState::Disconnected);
  assert_eq!(json!(1), object);
}

#[test]
fn serialize_js_engine() {
  let object = serialization_loop(&JavaScriptEngine::Hermes);
  assert_eq!(json!(2), object);
}

#[test]
fn serialize_mem_pressure_level() {
  let object = serialization_loop(&MemoryPressureLevel::Critical);
  assert_eq!(json!(3), object);
}

#[test]
fn serialize_rotation() {
  let object = serialization_loop(&Rotation::LandscapeLeft);
  assert_eq!(json!(3), object);
}

#[test]
fn serialize_frame_status() {
  let object = serialization_loop(&FrameStatus::Symbolicated);
  assert_eq!(json!(1), object);
}

#[test]
fn serialize_timestamp() {
  let object = serialization_loop(&Timestamp::new(1830254100, 27186761));
  assert_eq!(json!({"nanos": 27186761, "seconds": 1830254100}), object);
}

#[test]
fn serialize_memory() {
  let object = serialization_loop(&Memory::new(256, 19, 237));
  assert_eq!(json!({"total": 256, "free": 19, "used": 237}), object);
  assert_struct_size!(Memory, 24);
}

macro_rules! build_table {
  ($builder:ident, $typename:ident, $args:expr) => {{
    let args = $args;
    let input = $typename::create(&mut $builder, args);
    $builder.finish(input, None);
    flatbuffers::root::<$typename<'_>>($builder.finished_data()).unwrap()
  }};
}

#[test]
fn serialize_version_code() {
  let mut builder = flatbuffers::FlatBufferBuilder::new();
  let input = build_table!(
    builder,
    AppBuildNumber,
    &AppBuildNumberArgs {
      version_code: 56,
      ..Default::default()
    }
  );
  let object = serialization_loop(&input);
  assert_eq!(json!({"version_code": 56}), object);
  assert_struct_size!(AppBuildNumberArgs, 16);
}

#[test]
fn serialize_bundle_version() {
  let mut builder = flatbuffers::FlatBufferBuilder::new();
  let object = serialization_loop(&build_table!(
    builder,
    AppBuildNumber,
    &AppBuildNumberArgs {
      cf_bundle_version: Some(builder.create_string("4.5.22")),
      ..Default::default()
    }
  ));
  assert_eq!(json!({"cf_bundle_version": "4.5.22"}), object);
}

#[test]
fn serialize_feature_flag() {
  let mut builder = flatbuffers::FlatBufferBuilder::new();
  let object = serialization_loop(&build_table!(
    builder,
    FeatureFlag,
    &FeatureFlagArgs {
      name: Some(builder.create_string("option_a")),
      value: Some(builder.create_string("variant b")),
      ..Default::default()
    }
  ));
  assert_eq!(json!({"name": "option_a", "value": "variant b"}), object);
  assert_struct_size!(FeatureFlagArgs, 24);
}

#[test]
fn serialize_proc_usage() {
  let mut builder = flatbuffers::FlatBufferBuilder::new();
  let object = serialization_loop(&build_table!(
    builder,
    ProcessorUsage,
    &ProcessorUsageArgs {
      duration_seconds: 367,
      used_percent: 81,
    }
  ));
  assert_eq!(json!({"duration_seconds": 367, "used_percent": 81}), object);
  assert_struct_size!(ProcessorUsageArgs, 16);
}

#[test]
fn serialize_app_metrics() {
  let mut builder = flatbuffers::FlatBufferBuilder::new();
  let object = serialization_loop(&build_table!(
    builder,
    AppMetrics,
    &AppMetricsArgs {
      app_id: Some(builder.create_string("co.example.someapp")),
      memory: Some(&Memory::new(256, 19, 237)),
      region_format: Some(builder.create_string("MQ")),
      ..Default::default()
    }
  ));
  assert_eq!(
    json!({
      "app_id":"co.example.someapp",
      "region_format": "MQ",
      "memory": {"total": 256, "free": 19, "used": 237},
      "javascript_engine": 0,
      "memory_pressure_level": 0,
      "process_id": 0,
    }),
    object
  );
  assert_struct_size!(AppMetricsArgs, 72);
}

#[test]
fn serialize_error() {
  let mut builder = flatbuffers::FlatBufferBuilder::new();
  let args = &FrameArgs {
    type_: FrameType::JVM,
    symbol_name: Some(builder.create_string("Builder.make")),
    in_app: true,
    frame_address: 81716715,
    symbol_address: 961261,
    original_index: 1,
    ..Default::default()
  };
  let frame = Frame::create(&mut builder, args);
  let object = serialization_loop(&build_table!(
    builder,
    Error,
    &ErrorArgs {
      name: Some(builder.create_string("MissingArgumentError")),
      reason: Some(builder.create_string("required argument 'x' was unset")),
      stack_trace: Some(builder.create_vector(&[frame])),
      relation_to_next: ErrorRelation::CausedBy,
    }
  ));
  assert_eq!(
    json!({
      "name": "MissingArgumentError",
      "reason": "required argument 'x' was unset",
      "relation_to_next": 1,
      "stack_trace": [{
        "type": 1,
        "symbol_name": "Builder.make",
        "in_app": true,
        "frame_address": 81716715,
        "symbol_address": 961261,
        "frame_status": 0,
        "original_index": 1,
      }],
    }),
    object
  );
  assert_struct_size!(ErrorArgs, 28);
}

#[test]
fn serialize_source_file() {
  let mut builder = flatbuffers::FlatBufferBuilder::new();
  let object = serialization_loop(&build_table!(
    builder,
    SourceFile,
    &SourceFileArgs {
      path: Some(builder.create_string("src/tmp.cpp")),
      line: 189,
      column: 13,
    }
  ));
  assert_eq!(
    json!({
      "path": "src/tmp.cpp",
      "line": 189,
      "column": 13,
    }),
    object
  );
  assert_struct_size!(SourceFileArgs, 24);
}

#[test]
fn serialize_frame() {
  let mut builder = flatbuffers::FlatBufferBuilder::new();
  let object = serialization_loop(&build_table!(
    builder,
    Frame,
    &FrameArgs {
      type_: FrameType::JVM,
      symbol_name: Some(builder.create_string("Builder.make")),
      in_app: true,
      frame_address: 81716715,
      symbol_address: 961261,
      ..Default::default()
    }
  ));
  assert_eq!(
    json!({
      "type": 1,
      "symbol_name": "Builder.make",
      "in_app": true,
      "frame_address": 81716715,
      "symbol_address": 961261,
      "frame_status": 0,
      "original_index": 0,
    }),
    object
  );
  assert_struct_size!(FrameArgs, 96);
}

#[test]
fn serialize_report() {
  let mut builder = flatbuffers::FlatBufferBuilder::new();
  let app_id = Some(builder.create_string("co.example.someapp"));
  let app_metrics = Some(AppMetrics::create(
    &mut builder,
    &AppMetricsArgs {
      app_id,
      ..Default::default()
    },
  ));
  let device_metrics = Some(DeviceMetrics::create(
    &mut builder,
    &DeviceMetricsArgs {
      network_state: NetworkState::Disconnected,
      thermal_state: 5,
      platform: Platform::macOS,
      rotation: Rotation::LandscapeLeft,
      ..Default::default()
    },
  ));
  let frame = Frame::create(
    &mut builder,
    &FrameArgs {
      type_: FrameType::JVM,
      in_app: true,
      frame_address: 81716715,
      symbol_address: 961261,
      original_index: 1,
      ..Default::default()
    },
  );
  let stack_trace = Some(builder.create_vector(&[frame]));
  let error = Error::create(
    &mut builder,
    &ErrorArgs {
      stack_trace,
      relation_to_next: ErrorRelation::CausedBy,
      ..Default::default()
    },
  );
  let object = serialization_loop(&build_table!(
    builder,
    Report,
    &ReportArgs {
      type_: ReportType::AppNotResponding,
      app_metrics,
      device_metrics,
      errors: Some(builder.create_vector(&[error])),
      ..Default::default()
    }
  ));
  assert_eq!(
    json!({
      "type": 1,
      "app_metrics": {
        "app_id": "co.example.someapp",
        "javascript_engine": 0,
        "memory_pressure_level": 0,
        "process_id": 0,
      },
      "device_metrics": {
        "thermal_state": 5,
        "rotation": 3,
        "network_state": 1,
        "arch": 0,
        "platform": 3,
        "low_power_mode_enabled": false,
      },
      "errors": [{
        "relation_to_next": 1,
        "stack_trace": [{
          "type": 1,
          "in_app": true,
          "frame_address": 81716715,
          "symbol_address": 961261,
          "frame_status": 0,
          "original_index": 1,
        }],
      }],
    }),
    object
  );
  assert_struct_size!(ReportArgs, 68);
}

#[test]
fn serialize_thread() {
  let mut builder = flatbuffers::FlatBufferBuilder::new();
  let object = serialization_loop(&build_table!(
    builder,
    Thread,
    &ThreadArgs {
      name: Some(builder.create_string("main")),
      index: 12,
      active: true,
      priority: 3.0,
      quality_of_service: 5,
      summary: Some(builder.create_string("locked")),
      ..Default::default()
    }
  ));
  assert_eq!(
    json!({
      "name": "main",
      "index": 12,
      "active": true,
      "priority": 3.0,
      "quality_of_service": 5,
      "summary": "locked",
    }),
    object
  );
  assert_struct_size!(ThreadArgs, 44);
}

#[test]
fn serialize_thread_details() {
  let mut builder = flatbuffers::FlatBufferBuilder::new();
  let args = &ThreadArgs {
    name: Some(builder.create_string("main")),
    index: 12,
    active: true,
    priority: 3.0,
    quality_of_service: 5,
    summary: Some(builder.create_string("locked")),
    ..Default::default()
  };
  let thread = Thread::create(&mut builder, args);
  let object = serialization_loop(&build_table!(
    builder,
    ThreadDetails,
    &ThreadDetailsArgs {
      count: 12,
      threads: Some(builder.create_vector(&[thread]))
    }
  ));
  assert_eq!(
    json!({
      "count": 12,
        "threads": [{
          "name": "main",
          "index": 12,
          "active": true,
          "priority": 3.0,
          "quality_of_service": 5,
          "summary": "locked",
        }],
    }),
    object
  );
  assert_struct_size!(ThreadDetailsArgs, 12);
}

#[test]
fn serialize_device_metrics() {
  let mut builder = flatbuffers::FlatBufferBuilder::new();
  let power_metrics = PowerMetrics::create(
    &mut builder,
    &PowerMetricsArgs {
      power_state: PowerState::PluggedInCharging,
      charge_percent: 15,
    },
  );
  let object = serialization_loop(&build_table!(
    builder,
    DeviceMetrics,
    &DeviceMetricsArgs {
      timezone: Some(builder.create_string("AS")),
      power_metrics: Some(power_metrics),
      network_state: NetworkState::Disconnected,
      thermal_state: 5,
      platform: Platform::macOS,
      rotation: Rotation::LandscapeLeft,
      ..Default::default()
    }
  ));
  assert_eq!(
    json!({
      "timezone":"AS",
      "power_metrics": {"power_state": 3, "charge_percent": 15},
      "thermal_state": 5,
      "rotation": 3,
      "network_state": 1,
      "arch": 0,
      "platform": 3,
      "low_power_mode_enabled": false,
    }),
    object
  );
  assert_struct_size!(DeviceMetricsArgs, 80);
}
