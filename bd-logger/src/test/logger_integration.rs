// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

use super::setup::Setup;
use crate::logger::{Block, CaptureSession};
use crate::test::setup::SetupOptions;
use crate::{
  AnnotatedLogField,
  AppVersionExtra,
  InitParams,
  LogAttributesOverrides,
  LogMessage,
  StringOrBytes,
  log_level,
  wait_for,
};
use assert_matches::assert_matches;
use bd_error_reporter::reporter::UnexpectedErrorHandler;
use bd_log_matcher::builder::{field_equals, message_equals};
use bd_log_metadata::LogFields;
use bd_log_primitives::AnnotatedLogFields;
use bd_noop_network::NoopNetwork;
use bd_proto::protos::bdtail::bdtail_config::{BdTailConfigurations, BdTailStream};
use bd_proto::protos::client::api::configuration_update::StateOfTheWorld;
use bd_proto::protos::client::api::debug_data_request::WorkflowTransitionDebugData;
use bd_proto::protos::client::api::{DebugDataRequest, debug_data_request};
use bd_proto::protos::config::v1::config::BufferConfigList;
use bd_proto::protos::config::v1::config::buffer_config::Type;
use bd_proto::protos::filter::filter::Filter;
use bd_proto::protos::logging::payload::LogType;
use bd_proto::protos::logging::payload::log::CompressedContents;
use bd_runtime::runtime::FeatureFlag;
use bd_runtime::runtime::log_upload::MinLogCompressionSize;
use bd_session::fixed::{State, UUIDCallbacks};
use bd_session::{Strategy, fixed};
use bd_session_replay::SESSION_REPLAY_SCREENSHOT_LOG_MESSAGE;
use bd_stats_common::labels;
use bd_test_helpers::config_helper::{
  self,
  BufferConfigBuilder,
  ConfigurationUpdateParts,
  configuration_update,
  configuration_update_from_parts,
  default_buffer_config,
  invalid_configuration,
  make_buffer_matcher_matching_everything,
  make_buffer_matcher_matching_everything_except_internal_logs,
  make_buffer_matcher_matching_resource_logs,
  make_configuration_update_with_workflow_flushing_buffer_on_anything,
  make_workflow_config_flushing_buffer,
  match_message,
};
use bd_test_helpers::metadata::EmptyMetadata;
use bd_test_helpers::metadata_provider::LogMetadata;
use bd_test_helpers::resource_utilization::EmptyTarget;
use bd_test_helpers::runtime::{ValueKind, make_update};
use bd_test_helpers::session::in_memory_store;
use bd_test_helpers::stats::StatsRequestHelper;
use bd_test_helpers::test_api_server::StreamAction;
use bd_test_helpers::workflow::macros::rule;
use bd_test_helpers::workflow::{
  TestFieldRef,
  TestFieldType,
  WorkflowBuilder,
  extract_log_body_tag,
  extract_metric_tag,
  extract_metric_value,
  make_emit_counter_action,
  make_emit_histogram_action,
  make_flush_buffers_action,
  make_generate_log_action_proto,
  make_save_timestamp_extraction,
  make_state_change_rule,
  make_take_screenshot_action,
  metric_tag,
  metric_value,
  state,
};
use bd_test_helpers::{RecordingErrorReporter, field_value, set_field};
use bd_time::test::TestTicker;
use bd_time::{OffsetDateTimeExt, TestTimeProvider};
use debug_data_request::workflow_transition_debug_data::Transition_type;
use debug_data_request::{WorkflowDebugData, WorkflowStateDebugData};
use flate2::write::ZlibDecoder;
use parking_lot::Mutex;
use pretty_assertions::assert_eq;
use protobuf::Message;
use std::io::Write;
use std::ops::Add;
use std::sync::Arc;
use std::time::Instant;
use time::OffsetDateTime;
use time::ext::{NumericalDuration, NumericalStdDuration};
use time::macros::datetime;

#[test]
fn sleep_mode() {
  let mut setup = Setup::new_with_options(SetupOptions {
    start_in_sleep_mode: true,
    ..Default::default()
  });

  setup.logger_handle.transition_sleep_mode(false);
  setup.restart_stream(false);

  setup.logger_handle.transition_sleep_mode(true);
  setup.restart_stream(true);

  setup.flush_and_upload_stats();
  let stat_upload = StatsRequestHelper::new(setup.server.next_stat_upload().unwrap());
  assert_eq!(
    stat_upload.get_counter("sleep:transitions", labels! { "state" => "enabled" }),
    Some(1),
  );
  assert_eq!(
    stat_upload.get_counter("sleep:transitions", labels! { "state" => "disabled" }),
    Some(1),
  );
}

#[test]
fn attributes_accessors() {
  let setup = Setup::new();

  assert_eq!(36, setup.logger_handle.session_id().len());
  assert_eq!(36, setup.logger_handle.device_id().len());
}

#[test]
fn logger_api() {
  // Test basic handshaking which is handled by Setup.
  let mut setup = Setup::new();
  let nack = setup.send_configuration_update(
    make_configuration_update_with_workflow_flushing_buffer_on_anything(
      "default",
      Type::CONTINUOUS,
    ),
  );

  assert!(nack.is_none());

  // Verify that the buffer is written to the right location.
  assert!(setup.sdk_directory.path().join("buffers/default").exists());
}

#[test]
fn log_upload() {
  let mut setup = Setup::new();
  setup.send_configuration_update(
    make_configuration_update_with_workflow_flushing_buffer_on_anything(
      "default",
      Type::CONTINUOUS,
    ),
  );

  // TODO(snowp): Either figure out how to use test time or make the
  // intervals configurable so we can avoid having to log a full batch.
  for _ in 0 .. 10 {
    setup.log(
      log_level::DEBUG,
      LogType::NORMAL,
      "some log".into(),
      [].into(),
      [].into(),
      None,
    );
  }

  assert_matches!(setup.server.blocking_next_log_upload(), Some(log_upload) => {
    assert_eq!(log_upload.buffer_id(), "default");
    uuid::Uuid::parse_str(log_upload.upload_uuid()).unwrap();
    assert_eq!(log_upload.logs().len(), 10);

    assert_eq!(log_upload.logs()[0].message(), "some log");
  });
}

#[test]
fn log_upload_with_compression() {
  let mut setup = Setup::new();
  setup.send_configuration_update(
    make_configuration_update_with_workflow_flushing_buffer_on_anything(
      "default",
      Type::CONTINUOUS,
    ),
  );
  let mut runtime_values = Setup::get_default_runtime_values();
  runtime_values.push((MinLogCompressionSize::path(), ValueKind::Int(4096)));

  setup
    .current_api_stream()
    .blocking_stream_action(StreamAction::SendRuntime(make_update(
      runtime_values,
      "version".to_string(),
    )));
  let (_, response) = setup.server.blocking_next_runtime_ack();
  assert!(response.nack.is_none());

  for i in 0 .. 10 {
    setup.log(
      log_level::DEBUG,
      LogType::NORMAL,
      "some log".into(),
      AnnotatedLogFields::from([(
        "super_long".into(),
        AnnotatedLogField::new_custom("A".repeat((i + 1) * 1024)),
      )]),
      [].into(),
      None,
    );
  }

  assert_matches!(setup.server.blocking_next_log_upload(), Some(log_upload) => {
    assert_eq!(log_upload.buffer_id(), "default");
    uuid::Uuid::parse_str(log_upload.upload_uuid()).unwrap();
    assert_eq!(log_upload.logs().len(), 10);

    // With the 4KiB limit we expect the first three logs to be inline an the rest compressed.
    for i in 0.. 10 {
      if i < 3 {
        assert_eq!(log_upload.logs()[i].message(), "some log");
        assert!(log_upload.logs()[i].field("super_long") == "A".repeat((i + 1) * 1024));
      } else {
        assert!(!log_upload.logs()[i].has_message());
        assert!(!log_upload.logs()[i].has_field("super_long"));

        let decoded = Vec::new();
        let mut decoder = ZlibDecoder::new(decoded);
        decoder.write_all(log_upload.logs()[i].compressed_contents()).unwrap();
        let decoded = decoder.finish().unwrap();
        let compressed_contents = CompressedContents::parse_from_bytes(&decoded).unwrap();
        assert_eq!(compressed_contents.message.string_data(), "some log");
        assert_eq!(compressed_contents.fields[0].key, "super_long");
        assert_eq!(
          compressed_contents.fields[0].value.string_data(),
          "A".repeat((i + 1) * 1024)
        );
      }
    }
  });
}

#[test]
fn explicit_session_capture() {
  let mut setup = Setup::new();
  assert!(
    setup
      .send_configuration_update(config_helper::configuration_update_from_parts(
        "",
        ConfigurationUpdateParts {
          buffer_config: vec![
            default_buffer_config(
              Type::CONTINUOUS,
              make_buffer_matcher_matching_resource_logs().into(),
            ),
            BufferConfigBuilder {
              name: "trigger_buffer_id",
              buffer_type: Type::TRIGGER,
              filter: make_buffer_matcher_matching_everything_except_internal_logs().into(),
              non_volatile_size: 100_000,
              volatile_size: 10_000,
            }
            .build(),
          ],
          workflows: vec![],
          ..Default::default()
        },
      ))
      .is_none()
  );

  setup.send_runtime_update();

  for _ in 0 .. 9 {
    setup.log(
      log_level::DEBUG,
      LogType::NORMAL,
      "some log".into(),
      [].into(),
      [].into(),
      None,
    );
  }

  setup.log_with_session_capture(
    log_level::DEBUG,
    LogType::NORMAL,
    "some log".into(),
    [].into(),
    [].into(),
  );

  assert_matches!(setup.server.blocking_next_log_upload(), Some(log_upload) => {
    assert_eq!(log_upload.buffer_id(), "trigger_buffer_id");
    uuid::Uuid::parse_str(log_upload.upload_uuid()).unwrap();
    assert_eq!(log_upload.logs().len(), 10);

    assert_eq!(log_upload.logs()[0].message(), "some log");
  });

  // Verify that we start streaming logs.
  for _ in 0 .. 100 {
    setup.log(
      log_level::DEBUG,
      LogType::NORMAL,
      "some log".into(),
      [].into(),
      [].into(),
      None,
    );
  }

  assert_matches!(setup.server.blocking_next_log_upload(), Some(log_upload) => {
    assert_eq!(log_upload.buffer_id(), "default");
    uuid::Uuid::parse_str(log_upload.upload_uuid()).unwrap();
    assert_eq!(log_upload.logs().len(), 10);

    assert_eq!(log_upload.logs()[0].message(), "some log");
  });
}

#[test]
fn explicit_session_capture_disabled_streaming() {
  let mut setup = Setup::new();
  assert!(
    setup
      .send_configuration_update(config_helper::configuration_update_from_parts(
        "",
        ConfigurationUpdateParts {
          buffer_config: vec![
            default_buffer_config(
              Type::CONTINUOUS,
              make_buffer_matcher_matching_resource_logs().into(),
            ),
            BufferConfigBuilder {
              name: "trigger_buffer_id",
              buffer_type: Type::TRIGGER,
              filter: make_buffer_matcher_matching_everything_except_internal_logs().into(),
              non_volatile_size: 100_000,
              volatile_size: 10_000,
            }
            .build(),
          ],
          workflows: vec![],
          ..Default::default()
        },
      ))
      .is_none()
  );

  let mut runtime_values = Setup::get_default_runtime_values();
  runtime_values.push((
    bd_runtime::runtime::session_capture::StreamingLogCount::path(),
    ValueKind::Int(0),
  ));

  setup
    .current_api_stream()
    .blocking_stream_action(StreamAction::SendRuntime(make_update(
      runtime_values,
      "version".to_string(),
    )));

  for _ in 0 .. 9 {
    setup.log(
      log_level::DEBUG,
      LogType::NORMAL,
      "some log".into(),
      [].into(),
      [].into(),
      None,
    );
  }

  setup.log_with_session_capture(
    log_level::DEBUG,
    LogType::NORMAL,
    "some log".into(),
    [].into(),
    [].into(),
  );

  assert_matches!(setup.server.blocking_next_log_upload(), Some(log_upload) => {
    assert_eq!(log_upload.buffer_id(), "trigger_buffer_id");
    uuid::Uuid::parse_str(log_upload.upload_uuid()).unwrap();
    assert_eq!(log_upload.logs().len(), 10);

    assert_eq!(log_upload.logs()[0].message(), "some log");
  });

  // Verify that we do not stream further logs.
  assert_matches!(setup.server.blocking_next_log_upload(), None);
}

#[test]
fn log_upload_attributes_override() {
  let time_first = datetime!(2024-06-01 12:00:00 UTC);
  let mut setup = Setup::new_with_metadata(Arc::new(LogMetadata {
    timestamp: Mutex::new(time_first),
    ootb_fields: LogFields::from([("_some_version".into(), "400".into())]),
    ..Default::default()
  }));

  setup.send_configuration_update(
    make_configuration_update_with_workflow_flushing_buffer_on_anything(
      "default",
      Type::CONTINUOUS,
    ),
  );

  let time_second = datetime!(2024-06-01 13:00:00 UTC);

  let error_reporter = Arc::new(RecordingErrorReporter::default());
  UnexpectedErrorHandler::set_reporter(error_reporter);

  setup.store.set(
    &fixed::STATE_KEY,
    &State {
      session_id: "foo_overridden".to_string(),
    },
  );

  // This log should end up being emitted with an overridden session ID and timestamp,
  // and no fields associated with the current session
  setup.log(
    log_level::DEBUG,
    LogType::NORMAL,
    "log with overridden attributes".into(),
    [].into(),
    [].into(),
    Some(LogAttributesOverrides::PreviousRunSessionID(time_second)),
  );

  // This log should end up being emitted with an overridden session ID, timestamp,
  // and fields
  setup.log(
    log_level::DEBUG,
    LogType::NORMAL,
    "log with overridden attributes and fields".into(),
    AnnotatedLogFields::from([("_some_version".into(), AnnotatedLogField::new_ootb("350"))]),
    [].into(),
    Some(LogAttributesOverrides::PreviousRunSessionID(time_second)),
  );

  for _ in 0 .. 7 {
    setup.log(
      log_level::DEBUG,
      LogType::NORMAL,
      "some log".into(),
      [].into(),
      [].into(),
      None,
    );
  }

  // This log should end up with a custom occurred_at
  setup.log(
    log_level::DEBUG,
    LogType::NORMAL,
    "override time only".into(),
    [].into(),
    [].into(),
    Some(LogAttributesOverrides::OccurredAt(time_first)),
  );

  assert_matches!(setup.server.blocking_next_log_upload(), Some(log_upload) => {
    assert_eq!(log_upload.buffer_id(), "default");
    uuid::Uuid::parse_str(log_upload.upload_uuid()).unwrap();
    assert_eq!(log_upload.logs().len(), 10);

    // Confirm both session ID and timestamp are overridden.
    let first_uploaded_log = &log_upload.logs()[0];
    assert_eq!(first_uploaded_log.session_id(), "foo_overridden");
    assert_eq!(first_uploaded_log.timestamp(), time_second);
    assert_eq!(first_uploaded_log.field("_logged_at"), time_first.to_string());
    assert_eq!(first_uploaded_log.message(), "log with overridden attributes");
    // Confirm provider fields are not added to previous session logs
    assert!(!first_uploaded_log.has_field("_some_version"));

    // Confirm that session ID, timestamp, and fields are overridden
    let second_uploaded_log = &log_upload.logs()[1];
    assert_eq!(second_uploaded_log.session_id(), "foo_overridden");
    assert_eq!(second_uploaded_log.timestamp(), time_second);
    assert_eq!(second_uploaded_log.field("_logged_at"), time_first.to_string());
    assert_eq!(second_uploaded_log.message(), "log with overridden attributes and fields");
    // Confirm can override provider fields
    assert_eq!(second_uploaded_log.field("_some_version"), "350".to_string());

    // Confirm the log overriding the time worked.
    let occurred_at_overriden_log = &log_upload.logs()[9];
    assert_eq!(occurred_at_overriden_log.timestamp(), time_first);
    assert_eq!(occurred_at_overriden_log.field("_some_version"), "400".to_string());
  });
}

#[test]
fn api_bandwidth_counters() {
  let mut setup = Setup::new();

  setup
    .current_api_stream()
    .blocking_stream_action(StreamAction::SendRuntime(make_update(
      vec![(
        bd_runtime::runtime::resource_utilization::ResourceUtilizationEnabledFlag::path(),
        ValueKind::Bool(false),
      )],
      "version".to_string(),
    )));

  // Verify that we emit counters for how much data we transmit/receive.
  setup.flush_and_upload_stats();
  assert_matches!(setup.server.next_stat_upload(), Some(upload) => {
      let upload = StatsRequestHelper::new(upload);

      // If these numbers end up being too variable we do something more generic.
      let bandwidth_tx = upload.get_counter("api:bandwidth_tx", labels! {}).unwrap();
      let bandwidth_rx = upload.get_counter("api:bandwidth_rx", labels! {}).unwrap();
      assert_eq!(upload.get_counter("api:bandwidth_tx_uncompressed", labels! {}), Some(119));
      assert!(bandwidth_tx > 100, "bandwidth_tx = {bandwidth_tx}");
      assert!(bandwidth_rx < 400, "bandwidth_rx = {bandwidth_rx}");
      assert_eq!(upload.get_counter("api:bandwidth_rx_decompressed", labels! {}), Some(279));
      assert_eq!(upload.get_counter("api:stream_total", labels! {}), Some(1));
  });
}

#[test]
fn buffer_selection_update() {
  let mut setup = Setup::new();

  setup.send_configuration_update(configuration_update(
    "",
    StateOfTheWorld {
      buffer_config_list: Some(BufferConfigList {
        buffer_config: vec![default_buffer_config(
          Type::CONTINUOUS,
          make_buffer_matcher_matching_everything().into(),
        )],
        ..Default::default()
      })
      .into(),
      ..Default::default()
    },
  ));

  for _ in 0 .. 10 {
    setup.log(
      log_level::DEBUG,
      LogType::NORMAL,
      "something".into(),
      [].into(),
      [].into(),
      None,
    );
  }

  assert_matches!(setup.server.blocking_next_log_upload(), Some(log_upload) => {
    assert_eq!(log_upload.logs().len(), 10);
  });

  // Now update the configuration to drop all logs.
  setup.send_configuration_update(configuration_update(
    "update",
    StateOfTheWorld {
      buffer_config_list: Some(BufferConfigList {
        buffer_config: vec![default_buffer_config(Type::CONTINUOUS, None)],
        ..Default::default()
      })
      .into(),
      ..Default::default()
    },
  ));

  for _ in 0 .. 10 {
    setup.log(
      log_level::DEBUG,
      LogType::NORMAL,
      "something".into(),
      [].into(),
      [].into(),
      None,
    );
  }

  assert_matches!(setup.server.blocking_next_log_upload(), None);
}

#[test]
fn bad_config() {
  let mut setup = Setup::new();

  let maybe_nack = setup.send_configuration_update(invalid_configuration());
  assert_matches!(maybe_nack, Some(nack) => {
    assert_eq!(nack.error_details,
      "An invalid match configuration was received: missing oneof");
    assert_eq!(nack.version_nonce, "");
  });
}

#[test]
fn configuration_caching() {
  let sdk_directory = Arc::new(tempfile::TempDir::with_prefix("sdk").unwrap());

  // Initialize the logger once, sending it a configuration that will upload all logs.
  {
    let mut setup = Setup::new_with_options(SetupOptions {
      sdk_directory: sdk_directory.clone(),
      ..Default::default()
    });

    setup.configure_stream_all_logs();
  }

  // After shutting down the previous logger, create a new one with the same buffer directory.
  // It should reuse the previous configuration and upload logs without receiving a configuration
  // update.
  let mut setup = Setup::new_with_options(SetupOptions {
    sdk_directory,
    ..Default::default()
  });

  setup.upload_individual_logs();

  setup.log(
    log_level::DEBUG,
    LogType::NORMAL,
    "foo".into(),
    [].into(),
    [].into(),
    None,
  );

  assert_matches!(setup.server.blocking_next_log_upload(), Some(log_upload) => {
    assert_eq!(log_upload.buffer_id(), "default");
    assert_eq!(log_upload.logs().len(), 1);
  });
}

#[test]
fn trigger_buffers_not_uploaded() {
  let mut setup = Setup::new();

  let maybe_nack = setup.send_configuration_update(configuration_update(
    "",
    StateOfTheWorld {
      buffer_config_list: Some(BufferConfigList {
        buffer_config: vec![default_buffer_config(
          Type::TRIGGER,
          make_buffer_matcher_matching_everything().into(),
        )],
        ..Default::default()
      })
      .into(),
      ..Default::default()
    },
  ));
  assert!(maybe_nack.is_none());

  for _ in 0 .. 10 {
    setup.log(
      log_level::DEBUG,
      LogType::NORMAL,
      "something".into(),
      [].into(),
      [].into(),
      None,
    );
  }

  assert_matches!(setup.server.blocking_next_log_upload(), None);
}

#[test]
fn blocking_log() {
  let mut setup = Setup::new();

  setup.send_runtime_update();

  // Send down a configuration with a single 'default' buffer and a workflow that matches on 'foo'
  // log message.
  // After the log 'foo' is emitted the workflow should trigger a flush buffer action. In response
  // to this action th engine will persist workflows state to disk.
  let maybe_nack = setup.send_configuration_update(config_helper::configuration_update_from_parts(
    "",
    ConfigurationUpdateParts {
      buffer_config: vec![config_helper::default_buffer_config(
        Type::TRIGGER,
        make_buffer_matcher_matching_everything().into(),
      )],
      workflows: make_workflow_config_flushing_buffer("default", message_equals("foo")),
      ..Default::default()
    },
  ));
  assert!(maybe_nack.is_none());

  setup.blocking_log(
    log_level::DEBUG,
    LogType::NORMAL,
    "foo".into(),
    [].into(),
    [].into(),
  );

  // Confim that workflows state is persisted to disk after the processing of log completes.
  assert!(setup.workflows_state_file_path().exists());
  assert!(setup.pending_aggregation_index_file_path().exists());
}

#[test]
fn session_replay_actions() {
  let mut setup = Setup::new();
  setup.send_runtime_update();

  let b = state("B");
  let a = state("A").declare_transition_with_actions(
    &b,
    rule!(message_equals("take a screenshot")),
    &[make_take_screenshot_action()],
  );

  // Send a configuration that takes a screenshot on "foo" message.
  let maybe_nack = setup.send_configuration_update(config_helper::configuration_update_from_parts(
    "",
    ConfigurationUpdateParts {
      buffer_config: vec![config_helper::default_buffer_config(
        Type::TRIGGER,
        make_buffer_matcher_matching_everything().into(),
      )],
      workflows: vec![WorkflowBuilder::new("workflow", &[&a, &b]).build()],
      ..Default::default()
    },
  ));
  assert!(maybe_nack.is_none());

  // Emit a log that should not result in taking a screenshot.
  setup.blocking_log(
    log_level::DEBUG,
    LogType::NORMAL,
    "bar".into(),
    [].into(),
    [].into(),
  );
  // TODO(snowp): This is a bit of a brittle test as it relies on the timing of the screenshot
  // handling.
  std::thread::sleep(100.std_milliseconds());
  assert_eq!(
    0,
    setup
      .capture_screenshot_count
      .load(std::sync::atomic::Ordering::Relaxed)
  );

  // Emit a log that should result in taking a screenshot.
  setup.blocking_log(
    log_level::DEBUG,
    LogType::NORMAL,
    "take a screenshot".into(),
    [].into(),
    [].into(),
  );
  wait_for!(
    1 == setup
      .capture_screenshot_count
      .load(std::sync::atomic::Ordering::Relaxed)
  );

  // Simulate a capture of a screenshot.
  setup.blocking_log(
    log_level::DEBUG,
    LogType::REPLAY,
    SESSION_REPLAY_SCREENSHOT_LOG_MESSAGE.into(),
    [].into(),
    [].into(),
  );

  // Due to all the channels used to propagate the fact that we have taken a screenshot, we need
  // to block on this metric to ensure that we have transitioned into being able to take another
  // screenshot before proceeding.
  wait_for!(
    setup
      .logger
      .stats()
      .counter("logger:screenshots:received_total")
      .get()
      == 1
  );

  // Emit a log that should result in taking a screenshot.
  setup.blocking_log(
    log_level::DEBUG,
    LogType::NORMAL,
    "take a screenshot".into(),
    [].into(),
    [].into(),
  );
  wait_for!(
    2 == setup
      .capture_screenshot_count
      .load(std::sync::atomic::Ordering::Relaxed)
      && 1
        == setup
          .capture_screen_count
          .load(std::sync::atomic::Ordering::Relaxed)
  );
}

#[test]
fn flush_state() {
  let mut setup = Setup::new();

  // Send down a configuration with a single 'default' buffer.
  let maybe_nack = setup.send_configuration_update(config_helper::configuration_update(
    "",
    StateOfTheWorld {
      buffer_config_list: Some(BufferConfigList {
        buffer_config: vec![config_helper::default_buffer_config(
          Type::TRIGGER,
          make_buffer_matcher_matching_everything().into(),
        )],
        ..Default::default()
      })
      .into(),
      ..Default::default()
    },
  ));
  assert!(maybe_nack.is_none());

  setup.logger_handle.flush_state(Block::No);

  // File should not exist immediately after flush_state call.
  assert!(!setup.pending_aggregation_index_file_path().exists());

  // We should eventually see the stats aggregation file exist on disk.
  wait_for!(setup.pending_aggregation_index_file_path().exists());
}

#[test]
fn blocking_flush_state() {
  let mut setup = Setup::new();

  // Send down a configuration with a single 'default' buffer.
  let maybe_nack = setup.send_configuration_update(config_helper::configuration_update_from_parts(
    "",
    ConfigurationUpdateParts {
      buffer_config: vec![config_helper::default_buffer_config(
        Type::TRIGGER,
        make_buffer_matcher_matching_everything().into(),
      )],
      workflows: make_workflow_config_flushing_buffer("default", message_equals("foo")),
      ..Default::default()
    },
  ));
  assert!(maybe_nack.is_none());

  setup.log(
    log_level::DEBUG,
    LogType::NORMAL,
    "foo".into(),
    [].into(),
    [].into(),
    None,
  );

  setup
    .logger_handle
    .flush_state(Block::Yes(15.std_seconds()));

  assert!(setup.workflows_state_file_path().exists());
  assert!(setup.pending_aggregation_index_file_path().exists());
}

#[test]
fn flush_state_uninitialized() {
  let setup = Setup::new();

  setup.logger_handle.flush_state(Block::No);

  // File should not exist immediately after flush_state call.
  assert!(!setup.pending_aggregation_index_file_path().exists());

  // We should eventually see the stats aggregation file exist on disk.
  wait_for!(setup.pending_aggregation_index_file_path().exists());
}

#[test]
fn blocking_flush_state_uninitialized() {
  let setup = Setup::new();

  setup
    .logger_handle
    .flush_state(Block::Yes(15.std_seconds()));

  assert!(!setup.workflows_state_file_path().exists());
  assert!(setup.pending_aggregation_index_file_path().exists());
}

#[test]
fn log_tailing() {
  let mut setup = Setup::new();

  let maybe_nack = setup.send_configuration_update(config_helper::configuration_update(
    "",
    StateOfTheWorld {
      bdtail_configuration: Some(BdTailConfigurations {
        active_streams: vec![BdTailStream {
          stream_id: "all".into(),
          matcher: None.into(),
          ..Default::default()
        }],
        ..Default::default()
      })
      .into(),
      ..Default::default()
    },
  ));
  assert!(maybe_nack.is_none());

  setup.log(
    log_level::DEBUG,
    LogType::NORMAL,
    "something".into(),
    [].into(),
    [].into(),
    None,
  );

  // Logs are immediately uploaded with "streamed" as the buffer id.
  assert_matches!(setup.server.blocking_next_log_upload(), Some(log_upload) => {
    assert_eq!(log_upload.buffer_id(), "streamed");
    assert_eq!(log_upload.logs().len(), 1);
    assert_eq!("something", log_upload.logs()[0].message());
    assert_eq!(vec!["all"], *log_upload.logs()[0].stream_ids());
  });

  let maybe_nack = setup.send_configuration_update(config_helper::configuration_update(
    "",
    StateOfTheWorld {
      bdtail_configuration: Some(BdTailConfigurations {
        active_streams: vec![
          BdTailStream {
            stream_id: "all".into(),
            matcher: None.into(),
            ..Default::default()
          },
          BdTailStream {
            stream_id: "some".into(),
            matcher: Some(message_equals("something")).into(),
            ..Default::default()
          },
        ],
        ..Default::default()
      })
      .into(),
      ..Default::default()
    },
  ));
  assert!(maybe_nack.is_none());

  setup.log(
    log_level::DEBUG,
    LogType::NORMAL,
    "something".into(),
    [].into(),
    [].into(),
    None,
  );

  // When multiple streams match the same log the log is uploaded once with multiple tagged stream
  // IDs.
  assert_matches!(setup.server.blocking_next_log_upload(), Some(log_upload) => {
    assert_eq!(log_upload.buffer_id(), "streamed");
    assert_eq!(log_upload.logs().len(), 1);
    assert_eq!("something", log_upload.logs()[0].message());
    assert_eq!(vec!["all", "some"], *log_upload.logs()[0].stream_ids());
  });
}

#[test]
fn workflow_flush_buffers_action_uploads_buffer() {
  let mut setup = Setup::new();

  setup.send_runtime_update();

  // Send down a configuration with a single buffer ('default')
  // which accepts all logs and a single workflow which matches for logs
  // with the 'fire workflow action!' message in order to flush all buffers.
  let maybe_nack = setup.send_configuration_update(config_helper::configuration_update_from_parts(
    "",
    ConfigurationUpdateParts {
      buffer_config: vec![config_helper::default_buffer_config(
        Type::TRIGGER,
        make_buffer_matcher_matching_everything().into(),
      )],
      workflows: make_workflow_config_flushing_buffer(
        "default",
        message_equals("fire workflow action!"),
      ),
      ..Default::default()
    },
  ));
  assert!(maybe_nack.is_none());

  for _ in 0 .. 9 {
    setup.log(
      log_level::DEBUG,
      LogType::NORMAL,
      "something".into(),
      [].into(),
      [].into(),
      None,
    );
  }

  setup.log(
    log_level::DEBUG,
    LogType::NORMAL,
    "fire workflow action!".into(),
    [].into(),
    [].into(),
    None,
  );

  // Since there are 10 logs in the buffer at this point, we should now see an upload containing
  // 10 logs.
  assert_matches!(setup.server.blocking_next_log_upload(), Some(log_upload) => {
    assert_eq!(log_upload.buffer_id(), "default");
    assert_eq!(log_upload.logs().len(), 10);
    assert_eq!(vec!["flush_action_id"], *log_upload.logs()[9].workflow_action_ids());
  });
}

#[test]
fn workflow_flush_buffers_action_emits_synthetic_log_and_uploads_buffer_and_starts_streaming() {
  let b = state("B");
  let c = state("C");
  let a = state("A")
    .declare_transition_with_actions(
      &b,
      rule!(message_equals(
        "fire flush trigger buffer and start streaming action!"
      )),
      &[
        make_flush_buffers_action(
          &["trigger_buffer_id"],
          Some((vec!["default"], 10)),
          "flush_with_streaming_action_id",
        ),
        make_emit_counter_action("insight_action_id", metric_value(1), vec![]),
      ],
    )
    .declare_transition_with_actions(
      &c,
      rule!(message_equals("fire flush trigger buffer action!")),
      &[make_flush_buffers_action(
        &["trigger_buffer_id"],
        None,
        "flush_with_streaming_action_id",
      )],
    );

  let mut setup = Setup::new_with_metadata(Arc::new(LogMetadata {
    timestamp: Mutex::new(time::OffsetDateTime::now_utc()),
    custom_fields: [("k1".into(), "provider_value_1".into())].into(),
    ootb_fields: [("k2".into(), "provider_value_2".into())].into(),
  }));

  // Send down a configuration with a single buffer ('default')
  // which does not accept `InternalSDK` logs and a single workflow
  // which matches for logs with the 'fire workflow action!' message
  // in order to flush all buffers.
  let maybe_nack = setup.send_configuration_update(config_helper::configuration_update_from_parts(
    "",
    ConfigurationUpdateParts {
      buffer_config: vec![
        default_buffer_config(
          Type::CONTINUOUS,
          make_buffer_matcher_matching_resource_logs().into(),
        ),
        BufferConfigBuilder {
          name: "trigger_buffer_id",
          buffer_type: Type::TRIGGER,
          filter: make_buffer_matcher_matching_everything_except_internal_logs().into(),
          non_volatile_size: 100_000,
          volatile_size: 10_000,
        }
        .build(),
      ],
      workflows: vec![WorkflowBuilder::new("workflow", &[&a, &b, &c]).build()],
      ..Default::default()
    },
  ));
  assert!(maybe_nack.is_none());

  for _ in 0 .. 9 {
    setup.log(
      log_level::DEBUG,
      LogType::INTERNAL_SDK,
      "something".into(),
      [].into(),
      [].into(),
      None,
    );
  }

  setup.log(
    log_level::DEBUG,
    LogType::NORMAL,
    "fire flush trigger buffer and start streaming action!".into(),
    vec![("k3".into(), StringOrBytes::String("value_3".into()))]
      .into_iter()
      .map(|(key, value)| (key, AnnotatedLogField::new_ootb(value)))
      .collect(),
    [].into(),
    None,
  );

  setup.flush_and_upload_stats();
  let stat_upload = StatsRequestHelper::new(setup.server.next_stat_upload().unwrap());
  assert_eq!(
    stat_upload.get_workflow_counter("insight_action_id", labels! {}),
    Some(1),
  );

  // Out of 10 emitted logs none of them was stored in a configured
  // "default" buffer. Since the last of these logs triggered
  // flush buffers action a synthetic log resembling
  // the log that triggered the action was added to the "trigger_buffer_id" buffer.
  // We verify that this synthetic log is uploaded as part of flushing
  // the "trigger_buffer_id" buffer.
  assert_matches!(setup.server.blocking_next_log_upload(), Some(log_upload) => {
    assert_eq!(log_upload.buffer_id(), "trigger_buffer_id");
    assert_eq!(log_upload.logs().len(), 1);

    assert_eq!(vec!["flush_with_streaming_action_id"], *log_upload.logs()[0].workflow_action_ids());

    assert_eq!("provider_value_1", log_upload.logs()[0].field("k1"));
    assert_eq!("provider_value_2", log_upload.logs()[0].field( "k2"));
    assert_eq!("value_3", log_upload.logs()[0].field("k3"));

    assert_eq!(LogType::NORMAL, log_upload.logs()[0].log_type());
  });

  // Emit 10 logs that should go to a "trigger_buffer_id" but due to the streaming
  // activated by the flush buffer action above the first 10 logs ends up being redirected
  // to the `default` continuous buffer instead.
  for _ in 0 .. 10 {
    setup.log(
      log_level::DEBUG,
      LogType::NORMAL,
      "message that should be streamed".into(),
      [].into(),
      [].into(),
      None,
    );
  }

  // Confirm that the logs ended up in `default` continuous buffer.
  assert_matches!(setup.server.blocking_next_log_upload(), Some(log_upload) => {
    assert_eq!(log_upload.buffer_id(), "default");
    assert_eq!(log_upload.logs().len(), 10);
  });

  // Emit 9 logs that should go to the trigger buffer after streaming termination.
  for _ in 10 .. 19 {
    setup.log(
      log_level::DEBUG,
      LogType::NORMAL,
      "message that should not be streamed".into(),
      [].into(),
      [].into(),
      None,
    );
  }

  // Trigger the upload of a trigger "trigger_buffer_id" buffer.
  setup.log(
    log_level::DEBUG,
    LogType::NORMAL,
    "fire flush trigger buffer action!".into(),
    [].into(),
    [].into(),
    None,
  );

  // Confirm that the second ten out of nineteenth emitted logs ended up in `trigger_buffer_id`
  // continuous buffer. Assert for nine + the trigger log so ten logs in total.
  assert_matches!(setup.server.blocking_next_log_upload(), Some(log_upload) => {
    assert_eq!(log_upload.buffer_id(), "trigger_buffer_id");
    assert_eq!(log_upload.logs().len(), 10, "{:?}", log_upload.logs());

    assert_eq!("fire flush trigger buffer action!", log_upload.logs().last().unwrap().message());
  });
}

#[test]
fn workflow_generate_log_to_histogram() {
  let metadata = Arc::new(LogMetadata {
    timestamp: Mutex::new(datetime!(2023-10-01 00:00:00 UTC)),
    ..Default::default()
  });
  let mut setup = Setup::new_with_metadata(metadata.clone());
  setup.send_runtime_update();

  let mut a = state("A");
  let mut b = state("B");
  let mut c = state("C");
  let d = state("D");

  a = a.declare_transition_with_extractions(
    &b,
    rule!(message_equals("foo")),
    &[make_save_timestamp_extraction("timestamp1")],
  );

  b = b.declare_transition_with_all(
    &c,
    rule!(message_equals("bar")),
    &[
      make_flush_buffers_action(&["default"], None, "flush_action_id"),
      make_generate_log_action_proto(
        "message",
        &[
          (
            "_duration_ms",
            TestFieldType::Subtract(
              TestFieldRef::SavedTimestampId("timestamp2"),
              TestFieldRef::SavedTimestampId("timestamp1"),
            ),
          ),
          (
            "other",
            TestFieldType::Single(TestFieldRef::SavedFieldId("id1")),
          ),
        ],
        "id",
        LogType::SPAN,
      ),
    ],
    &[make_save_timestamp_extraction("timestamp2")],
  );

  c = c.declare_transition_with_actions(
    &d,
    rule!(field_equals("_generate_log_id", "id")),
    &[make_emit_histogram_action(
      "foo_id",
      extract_metric_value("_duration_ms"),
      vec![metric_tag("fixed_key", "fixed_value")],
    )],
  );

  let maybe_nack = setup.send_configuration_update(config_helper::configuration_update_from_parts(
    "",
    ConfigurationUpdateParts {
      buffer_config: vec![config_helper::default_buffer_config(
        Type::TRIGGER,
        make_buffer_matcher_matching_everything().into(),
      )],
      workflows: vec![WorkflowBuilder::new("workflow_1", &[&a, &b, &c, &d]).build()],
      ..Default::default()
    },
  ));
  assert!(maybe_nack.is_none());

  setup.blocking_log(
    log_level::DEBUG,
    LogType::NORMAL,
    "foo".into(),
    [].into(),
    [].into(),
  );

  *metadata.timestamp.lock() = datetime!(2023-10-01 00:00:01.003 UTC);

  setup.blocking_log(
    log_level::DEBUG,
    LogType::NORMAL,
    "bar".into(),
    [].into(),
    [].into(),
  );

  setup.flush_and_upload_stats();
  let stat_upload = StatsRequestHelper::new(setup.server.next_stat_upload().unwrap());
  assert_eq!(
    stat_upload.get_inline_histogram(
      "foo_id",
      labels! {
        "fixed_key" => "fixed_value",
      }
    ),
    Some(vec![1003.0]),
  );

  assert_matches!(setup.server.blocking_next_log_upload(), Some(log_upload) => {
    assert_eq!(log_upload.buffer_id(), "default");
    assert_eq!(log_upload.logs().len(), 3);
    assert_eq!(log_upload.logs()[2].message(), "message");
    assert_eq!(log_upload.logs()[2].log_type(), LogType::SPAN);
    assert_eq!(log_upload.logs()[2].field("_duration_ms"), "1003");
    assert_ne!(log_upload.logs()[2].session_id(), "");
    assert_ne!(log_upload.logs()[2].timestamp(), OffsetDateTime::UNIX_EPOCH);
  });
}

#[test]
fn workflow_debugging() {
  let time_provider = Arc::new(TestTimeProvider::new(datetime!(2023-10-01 00:00:00 UTC)));
  let mut setup = Setup::new_with_options(SetupOptions {
    time_provider: Some(time_provider.clone()),
    ..Default::default()
  });
  setup.send_runtime_update();

  setup.send_runtime_update();

  let b = state("B");
  let a = state("A").declare_transition_with_actions(
    &b,
    rule!(message_equals("fire workflow action!")),
    &[make_emit_counter_action(
      "foo_id",
      metric_value(123),
      vec![],
    )],
  );

  let maybe_nack = setup.send_configuration_update(config_helper::configuration_update_from_parts(
    "1",
    ConfigurationUpdateParts {
      debug_workflows: vec![WorkflowBuilder::new("workflow_1", &[&a, &b]).build()],
      ..Default::default()
    },
  ));
  assert!(maybe_nack.is_none());

  // We should send a debug data upload even when there is no state change every 1s driven by
  // when logs are processed.
  setup.blocking_log(
    log_level::DEBUG,
    LogType::NORMAL,
    "something else".into(),
    [].into(),
    [].into(),
  );
  let debug_data = setup.server.next_debug_data_request().unwrap();
  assert_eq!(
    debug_data,
    DebugDataRequest {
      workflow_debug_data: [(
        "workflow_1".into(),
        WorkflowDebugData {
          start_reset: Some(WorkflowTransitionDebugData {
            transition_count: 1,
            last_transition_time: datetime!(2023-10-01 00:00:00 UTC).into_proto(),
            ..Default::default()
          })
          .into(),
          ..Default::default()
        }
      )]
      .into(),
      ..Default::default()
    }
  );

  // Now send a log that will match.
  time_provider.advance(1.minutes());
  setup.blocking_log(
    log_level::DEBUG,
    LogType::NORMAL,
    "fire workflow action!".into(),
    [].into(),
    [].into(),
  );

  // Make sure there is no metric as we are only in debug mode.
  setup.flush_and_upload_stats();
  let stat_upload = StatsRequestHelper::new(setup.server.next_stat_upload().unwrap());
  assert!(
    stat_upload
      .get_workflow_counter("foo_id", labels! {})
      .is_none()
  );

  let debug_data = setup.server.next_debug_data_request().unwrap();
  assert_eq!(
    debug_data,
    DebugDataRequest {
      workflow_debug_data: [(
        "workflow_1".into(),
        WorkflowDebugData {
          start_reset: Some(WorkflowTransitionDebugData {
            transition_count: 1,
            last_transition_time: datetime!(2023-10-01 00:00:00 UTC).into_proto(),
            ..Default::default()
          })
          .into(),
          states: [(
            "A".into(),
            WorkflowStateDebugData {
              transitions: vec![WorkflowTransitionDebugData {
                transition_type: Some(Transition_type::TransitionIndex(0)),
                transition_count: 1,
                last_transition_time: datetime!(2023-10-01 00:01:00 UTC).into_proto(),
                ..Default::default()
              }],
              ..Default::default()
            }
          )]
          .into(),
          ..Default::default()
        }
      )]
      .into(),
      ..Default::default()
    }
  );

  // Deploy the workflow and also send it in debug mode, so it should send debug data and also
  // emit metrics.
  let maybe_nack = setup.send_configuration_update(config_helper::configuration_update_from_parts(
    "2",
    ConfigurationUpdateParts {
      workflows: vec![WorkflowBuilder::new("workflow_1", &[&a, &b]).build()],
      debug_workflows: vec![WorkflowBuilder::new("workflow_1", &[&a, &b]).build()],
      ..Default::default()
    },
  ));
  assert!(maybe_nack.is_none());

  time_provider.advance(1.minutes());
  setup.blocking_log(
    log_level::DEBUG,
    LogType::NORMAL,
    "fire workflow action!".into(),
    [].into(),
    [].into(),
  );

  setup.flush_and_upload_stats();
  let stat_upload = StatsRequestHelper::new(setup.server.next_stat_upload().unwrap());
  assert_eq!(
    stat_upload.get_workflow_counter("foo_id", labels! {}),
    Some(123),
  );

  let debug_data = setup.server.next_debug_data_request().unwrap();
  assert_eq!(
    debug_data,
    DebugDataRequest {
      workflow_debug_data: [(
        "workflow_1".into(),
        WorkflowDebugData {
          start_reset: Some(WorkflowTransitionDebugData {
            transition_count: 1,
            last_transition_time: datetime!(2023-10-01 00:00:00 UTC).into_proto(),
            ..Default::default()
          })
          .into(),
          states: [(
            "A".into(),
            WorkflowStateDebugData {
              transitions: vec![WorkflowTransitionDebugData {
                transition_type: Some(Transition_type::TransitionIndex(0)),
                transition_count: 2,
                last_transition_time: datetime!(2023-10-01 00:02:00 UTC).into_proto(),
                ..Default::default()
              }],
              ..Default::default()
            }
          )]
          .into(),
          ..Default::default()
        }
      )]
      .into(),
      ..Default::default()
    }
  );
}

#[test]
fn workflow_emit_metric_action_emits_metric() {
  let mut setup = Setup::new();

  setup.send_runtime_update();

  let b = state("B");
  let a = state("A").declare_transition_with_actions(
    &b,
    rule!(message_equals("fire workflow action!")),
    &[make_emit_counter_action(
      "foo_id",
      metric_value(123),
      vec![
        metric_tag("fixed_key", "fixed_value"),
        extract_metric_tag("extraction_key_from", "extraction_key_to"),
        extract_log_body_tag(),
      ],
    )],
  );

  // Send down a configuration with a single buffer ('default') buffer
  // and our workflow that matches on "fire workflow action!" log.
  let maybe_nack = setup.send_configuration_update(config_helper::configuration_update_from_parts(
    "",
    ConfigurationUpdateParts {
      buffer_config: vec![config_helper::default_buffer_config(
        Type::TRIGGER,
        make_buffer_matcher_matching_everything().into(),
      )],
      workflows: vec![
        WorkflowBuilder::new("workflow_1", &[&a, &b]).build(),
        WorkflowBuilder::new("workflow_2", &[&a, &b]).build(),
      ],
      ..Default::default()
    },
  ));
  assert!(maybe_nack.is_none());

  setup.blocking_log(
    log_level::DEBUG,
    LogType::NORMAL,
    "fire workflow action!".into(),
    std::iter::once((
      "extraction_key_from".into(),
      StringOrBytes::String("extracted_value".into()),
    ))
    .map(|(key, value)| (key, AnnotatedLogField::new_ootb(value)))
    .collect(),
    [].into(),
  );

  setup.flush_and_upload_stats();
  let stat_upload = StatsRequestHelper::new(setup.server.next_stat_upload().unwrap());
  assert_eq!(
    stat_upload.get_workflow_counter(
      "foo_id",
      labels! {
        "fixed_key" => "fixed_value",
        "extraction_key_to" => "extracted_value",
        "__log_body__" => "fire workflow action!",
      }
    ),
    // There are 2 emit metric actions that increment a counter with `_id=foo_id` by 123
    // but since both of them have the same action ID we dedup them and perform action only once.
    Some(123),
  );
  assert_eq!(
    stat_upload.request.snapshot[0].workflow_debug_data,
    [
      (
        "workflow_1".into(),
        WorkflowDebugData {
          states: [(
            "A".into(),
            WorkflowStateDebugData {
              transitions: vec![WorkflowTransitionDebugData {
                transition_type: Some(Transition_type::TransitionIndex(0)),
                transition_count: 1,
                ..Default::default()
              }],
              ..Default::default()
            }
          )]
          .into(),
          start_reset: Some(WorkflowTransitionDebugData {
            transition_count: 1,
            ..Default::default()
          })
          .into(),
          ..Default::default()
        }
      ),
      (
        "workflow_2".into(),
        WorkflowDebugData {
          states: [(
            "A".into(),
            WorkflowStateDebugData {
              transitions: vec![WorkflowTransitionDebugData {
                transition_type: Some(Transition_type::TransitionIndex(0)),
                transition_count: 1,
                ..Default::default()
              }],
              ..Default::default()
            }
          )]
          .into(),
          start_reset: Some(WorkflowTransitionDebugData {
            transition_count: 1,
            ..Default::default()
          })
          .into(),
          ..Default::default()
        }
      ),
    ]
    .into()
  );
}

#[test]
fn workflow_emit_metric_action_triggers_runtime_limits() {
  let mut setup = Setup::new();

  setup
    .current_api_stream()
    .blocking_stream_action(StreamAction::SendRuntime(make_update(
      vec![(
        bd_runtime::runtime::stats::MaxDynamicCountersFlag::path(),
        ValueKind::Int(1),
      )],
      "stats cap".to_string(),
    )));

  let b = state("b");
  let c = state("c");

  let a = state("a")
    .declare_transition_with_actions(
      &b,
      rule!(message_equals("first log")),
      &[make_emit_counter_action(
        "foo",
        metric_value(1),
        vec![extract_metric_tag("extracted", "extracted")],
      )],
    )
    .declare_transition_with_actions(
      &c,
      rule!(message_equals("second log")),
      &[make_emit_counter_action("bar", metric_value(1), vec![])],
    );

  let workflow = WorkflowBuilder::new("1", &[&a, &b, &c]).build();

  let maybe_nack = setup.send_configuration_update(configuration_update_from_parts(
    "",
    ConfigurationUpdateParts {
      workflows: vec![workflow],
      ..Default::default()
    },
  ));
  assert!(maybe_nack.is_none());

  setup.blocking_log(
    log_level::DEBUG,
    LogType::NORMAL,
    "first log".into(),
    [("extracted".into(), AnnotatedLogField::new_custom("hello"))].into(),
    [].into(),
  );

  // Blocked due to cardinality limits on the action ID.
  setup.blocking_log(
    log_level::DEBUG,
    LogType::NORMAL,
    "first log".into(),
    [("extracted".into(), AnnotatedLogField::new_custom("world"))].into(),
    [].into(),
  );

  // Allowed as it is a different action ID.
  setup.blocking_log(
    log_level::DEBUG,
    LogType::NORMAL,
    "second log".into(),
    [].into(),
    [].into(),
  );

  setup.send_runtime_update();
  setup.flush_and_upload_stats();
  let stat_upload = StatsRequestHelper::new(setup.server.next_stat_upload().unwrap());

  assert_eq!(
    stat_upload.get_workflow_counter(
      "foo",
      labels!(
        "extracted" => "hello",
      )
    ),
    Some(1)
  );
  assert_eq!(
    stat_upload.get_workflow_counter(
      "foo",
      labels!(
        "extracted" => "world",
      )
    ),
    None
  );

  assert_eq!(stat_upload.get_workflow_counter("bar", labels! {}), Some(1));
  assert_eq!(stat_upload.overflows().len(), 1);
  assert_eq!(stat_upload.overflows()["foo"], 1);
}

#[test]
fn transforms_emitted_logs_according_to_filters() {
  let mut setup = Setup::new();

  setup.send_runtime_update();

  // Send down a configuration:
  //  * with a single buffer ('default') which accepts all logs
  //  * a single workflow which flushes all buffers when it sees a log with field "foo" equal to
  //    'fire workflow action!'
  //  * a filter that adds a field "foo" with value 'fire workflow action!'
  let maybe_nack = setup.send_configuration_update(config_helper::configuration_update_from_parts(
    "",
    ConfigurationUpdateParts {
      buffer_config: vec![config_helper::default_buffer_config(
        Type::TRIGGER,
        make_buffer_matcher_matching_everything().into(),
      )],
      workflows: make_workflow_config_flushing_buffer(
        "default",
        field_equals("foo", "fire workflow action!"),
      ),
      filters_configuration: vec![Filter {
        matcher: Some(message_equals("yet another message!")).into(),
        transforms: vec![set_field!(
          captured("foo") = field_value!("fire workflow action!")
        )],
        ..Default::default()
      }],
      ..Default::default()
    },
  ));

  assert!(maybe_nack.is_none());

  for _ in 0 .. 9 {
    setup.log(
      log_level::DEBUG,
      LogType::NORMAL,
      "message".into(),
      [].into(),
      [].into(),
      None,
    );
  }

  setup.log(
    log_level::DEBUG,
    LogType::NORMAL,
    "yet another message!".into(),
    [(
      "foo".into(),
      AnnotatedLogField::new_custom("do not fire workflow action!"),
    )]
    .into(),
    [].into(),
    None,
  );

  // Since there are 10 logs in the buffer at this point, we should now see an upload containing
  // 10 logs.
  assert_matches!(setup.server.blocking_next_log_upload(), Some(log_upload) => {
    assert_eq!(log_upload.buffer_id(), "default");
    assert_eq!(log_upload.logs().len(), 10);
    assert_eq!("fire workflow action!", log_upload.logs()[9].field("foo"));
    assert_eq!(vec!["flush_action_id"], *log_upload.logs()[9].workflow_action_ids());
  });
}

#[test]
fn remote_buffer_upload() {
  let mut setup = Setup::new();

  // Send down a configuration with a trigger buffer ('default') which accepts all logs with no
  // local listeners that would cause it to trigger.
  let maybe_nack = setup.send_configuration_update(configuration_update(
    "",
    StateOfTheWorld {
      buffer_config_list: Some(BufferConfigList {
        buffer_config: vec![
          BufferConfigBuilder {
            name: "default",
            buffer_type: Type::TRIGGER,
            filter: make_buffer_matcher_matching_everything_except_internal_logs().into(),
            non_volatile_size: 100_000,
            volatile_size: 10_000,
          }
          .build(),
        ],
        ..Default::default()
      })
      .into(),
      ..Default::default()
    },
  ));

  assert!(maybe_nack.is_none());

  // Do some logging.
  for _ in 0 .. 10 {
    setup.log(
      log_level::DEBUG,
      LogType::NORMAL,
      "test".into(),
      [].into(),
      [].into(),
      None,
    );
  }

  // No logs should be uploaded at this point.
  assert_matches!(setup.server.blocking_next_log_upload(), None);

  // Trigger a remote upload of the `default` buffer.
  setup
    .current_api_stream()
    .blocking_stream_action(StreamAction::FlushBuffers(vec!["default".to_string()]));

  // We receive a log upload without intent negotiation.
  assert_matches!(setup.server.blocking_next_log_upload(), Some(log_upload) => {
      assert_eq!(log_upload.logs().len(), 10);
  });
}

#[test]
fn continuous_and_trigger_buffer() {
  let mut setup = Setup::new();

  // Send down a configuration with a trigger buffer ('trigger')
  // which accepts all logs and a single workflow which matches for logs
  // with the 'fire!' message in order to flush the default buffer.
  // Also send down a continuous buffer which matches a smaller subset of the logs.
  let maybe_nack = setup.send_configuration_update(configuration_update_from_parts(
    "",
    ConfigurationUpdateParts {
      buffer_config: vec![
        default_buffer_config(
          Type::CONTINUOUS,
          make_buffer_matcher_matching_everything_except_internal_logs().into(),
        ),
        BufferConfigBuilder {
          name: "trigger",
          buffer_type: Type::TRIGGER,
          filter: make_buffer_matcher_matching_everything_except_internal_logs().into(),
          non_volatile_size: 100_000,
          volatile_size: 10_000,
        }
        .build(),
      ],
      workflows: make_workflow_config_flushing_buffer("trigger", message_equals("fire!")),
      ..Default::default()
    },
  ));
  assert!(maybe_nack.is_none());

  for _ in 0 .. 10 {
    setup.log(
      log_level::DEBUG,
      LogType::NORMAL,
      "test".into(),
      [].into(),
      [].into(),
      None,
    );
  }

  assert_matches!(setup.server.blocking_next_log_upload(), Some(log_upload) => {
    assert_eq!(log_upload.buffer_id(), "default");
    assert_eq!(log_upload.logs().len(), 10);
    assert_eq!(log_upload.logs()[0].message(), "test");
  });

  setup.log(
    log_level::DEBUG,
    LogType::NORMAL,
    "fire!".into(),
    [].into(),
    [].into(),
    None,
  );

  // After writing this log we expect to see two uploads:
  // * from the trigger upload uploading
  // * from the continuous buffer uploading the single trigger line.

  assert_matches!(setup.server.next_log_intent(), Some(_intent));
  assert_matches!(setup.server.blocking_next_log_upload(), Some(log_upload) => {
    assert_eq!(log_upload.buffer_id(), "trigger");
    assert_eq!(log_upload.logs().len(), 10);
    assert_eq!(log_upload.logs()[0].message(), "test");
  });

  assert_matches!(setup.server.blocking_next_log_upload(), Some(log_upload) => {
    assert_eq!(log_upload.buffer_id(), "trigger");
    assert_eq!(log_upload.logs().len(), 1);
    assert_eq!(log_upload.logs()[0].message(), "fire!");
  });

  // Write an additional 9 logs to trigger an immediate continuous log batch upload, to avoid
  // waiting for the batch deadline.
  // TODO(snowp): Configurable batch timeouts or test time to better manage this kinda stuff.
  for _ in 0 .. 9 {
    setup.log(
      log_level::DEBUG,
      LogType::NORMAL,
      "test".into(),
      [].into(),
      [].into(),
      None,
    );
  }

  assert_matches!(setup.server.blocking_next_log_upload(), Some(log_upload) => {
    assert_eq!(log_upload.buffer_id(), "default");
    assert_eq!(log_upload.logs().len(), 10);
    assert_eq!(log_upload.logs()[0].message(), "fire!");
    assert_eq!(log_upload.logs()[1].message(), "test");
  });
}

#[test]
fn device_id_matching() {
  let mut setup = Setup::new();

  let device_id = setup.logger.new_logger_handle().device_id();

  // Send down a configuration with a trigger buffer ('trigger') which accepts all logs and a
  // single workflow which matches for logs with the 'fire!' message in order to flush the
  // default buffer.
  let maybe_nack = setup.send_configuration_update(configuration_update_from_parts(
    "",
    ConfigurationUpdateParts {
      buffer_config: vec![
        BufferConfigBuilder {
          name: "trigger",
          buffer_type: Type::TRIGGER,
          filter: make_buffer_matcher_matching_everything_except_internal_logs().into(),
          non_volatile_size: 100_000,
          volatile_size: 10_000,
        }
        .build(),
      ],
      workflows: make_workflow_config_flushing_buffer(
        "trigger",
        field_equals("_device_id", &device_id),
      ),
      ..Default::default()
    },
  ));
  assert!(maybe_nack.is_none());

  for _ in 0 .. 9 {
    setup.log(
      log_level::DEBUG,
      LogType::NORMAL,
      "test".into(),
      [].into(),
      [].into(),
      None,
    );
  }

  setup.log(
    log_level::DEBUG,
    LogType::INTERNAL_SDK,
    "fire!".into(),
    [].into(),
    [].into(),
    None,
  );

  assert_matches!(setup.server.next_log_intent(), Some(_intent));
  assert_matches!(setup.server.blocking_next_log_upload(), Some(log_upload) => {
    assert_eq!(log_upload.buffer_id(), "trigger");
    assert_eq!(log_upload.logs().len(), 10);

    for log in &log_upload.logs() {
      assert!(!log.has_field("device_id"));
    }
  });
}

#[test]
fn matching_on_but_not_capturing_matching_fields() {
  let mut setup = Setup::new();

  // Send down a configuration with a trigger buffer ('trigger') which accepts all logs and a
  // single workflow which matches for logs with the 'fire!' message in order to flush the
  // default buffer.
  let maybe_nack = setup.send_configuration_update(configuration_update_from_parts(
    "",
    ConfigurationUpdateParts {
      buffer_config: vec![
        BufferConfigBuilder {
          name: "trigger",
          buffer_type: Type::TRIGGER,
          filter: make_buffer_matcher_matching_everything_except_internal_logs().into(),
          non_volatile_size: 100_000,
          volatile_size: 10_000,
        }
        .build(),
      ],
      workflows: make_workflow_config_flushing_buffer("trigger", field_equals("key", "value")),
      ..Default::default()
    },
  ));
  assert!(maybe_nack.is_none());

  for _ in 0 .. 9 {
    setup.log(
      log_level::DEBUG,
      LogType::NORMAL,
      "test".into(),
      [].into(),
      [].into(),
      None,
    );
  }

  setup.log(
    log_level::DEBUG,
    LogType::INTERNAL_SDK,
    "fire!".into(),
    [
      (
        "_should_be_dropped_starting_with_underscore_key".into(),
        AnnotatedLogField::new_custom("should be dropped value"),
      ),
      (
        "_key".into(),
        AnnotatedLogField::new_custom("_should_be_overridden_due_to_conflict_with_ootb_field"),
      ),
      ("_key".into(), AnnotatedLogField::new_ootb("_value")),
      ("key".into(), AnnotatedLogField::new_custom("value")),
    ]
    .into(),
    [(
      "_phantom_key".into(),
      AnnotatedLogField::new_ootb("_phantom_value"),
    )]
    .into(),
    None,
  );

  // After writing this log we expect to see two uploads:
  //  1. from the trigger upload uploading
  //  2. from the continuous buffer uploading the single trigger line.
  assert_matches!(setup.server.next_log_intent(), Some(_intent));
  assert_matches!(setup.server.blocking_next_log_upload(), Some(log_upload) => {
    assert_eq!(log_upload.buffer_id(), "trigger");
    assert_eq!(log_upload.logs().len(), 10);

    let log = &log_upload.logs()[9];
    assert_eq!(log.message(), "fire!");
    assert_eq!(log.field("key"), "value");
    assert_eq!(log.field("_key"), "_value");
    assert!(!log.has_field("_should_be_dropped_starting_with_underscore_key"));
    assert!(!log.has_field("_phantom_key"));
  });
}

#[test]
fn log_app_update() {
  let mut setup = Setup::new();

  setup.configure_stream_all_logs();

  setup.logger_handle.log_app_update(
    "1".to_string(),
    AppVersionExtra::BuildNumber("2".to_string()),
    Some(123),
    [].into(),
    time::Duration::seconds(1),
  );
  setup.logger_handle.log_app_update(
    "2".to_string(),
    AppVersionExtra::BuildNumber("3".to_string()),
    Some(123),
    [].into(),
    time::Duration::seconds(1),
  );

  for _ in 0 .. 9 {
    setup.log(
      log_level::DEBUG,
      LogType::NORMAL,
      "test".into(),
      [].into(),
      [].into(),
      None,
    );
  }

  assert_matches!(setup.server.blocking_next_log_upload(), Some(log_upload) => {
    assert_eq!(log_upload.buffer_id(), "default");
    assert_eq!(log_upload.logs().len(), 10);

    let app_update_log = &log_upload.logs()[0];
    assert_eq!("AppUpdated", app_update_log.message());
    assert_eq!("1", app_update_log.field("_previous_app_version"));
    assert_eq!("2", app_update_log.field("_previous_build_number"));

    let test_log = &log_upload.logs()[1];
    assert_eq!("test", test_log.message());
  });
}

#[test]
fn continuous_buffer_resume_with_full_buffer() {
  let mut setup = Setup::new();

  // Start by making a simple buffer configuration with a single continuous buffer with a
  // relatively small capacity.
  let maybe_nack = setup.send_configuration_update(configuration_update(
    "",
    StateOfTheWorld {
      buffer_config_list: Some(BufferConfigList {
        buffer_config: vec![
          BufferConfigBuilder {
            name: "continuous",
            buffer_type: Type::CONTINUOUS,
            filter: make_buffer_matcher_matching_everything_except_internal_logs().into(),
            non_volatile_size: 240,
            volatile_size: 200,
          }
          .build(),
        ],
        ..Default::default()
      })
      .into(),
      ..Default::default()
    },
  ));
  assert!(maybe_nack.is_none());

  // Log a single log to the continuous buffer.
  setup.blocking_log(
    log_level::DEBUG,
    LogType::NORMAL,
    "test".into(),
    [].into(),
    [].into(),
  );

  // Shut down the logger. The buffer should now be "full" and only have a single log in place.

  let sdk_directory = setup.sdk_directory.clone();
  std::mem::drop(setup);

  // Restart the logger + server from the previous directory. This should resume us from a full
  // buffer.
  let mut setup = Setup::new_with_options(SetupOptions {
    sdk_directory,
    ..Default::default()
  });

  setup.upload_individual_logs();

  // Despite being full on startup the logger is able to free up space by immediately starting
  // uploading, allowing us to capture both the new and old log.
  //
  // Note that as the first log is being uploaded, the second log is stuck in the volatile buffer
  // due to the continuous buffer blocking writes into concurrent reads (and the logs pending
  // uploads are still being read). Because of this we must first complete an upload for the
  // log taking up space in the non-volatile buffer before the second log can be written to
  // disk and be considered for an upload. In prod this would fix itself due to upload deadlines
  // for pending batches, but in test we rely on setting a low batch size to avoid the extra
  // wait.

  setup.log(
    log_level::DEBUG,
    LogType::NORMAL,
    "after startup".into(),
    [].into(),
    [].into(),
    None,
  );
  assert_matches!(setup.server.blocking_next_log_upload(), Some(log_upload) => {
    assert_eq!(log_upload.buffer_id(), "continuous");
    assert_eq!(log_upload.logs().len(), 1);
    assert_eq!(log_upload.logs()[0].message(), "test");
  });

  assert_matches!(setup.server.blocking_next_log_upload(), Some(log_upload) => {
    assert_eq!(log_upload.buffer_id(), "continuous");
    assert_eq!(log_upload.logs().len(), 1);
    assert_eq!(log_upload.logs()[0].message(), "after startup");
  });
}

#[test]
fn runtime_update() {
  let mut setup = Setup::new();

  setup
    .current_api_stream()
    .blocking_stream_action(StreamAction::SendRuntime(make_update(
      vec![("test", ValueKind::Bool(true))],
      "something".to_string(),
    )));

  let (_, update) = setup.server.blocking_next_runtime_ack();
  assert_eq!(update.last_applied_version_nonce, "something");
  assert!(update.nack.is_none());

  let snapshot = setup.logger.runtime_snapshot();
  assert!(snapshot.get_bool("test", false));
}

// Verifies that stat uploading works by checking that we record stats for the number of error
// logs written (amongst other stats).
#[test]
fn stats_upload() {
  let mut setup = Setup::new();

  // Note that we need to send a configuration update due to how we propagate the counter for
  // error logs. As we add better support for log tagging this can probably be improved.
  let maybe_nack = setup.send_configuration_update(configuration_update(
    "",
    StateOfTheWorld {
      buffer_config_list: Some(BufferConfigList {
        buffer_config: vec![default_buffer_config(
          Type::TRIGGER,
          make_buffer_matcher_matching_everything().into(),
        )],
        ..Default::default()
      })
      .into(),
      ..Default::default()
    },
  ));
  assert!(maybe_nack.is_none());

  // Log error and info logs twice, other levels once.
  for level in &[
    log_level::ERROR,
    log_level::INFO,
    log_level::ERROR,
    log_level::INFO,
    log_level::TRACE,
    log_level::DEBUG,
    log_level::WARNING,
  ] {
    setup.log(
      *level,
      LogType::NORMAL,
      "log".into(),
      [].into(),
      [].into(),
      None,
    );
  }

  let stats = setup.logger.stats();

  // Create an unused stat that will not be incremented.
  let _counter = stats.scope("test").counter("unused");

  // Create one stat that is incremented.
  stats.scope("test").counter("used").inc();

  setup.send_runtime_update();
  setup.flush_and_upload_stats();
  let stat_upload = StatsRequestHelper::new(setup.server.next_stat_upload().unwrap());
  assert_eq!(
    stat_upload.get_counter("logger:logs_received", labels! {}),
    Some(7),
  );
  assert_eq!(stat_upload.get_counter("test:used", labels! {}), Some(1));
  assert_eq!(stat_upload.get_counter("test:unused", labels! {}), None);
}

// Verifies end to end processing of binary messages and fields, ensuring that the binary data
// is preserved all the way to the test server.
#[test]
fn binary_message_and_fields() {
  let mut setup = Setup::new();

  setup
    .current_api_stream()
    .blocking_stream_action(StreamAction::SendRuntime(make_update(
      vec![
        (
          bd_runtime::runtime::log_upload::BatchSizeFlag::path(),
          ValueKind::Int(1),
        ),
        (
          bd_runtime::runtime::resource_utilization::ResourceUtilizationEnabledFlag::path(),
          ValueKind::Bool(false),
        ),
      ],
      "version".to_string(),
    )));

  setup.send_configuration_update(
    make_configuration_update_with_workflow_flushing_buffer_on_anything(
      "default",
      Type::CONTINUOUS,
    ),
  );

  setup.log(
    log_level::DEBUG,
    LogType::NORMAL,
    LogMessage::Bytes(vec![1, 2, 3]),
    [
      ("str".into(), StringOrBytes::String("str-data".to_string())),
      ("binary".into(), StringOrBytes::Bytes(vec![0, 0, 0])),
    ]
    .into_iter()
    .map(|(key, value)| (key, AnnotatedLogField::new_ootb(value)))
    .collect(),
    [].into(),
    None,
  );
  assert_matches!(setup.server.blocking_next_log_upload(), Some(log_upload) => {
    assert_eq!([1, 2, 3], log_upload.logs()[0].binary_message());
    assert_eq!("str-data", log_upload.logs()[0].field("str"));
    assert_eq!([0, 0, 0], log_upload.logs()[0].binary_field("binary"));
  });
}

#[test]
fn logs_before_cache_load() {
  let mut setup = Setup::new();

  // Write logs *before* configuration arrives.

  // These first ten logs should be continuously uploaded.
  for i in 0 .. 9 {
    let msg = i.to_string();

    setup.logger_handle.log(
      log_level::ERROR,
      LogType::NORMAL,
      msg.as_str().into(),
      [].into(),
      [].into(),
      None,
      Block::No,
      &CaptureSession::default(),
    );
  }

  // This log should trigger a trigger buffer upload.
  setup.logger_handle.log(
    log_level::DEBUG,
    LogType::NORMAL,
    "trigger".into(),
    [].into(),
    [].into(),
    None,
    Block::No,
    &CaptureSession::default(),
  );

  setup
    .current_api_stream()
    .blocking_stream_action(StreamAction::SendConfiguration(
      configuration_update_from_parts(
        "test",
        ConfigurationUpdateParts {
          buffer_config: vec![
            default_buffer_config(
              Type::CONTINUOUS,
              make_buffer_matcher_matching_everything().into(),
            ),
            BufferConfigBuilder {
              name: "trigger",
              buffer_type: Type::TRIGGER,
              non_volatile_size: 100_000,
              volatile_size: 10_000,
              filter: match_message("trigger").into(),
            }
            .build(),
          ],
          workflows: make_workflow_config_flushing_buffer("trigger", message_equals("trigger")),
          ..Default::default()
        },
      ),
    ));

  setup.server.blocking_next_configuration_ack();

  // At this point we should see both a trigger upload and a continuous upload.
  let mut verify_upload = || {
    assert_matches!(setup.server.blocking_next_log_upload(), Some(log_upload) => {
        match log_upload.buffer_id() {
            "trigger" => {
                assert_eq!(log_upload.logs().len(), 1);
            }
            "default" => {
            assert_eq!(log_upload.logs().len(), 10);
            for i in 0..9 {
                assert_eq!(log_upload.logs()[i].message(), i.to_string());
            }
            }
            buffer => panic!("unknown buffer {buffer}"),
        }

        log_upload
    })
  };

  verify_upload();
  verify_upload();
}

#[test]
fn runtime_caching() {
  let sdk_directory = {
    let mut setup = Setup::new();

    setup.upload_individual_logs();

    setup.sdk_directory.clone()
  };

  let retry_file = sdk_directory.path().join("runtime").join("retry_count");
  assert!(
    sdk_directory
      .path()
      .join("runtime")
      .join("protobuf.pb")
      .exists()
  );
  assert!(retry_file.exists());
  assert_eq!(std::fs::read(&retry_file).unwrap(), &[0]);

  let network = Box::new(NoopNetwork);
  let (_flush_tick_tx, flush_ticker) = TestTicker::new();
  let (_upload_tick_tx, upload_ticker) = TestTicker::new();

  // Start up a new logger that won't be able to connect to the server.
  {
    let store = in_memory_store();
    let device = Arc::new(bd_device::Device::new(store.clone()));

    let logger = crate::LoggerBuilder::new(InitParams {
      api_key: "foo-api-key".to_string(),
      network,
      session_strategy: Arc::new(Strategy::Fixed(fixed::Strategy::new(
        store.clone(),
        Arc::new(UUIDCallbacks),
      ))),
      static_metadata: Arc::new(EmptyMetadata),
      store,
      resource_utilization_target: Box::new(EmptyTarget),
      session_replay_target: Box::new(bd_test_helpers::session_replay::NoOpTarget),
      events_listener_target: Box::new(bd_test_helpers::events::NoOpListenerTarget),
      sdk_directory: sdk_directory.path().into(),
      metadata_provider: Arc::new(LogMetadata::default()),
      device,
      start_in_sleep_mode: false,
    })
    .with_client_stats_tickers(Box::new(flush_ticker), Box::new(upload_ticker))
    .build_dedicated_thread()
    .unwrap()
    .0;

    // The runtime configuration should use the cached value. As we load the cached config from
    // the event loop thread, there is a slight delay before we pick up on this cached value.

    let deadline = Instant::now().add(1.seconds());

    let mut deadline_elapsed = true;
    while Instant::now() < deadline {
      if 1
        == logger
          .runtime_snapshot()
          .get_integer(bd_runtime::runtime::log_upload::BatchSizeFlag::path(), 0)
      {
        deadline_elapsed = false;
        break;
      }
    }

    assert!(!deadline_elapsed);
    logger.shutdown(true);
  }

  assert_eq!(std::fs::read(&retry_file).unwrap(), &[1]);

  // Now start another logger with the same directory, this time going through the standard
  // handshake initialization.
  let _setup = Setup::new_with_options(SetupOptions {
    sdk_directory,
    ..Default::default()
  });

  // At this point the retry count should have been reset since we were able to verify that we
  // can connect to the backend with this runtime configuration.
  assert_eq!(std::fs::read(&retry_file).unwrap(), &[0]);
}

#[test]
fn workflow_state_change_match_advances_workflow() {
  let mut setup = Setup::new();

  setup.send_runtime_update();

  // Create a workflow that:
  // - Initial state A
  // - Transitions to B when feature flag "test_flag" is set to "enabled"
  // - B transitions to C and emits a metric when feature flag "test_flag" is set to "disabled"
  let c = state("C");
  let b = state("B").declare_transition_with_actions(
    &c,
    make_state_change_rule(
      bd_proto::protos::state::scope::StateScope::FEATURE_FLAG,
      "test_flag",
      Some("disabled"),
    ),
    &[make_emit_counter_action("state_change_action", metric_value(42), vec![])],
  );
  let a = state("A").declare_transition(
    &b,
    make_state_change_rule(
      bd_proto::protos::state::scope::StateScope::FEATURE_FLAG,
      "test_flag",
      Some("enabled"),
    ),
  );

  // Send down the configuration with the workflow
  let maybe_nack = setup.send_configuration_update(configuration_update_from_parts(
    "",
    ConfigurationUpdateParts {
      buffer_config: vec![default_buffer_config(
        Type::CONTINUOUS,
        make_buffer_matcher_matching_everything().into(),
      )],
      workflows: vec![WorkflowBuilder::new("state_workflow", &[&a, &b, &c]).build()],
      ..Default::default()
    },
  ));
  assert!(maybe_nack.is_none());

  // Set the feature flag to "enabled" - should transition from A to B
  setup
    .logger_handle
    .set_feature_flag_exposure("test_flag".to_string(), Some("enabled".to_string()));

  // Wait a moment for the state change to be processed
  std::thread::sleep(std::time::Duration::from_millis(100));

  // Set the feature flag to "disabled" - should transition from B to C and emit metric
  setup
    .logger_handle
    .set_feature_flag_exposure("test_flag".to_string(), Some("disabled".to_string()));

  // Wait a moment for the state change to be processed
  std::thread::sleep(std::time::Duration::from_millis(100));

  // Flush stats and verify the metric was emitted
  setup.flush_and_upload_stats();
  let stat_upload = StatsRequestHelper::new(setup.server.next_stat_upload().unwrap());
  assert_eq!(
    stat_upload.get_workflow_counter("state_change_action", labels! {}),
    Some(42),
  );
}

#[test]
fn workflow_state_change_match_flush_buffers() {
  let mut setup = Setup::new();

  setup.send_runtime_update();

  // Create a workflow that flushes buffers when a feature flag is set
  let b = state("B");
  let a = state("A").declare_transition_with_actions(
    &b,
    make_state_change_rule(
      bd_proto::protos::state::scope::StateScope::FEATURE_FLAG,
      "trigger_flush",
      Some("true"),
    ),
    &[make_flush_buffers_action(&["default"], None, "flush_action_id")],
  );

  // Send down the configuration with a trigger buffer and the workflow
  let maybe_nack = setup.send_configuration_update(configuration_update_from_parts(
    "",
    ConfigurationUpdateParts {
      buffer_config: vec![default_buffer_config(
        Type::TRIGGER,
        make_buffer_matcher_matching_everything().into(),
      )],
      workflows: vec![WorkflowBuilder::new("flush_workflow", &[&a, &b]).build()],
      ..Default::default()
    },
  ));
  assert!(maybe_nack.is_none());

  // Write some logs to the buffer
  for i in 0 .. 5 {
    setup.log(
      log_level::DEBUG,
      LogType::NORMAL,
      format!("test log {i}").into(),
      [].into(),
      [].into(),
      None,
    );
  }

  // Set the feature flag to trigger the flush
  setup
    .logger_handle
    .set_feature_flag_exposure("trigger_flush".to_string(), Some("true".to_string()));

  // The workflow should trigger a buffer flush, which should result in an upload
  assert_matches!(setup.server.blocking_next_log_upload(), Some(log_upload) => {
    assert_eq!(log_upload.buffer_id(), "default");
    // Should have 5 regular logs + 1 synthetic "State Change" log
    assert_eq!(log_upload.logs().len(), 6);
    
    // Verify the last log is the synthetic state change log with the flush action ID
    let last_log = &log_upload.logs()[5];
    assert_eq!(last_log.message(), "State Change");
    assert_eq!(vec!["flush_action_id"], *last_log.workflow_action_ids());
  });
}

