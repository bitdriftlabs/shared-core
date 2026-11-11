// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE.polyform file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

use super::setup::Setup;
use crate::{AnnotatedLogField, LogAttributesOverrides, log_level};
use assert_matches::assert_matches;
use bd_log_primitives::AnnotatedLogFields;
use bd_proto::protos::config::v1::config::buffer_config::Type;
use bd_proto::protos::logging::payload::LogType;
use bd_runtime::runtime::{FeatureFlag as _, field_interning};
use bd_test_helpers::config_helper::{
  BufferConfigBuilder,
  ConfigurationUpdateParts,
  configuration_update_from_parts,
  default_buffer_config,
  make_buffer_matcher_matching_everything_except_internal_logs,
  make_buffer_matcher_matching_resource_logs,
  make_configuration_update_with_workflow_flushing_buffer_on_anything,
};
use bd_test_helpers::runtime::{ValueKind, make_update};
use bd_test_helpers::test_api_server::StreamAction;
use bd_test_helpers::test_api_server::log_upload::LogUpload;
use std::collections::BTreeSet;

fn enable_field_interning(setup: &mut Setup) {
  let mut values = Setup::get_default_runtime_values();
  values.push((field_interning::Enabled::path(), ValueKind::Bool(true)));
  setup
    .current_api_stream()
    .blocking_stream_action(StreamAction::SendRuntime(make_update(
      values,
      "field_interning".to_string(),
    )));
  let (_, response) = setup.server.blocking_next_runtime_ack();
  assert!(response.nack.is_none());
}

fn log(setup: &Setup, fields: AnnotatedLogFields) {
  setup.log(
    log_level::DEBUG,
    LogType::NORMAL,
    "some log".into(),
    fields,
    [].into(),
    None,
  );
}

/// Asserts that the persistent fields left the device only in the upload's field sets, and that
/// the logs the server passes on carry them.
fn assert_interned(log_upload: &LogUpload, logs: usize) {
  let raw = log_upload.raw_logs();
  assert_eq!(raw.len(), logs);
  let referenced: BTreeSet<u64> = raw.iter().map(|log| log.field_set_version).collect();
  assert!(!referenced.contains(&0), "every log should be interned");
  for log in &raw {
    assert!(
      log.fields.iter().all(|field| field.key != "user_id"),
      "user_id should not be inline"
    );
  }
  let uploaded: BTreeSet<u64> = log_upload
    .field_sets()
    .iter()
    .map(|set| set.version)
    .collect();
  assert_eq!(uploaded, referenced);

  for log in log_upload.logs() {
    assert_eq!(log.field("user_id"), "abc");
    assert!(!log.has_field("_persistent_fields_unavailable"));
  }
}

#[test]
fn interned_continuous_upload_is_hydrated() {
  let mut setup = Setup::new();
  setup.send_configuration_update(
    make_configuration_update_with_workflow_flushing_buffer_on_anything(
      "default",
      Type::CONTINUOUS,
    ),
  );
  enable_field_interning(&mut setup);
  setup
    .logger_handle
    .add_log_field("user_id".to_string(), "abc".into());
  setup
    .logger_handle
    .add_log_field("theme".to_string(), "dark".into());

  for i in 0 .. 10 {
    // One log overrides a persistent field, which stays inline.
    let fields = if i == 3 {
      AnnotatedLogFields::from([("theme".into(), AnnotatedLogField::new_custom("light"))])
    } else {
      [].into()
    };
    log(&setup, fields);
  }

  assert_matches!(setup.server.blocking_next_log_upload(), Some(log_upload) => {
    assert_eq!(log_upload.buffer_id(), "default");
    assert_interned(&log_upload, 10);
    let logs = log_upload.logs();
    for (i, log) in logs.iter().enumerate() {
      assert_eq!(log.field("theme"), if i == 3 { "light" } else { "dark" });
    }
  });
}

#[test]
fn interned_trigger_and_streamed_uploads_are_hydrated() {
  let mut setup = Setup::new();
  assert!(
    setup
      .send_configuration_update(configuration_update_from_parts(
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
  enable_field_interning(&mut setup);
  setup
    .logger_handle
    .add_log_field("user_id".to_string(), "abc".into());

  for _ in 0 .. 9 {
    log(&setup, [].into());
  }
  setup.log_with_session_capture(
    log_level::DEBUG,
    LogType::NORMAL,
    "some log".into(),
    [].into(),
    [].into(),
  );
  assert!(setup.server.next_log_intent().is_some());

  // The trigger upload is staged with its field sets before it is sent.
  assert_matches!(setup.server.blocking_next_log_upload(), Some(log_upload) => {
    assert_eq!(log_upload.buffer_id(), "trigger_buffer_id");
    assert_interned(&log_upload, 10);
  });

  // Logs written after the capture keep uploading, with their field sets.
  for _ in 0 .. 100 {
    log(&setup, [].into());
  }
  assert_matches!(setup.server.blocking_next_log_upload(), Some(log_upload) => {
    assert_interned(&log_upload, 10);
  });
}

#[test]
fn previous_run_logs_are_not_interned() {
  let mut setup = Setup::new();
  setup.send_configuration_update(
    make_configuration_update_with_workflow_flushing_buffer_on_anything(
      "default",
      Type::CONTINUOUS,
    ),
  );
  enable_field_interning(&mut setup);
  setup
    .logger_handle
    .add_log_field("user_id".to_string(), "abc".into());

  // A log replayed for the previous run carries that run's fields, not this process's set.
  setup.log(
    log_level::DEBUG,
    LogType::NORMAL,
    "previous run".into(),
    [].into(),
    [].into(),
    Some(LogAttributesOverrides::PreviousRunSessionID(
      time::OffsetDateTime::now_utc(),
    )),
  );
  for _ in 0 .. 9 {
    log(&setup, [].into());
  }

  assert_matches!(setup.server.blocking_next_log_upload(), Some(log_upload) => {
    let raw = log_upload.raw_logs();
    assert_eq!(raw.len(), 10);
    for log in &raw {
      let previous_run = log.message.string_data() == "previous run";
      assert_eq!(log.field_set_version == 0, previous_run, "{}", log.message.string_data());
      assert!(log.fields.iter().all(|field| !field.cleared()));
    }
  });
}

fn table_contains(setup: &Setup, needle: &[u8]) -> bool {
  std::fs::read_dir(setup.sdk_directory.path().join("field_sets"))
    .into_iter()
    .flatten()
    .filter_map(Result::ok)
    .any(|entry| {
      std::fs::read(entry.path())
        .is_ok_and(|bytes| bytes.windows(needle.len()).any(|window| window == needle))
    })
}

#[test]
fn logs_that_reach_no_buffer_write_no_version() {
  let mut setup = Setup::new();
  assert!(
    setup
      .send_configuration_update(configuration_update_from_parts(
        "",
        ConfigurationUpdateParts {
          buffer_config: vec![default_buffer_config(
            Type::CONTINUOUS,
            make_buffer_matcher_matching_resource_logs().into(),
          )],
          workflows: vec![],
          ..Default::default()
        },
      ))
      .is_none()
  );
  enable_field_interning(&mut setup);
  setup
    .logger_handle
    .add_log_field("user_id".to_string(), "abc".into());

  // Normal logs match no buffer, so no version is written for them.
  setup.log_then_flush(
    log_level::DEBUG,
    LogType::NORMAL,
    "dropped".into(),
    [].into(),
    [].into(),
  );
  assert!(!table_contains(&setup, b"user_id"));

  // A resource log is written to the buffer, so it materializes the version.
  setup.log_then_flush(
    log_level::DEBUG,
    LogType::RESOURCE,
    "kept".into(),
    [].into(),
    [].into(),
  );
  assert!(table_contains(&setup, b"user_id"));
}
