// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

use crate::metadata::MetadataCollector;
use assert_matches::assert_matches;
use bd_crash_handler::global_state::{self, Reader};
use bd_log_primitives::{AnnotatedLogField, LogFields, StringOrBytes};
use bd_proto::protos::logging::payload::LogType;
use bd_runtime::runtime::Watch;
use bd_test_helpers::metadata_provider::LogMetadata;
use bd_test_helpers::session::in_memory_store;
use itertools::Itertools as _;
use parking_lot::Mutex;
use std::sync::Arc;
use time::ext::NumericalDuration;


#[test]
fn collector_attaches_provider_fields_as_matching_fields() {
  let metadata = LogMetadata {
    timestamp: Mutex::new(time::OffsetDateTime::now_utc()),
    ootb_fields: [("key".into(), "provider_value".into())].into(),
    ..Default::default()
  };

  let collector = MetadataCollector::new(Arc::new(metadata));

  let types = vec![LogType::REPLAY, LogType::RESOURCE];

  let mut tracker =
    global_state::Tracker::new(in_memory_store(), Watch::new_for_testing(10.seconds()));

  for log_type in types {
    let metadata = collector
      .normalized_metadata_with_extra_fields(
        [
          ("key".into(), AnnotatedLogField::new_ootb("value")),
          ("key2".into(), AnnotatedLogField::new_ootb("value2")),
        ]
        .into(),
        [].into(),
        log_type,
        &mut tracker,
      )
      .unwrap();

    assert_eq!(metadata.fields.len(), 2);
    assert_eq!(
      "value",
      expected_field_value(&metadata.fields, "key").unwrap()
    );
    assert_eq!(
      "value2",
      expected_field_value(&metadata.fields, "key2").unwrap()
    );
    assert_eq!(metadata.matching_fields.len(), 1);
    assert_eq!(
      "provider_value",
      expected_field_value(&metadata.matching_fields, "key").unwrap()
    );
  }
}

#[test]
fn collector_fields_hierarchy() {
  let metadata = LogMetadata {
    timestamp: Mutex::new(time::OffsetDateTime::now_utc()),
    custom_fields: [
      ("custom_provider_key".into(), "custom_provider_value".into()),
      ("provider_key".into(), "custom_provider_value".into()),
      ("collector_key".into(), "custom_provider_value".into()),
    ]
    .into(),
    ootb_fields: [
      ("ootb_provider_key_1".into(), "ootb_provider_value_1".into()),
      ("ootb_provider_key_2".into(), "ootb_provider_value_2".into()),
      ("provider_key".into(), "ootb_provider_value".into()),
    ]
    .into(),
  };

  let mut collector = MetadataCollector::new(Arc::new(metadata));
  collector
    .add_field(
      "collector_key".into(),
      StringOrBytes::String("collector_value".into()),
    )
    .unwrap();
  collector
    .add_field(
      "key".into(),
      StringOrBytes::String("collector_value".into()),
    )
    .unwrap();

  let store = in_memory_store();
  let mut tracker = global_state::Tracker::new(store.clone(), Watch::new_for_testing(10.seconds()));

  let metadata = collector
    .normalized_metadata_with_extra_fields(
      [
        ("key".into(), AnnotatedLogField::new_ootb("value")),
        ("_key".into(), AnnotatedLogField::new_ootb("_value")),
        (
          "_should_be_dropped_key".into(),
          AnnotatedLogField::new_custom("should be dropped as it uses reserved _ prefix"),
        ),
        (
          "ootb_provider_key_1".into(),
          AnnotatedLogField::new_custom("should be ignored as it conflicts with ootb provider key"),
        ),
        (
          "ootb_provider_key_2".into(),
          AnnotatedLogField::new_ootb("should be ignored as it conflicts with ootb provider key"),
        ),
      ]
      .into(),
      [].into(),
      LogType::LIFECYCLE,
      &mut tracker,
    )
    .unwrap();

  let captured_global_fields = Reader::new(store)
    .global_state_fields()
    .into_iter()
    .sorted_by_key(|(k, _)| k.clone())
    .collect::<Vec<_>>();
  pretty_assertions::assert_eq!(
    captured_global_fields,
    [
      ("collector_key".into(), "custom_provider_value".into()),
      ("custom_provider_key".into(), "custom_provider_value".into()),
      ("key".into(), "collector_value".into()),
      ("ootb_provider_key_1".into(), "ootb_provider_value_1".into()),
      ("ootb_provider_key_2".into(), "ootb_provider_value_2".into()),
      ("provider_key".into(), "custom_provider_value".into()),
    ]
    .into_iter()
    .collect_vec()
  );

  assert_eq!(metadata.fields.len(), 7, "{:?}", metadata.fields);
  assert_eq!(
    "value",
    expected_field_value(&metadata.fields, "key").unwrap()
  );
  assert_eq!(
    "_value",
    expected_field_value(&metadata.fields, "_key").unwrap()
  );
  assert_eq!(
    "ootb_provider_value_1",
    expected_field_value(&metadata.fields, "ootb_provider_key_1").unwrap()
  );
  assert_eq!(
    "ootb_provider_value_2",
    expected_field_value(&metadata.fields, "ootb_provider_key_2").unwrap()
  );
  assert_eq!(
    "custom_provider_value",
    expected_field_value(&metadata.fields, "custom_provider_key").unwrap()
  );
  assert_eq!(
    "ootb_provider_value",
    expected_field_value(&metadata.fields, "provider_key").unwrap()
  );
  assert_eq!(
    "collector_value",
    expected_field_value(&metadata.fields, "collector_key").unwrap()
  );
}

#[test]
fn collector_does_not_accept_reserved_fields() {
  let metadata = LogMetadata {
    timestamp: Mutex::new(time::OffsetDateTime::now_utc()),
    custom_fields: [(
      "_custom_provider_key".into(),
      "custom_provider_value".into(),
    )]
    .into(),
    ootb_fields: [("_ootb_provider_key".into(), "ootb_provider_value".into())].into(),
  };

  let mut collector = MetadataCollector::new(Arc::new(metadata));
  assert!(
    collector
      .add_field(
        "_collector_key".into(),
        StringOrBytes::String("collector_value".into()),
      )
      .is_err()
  );

  let mut tracker =
    global_state::Tracker::new(in_memory_store(), Watch::new_for_testing(10.seconds()));

  let metadata = collector
    .normalized_metadata_with_extra_fields([].into(), [].into(), LogType::NORMAL, &mut tracker)
    .unwrap();

  assert!(
    collector
      .add_field(
        "app_id".into(),
        StringOrBytes::String("collector_value".into()),
      )
      .is_err()
  );

  assert_eq!(metadata.fields.len(), 1);
  assert_eq!(
    "ootb_provider_value",
    expected_field_value(&metadata.fields, "_ootb_provider_key").unwrap()
  );
}

#[test]
fn collector_fields_management() {
  let metadata = LogMetadata {
    timestamp: Mutex::new(time::OffsetDateTime::now_utc()),
    ..Default::default()
  };

  let mut collector = MetadataCollector::new(Arc::new(metadata));

  collector
    .add_field(
      "key".into(),
      StringOrBytes::String("collector_field_1".into()),
    )
    .unwrap();

  collector
    .add_field(
      "key".into(),
      StringOrBytes::String("collector_field_2".into()),
    )
    .unwrap();

  assert_eq!(collector.fields().len(), 1);
  assert_matches!(&collector.fields()["key"], StringOrBytes::String(value) if value == "collector_field_2");

  collector.remove_field("key");

  assert!(collector.fields().is_empty());
}

fn expected_field_value(fields: &LogFields, key: &str) -> Option<String> {
  Some(fields.get(key)?.as_str()?.to_string())
}

#[test]
fn metadata_from_fields_with_previous_global_state_includes_global_fields() {
  let metadata = LogMetadata {
    timestamp: Mutex::new(time::OffsetDateTime::now_utc()),
    ..Default::default()
  };
  let collector = MetadataCollector::new(Arc::new(metadata));

  let store = in_memory_store();
  let mut tracker = global_state::Tracker::new(store.clone(), Watch::new_for_testing(10.seconds()));

  // Setup global state
  let global_fields = [
    ("global_key".into(), "global_value".into()),
    ("shared_custom_key".into(), "global_value".into()),
    ("shared_ootb_key".into(), "global_value".into()),
  ]
  .into();
  tracker.maybe_update_global_state(&global_fields);

  // Setup input fields
  let input_fields = [
    ("local_key".into(), AnnotatedLogField::new_custom("local_value")),
    (
      "shared_custom_key".into(),
      AnnotatedLogField::new_custom("local_value"),
    ),
    (
      "shared_ootb_key".into(),
      AnnotatedLogField::new_ootb("local_value"),
    ),
  ]
  .into();

  let reader = Reader::new(store);

  let metadata = collector
    .metadata_from_fields_with_previous_global_state(input_fields, [].into(), &reader)
    .unwrap();

  // Verify fields

  // 1. Unique global field should be present
  assert_eq!(
    "global_value",
    expected_field_value(&metadata.fields, "global_key").unwrap()
  );

  // 2. Unique local field should be present
  assert_eq!(
    "local_value",
    expected_field_value(&metadata.fields, "local_key").unwrap()
  );

  // 3. Precedence check:
  // Order: OOTB -> Global -> Custom

  // shared_ootb_key: OOTB ("local_value") vs Global ("global_value")
  // Global comes AFTER OOTB in chain, so Global should win.
  assert_eq!(
    "global_value",
    expected_field_value(&metadata.fields, "shared_ootb_key").unwrap()
  );

  // shared_custom_key: Custom ("local_value") vs Global ("global_value")
  // Custom comes AFTER Global in chain, so Custom should win.
  assert_eq!(
    "local_value",
    expected_field_value(&metadata.fields, "shared_custom_key").unwrap()
  );
}
