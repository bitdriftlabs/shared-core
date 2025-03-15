// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

use crate::global_state::Tracker;
use crate::metadata::MetadataCollector;
use assert_matches::assert_matches;
use bd_device::Store;
use bd_log_primitives::{AnnotatedLogField, LogFields, StringOrBytes};
use bd_proto::flatbuffers::buffer_log::bitdrift_public::fbs::logging::v_1::LogType;
use bd_test_helpers::metadata_provider::LogMetadata;
use bd_test_helpers::session::InMemoryStorage;
use parking_lot::Mutex;
use std::sync::Arc;


#[test]
fn collector_attaches_provider_fields_as_matching_fields() {
  let metadata = LogMetadata {
    timestamp: Mutex::new(time::OffsetDateTime::now_utc()),
    fields: [(
      "key".into(),
      AnnotatedLogField::new_ootb("provider_value".into()),
    )]
    .into(),
  };

  let collector = MetadataCollector::new(Arc::new(metadata));

  let types = vec![LogType::Replay, LogType::Resource];

  let mut tracker = Tracker::new(Arc::new(Store::new(Box::<InMemoryStorage>::default())));

  for log_type in types {
    let metadata = collector
      .normalized_metadata_with_extra_fields(
        [
          ("key".into(), AnnotatedLogField::new_ootb("value".into())),
          ("key2".into(), AnnotatedLogField::new_ootb("value2".into())),
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
    fields: [
      (
        "custom_provider_key".into(),
        AnnotatedLogField::new_custom(StringOrBytes::String("custom_provider_value".into())),
      ),
      (
        "ootb_provider_key_1".into(),
        AnnotatedLogField::new_ootb(StringOrBytes::String("ootb_provider_value_1".into())),
      ),
      (
        "ootb_provider_key_2".into(),
        AnnotatedLogField::new_ootb(StringOrBytes::String("ootb_provider_value_2".into())),
      ),
      (
        "provider_key".into(),
        AnnotatedLogField::new_custom(StringOrBytes::String("custom_provider_value".into())),
      ),
      (
        "provider_key".into(),
        AnnotatedLogField::new_ootb(StringOrBytes::String("ootb_provider_value".into())),
      ),
      (
        "collector_key".into(),
        AnnotatedLogField::new_custom(StringOrBytes::String("custom_provider_value".into())),
      ),
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

  let mut tracker = Tracker::new(Arc::new(Store::new(Box::<InMemoryStorage>::default())));

  let metadata = collector
    .normalized_metadata_with_extra_fields(
      [
        ("key".into(), AnnotatedLogField::new_ootb("value".into())),
        ("_key".into(), AnnotatedLogField::new_ootb("_value".into())),
        (
          "_should_be_dropped_key".into(),
          AnnotatedLogField::new_custom("should be dropped as it uses reserved _ prefix".into()),
        ),
        (
          "ootb_provider_key_1".into(),
          AnnotatedLogField::new_custom(
            "should be ignored as it conflicts with ootb provider key".into(),
          ),
        ),
        (
          "ootb_provider_key_2".into(),
          AnnotatedLogField::new_ootb(
            "should be ignored as it conflicts with ootb provider key".into(),
          ),
        ),
      ]
      .into(),
      [].into(),
      LogType::Lifecycle,
      &mut tracker,
    )
    .unwrap();

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
    fields: [
      (
        "_custom_provider_key".into(),
        AnnotatedLogField::new_custom("custom_provider_value".into()),
      ),
      (
        "_ootb_provider_key".into(),
        AnnotatedLogField::new_ootb("ootb_provider_value".into()),
      ),
    ]
    .into(),
  };

  let mut collector = MetadataCollector::new(Arc::new(metadata));
  assert!(collector
    .add_field(
      "_collector_key".into(),
      StringOrBytes::String("collector_value".into()),
    )
    .is_err());

  let mut tracker = Tracker::new(Arc::new(Store::new(Box::<InMemoryStorage>::default())));

  let metadata = collector
    .normalized_metadata_with_extra_fields([].into(), [].into(), LogType::Normal, &mut tracker)
    .unwrap();

  assert!(collector
    .add_field(
      "app_id".into(),
      StringOrBytes::String("collector_value".into()),
    )
    .is_err());

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
    fields: [].into(),
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
