// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

use crate::metadata::MetadataCollector;
use assert_matches::assert_matches;
use bd_log_primitives::{AnnotatedLogField, LogField, LogFields, StringOrBytes};
use bd_proto::flatbuffers::buffer_log::bitdrift_public::fbs::logging::v_1::LogType;
use bd_test_helpers::metadata_provider::LogMetadata;
use parking_lot::Mutex;
use std::sync::Arc;


#[test]
fn collector_attaches_provider_fields_as_matching_fields() {
  let metadata = LogMetadata {
    timestamp: Mutex::new(time::OffsetDateTime::now_utc()),
    fields: vec![AnnotatedLogField::new_ootb(
      "key".into(),
      "provider_value".into(),
    )],
  };

  let collector = MetadataCollector::new(Arc::new(metadata));

  let types = vec![LogType::Replay, LogType::Resource];

  for log_type in types {
    let metadata = collector
      .normalized_metadata_with_extra_fields(
        vec![
          AnnotatedLogField::new_ootb("key".into(), "value".into()),
          AnnotatedLogField::new_ootb("key2".into(), "value2".into()),
        ],
        vec![],
        log_type,
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
    fields: vec![
      AnnotatedLogField::new_custom(
        "custom_provider_key".into(),
        StringOrBytes::String("custom_provider_value".into()),
      ),
      AnnotatedLogField::new_ootb(
        "ootb_provider_key_1".into(),
        StringOrBytes::String("ootb_provider_value_1".into()),
      ),
      AnnotatedLogField::new_ootb(
        "ootb_provider_key_2".into(),
        StringOrBytes::String("ootb_provider_value_2".into()),
      ),
      AnnotatedLogField::new_custom(
        "provider_key".into(),
        StringOrBytes::String("custom_provider_value".into()),
      ),
      AnnotatedLogField::new_ootb(
        "provider_key".into(),
        StringOrBytes::String("ootb_provider_value".into()),
      ),
      AnnotatedLogField::new_custom(
        "collector_key".into(),
        StringOrBytes::String("custom_provider_value".into()),
      ),
    ],
  };

  let mut collector = MetadataCollector::new(Arc::new(metadata));
  collector
    .add_field(LogField {
      key: "collector_key".into(),
      value: StringOrBytes::String("collector_value".into()),
    })
    .unwrap();
  collector
    .add_field(LogField {
      key: "key".into(),
      value: StringOrBytes::String("collector_value".into()),
    })
    .unwrap();


  let metadata = collector
    .normalized_metadata_with_extra_fields(
      vec![
        AnnotatedLogField::new_ootb("key".into(), "value".into()),
        AnnotatedLogField::new_ootb("_key".into(), "_value".into()),
        AnnotatedLogField::new_custom(
          "_should_be_dropped_key".into(),
          "should be dropped as it uses reserved _ prefix".into(),
        ),
        AnnotatedLogField::new_custom(
          "ootb_provider_key_1".into(),
          "should be ignored as it conflicts with ootb provider key".into(),
        ),
        AnnotatedLogField::new_ootb(
          "ootb_provider_key_2".into(),
          "should be ignored as it conflicts with ootb provider key".into(),
        ),
      ],
      vec![],
      LogType::Lifecycle,
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
    fields: vec![
      AnnotatedLogField::new_custom(
        "_custom_provider_key".into(),
        "custom_provider_value".into(),
      ),
      AnnotatedLogField::new_ootb("_ootb_provider_key".into(), "ootb_provider_value".into()),
    ],
  };

  let mut collector = MetadataCollector::new(Arc::new(metadata));
  assert!(collector
    .add_field(LogField {
      key: "_collector_key".into(),
      value: StringOrBytes::String("collector_value".into()),
    })
    .is_err());

  let metadata = collector
    .normalized_metadata_with_extra_fields(vec![], vec![], LogType::Normal)
    .unwrap();

  assert!(collector
    .add_field(LogField {
      key: "app_id".into(),
      value: StringOrBytes::String("collector_value".into()),
    })
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
    fields: vec![],
  };

  let mut collector = MetadataCollector::new(Arc::new(metadata));

  collector
    .add_field(LogField {
      key: "key".into(),
      value: StringOrBytes::String("collector_field_1".into()),
    })
    .unwrap();

  collector
    .add_field(LogField {
      key: "key".into(),
      value: StringOrBytes::String("collector_field_2".into()),
    })
    .unwrap();

  assert_eq!(collector.fields().len(), 1);
  assert_matches!(&collector.fields()[0].value, StringOrBytes::String(value) if value == "collector_field_2");

  collector.remove_field("key");

  assert!(collector.fields().is_empty());
}

#[test]
fn provider_fields_earlier_in_the_list_take_precedence() {
  let metadata = LogMetadata {
    timestamp: Mutex::new(time::OffsetDateTime::now_utc()),
    fields: vec![
      AnnotatedLogField::new_custom("key_1".into(), "value_1".into()),
      AnnotatedLogField::new_custom("key_1".into(), "to_be_overridden_value_1".into()),
    ],
  };

  let collector = MetadataCollector::new(Arc::new(metadata));

  let metadata = collector
    .normalized_metadata_with_extra_fields(
      vec![AnnotatedLogField::new_ootb(
        "key_3".into(),
        "value_3".into(),
      )],
      vec![],
      LogType::Lifecycle,
    )
    .unwrap();

  assert_eq!(metadata.fields.len(), 2);
  assert_eq!(
    "value_1",
    expected_field_value(&metadata.fields, "key_1").unwrap()
  );
  assert_eq!(
    "value_3",
    expected_field_value(&metadata.fields, "key_3").unwrap()
  );
}

#[test]
fn passed_fields_earlier_in_the_list_take_precedence() {
  let metadata = LogMetadata {
    timestamp: Mutex::new(time::OffsetDateTime::now_utc()),
    fields: vec![
      AnnotatedLogField::new_custom("key_1".into(), "value_1".into()),
      AnnotatedLogField::new_ootb("key_2".into(), "value_2".into()),
    ],
  };

  let collector = MetadataCollector::new(Arc::new(metadata));

  let metadata = collector
    .normalized_metadata_with_extra_fields(
      vec![
        AnnotatedLogField::new_ootb("key_3".into(), "value_3".into()),
        AnnotatedLogField::new_ootb("key_4".into(), "value_4".into()),
        AnnotatedLogField::new_ootb("key_4".into(), "to_be_override_value_4".into()),
      ],
      vec![],
      LogType::Lifecycle,
    )
    .unwrap();

  assert_eq!(metadata.fields.len(), 4);
  assert_eq!(
    "value_1",
    expected_field_value(&metadata.fields, "key_1").unwrap()
  );
  assert_eq!(
    "value_2",
    expected_field_value(&metadata.fields, "key_2").unwrap()
  );
  assert_eq!(
    "value_3",
    expected_field_value(&metadata.fields, "key_3").unwrap()
  );
  assert_eq!(
    "value_4",
    expected_field_value(&metadata.fields, "key_4").unwrap()
  );
}

fn expected_field_value(fields: &LogFields, key: &str) -> Option<String> {
  let field = fields.iter().find(|field| key == field.key).unwrap();

  if let StringOrBytes::String(s) = &field.value {
    Some(s.clone())
  } else {
    None
  }
}
