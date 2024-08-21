// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

use crate::metadata::MetadataCollector;
use assert_matches::assert_matches;
use bd_log_metadata::LogFieldKind;
use bd_log_primitives::{AnnotatedLogField, LogField, LogFields, StringOrBytes};
use bd_proto::flatbuffers::buffer_log::bitdrift_public::fbs::logging::v_1::LogType;
use bd_test_helpers::metadata_provider::LogMetadata;
use std::sync::Arc;


#[test]
fn collector_attaches_provider_fields_as_matching_fields() {
  let metadata = LogMetadata {
    timestamp: time::OffsetDateTime::now_utc(),
    fields: vec![AnnotatedLogField {
      field: LogField {
        key: "key".into(),
        value: StringOrBytes::String("provider_value".into()),
      },
      kind: LogFieldKind::Ootb,
    }],
  };

  let collector = MetadataCollector::new(Arc::new(metadata));

  let types = vec![LogType::Replay, LogType::Resource];

  for log_type in types {
    let metadata = collector
      .normalized_metadata_with_extra_fields(
        vec![
          AnnotatedLogField {
            field: LogField {
              key: "key".into(),
              value: StringOrBytes::String("value".into()),
            },
            kind: LogFieldKind::Ootb,
          },
          AnnotatedLogField {
            field: LogField {
              key: "key2".into(),
              value: StringOrBytes::String("value2".into()),
            },
            kind: LogFieldKind::Ootb,
          },
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
    timestamp: time::OffsetDateTime::now_utc(),
    fields: vec![
      AnnotatedLogField {
        field: LogField {
          key: "custom_provider_key".into(),
          value: StringOrBytes::String("custom_provider_value".into()),
        },
        kind: LogFieldKind::Custom,
      },
      AnnotatedLogField {
        field: LogField {
          key: "ootb_provider_key_1".into(),
          value: StringOrBytes::String("ootb_provider_value_1".into()),
        },
        kind: LogFieldKind::Ootb,
      },
      AnnotatedLogField {
        field: LogField {
          key: "ootb_provider_key_2".into(),
          value: StringOrBytes::String("ootb_provider_value_2".into()),
        },
        kind: LogFieldKind::Ootb,
      },
      AnnotatedLogField {
        field: LogField {
          key: "provider_key".into(),
          value: StringOrBytes::String("custom_provider_value".into()),
        },
        kind: LogFieldKind::Custom,
      },
      AnnotatedLogField {
        field: LogField {
          key: "provider_key".into(),
          value: StringOrBytes::String("ootb_provider_value".into()),
        },
        kind: LogFieldKind::Ootb,
      },
      AnnotatedLogField {
        field: LogField {
          key: "collector_key".into(),
          value: StringOrBytes::String("custom_provider_value".into()),
        },
        kind: LogFieldKind::Custom,
      },
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
        AnnotatedLogField {
          field: LogField {
            key: "key".into(),
            value: StringOrBytes::String("value".into()),
          },
          kind: LogFieldKind::Ootb,
        },
        AnnotatedLogField {
          field: LogField {
            key: "_key".into(),
            value: StringOrBytes::String("_value".into()),
          },
          kind: LogFieldKind::Ootb,
        },
        AnnotatedLogField {
          field: LogField {
            key: "_should_be_dropped_key".into(),
            value: StringOrBytes::String("should be dropped as it uses reserved _ prefix".into()),
          },
          kind: LogFieldKind::Custom,
        },
        AnnotatedLogField {
          field: LogField {
            key: "ootb_provider_key_1".into(),
            value: StringOrBytes::String(
              "should be ignored as it conflicts with ootb provider key".into(),
            ),
          },
          kind: LogFieldKind::Custom,
        },
        AnnotatedLogField {
          field: LogField {
            key: "ootb_provider_key_2".into(),
            value: StringOrBytes::String(
              "should be ignored as it conflicts with ootb provider key".into(),
            ),
          },
          kind: LogFieldKind::Ootb,
        },
      ],
      vec![],
      LogType::Lifecycle,
    )
    .unwrap();

  assert_eq!(metadata.fields.len(), 7);
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
    timestamp: time::OffsetDateTime::now_utc(),
    fields: vec![
      AnnotatedLogField {
        field: LogField {
          key: "_custom_provider_key".into(),
          value: StringOrBytes::String("custom_provider_value".into()),
        },
        kind: LogFieldKind::Custom,
      },
      AnnotatedLogField {
        field: LogField {
          key: "_ootb_provider_key".into(),
          value: StringOrBytes::String("ootb_provider_value".into()),
        },
        kind: LogFieldKind::Ootb,
      },
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
    timestamp: time::OffsetDateTime::now_utc(),
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
    timestamp: time::OffsetDateTime::now_utc(),
    fields: vec![
      AnnotatedLogField {
        field: LogField {
          key: "key_1".into(),
          value: StringOrBytes::String("value_1".into()),
        },
        kind: LogFieldKind::Custom,
      },
      AnnotatedLogField {
        field: LogField {
          key: "key_1".into(),
          value: StringOrBytes::String("to_be_overridden_value_1".into()),
        },
        kind: LogFieldKind::Custom,
      },
    ],
  };

  let collector = MetadataCollector::new(Arc::new(metadata));

  let metadata = collector
    .normalized_metadata_with_extra_fields(
      vec![AnnotatedLogField {
        field: LogField {
          key: "key_3".into(),
          value: StringOrBytes::String("value_3".into()),
        },
        kind: LogFieldKind::Ootb,
      }],
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
    timestamp: time::OffsetDateTime::now_utc(),
    fields: vec![
      AnnotatedLogField {
        field: LogField {
          key: "key_1".into(),
          value: StringOrBytes::String("value_1".into()),
        },
        kind: LogFieldKind::Custom,
      },
      AnnotatedLogField {
        field: LogField {
          key: "key_2".into(),
          value: StringOrBytes::String("value_2".into()),
        },
        kind: LogFieldKind::Ootb,
      },
    ],
  };

  let collector = MetadataCollector::new(Arc::new(metadata));

  let metadata = collector
    .normalized_metadata_with_extra_fields(
      vec![
        AnnotatedLogField {
          field: LogField {
            key: "key_3".into(),
            value: StringOrBytes::String("value_3".into()),
          },
          kind: LogFieldKind::Ootb,
        },
        AnnotatedLogField {
          field: LogField {
            key: "key_4".into(),
            value: StringOrBytes::String("value_4".into()),
          },
          kind: LogFieldKind::Ootb,
        },
        AnnotatedLogField {
          field: LogField {
            key: "key_4".into(),
            value: StringOrBytes::String("to_be_override_value_4".into()),
          },
          kind: LogFieldKind::Ootb,
        },
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
