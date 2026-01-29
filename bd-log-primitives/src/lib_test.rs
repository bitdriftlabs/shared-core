// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

#![allow(clippy::cast_possible_truncation, clippy::unwrap_used)]

use crate::{DataValue, EncodableLog, Log, LogFieldValue, LogType};
use ahash::AHashMap;
use bd_proto::protos::logging::payload::data::Data_type;
use bd_proto::protos::logging::payload::log::Field;
use bd_proto::protos::logging::payload::{BinaryData, Data};
use ordered_float::NotNan;
use protobuf::{Message, MessageFull};
use std::borrow::Cow;
use time::OffsetDateTime;

#[test]
fn custom_proto_encoder() {
  // If any of these fail, new fields have been added. Adjust the encoder or fix the tests
  // as needed.
  bd_proto::protos::logging::payload::Log::descriptor()
    .fields()
    .for_each(|field| match field.name() {
      "timestamp_unix_micro"
      | "log_level"
      | "message"
      | "fields"
      | "session_id"
      | "action_ids"
      | "log_type"
      | "stream_ids"
      | "compressed_contents" => {},
      other => panic!("unexpected field added to Log proto: {other}"),
    });
  Field::descriptor()
    .fields()
    .for_each(|field| match field.name() {
      "key" | "value" => {},
      other => panic!("unexpected field added to Field proto: {other}"),
    });
  Data::descriptor()
    .fields()
    .for_each(|field| match field.name() {
      "string_data" | "binary_data" | "int_data" | "double_data" | "bool_data" | "sint_data" => {},
      other => panic!("unexpected field added to Data proto: {other}"),
    });
  // Note that "type" is unused currently and not encoded.
  BinaryData::descriptor()
    .fields()
    .for_each(|field| match field.name() {
      "type" | "payload" => {},
      other => panic!("unexpected field added to BinaryData proto: {other}"),
    });
}

#[test]
fn data_encoding() {
  let cases = vec![
    (
      LogFieldValue::String("test".to_string()),
      Data {
        data_type: Some(Data_type::StringData("test".to_string())),
        ..Default::default()
      },
    ),
    (
      LogFieldValue::Boolean(true),
      Data {
        data_type: Some(Data_type::BoolData(true)),
        ..Default::default()
      },
    ),
    (
      LogFieldValue::U64(123),
      Data {
        data_type: Some(Data_type::IntData(123)),
        ..Default::default()
      },
    ),
    (
      LogFieldValue::I64(-123),
      Data {
        data_type: Some(Data_type::SintData(-123)),
        ..Default::default()
      },
    ),
    (
      LogFieldValue::Double(NotNan::new(1.23).unwrap()),
      Data {
        data_type: Some(Data_type::DoubleData(1.23)),
        ..Default::default()
      },
    ),
    (
      LogFieldValue::Bytes(vec![1, 2, 3, 4].into()),
      Data {
        data_type: Some(Data_type::BinaryData(BinaryData {
          payload: vec![1, 2, 3, 4],
          ..Default::default()
        })),
        ..Default::default()
      },
    ),
  ];

  for (input, expected) in cases {
    let log = Log {
      log_level: 1,
      log_type: LogType::NORMAL,
      message: input.clone(),
      fields: AHashMap::new(),
      matching_fields: AHashMap::new(),
      session_id: "test_session".to_string(),
      occurred_at: OffsetDateTime::now_utc(),
      capture_session: None,
    };
    let mut encodable = EncodableLog::new(log, 1000);

    let size = encodable.compute_size(&[], &[]).unwrap();
    let mut output_bytes = vec![0; usize::try_from(size).unwrap()];
    encodable
      .serialize_to_bytes(&[], &[], &mut output_bytes)
      .unwrap();

    let decoded_log =
      bd_proto::protos::logging::payload::Log::parse_from_bytes(&output_bytes).unwrap();
    assert_eq!(
      decoded_log.message.unwrap(),
      expected,
      "Failed for {input:?}"
    );
  }
}

#[test]
fn encodable_log_produces_valid_proto() {
  // Verify that EncodableLog produces valid protobuf that can be decoded.
  let log = Log {
    log_level: 2,
    log_type: LogType::REPLAY,
    message: DataValue::String("test message".to_string()),
    fields: AHashMap::from_iter([
      (
        Cow::Borrowed("key1"),
        LogFieldValue::String("value1".to_string()),
      ),
      (Cow::Borrowed("key2"), LogFieldValue::U64(42)),
    ]),
    matching_fields: AHashMap::new(),
    session_id: "test_session_123".to_string(),
    occurred_at: OffsetDateTime::from_unix_timestamp(1_700_000_000).unwrap(),
    capture_session: None,
  };

  let action_ids: Vec<&str> = vec!["action1", "action2"];
  let stream_ids: Vec<&str> = vec!["stream1"];

  let mut encodable = EncodableLog::new(log, 1_000_000); // High threshold = no compression

  // Compute size and serialize
  let size = encodable.compute_size(&action_ids, &stream_ids).unwrap();
  let mut output_bytes = vec![0u8; size as usize];
  encodable
    .serialize_to_bytes(&action_ids, &stream_ids, &mut output_bytes)
    .unwrap();

  // Verify the output parses correctly
  let decoded = bd_proto::protos::logging::payload::Log::parse_from_bytes(&output_bytes).unwrap();

  assert_eq!(decoded.timestamp_unix_micro, 1_700_000_000_000_000u64);
  assert_eq!(decoded.log_level, 2);
  assert_eq!(decoded.session_id, "test_session_123");
  assert_eq!(
    decoded.log_type.unwrap(),
    bd_proto::protos::logging::payload::LogType::REPLAY
  );
  assert_eq!(decoded.action_ids, vec!["action1", "action2"]);
  assert_eq!(decoded.stream_ids, vec!["stream1"]);

  // Verify the message
  let message_data = decoded.message.unwrap();
  assert_eq!(
    message_data.data_type,
    Some(Data_type::StringData("test message".to_string()))
  );

  // Verify fields (order may vary, so check both are present)
  assert_eq!(decoded.fields.len(), 2);
  let field_map: AHashMap<_, _> = decoded
    .fields
    .iter()
    .map(|f| (f.key.as_str(), f.value.clone()))
    .collect();
  assert_eq!(
    field_map.get("key1").unwrap().as_ref().unwrap().data_type,
    Some(Data_type::StringData("value1".to_string()))
  );
  assert_eq!(
    field_map.get("key2").unwrap().as_ref().unwrap().data_type,
    Some(Data_type::IntData(42))
  );
}

#[test]
fn encodable_log_compression_works() {
  // Test that compression works correctly when the data exceeds the threshold.

  // Create a log with a large message that will trigger compression
  let large_message = "A".repeat(2000);
  let log = Log {
    log_level: 1,
    log_type: LogType::NORMAL,
    message: DataValue::String(large_message),
    fields: AHashMap::from_iter([(
      Cow::Borrowed("key"),
      LogFieldValue::String("value".to_string()),
    )]),
    matching_fields: AHashMap::new(),
    session_id: "sess".to_string(),
    occurred_at: OffsetDateTime::from_unix_timestamp(1_500_000_000).unwrap(),
    capture_session: None,
  };

  let action_ids: Vec<&str> = vec!["a1"];
  let stream_ids: Vec<&str> = vec!["s1"];

  // Serialize using EncodableLog with LOW threshold to trigger compression
  let mut encodable = EncodableLog::new(log, 100); // Low threshold to force compression
  let size = encodable.compute_size(&action_ids, &stream_ids).unwrap();
  let mut output_bytes = vec![0u8; size as usize];
  encodable
    .serialize_to_bytes(&action_ids, &stream_ids, &mut output_bytes)
    .unwrap();

  // Parse the output
  let decoded = bd_proto::protos::logging::payload::Log::parse_from_bytes(&output_bytes).unwrap();

  // For compressed logs, message and fields should be empty in the proto
  assert!(decoded.message.is_none());
  assert!(decoded.fields.is_empty());

  // compressed_contents should be non-empty
  assert!(!decoded.compressed_contents.is_empty());

  // Verify other fields still match
  assert_eq!(decoded.timestamp_unix_micro, 1_500_000_000_000_000u64);
  assert_eq!(decoded.log_level, 1);
  assert_eq!(decoded.session_id, "sess");
  assert_eq!(
    decoded.log_type.unwrap(),
    bd_proto::protos::logging::payload::LogType::NORMAL
  );
  assert_eq!(decoded.action_ids, vec!["a1"]);
  assert_eq!(decoded.stream_ids, vec!["s1"]);
}

#[test]
fn extract_timestamp_works() {
  let log = Log {
    log_level: 1,
    log_type: LogType::NORMAL,
    message: DataValue::String("test".to_string()),
    fields: AHashMap::new(),
    matching_fields: AHashMap::new(),
    session_id: "test".to_string(),
    occurred_at: OffsetDateTime::from_unix_timestamp(1_700_000_000).unwrap(),
    capture_session: None,
  };

  let mut encodable = EncodableLog::new(log, 1_000_000);
  let size = encodable.compute_size(&[], &[]).unwrap();
  let mut output_bytes = vec![0u8; size as usize];
  encodable
    .serialize_to_bytes(&[], &[], &mut output_bytes)
    .unwrap();

  let extracted = EncodableLog::extract_timestamp(&output_bytes).unwrap();
  assert_eq!(
    extracted,
    OffsetDateTime::from_unix_timestamp(1_700_000_000).unwrap()
  );
}
