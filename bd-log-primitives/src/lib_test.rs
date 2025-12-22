// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

use crate::{Log, LogEncodingHelper, LogFieldValue, LogType};
use ahash::AHashMap;
use bd_proto::protos::logging::payload::data::Data_type;
use bd_proto::protos::logging::payload::log::Field;
use bd_proto::protos::logging::payload::{BinaryData, Data};
use ordered_float::NotNan;
use protobuf::{Message, MessageFull};
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
fn test_data_encoding() {
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
    let mut helper = LogEncodingHelper::new(log, 1000);

    // Test size calculation
    // We can't easily test just the Data size without exposing private methods,
    // but we can verify end-to-end serialization

    // Manually serialize just the data part using the public inner method style
    // Since serialize_proto_data is private, we'll verify via the Log serialization
    // by creating a log with this message

    let size = helper.serialized_proto_size(&[], &[]).unwrap();
    let mut output_bytes = vec![0; usize::try_from(size).unwrap()];
    helper
      .serialized_proto_to_bytes(&[], &[], &mut output_bytes)
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
