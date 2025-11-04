// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

use bd_proto::protos::logging::payload::log::Field;
use bd_proto::protos::logging::payload::{BinaryData, Data, Log};
use protobuf::MessageFull;

#[test]
fn custom_proto_encoder() {
  // If any of these fail, new fields have been added. Adjust the encoder or fix the tests
  // as needed.
  Log::descriptor()
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
      "string_data" | "binary_data" => {},
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
