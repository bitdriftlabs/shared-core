// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

use super::validate;
use crate::error;
use crate::generated::test_protos;
use crate::generated::test_protos::test_validate::{Duration, Int32, Int64, Uint64};
use bd_time::ToProtoDuration;
use protobuf::Message as ProtoMessage;
use protobuf::well_known_types::duration::Duration as ProtoDuration;
use test_protos::test_validate::{
  Bool,
  EnumNew,
  EnumOld,
  Message,
  NotImplemented,
  OneOf,
  Repeated,
  String,
  Uint32,
  message,
  one_of,
  repeated,
};
use time::ext::NumericalDuration;

#[test]
fn duration() {
  let message = Duration {
    field: Some(ProtoDuration::default()).into(),
    ..Default::default()
  };
  matches::assert_matches!(
    validate(&message),
    Err(error::Error::ProtoValidation(message)) if message ==
    "duration 'proto_validate.test.Duration.field' in message 'proto_validate.test.Duration' \
    requires > 0ns");

  let message = Duration {
    field: Some(ProtoDuration {
      seconds: -1,
      ..Default::default()
    })
    .into(),
    ..Default::default()
  };
  matches::assert_matches!(
    validate(&message),
    Err(error::Error::ProtoValidation(message)) if message ==
    "negative proto duration not supported");

  let message = Duration {
    field: 1.seconds().into_proto(),
    ..Default::default()
  };
  assert!(validate(&message).is_ok());
}

#[test]
fn bool() {
  let message = Bool::default();
  matches::assert_matches!(
    validate(&message),
    Err(error::Error::ProtoValidation(message)) if message ==
    "field 'proto_validate.test.Bool.field' in message 'proto_validate.test.Bool' \
    must be constant true");

  let message = Bool {
    field: true,
    ..Default::default()
  };
  assert!(validate(&message).is_ok());
}

#[test]
fn string() {
  let message = String::default();
  matches::assert_matches!(
    validate(&message),
    Err(error::Error::ProtoValidation(message)) if message ==
    "field 'proto_validate.test.String.field' in message 'proto_validate.test.String' requires \
    string length >= 1");

  let mut message = String {
    field: "hello".to_string(),
    ..Default::default()
  };
  assert!(validate(&message).is_ok());
  message.field2 = "world".to_string();
  matches::assert_matches!(
    validate(&message),
    Err(error::Error::ProtoValidation(message)) if message ==
    "field 'proto_validate.test.String.field2' in message 'proto_validate.test.String' requires \
    string length <= 2");
}

#[test]
fn repeated() {
  let message = Repeated::default();
  matches::assert_matches!(
    validate(&message),
    Err(error::Error::ProtoValidation(message)) if message ==
    "field 'proto_validate.test.Repeated.strings' in message 'proto_validate.test.Repeated' \
    requires repeated items >= 1");

  let message = Repeated {
    strings: vec!["hello".to_string()],
    ..Default::default()
  };
  matches::assert_matches!(
    validate(&message),
    Err(error::Error::ProtoValidation(message)) if message ==
    "field 'proto_validate.test.Repeated.messages' in message 'proto_validate.test.Repeated' \
    requires repeated items >= 1");

  let message = Repeated {
    strings: vec!["hello".to_string()],
    messages: vec![repeated::Inner::default()],
    ..Default::default()
  };
  assert!(validate(&message).is_ok());
}

#[test]
fn message() {
  let message = Message::default();
  matches::assert_matches!(
    validate(&message),
    Err(error::Error::ProtoValidation(message)) if message ==
    "field 'proto_validate.test.Message.inner' in message 'proto_validate.test.Message' is \
    required");

  let message = Message {
    inner: Some(message::Inner::default()).into(),
    ..Default::default()
  };
  assert!(validate(&message).is_ok());
}

#[test]
fn oneof() {
  let message = OneOf::default();
  matches::assert_matches!(
    validate(&message),
    Err(error::Error::ProtoValidation(message)) if message ==
    "oneof 'proto_validate.test.OneOf.test' in message 'proto_validate.test.OneOf' is required to \
    be set");

  let message = OneOf {
    test: Some(one_of::Test::Field1(true)),
    ..Default::default()
  };
  assert!(validate(&message).is_ok());
}

#[test]
fn not_implemented() {
  // This is obviously not exhaustive but at least make sure the function works.
  let message = NotImplemented::default();
  matches::assert_matches!(
    validate(&message),
    Err(error::Error::ProtoValidation(message)) if message ==
    "not implemented: string rules max_bytes");
}

#[test]
fn enum_defined_only() {
  let message = EnumOld::default();
  assert!(validate(&message).is_ok());

  // Round trip from a version of the message with an enum value that the old one doesn't know
  // about.
  let mut new_message = EnumNew::new();
  new_message.field = test_protos::test_validate::enum_new::Enum::BAR.into();
  let round_trip_message =
    EnumOld::parse_from_bytes(&new_message.write_to_bytes().unwrap()).unwrap();
  matches::assert_matches!(
      validate(&round_trip_message),
      Err(error::Error::ProtoValidation(message)) if message ==
      "field 'proto_validate.test.EnumOld.field' in message 'proto_validate.test.EnumOld' must be \
      a defined enum. Got 1");
}

#[test]
fn int() {
  let mut message = Uint32::new();
  matches::assert_matches!(
    validate(&message),
    Err(error::Error::ProtoValidation(message)) if message ==
    "field 'proto_validate.test.Uint32.field' in message 'proto_validate.test.Uint32' must be > 0");

  message.field = 1;
  assert!(validate(&message).is_ok());

  let mut message = Uint64::new();
  matches::assert_matches!(
    validate(&message),
    Err(error::Error::ProtoValidation(message)) if message ==
    "field 'proto_validate.test.Uint64.field' in message 'proto_validate.test.Uint64' must be > 0");

  message.field = 1;
  assert!(validate(&message).is_ok());

  let mut message = Int32::new();
  matches::assert_matches!(
    validate(&message),
    Err(error::Error::ProtoValidation(message)) if message ==
    "field 'proto_validate.test.Int32.field' in message 'proto_validate.test.Int32' must be > 0");

  message.field = 1;
  assert!(validate(&message).is_ok());

  let mut message = Int64::new();
  matches::assert_matches!(
    validate(&message),
    Err(error::Error::ProtoValidation(message)) if message ==
    "field 'proto_validate.test.Int64.field' in message 'proto_validate.test.Int64' must be > 0");

  message.field = 1;
  assert!(validate(&message).is_ok());
}
