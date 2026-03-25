// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

#![allow(clippy::unwrap_used)]

use crate::annotations::{decode_repeated_enum_unknown_fields, extension_field_number};
use bd_proto::protos::google::api::annotations;
use protobuf::descriptor::MethodOptions;
use protobuf::descriptor::field_descriptor_proto::Type;
use protobuf::{Enum, Message};

#[test]
fn extension_field_number_resolves_generated_extension() {
  assert_eq!(
    extension_field_number(annotations::file_descriptor(), "http"),
    Some(72_295_728)
  );
}

#[test]
fn decode_repeated_enum_unknown_fields_decodes_varint_and_packed_values() {
  let mut method_options = MethodOptions::new();
  method_options
    .mut_unknown_fields()
    .add_varint(12_345, u64::try_from(Type::TYPE_STRING.value()).unwrap());
  method_options.mut_unknown_fields().add_length_delimited(
    12_345,
    vec![
      u8::try_from(Type::TYPE_BOOL.value()).unwrap(),
      u8::try_from(Type::TYPE_STRING.value()).unwrap(),
      0xff,
    ],
  );
  method_options.mut_unknown_fields().add_varint(54_321, 999);

  assert_eq!(
    decode_repeated_enum_unknown_fields::<Type>(
      method_options.special_fields.unknown_fields(),
      12_345
    ),
    vec![Type::TYPE_STRING, Type::TYPE_BOOL]
  );
}
