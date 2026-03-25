// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

#[cfg(test)]
#[path = "./annotations_test.rs"]
mod tests;

use protobuf::reflect::FileDescriptor;
use protobuf::{CodedInputStream, Enum, UnknownFields, UnknownValueRef};

//
// Annotation helpers
//

/// Resolve a top-level protobuf extension field number by name from the generated file descriptor.
#[must_use]
pub fn extension_field_number(
  file_descriptor: &FileDescriptor,
  extension_name: &str,
) -> Option<u32> {
  file_descriptor
    .extensions()
    .find(|extension| extension.name() == extension_name)
    .and_then(|extension| u32::try_from(extension.number()).ok())
}

/// Decode repeated enum values from protobuf unknown fields.
///
/// This handles both unpacked varints and packed length-delimited enum encodings, preserving
/// order while skipping invalid values and duplicates.
#[must_use]
pub fn decode_repeated_enum_unknown_fields<EnumType: Enum + Eq>(
  unknown_fields: &UnknownFields,
  field_number: u32,
) -> Vec<EnumType> {
  let mut values = Vec::new();

  for (unknown_field_number, value) in unknown_fields {
    if unknown_field_number != field_number {
      continue;
    }

    match value {
      UnknownValueRef::Varint(raw_value) => {
        push_decoded_enum(raw_value, &mut values);
      },
      UnknownValueRef::LengthDelimited(raw_values) => {
        push_packed_enum_values(raw_values, &mut values);
      },
      _ => {},
    }
  }

  values
}

fn push_decoded_enum<EnumType: Enum + Eq>(raw_value: u64, values: &mut Vec<EnumType>) {
  let Ok(raw_value) = i32::try_from(raw_value) else {
    return;
  };
  let Some(value) = EnumType::from_i32(raw_value) else {
    return;
  };
  if values.contains(&value) {
    return;
  }

  values.push(value);
}

fn push_packed_enum_values<EnumType: Enum + Eq>(raw_values: &[u8], values: &mut Vec<EnumType>) {
  let mut input = CodedInputStream::from_bytes(raw_values);

  while !input.eof().unwrap_or(true) {
    let Ok(raw_value) = input.read_raw_varint64() else {
      break;
    };
    push_decoded_enum(raw_value, values);
  }
}
