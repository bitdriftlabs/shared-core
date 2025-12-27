// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

use anyhow::Result;
use protobuf::CodedInputStream;
use protobuf::rt::WireType;

/// Reads a length-delimited nested message from the protobuf stream.
///
/// The visitor closure is called for each field within the nested message.
/// - If the visitor returns `Ok(true)`, it means the field was handled (read)
/// - If the visitor returns `Ok(false)`, the field is skipped using the wire type
///
/// This function is used by the proc macro for deserializing nested messages.
pub fn read_nested(
  is: &mut CodedInputStream<'_>,
  mut visitor: impl FnMut(&mut CodedInputStream<'_>, u32, WireType) -> Result<bool>,
) -> Result<()> {
  let len = is.read_raw_varint32()?;
  let old_limit = is.push_limit(u64::from(len))?;

  while !is.eof()? {
    let tag = is.read_raw_varint32()?;
    let field_number = tag >> 3;
    let wire_type_bits = tag & 0x07;
    let wire_type = match wire_type_bits {
      0 => WireType::Varint,
      1 => WireType::Fixed64,
      2 => WireType::LengthDelimited,
      3 => WireType::StartGroup,
      4 => WireType::EndGroup,
      5 => WireType::Fixed32,
      _ => {
        return Err(anyhow::anyhow!(
          "Unknown wire type {wire_type_bits} (tag={tag}, field={field_number})"
        ));
      },
    };

    if !visitor(is, field_number, wire_type)? {
      is.skip_field(wire_type)?;
    }
  }

  is.pop_limit(old_limit);
  Ok(())
}
