// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

use anyhow::Result;
use protobuf::CodedInputStream;
use protobuf::rt::WireType;

pub(crate) const TAG_TYPE_BITS: u32 = 3;
/// Apply this mask to varint value to obtain a tag.
pub(crate) const TAG_TYPE_MASK: u32 = (1u32 << TAG_TYPE_BITS as usize) - 1;

/// Parsed field tag (a pair of field number and wire type)
///
/// This is a reimplementation of `protobuf::wire_format::Tag`, since that type is not
/// publicly exposed.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub struct Tag {
  pub field_number: u32,
  pub wire_type: WireType,
}

impl Tag {
  /// Extract wire type and field number from integer tag
  pub fn new(value: u32) -> anyhow::Result<Self> {
    let Some(wire_type) = WireType::new(value & TAG_TYPE_MASK) else {
      anyhow::bail!("Incorrect tag value: {value}");
    };
    let field_number = value >> TAG_TYPE_BITS;
    if field_number == 0 {
      anyhow::bail!("Incorrect tag value: {value}");
    }
    Ok(Self {
      field_number,
      wire_type,
    })
  }
}

/// Reads a length-delimited nested message from the protobuf stream.
///
/// The visitor closure is called for each field within the nested message.
/// - If the visitor returns `Ok(true)`, it means the field was handled (read)
/// - If the visitor returns `Ok(false)`, the field is skipped using the wire type
///
/// This function is used by the proc macro for deserializing nested messages.
pub fn read_nested(
  is: &mut CodedInputStream<'_>,
  mut visitor: impl FnMut(&mut CodedInputStream<'_>, Tag) -> Result<bool>,
) -> Result<()> {
  let len = is.read_raw_varint32()?;
  let old_limit = is.push_limit(u64::from(len))?;

  while !is.eof()? {
    let tag = is.read_raw_varint32()?;
    let tag = Tag::new(tag)?;

    if !visitor(is, tag)? {
      is.skip_field(tag.wire_type)?;
    }
  }

  is.pop_limit(old_limit);
  Ok(())
}
