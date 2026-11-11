// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE.polyform file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

#[cfg(test)]
#[path = "./interned_test.rs"]
mod interned_test;

use crate::{DataValue, LogFieldKey, LogFieldValue, LogFields};
use bd_proto_util::serialization::{CanonicalType, ProtoFieldSerialize, ProtoType};
use protobuf::rt::WireType;
use protobuf::{CodedInputStream, CodedOutputStream};
use std::sync::Arc;

/// `uint64 field_set_version = 11;` in `bitdrift_public.protobuf.logging.v1.Log`.
pub const FIELD_SET_VERSION_FIELD_NUMBER: u32 = 11;

/// `bool cleared = 3;` in `bitdrift_public.protobuf.logging.v1.Log.Field`.
const CLEARED_FIELD_NUMBER: u32 = 3;

//
// InternedFields
//

/// The persistent field set a log was compared with when it was written to a ring buffer.
///
/// Every field of the set whose value the log carries unchanged is left out of the encoded log,
/// which records the set's version instead. A key of the set that the log does not carry, because
/// a filter removed it, is encoded as a `cleared` field so the server does not restore it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InternedFields {
  pub version: u64,
  pub field_set: Arc<LogFields>,
  pub cleared: Vec<LogFieldKey>,
}

/// Whether two field values encode identically. String values compare by content, so the
/// static, shared and owned representations of one string are equal; doubles compare by bits, so
/// `0.0` and `-0.0` differ.
#[must_use]
pub fn same_value(a: &LogFieldValue, b: &LogFieldValue) -> bool {
  match (a, b) {
    (DataValue::Double(a), DataValue::Double(b)) => {
      a.into_inner().to_bits() == b.into_inner().to_bits()
    },
    (DataValue::Map(a), DataValue::Map(b)) => {
      a.entries().len() == b.entries().len()
        && a
          .entries()
          .iter()
          .all(|(key, a)| b.entries().get(key).is_some_and(|b| same_value(a, b)))
    },
    (DataValue::Array(a), DataValue::Array(b)) => {
      a.items().len() == b.items().len()
        && a
          .items()
          .iter()
          .zip(b.items())
          .all(|(a, b)| same_value(a, b))
    },
    _ => match (a.as_str(), b.as_str()) {
      (Some(a), Some(b)) => a == b,
      (None, None) => a == b,
      _ => false,
    },
  }
}

//
// LogFieldsRef
//

/// The fields of a log as they are encoded: the inline fields minus those left out by interning,
/// followed by any `cleared` markers.
#[derive(Debug, Clone, Copy)]
pub struct LogFieldsRef<'a> {
  fields: &'a LogFields,
  interned: Option<&'a InternedFields>,
}

impl<'a> LogFieldsRef<'a> {
  /// Encodes every field inline.
  #[must_use]
  pub const fn inline(fields: &'a LogFields) -> Self {
    Self {
      fields,
      interned: None,
    }
  }

  #[must_use]
  pub const fn new(fields: &'a LogFields, interned: Option<&'a InternedFields>) -> Self {
    Self { fields, interned }
  }

  #[must_use]
  pub fn field_set_version(&self) -> u64 {
    self.interned.map_or(0, |interned| interned.version)
  }

  fn encoded_fields(&self) -> impl Iterator<Item = (&'a LogFieldKey, &'a LogFieldValue)> {
    let interned = self.interned;
    self.fields.iter().filter(move |(key, value)| {
      interned.is_none_or(|interned| {
        !interned
          .field_set
          .get(*key)
          .is_some_and(|set_value| same_value(set_value, value))
      })
    })
  }

  fn cleared(&self) -> &'a [LogFieldKey] {
    self.interned.map_or(&[], |interned| &interned.cleared)
  }

  fn cleared_entry_size(key: &LogFieldKey) -> u64 {
    // The key, then `cleared` as tag and a one-byte varint.
    key.compute_size(1) + protobuf::rt::tag_size(CLEARED_FIELD_NUMBER) + 1
  }

  fn compute(&self, field_number: u32) -> u64 {
    let tag_size = protobuf::rt::tag_size(field_number);
    let entry =
      |entry_size: u64| tag_size + protobuf::rt::compute_raw_varint64_size(entry_size) + entry_size;
    self
      .encoded_fields()
      .map(|(key, value)| entry(key.compute_size(1) + value.compute_size(2)))
      .chain(
        self
          .cleared()
          .iter()
          .map(|key| entry(Self::cleared_entry_size(key))),
      )
      .sum()
  }

  fn write(&self, field_number: u32, os: &mut CodedOutputStream<'_>) -> anyhow::Result<()> {
    for (key, value) in self.encoded_fields() {
      os.write_tag(field_number, WireType::LengthDelimited)?;
      os.write_raw_varint64(key.compute_size(1) + value.compute_size(2))?;
      key.serialize(1, os)?;
      value.serialize(2, os)?;
    }
    for key in self.cleared() {
      os.write_tag(field_number, WireType::LengthDelimited)?;
      os.write_raw_varint64(Self::cleared_entry_size(key))?;
      key.serialize(1, os)?;
      os.write_bool(CLEARED_FIELD_NUMBER, true)?;
    }
    Ok(())
  }
}

impl ProtoType for LogFieldsRef<'_> {
  fn wire_type() -> WireType {
    WireType::LengthDelimited
  }

  fn canonical_type() -> CanonicalType {
    <LogFields as ProtoType>::canonical_type()
  }
}

impl ProtoFieldSerialize for LogFieldsRef<'_> {
  fn compute_size(&self, field_number: u32) -> u64 {
    self.compute(field_number)
  }

  fn serialize(&self, field_number: u32, os: &mut CodedOutputStream<'_>) -> anyhow::Result<()> {
    self.write(field_number, os)
  }

  fn compute_size_explicit(&self, field_number: u32) -> u64 {
    self.compute(field_number)
  }

  fn serialize_explicit(
    &self,
    field_number: u32,
    os: &mut CodedOutputStream<'_>,
  ) -> anyhow::Result<()> {
    self.write(field_number, os)
  }
}

/// Returns the `field_set_version` of an encoded log, or 0 if it has none or cannot be read.
///
/// Only top-level tags are read, so compressed contents are never decompressed. If the field is
/// repeated, the last value wins, as in the generated parser.
#[must_use]
pub fn encoded_field_set_version(log: &[u8]) -> u64 {
  let mut input = CodedInputStream::from_bytes(log);
  let mut version = 0;
  loop {
    let tag = match input.read_raw_tag_or_eof() {
      Ok(Some(tag)) => tag,
      Ok(None) => return version,
      Err(_) => return 0,
    };
    let (number, wire_type) = (tag >> 3, tag & 7);
    let result = if number == FIELD_SET_VERSION_FIELD_NUMBER && wire_type == 0 {
      input.read_uint64().map(|value| version = value)
    } else if let Some(wire_type) = WireType::new(wire_type) {
      input.skip_field(wire_type)
    } else {
      return 0;
    };
    if result.is_err() {
      return 0;
    }
  }
}
