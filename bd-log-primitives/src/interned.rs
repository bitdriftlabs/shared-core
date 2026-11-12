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
use bd_proto_util::serialization::{
  CanonicalType,
  ProtoFieldSerialize,
  ProtoType,
  compute_map_size,
  serialize_map,
};
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
  /// For each field of the log, in the log's iteration order, whether it is left out. The log's
  /// fields must not change after this is computed, as with the rest of the log's encoding.
  left_out: Vec<bool>,
}

impl InternedFields {
  /// Compares `fields` with the field set in one pass. Returns `None` if no field would be left
  /// out, so the log should be written fully inline.
  #[must_use]
  pub fn new(fields: &LogFields, version: u64, field_set: Arc<LogFields>) -> Option<Self> {
    let mut present = 0;
    let mut left_out_count = 0;
    let left_out: Vec<bool> = fields
      .iter()
      .map(|(key, value)| {
        let Some(set_value) = field_set.get(key) else {
          return false;
        };
        present += 1;
        let same = same_value(set_value, value);
        left_out_count += usize::from(same);
        same
      })
      .collect();
    if left_out_count == 0 {
      return None;
    }

    // A key of the set that the log does not carry was removed by a filter.
    let cleared = if present == field_set.len() {
      Vec::new()
    } else {
      field_set
        .keys()
        .filter(|key| !fields.contains_key(*key))
        .cloned()
        .collect()
    };
    Some(Self {
      version,
      field_set,
      cleared,
      left_out,
    })
  }
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
///
/// This replaces encoding `&LogFields` directly for two reasons. Leaving fields out by filtering
/// the log's own map avoids copying it on every buffer write. And a `cleared` marker is a
/// `Log.Field` with `cleared = 3` set instead of a value, so it cannot be written as a map entry;
/// it shares the repeated `fields` field with the entries, so the two are written together here.
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
    let left_out = self.interned.map(|interned| {
      debug_assert_eq!(
        interned.left_out.len(),
        self.fields.len(),
        "log fields changed after interning"
      );
      &interned.left_out
    });
    let mut left_out = left_out.map(|left_out| left_out.iter());
    self.fields.iter().filter(move |_| {
      !left_out
        .as_mut()
        .is_some_and(|left_out| left_out.next().copied().unwrap_or(false))
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
    let cleared: u64 = self
      .cleared()
      .iter()
      .map(|key| {
        let entry_size = Self::cleared_entry_size(key);
        tag_size + protobuf::rt::compute_raw_varint64_size(entry_size) + entry_size
      })
      .sum();
    compute_map_size(self.encoded_fields(), field_number) + cleared
  }

  fn write(&self, field_number: u32, os: &mut CodedOutputStream<'_>) -> anyhow::Result<()> {
    serialize_map(self.encoded_fields(), field_number, os)?;
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
/// This runs for every uploaded log, so it reads only top-level tags instead of parsing the whole
/// log, and compressed contents are never decompressed. If the field is repeated, the last value
/// wins, as in the generated parser.
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
