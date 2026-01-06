// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

//! Trait implementations for common types.
//!
//! This module contains ProtoFieldSerialize/ProtoFieldDeserialize implementations for:
//! - Primitive types (String, integers, bool, f64)
//! - Standard library types (`Vec<u8>`, `Option<T>`, `Box<T>`, `Cow<str>`, `Arc<str>`)
//! - Collection types (`HashMap`, `AHashMap`, `Vec<T>`, `BTreeSet<T>`)
//! - Time types (`TimestampMicros`)
//! - Special types (`LogType` enum, `NotNan<f64>`, unit type)

use crate::serialization::{
  CanonicalType,
  ProtoFieldDeserialize,
  ProtoFieldSerialize,
  ProtoType,
  RepeatedFieldDeserialize,
  wire,
};
use anyhow::Result;
use bd_time::OffsetDateTimeExt;
use ordered_float::FloatCore;
use protobuf::rt::WireType;
use protobuf::{CodedInputStream, CodedOutputStream};
use std::sync::Arc;

// Primitive scalar types - delegate to wire format types
impl_proto_scalar!(u32, wire::U32);
impl_proto_scalar!(u64, wire::U64);
impl_proto_scalar!(i32, wire::I32);
impl_proto_scalar!(i64, wire::I64);
impl_proto_scalar!(f32, wire::F32);
impl_proto_scalar!(f64, wire::F64);
impl_proto_scalar!(bool, wire::Bool);
impl_proto_scalar!(String, wire::StringOwned);
impl_proto_scalar!(Vec<u8>, wire::BytesOwned);

// NOTE: We intentionally do NOT implement ProtoFieldSerialize/ProtoFieldDeserialize for
// usize/isize.
//
// Rationale:
// - usize/isize have platform-dependent sizes (32-bit on some platforms, 64-bit on others)
// - Serializing them would require choosing either: a) Platform-specific wire format (breaks
//   cross-platform compatibility) b) Fixed wire format (uint64) with validation (adds surprising
//   behavior and potential runtime errors)
//
// If you need to serialize a size/length/index, explicitly choose:
// - u32 if your values will always fit in 4 bytes (most cases)
// - u64 if you need the full range

// String-like types that need conversion (implement AsRef<str>)
impl_proto_string_like!(std::borrow::Cow<'static, str>, |s: String| {
  std::borrow::Cow::Owned(s)
});
impl_proto_string_like!(std::sync::Arc<str>, |s: String| s.into());

// Transparent wrapper types (Box, Arc) - delegate to inner type
crate::impl_proto_for_type_wrapper!(Box<T>, T, Box::new);
crate::impl_proto_for_type_wrapper!(Arc<T>, T, std::sync::Arc::new);

impl<T: RepeatedFieldDeserialize> RepeatedFieldDeserialize for Box<T> {
  type Element = T::Element;

  fn deserialize_element(is: &mut CodedInputStream<'_>) -> anyhow::Result<Self::Element> {
    T::deserialize_element(is)
  }

  fn add_element(&mut self, element: Self::Element) {
    (**self).add_element(element);
  }
}

// Blanket implementation for Option<T>
// Note: For deserialization, this is slightly tricky because the tag handles the "None" case (by
// absence). This deserialize implementation assumes we ARE reading a value.

impl<T: ProtoType> ProtoType for Option<T> {
  fn wire_type() -> WireType {
    T::wire_type()
  }

  fn canonical_type() -> CanonicalType {
    CanonicalType::Optional(Box::new(T::canonical_type()))
  }
}

impl<T: ProtoFieldSerialize> ProtoFieldSerialize for Option<T> {
  fn compute_size(&self, field_number: u32) -> u64 {
    self.as_ref().map_or(0, |v| v.compute_size(field_number))
  }

  fn serialize(&self, field_number: u32, os: &mut CodedOutputStream<'_>) -> Result<()> {
    if let Some(v) = self {
      v.serialize(field_number, os)?;
    }
    Ok(())
  }

  fn compute_size_explicit(&self, field_number: u32) -> u64 {
    // Option always has explicit presence via Some/None, so when Some, always serialize the inner
    // value even if it's a default value
    self
      .as_ref()
      .map_or(0, |v| v.compute_size_explicit(field_number))
  }

  fn serialize_explicit(&self, field_number: u32, os: &mut CodedOutputStream<'_>) -> Result<()> {
    // Option always has explicit presence via Some/None, so when Some, always serialize the inner
    // value even if it's a default value
    if let Some(v) = self {
      v.serialize_explicit(field_number, os)?;
    }
    Ok(())
  }
}

impl<T: ProtoFieldDeserialize> ProtoFieldDeserialize for Option<T> {
  fn deserialize(is: &mut CodedInputStream<'_>) -> Result<Self> {
    Ok(Some(T::deserialize(is)?))
  }
}

// Blanket implementation for &T (serialization only)
impl<T: ProtoType> ProtoType for &T {
  fn wire_type() -> WireType {
    T::wire_type()
  }

  fn canonical_type() -> CanonicalType {
    T::canonical_type()
  }
}

impl<T: ProtoFieldSerialize> ProtoFieldSerialize for &T {
  fn compute_size(&self, field_number: u32) -> u64 {
    (**self).compute_size(field_number)
  }

  fn serialize(&self, field_number: u32, os: &mut CodedOutputStream<'_>) -> Result<()> {
    (**self).serialize(field_number, os)
  }

  fn compute_size_explicit(&self, field_number: u32) -> u64 {
    (**self).compute_size_explicit(field_number)
  }

  fn serialize_explicit(&self, field_number: u32, os: &mut CodedOutputStream<'_>) -> Result<()> {
    (**self).serialize_explicit(field_number, os)
  }
}

// &'static str implementation is special in that we can only serialize it.
impl ProtoType for &'static str {
  fn wire_type() -> WireType {
    WireType::LengthDelimited
  }

  fn canonical_type() -> CanonicalType {
    CanonicalType::String
  }
}

impl ProtoFieldSerialize for &'static str {
  fn compute_size(&self, field_number: u32) -> u64 {
    protobuf::rt::string_size(field_number, self)
  }

  fn serialize(&self, field_number: u32, os: &mut CodedOutputStream<'_>) -> Result<()> {
    os.write_string(field_number, self)?;
    Ok(())
  }

  fn compute_size_explicit(&self, field_number: u32) -> u64 {
    // Strings always have explicit presence (empty string is different from not-present), so
    // explicit and implicit are the same
    protobuf::rt::string_size(field_number, self)
  }

  fn serialize_explicit(&self, field_number: u32, os: &mut CodedOutputStream<'_>) -> Result<()> {
    // Strings always have explicit presence (empty string is different from not-present), so
    // explicit and implicit are the same
    os.write_string(field_number, self)?;
    Ok(())
  }
}

/// A timestamp stored as microseconds since Unix epoch (uint64).
///
/// This serializes an `OffsetDateTime` as a uint64 representing microseconds since Unix epoch.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct TimestampMicros(pub time::OffsetDateTime);

impl Default for TimestampMicros {
  fn default() -> Self {
    Self(time::OffsetDateTime::UNIX_EPOCH)
  }
}

impl TimestampMicros {
  /// Creates a new `TimestampMicros` from an `OffsetDateTime`.
  #[must_use]
  pub const fn new(datetime: time::OffsetDateTime) -> Self {
    Self(datetime)
  }

  /// Returns the inner `OffsetDateTime`.
  #[must_use]
  pub const fn into_inner(self) -> time::OffsetDateTime {
    self.0
  }

  /// Returns a reference to the inner `OffsetDateTime`.
  #[must_use]
  pub const fn as_inner(&self) -> &time::OffsetDateTime {
    &self.0
  }

  /// Converts the timestamp to microseconds since Unix epoch.
  #[must_use]
  pub fn as_micros(&self) -> i64 {
    self.0.unix_timestamp_micros()
  }

  /// Creates a `TimestampMicros` from microseconds since Unix epoch.
  pub fn from_micros(micros: i64) -> anyhow::Result<Self> {
    Ok(Self(time::OffsetDateTime::from_unix_timestamp_micros(
      micros,
    )?))
  }
}

impl std::ops::Deref for TimestampMicros {
  type Target = time::OffsetDateTime;

  fn deref(&self) -> &Self::Target {
    &self.0
  }
}

impl From<time::OffsetDateTime> for TimestampMicros {
  fn from(datetime: time::OffsetDateTime) -> Self {
    Self(datetime)
  }
}

impl From<TimestampMicros> for time::OffsetDateTime {
  fn from(timestamp: TimestampMicros) -> Self {
    timestamp.0
  }
}

impl ProtoType for TimestampMicros {
  fn wire_type() -> WireType {
    WireType::Varint
  }

  fn canonical_type() -> CanonicalType {
    // TimestampMicros serializes as i64 (microseconds since epoch)
    CanonicalType::I64
  }
}

impl ProtoFieldSerialize for TimestampMicros {
  fn compute_size(&self, field_number: u32) -> u64 {
    self.as_micros().compute_size(field_number)
  }

  fn serialize(&self, field_number: u32, os: &mut CodedOutputStream<'_>) -> Result<()> {
    self.as_micros().serialize(field_number, os)
  }

  fn compute_size_explicit(&self, field_number: u32) -> u64 {
    self.as_micros().compute_size_explicit(field_number)
  }

  fn serialize_explicit(&self, field_number: u32, os: &mut CodedOutputStream<'_>) -> Result<()> {
    self.as_micros().serialize_explicit(field_number, os)
  }
}

impl ProtoFieldDeserialize for TimestampMicros {
  fn deserialize(is: &mut CodedInputStream<'_>) -> Result<Self> {
    Self::from_micros(i64::deserialize(is)?)
  }
}

crate::impl_proto_map!(
  std::collections::HashMap<K, V, S>,
  K,
  V,
  where S: std::hash::BuildHasher + Default
);

crate::impl_proto_map!(ahash::AHashMap<K, V>, K, V);

impl_proto_repeated!(std::collections::BTreeSet<T>, T, Ord);

impl_proto_repeated!(Vec<T>, T);

// Implementation for ordered_float::NotNan<f64>
impl<T: ProtoType> ProtoType for ordered_float::NotNan<T> {
  fn wire_type() -> WireType {
    WireType::Fixed64
  }

  fn canonical_type() -> CanonicalType {
    T::canonical_type()
  }
}

impl<T: ProtoFieldSerialize + FloatCore> ProtoFieldSerialize for ordered_float::NotNan<T> {
  fn compute_size(&self, field_number: u32) -> u64 {
    self.as_ref().compute_size(field_number)
  }

  fn serialize(&self, field_number: u32, os: &mut CodedOutputStream<'_>) -> Result<()> {
    self.as_ref().serialize(field_number, os)
  }

  fn compute_size_explicit(&self, field_number: u32) -> u64 {
    self.as_ref().compute_size_explicit(field_number)
  }

  fn serialize_explicit(&self, field_number: u32, os: &mut CodedOutputStream<'_>) -> Result<()> {
    self.as_ref().serialize_explicit(field_number, os)
  }
}

impl<T: ProtoFieldDeserialize + FloatCore> ProtoFieldDeserialize for ordered_float::NotNan<T> {
  fn deserialize(is: &mut CodedInputStream<'_>) -> Result<Self> {
    Ok(Self::new(T::deserialize(is)?)?)
  }
}

// Unit type () implementation - serialize as nothing (0 bytes)
impl ProtoType for () {
  fn wire_type() -> WireType {
    WireType::LengthDelimited
  }

  fn canonical_type() -> CanonicalType {
    // Unit type serializes as an empty message
    CanonicalType::Message
  }
}

impl ProtoFieldSerialize for () {
  fn compute_size(&self, field_number: u32) -> u64 {
    // Unit type serializes as an empty length-delimited field
    protobuf::rt::tag_size(field_number) + 1 // tag + length(0)
  }

  fn serialize(&self, field_number: u32, os: &mut CodedOutputStream<'_>) -> Result<()> {
    os.write_tag(field_number, WireType::LengthDelimited)?;
    os.write_raw_varint32(0)?; // length = 0
    Ok(())
  }

  fn compute_size_explicit(&self, field_number: u32) -> u64 {
    // Unit type always has explicit presence (it's either present or not), so explicit and
    // implicit are the same
    protobuf::rt::tag_size(field_number) + 1 // tag + length(0)
  }

  fn serialize_explicit(&self, field_number: u32, os: &mut CodedOutputStream<'_>) -> Result<()> {
    // Unit type always has explicit presence (it's either present or not), so explicit and
    // implicit are the same
    os.write_tag(field_number, WireType::LengthDelimited)?;
    os.write_raw_varint32(0)?; // length = 0
    Ok(())
  }
}

impl ProtoFieldDeserialize for () {
  fn deserialize(is: &mut CodedInputStream<'_>) -> Result<Self> {
    // Read the length (should be 0)
    let _len = is.read_raw_varint32()?;
    Ok(())
  }
}
