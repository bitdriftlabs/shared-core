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
  ProtoFieldDeserialize,
  ProtoFieldSerialize,
  ProtoType,
  RepeatedFieldDeserialize,
  compute_map_size,
  deserialize_map_entry,
  serialize_map,
};
use anyhow::Result;
use bd_time::OffsetDateTimeExt;
use protobuf::rt::WireType;
use protobuf::{CodedInputStream, CodedOutputStream};

impl ProtoType for String {
  fn wire_type() -> WireType {
    WireType::LengthDelimited
  }
}

impl ProtoFieldSerialize for String {
  fn compute_size(&self, field_number: u32) -> u64 {
    protobuf::rt::string_size(field_number, self)
  }

  fn serialize(&self, field_number: u32, os: &mut CodedOutputStream<'_>) -> Result<()> {
    os.write_string(field_number, self)?;
    Ok(())
  }
}

impl ProtoFieldDeserialize for String {
  fn deserialize(is: &mut CodedInputStream<'_>) -> Result<Self> {
    Ok(is.read_string()?)
  }
}

impl ProtoType for u32 {
  fn wire_type() -> WireType {
    WireType::Varint
  }
}

impl ProtoFieldSerialize for u32 {
  fn compute_size(&self, field_number: u32) -> u64 {
    protobuf::rt::uint32_size(field_number, *self)
  }

  fn serialize(&self, field_number: u32, os: &mut CodedOutputStream<'_>) -> Result<()> {
    os.write_uint32(field_number, *self)?;
    Ok(())
  }
}

impl ProtoFieldDeserialize for u32 {
  fn deserialize(is: &mut CodedInputStream<'_>) -> Result<Self> {
    Ok(is.read_uint32()?)
  }
}

impl ProtoType for u64 {
  fn wire_type() -> WireType {
    WireType::Varint
  }
}

impl ProtoFieldSerialize for u64 {
  fn compute_size(&self, field_number: u32) -> u64 {
    protobuf::rt::uint64_size(field_number, *self)
  }

  fn serialize(&self, field_number: u32, os: &mut CodedOutputStream<'_>) -> Result<()> {
    os.write_uint64(field_number, *self)?;
    Ok(())
  }
}

impl ProtoFieldDeserialize for u64 {
  fn deserialize(is: &mut CodedInputStream<'_>) -> Result<Self> {
    Ok(is.read_uint64()?)
  }
}

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

impl ProtoType for i32 {
  fn wire_type() -> WireType {
    WireType::Varint
  }
}

impl ProtoFieldSerialize for i32 {
  fn compute_size(&self, field_number: u32) -> u64 {
    protobuf::rt::int32_size(field_number, *self)
  }

  fn serialize(&self, field_number: u32, os: &mut CodedOutputStream<'_>) -> Result<()> {
    os.write_int32(field_number, *self)?;
    Ok(())
  }
}

impl ProtoFieldDeserialize for i32 {
  fn deserialize(is: &mut CodedInputStream<'_>) -> Result<Self> {
    Ok(is.read_int32()?)
  }
}

impl ProtoType for i64 {
  fn wire_type() -> WireType {
    WireType::Varint
  }
}

impl ProtoFieldSerialize for i64 {
  fn compute_size(&self, field_number: u32) -> u64 {
    protobuf::rt::int64_size(field_number, *self)
  }

  fn serialize(&self, field_number: u32, os: &mut CodedOutputStream<'_>) -> Result<()> {
    os.write_int64(field_number, *self)?;
    Ok(())
  }
}

impl ProtoFieldDeserialize for i64 {
  fn deserialize(is: &mut CodedInputStream<'_>) -> Result<Self> {
    Ok(is.read_int64()?)
  }
}

impl ProtoType for bool {
  fn wire_type() -> WireType {
    WireType::Varint
  }
}

impl ProtoFieldSerialize for bool {
  fn compute_size(&self, field_number: u32) -> u64 {
    // bool is varint, so compute_raw_varint64_size of 1 + tag size
    let tag_size = protobuf::rt::tag_size(field_number);
    // bool payload is always 1 byte
    tag_size + 1
  }

  fn serialize(&self, field_number: u32, os: &mut CodedOutputStream<'_>) -> Result<()> {
    os.write_bool(field_number, *self)?;
    Ok(())
  }
}

impl ProtoFieldDeserialize for bool {
  fn deserialize(is: &mut CodedInputStream<'_>) -> Result<Self> {
    Ok(is.read_bool()?)
  }
}

impl ProtoType for f64 {
  fn wire_type() -> WireType {
    WireType::Fixed64
  }
}

impl ProtoFieldSerialize for f64 {
  fn compute_size(&self, field_number: u32) -> u64 {
    // double is always 8 bytes + tag size
    let tag_size = protobuf::rt::tag_size(field_number);
    tag_size + 8
  }

  fn serialize(&self, field_number: u32, os: &mut CodedOutputStream<'_>) -> Result<()> {
    os.write_double(field_number, *self)?;
    Ok(())
  }
}

impl ProtoFieldDeserialize for f64 {
  fn deserialize(is: &mut CodedInputStream<'_>) -> Result<Self> {
    Ok(is.read_double()?)
  }
}

impl ProtoType for Vec<u8> {
  fn wire_type() -> WireType {
    WireType::LengthDelimited
  }
}

impl ProtoFieldSerialize for Vec<u8> {
  fn compute_size(&self, field_number: u32) -> u64 {
    protobuf::rt::bytes_size(field_number, self)
  }

  fn serialize(&self, field_number: u32, os: &mut CodedOutputStream<'_>) -> Result<()> {
    os.write_bytes(field_number, self)?;
    Ok(())
  }
}

impl ProtoFieldDeserialize for Vec<u8> {
  fn deserialize(is: &mut CodedInputStream<'_>) -> Result<Self> {
    Ok(is.read_bytes()?)
  }
}

// Blanket implementation for Option<T>
// Note: For deserialization, this is slightly tricky because the tag handles the "None" case (by
// absence). This deserialize implementation assumes we ARE reading a value.

impl<T: ProtoType> ProtoType for Option<T> {
  fn wire_type() -> WireType {
    T::wire_type()
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
}

impl<T: ProtoFieldSerialize> ProtoFieldSerialize for &T {
  fn compute_size(&self, field_number: u32) -> u64 {
    (**self).compute_size(field_number)
  }

  fn serialize(&self, field_number: u32, os: &mut CodedOutputStream<'_>) -> Result<()> {
    (**self).serialize(field_number, os)
  }
}

// Box<T> implementation - transparent wrapper
impl<T: ProtoType> ProtoType for Box<T> {
  fn wire_type() -> WireType {
    T::wire_type()
  }
}

impl<T: ProtoFieldSerialize> ProtoFieldSerialize for Box<T> {
  fn compute_size(&self, field_number: u32) -> u64 {
    (**self).compute_size(field_number)
  }

  fn serialize(&self, field_number: u32, os: &mut CodedOutputStream<'_>) -> Result<()> {
    (**self).serialize(field_number, os)
  }
}

impl<T: ProtoFieldDeserialize> ProtoFieldDeserialize for Box<T> {
  fn deserialize(is: &mut CodedInputStream<'_>) -> Result<Self> {
    Ok(Self::new(T::deserialize(is)?))
  }
}

// ============================================================================
// String types
// ============================================================================

// Implementation for Cow<'a, str>
impl ProtoType for std::borrow::Cow<'_, str> {
  fn wire_type() -> WireType {
    WireType::LengthDelimited
  }
}

impl ProtoFieldSerialize for std::borrow::Cow<'_, str> {
  fn compute_size(&self, field_number: u32) -> u64 {
    protobuf::rt::string_size(field_number, self.as_ref())
  }

  fn serialize(&self, field_number: u32, os: &mut CodedOutputStream<'_>) -> Result<()> {
    os.write_string(field_number, self.as_ref())?;
    Ok(())
  }
}

impl ProtoFieldDeserialize for std::borrow::Cow<'_, str> {
  fn deserialize(is: &mut CodedInputStream<'_>) -> Result<Self> {
    Ok(std::borrow::Cow::Owned(is.read_string()?))
  }
}

impl ProtoType for std::sync::Arc<str> {
  fn wire_type() -> WireType {
    WireType::LengthDelimited
  }
}

impl ProtoFieldSerialize for std::sync::Arc<str> {
  fn compute_size(&self, field_number: u32) -> u64 {
    protobuf::rt::string_size(field_number, self.as_ref())
  }

  fn serialize(&self, field_number: u32, os: &mut CodedOutputStream<'_>) -> Result<()> {
    os.write_string(field_number, self.as_ref())?;
    Ok(())
  }
}

impl ProtoFieldDeserialize for std::sync::Arc<str> {
  fn deserialize(is: &mut CodedInputStream<'_>) -> Result<Self> {
    Ok(is.read_string()?.into())
  }
}

impl ProtoType for &'static str {
  fn wire_type() -> WireType {
    WireType::LengthDelimited
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
  #[allow(clippy::cast_possible_truncation, clippy::cast_sign_loss)]
  pub fn as_micros(&self) -> u64 {
    self.0.unix_timestamp_micros() as u64
  }

  /// Creates a `TimestampMicros` from microseconds since Unix epoch.
  pub fn from_micros(micros: u64) -> anyhow::Result<Self> {
    Ok(Self(time::OffsetDateTime::from_unix_timestamp_nanos(
      i128::from(micros) * 1000,
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
}

impl ProtoFieldSerialize for TimestampMicros {
  fn compute_size(&self, field_number: u32) -> u64 {
    protobuf::rt::uint64_size(field_number, self.as_micros())
  }

  fn serialize(&self, field_number: u32, os: &mut CodedOutputStream<'_>) -> Result<()> {
    os.write_uint64(field_number, self.as_micros())?;
    Ok(())
  }
}

impl ProtoFieldDeserialize for TimestampMicros {
  fn deserialize(is: &mut CodedInputStream<'_>) -> Result<Self> {
    let micros = is.read_uint64()?;
    Self::from_micros(micros)
  }
}

// ============================================================================
// Collection types (HashMap, AHashMap)
// ============================================================================

impl<K, V, S: std::hash::BuildHasher> ProtoType for std::collections::HashMap<K, V, S> {
  fn wire_type() -> WireType {
    WireType::LengthDelimited
  }
}

impl<K, V, S: std::hash::BuildHasher> ProtoFieldSerialize for std::collections::HashMap<K, V, S>
where
  K: ProtoFieldSerialize + Eq + std::hash::Hash,
  V: ProtoFieldSerialize,
{
  fn compute_size(&self, field_number: u32) -> u64 {
    compute_map_size(self, field_number)
  }

  fn serialize(&self, field_number: u32, os: &mut CodedOutputStream<'_>) -> Result<()> {
    serialize_map(self, field_number, os)
  }
}

impl<K, V, S: std::hash::BuildHasher + Default> RepeatedFieldDeserialize
  for std::collections::HashMap<K, V, S>
where
  K: ProtoFieldDeserialize + Eq + std::hash::Hash + Default,
  V: ProtoFieldDeserialize + Default,
{
  type Element = (K, V);

  fn deserialize_element(is: &mut CodedInputStream<'_>) -> Result<Self::Element> {
    deserialize_map_entry(is)
  }

  fn add_element(&mut self, (key, value): Self::Element) {
    self.insert(key, value);
  }
}

// AHashMap is a newtype around HashMap with a different hasher, so we have to re-implement
// the traits for it and delegate to the inner type.

impl<K, V> ProtoType for ahash::AHashMap<K, V> {
  fn wire_type() -> WireType {
    WireType::LengthDelimited
  }
}

impl<K, V> ProtoFieldSerialize for ahash::AHashMap<K, V>
where
  K: ProtoFieldSerialize + Eq + std::hash::Hash,
  V: ProtoFieldSerialize,
{
  fn compute_size(&self, field_number: u32) -> u64 {
    let inner: &std::collections::HashMap<K, V, ahash::RandomState> = self;
    inner.compute_size(field_number)
  }

  fn serialize(&self, field_number: u32, os: &mut CodedOutputStream<'_>) -> Result<()> {
    let inner: &std::collections::HashMap<K, V, ahash::RandomState> = self;
    inner.serialize(field_number, os)
  }
}

impl<K, V> RepeatedFieldDeserialize for ahash::AHashMap<K, V>
where
  K: ProtoFieldDeserialize + Eq + std::hash::Hash + Default,
  V: ProtoFieldDeserialize + Default,
{
  type Element = (K, V);

  fn deserialize_element(is: &mut CodedInputStream<'_>) -> Result<Self::Element> {
    deserialize_map_entry(is)
  }

  fn add_element(&mut self, (key, value): Self::Element) {
    self.insert(key, value);
  }
}

// Note: Vec<u8> is intentionally NOT implemented here because it has special handling
// as a bytes field (single blob, not repeated elements).
// This implementation is for Vec<T> where T != u8, which are true repeated fields.
impl<T: ProtoType> ProtoType for Vec<T> {
  fn wire_type() -> WireType {
    T::wire_type()
  }
}

impl<T: ProtoFieldSerialize> ProtoFieldSerialize for Vec<T> {
  fn compute_size(&self, field_number: u32) -> u64 {
    self
      .iter()
      .map(|item| item.compute_size(field_number))
      .sum()
  }

  fn serialize(&self, field_number: u32, os: &mut CodedOutputStream<'_>) -> Result<()> {
    for item in self {
      item.serialize(field_number, os)?;
    }
    Ok(())
  }
}

impl<T> RepeatedFieldDeserialize for Vec<T>
where
  T: ProtoFieldDeserialize,
{
  type Element = T;

  fn deserialize_element(is: &mut CodedInputStream<'_>) -> Result<Self::Element> {
    T::deserialize(is)
  }

  fn add_element(&mut self, element: Self::Element) {
    self.push(element);
  }
}

// Implementation for BTreeSet<T> as a repeated field
impl<T: ProtoType + Ord> ProtoType for std::collections::BTreeSet<T> {
  fn wire_type() -> WireType {
    T::wire_type()
  }
}

impl<T: ProtoFieldSerialize + Ord> ProtoFieldSerialize for std::collections::BTreeSet<T> {
  fn compute_size(&self, field_number: u32) -> u64 {
    self
      .iter()
      .map(|item| item.compute_size(field_number))
      .sum()
  }

  fn serialize(&self, field_number: u32, os: &mut CodedOutputStream<'_>) -> Result<()> {
    for item in self {
      item.serialize(field_number, os)?;
    }
    Ok(())
  }
}

impl<T> crate::serialization::RepeatedFieldDeserialize for std::collections::BTreeSet<T>
where
  T: ProtoFieldDeserialize + Ord,
{
  type Element = T;

  fn deserialize_element(is: &mut CodedInputStream<'_>) -> Result<Self::Element> {
    T::deserialize(is)
  }

  fn add_element(&mut self, element: Self::Element) {
    self.insert(element);
  }
}


// ============================================================================
// Special types
// ============================================================================

// Implementation for LogType enum (int32)
// `LogType` is from `bd_proto::protos::logging::payload`.
impl ProtoType for bd_proto::protos::logging::payload::LogType {
  fn wire_type() -> WireType {
    WireType::Varint
  }
}

impl ProtoFieldSerialize for bd_proto::protos::logging::payload::LogType {
  fn compute_size(&self, field_number: u32) -> u64 {
    use protobuf::Enum;
    let val = self.value();
    if val == 0 {
      return 0;
    }
    protobuf::rt::int32_size(field_number, val)
  }

  fn serialize(&self, field_number: u32, os: &mut CodedOutputStream<'_>) -> Result<()> {
    use protobuf::Enum;
    let val = self.value();
    if val == 0 {
      return Ok(());
    }
    os.write_enum(field_number, val)?;
    Ok(())
  }
}

impl ProtoFieldDeserialize for bd_proto::protos::logging::payload::LogType {
  fn deserialize(is: &mut CodedInputStream<'_>) -> Result<Self> {
    use protobuf::Enum;
    let val = is.read_int32()?;
    Ok(Self::from_i32(val).unwrap_or(Self::NORMAL))
  }
}

// Implementation for ordered_float::NotNan<f64>
impl ProtoType for ordered_float::NotNan<f64> {
  fn wire_type() -> WireType {
    WireType::Fixed64
  }
}

impl ProtoFieldSerialize for ordered_float::NotNan<f64> {
  fn compute_size(&self, field_number: u32) -> u64 {
    // double is always 8 bytes + tag size
    let tag_size = protobuf::rt::tag_size(field_number);
    tag_size + 8
  }

  fn serialize(&self, field_number: u32, os: &mut CodedOutputStream<'_>) -> Result<()> {
    os.write_double(field_number, **self)?;
    Ok(())
  }
}

impl ProtoFieldDeserialize for ordered_float::NotNan<f64> {
  fn deserialize(is: &mut CodedInputStream<'_>) -> Result<Self> {
    Ok(Self::new(is.read_double()?)?)
  }
}

// Unit type () implementation - serialize as nothing (0 bytes)
impl ProtoType for () {
  fn wire_type() -> WireType {
    WireType::LengthDelimited
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
}

impl ProtoFieldDeserialize for () {
  fn deserialize(is: &mut CodedInputStream<'_>) -> Result<Self> {
    // Read the length (should be 0)
    let _len = is.read_raw_varint32()?;
    Ok(())
  }
}
