// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

//! Wire format types and traits for protobuf serialization.
//!
//! This module defines unit structs for each protobuf wire format type and a trait that
//! centralizes the serialization logic for each type. This allows the actual Rust type
//! implementations to simply delegate to their canonical wire format type.
//!
//! # Design
//!
//! Each protobuf scalar type has a corresponding unit struct (e.g., `U32`, `I64`, `String_`)
//! that implements [`WireFormat`]. This trait provides:
//! - The wire type (varint, fixed32, fixed64, length-delimited)
//! - The canonical type for validation
//! - Size computation
//! - Serialization
//! - Deserialization
//!
//! Rust types then implement [`super::ProtoType`] with an associated `Canonical` type that
//! points to the appropriate wire format struct.

use super::validation::CanonicalType;
use anyhow::Result;
use protobuf::rt::WireType;
use protobuf::{CodedInputStream, CodedOutputStream};

/// Trait for protobuf wire format types.
///
/// Each implementation represents a specific protobuf scalar type and encapsulates all the
/// serialization logic for that type.
pub trait WireFormat {
  /// The Rust type that this wire format serializes/deserializes.
  type Value: ?Sized;

  /// The protobuf wire type used for encoding.
  const WIRE_TYPE: WireType;

  /// The canonical type for validation against protobuf descriptors.
  const CANONICAL: CanonicalType;

  /// Computes the serialized size of the value including the tag.
  fn compute_size(value: &Self::Value, field_number: u32) -> u64;

  /// Serializes the value to the output stream including the tag.
  fn write(value: &Self::Value, field_number: u32, os: &mut CodedOutputStream<'_>) -> Result<()>;

  /// Deserializes a value from the input stream (tag already consumed).
  fn read(is: &mut CodedInputStream<'_>) -> Result<Self::Value>
  where
    Self::Value: Sized;
}

// =============================================================================
// Scalar Types
// =============================================================================

/// Wire format for unsigned 32-bit integers (protobuf `uint32`).
pub struct U32;

impl WireFormat for U32 {
  type Value = u32;
  const WIRE_TYPE: WireType = WireType::Varint;
  const CANONICAL: CanonicalType = CanonicalType::U32;

  fn compute_size(value: &u32, field_number: u32) -> u64 {
    protobuf::rt::uint32_size(field_number, *value)
  }

  fn write(value: &u32, field_number: u32, os: &mut CodedOutputStream<'_>) -> Result<()> {
    os.write_uint32(field_number, *value)?;
    Ok(())
  }

  fn read(is: &mut CodedInputStream<'_>) -> Result<u32> {
    Ok(is.read_uint32()?)
  }
}

/// Wire format for unsigned 64-bit integers (protobuf `uint64`).
pub struct U64;

impl WireFormat for U64 {
  type Value = u64;
  const WIRE_TYPE: WireType = WireType::Varint;
  const CANONICAL: CanonicalType = CanonicalType::U64;

  fn compute_size(value: &u64, field_number: u32) -> u64 {
    protobuf::rt::uint64_size(field_number, *value)
  }

  fn write(value: &u64, field_number: u32, os: &mut CodedOutputStream<'_>) -> Result<()> {
    os.write_uint64(field_number, *value)?;
    Ok(())
  }

  fn read(is: &mut CodedInputStream<'_>) -> Result<u64> {
    Ok(is.read_uint64()?)
  }
}

/// Wire format for signed 32-bit integers (protobuf `int32`).
pub struct I32;

impl WireFormat for I32 {
  type Value = i32;
  const WIRE_TYPE: WireType = WireType::Varint;
  const CANONICAL: CanonicalType = CanonicalType::I32;

  fn compute_size(value: &i32, field_number: u32) -> u64 {
    protobuf::rt::int32_size(field_number, *value)
  }

  fn write(value: &i32, field_number: u32, os: &mut CodedOutputStream<'_>) -> Result<()> {
    os.write_int32(field_number, *value)?;
    Ok(())
  }

  fn read(is: &mut CodedInputStream<'_>) -> Result<i32> {
    Ok(is.read_int32()?)
  }
}

/// Wire format for signed 64-bit integers (protobuf `int64`).
pub struct I64;

impl WireFormat for I64 {
  type Value = i64;
  const WIRE_TYPE: WireType = WireType::Varint;
  const CANONICAL: CanonicalType = CanonicalType::I64;

  fn compute_size(value: &i64, field_number: u32) -> u64 {
    protobuf::rt::int64_size(field_number, *value)
  }

  fn write(value: &i64, field_number: u32, os: &mut CodedOutputStream<'_>) -> Result<()> {
    os.write_int64(field_number, *value)?;
    Ok(())
  }

  fn read(is: &mut CodedInputStream<'_>) -> Result<i64> {
    Ok(is.read_int64()?)
  }
}

/// Wire format for 32-bit floating point (protobuf `float`).
pub struct F32;

impl WireFormat for F32 {
  type Value = f32;
  const WIRE_TYPE: WireType = WireType::Fixed32;
  const CANONICAL: CanonicalType = CanonicalType::F32;

  fn compute_size(_value: &f32, field_number: u32) -> u64 {
    protobuf::rt::tag_size(field_number) + 4
  }

  fn write(value: &f32, field_number: u32, os: &mut CodedOutputStream<'_>) -> Result<()> {
    os.write_float(field_number, *value)?;
    Ok(())
  }

  fn read(is: &mut CodedInputStream<'_>) -> Result<f32> {
    Ok(is.read_float()?)
  }
}

/// Wire format for 64-bit floating point (protobuf `double`).
pub struct F64;

impl WireFormat for F64 {
  type Value = f64;
  const WIRE_TYPE: WireType = WireType::Fixed64;
  const CANONICAL: CanonicalType = CanonicalType::F64;

  fn compute_size(_value: &f64, field_number: u32) -> u64 {
    protobuf::rt::tag_size(field_number) + 8
  }

  fn write(value: &f64, field_number: u32, os: &mut CodedOutputStream<'_>) -> Result<()> {
    os.write_double(field_number, *value)?;
    Ok(())
  }

  fn read(is: &mut CodedInputStream<'_>) -> Result<f64> {
    Ok(is.read_double()?)
  }
}

/// Wire format for booleans (protobuf `bool`).
pub struct Bool;

impl WireFormat for Bool {
  type Value = bool;
  const WIRE_TYPE: WireType = WireType::Varint;
  const CANONICAL: CanonicalType = CanonicalType::Bool;

  fn compute_size(_value: &bool, field_number: u32) -> u64 {
    // bool is always 1 byte payload + tag
    protobuf::rt::tag_size(field_number) + 1
  }

  fn write(value: &bool, field_number: u32, os: &mut CodedOutputStream<'_>) -> Result<()> {
    os.write_bool(field_number, *value)?;
    Ok(())
  }

  fn read(is: &mut CodedInputStream<'_>) -> Result<bool> {
    Ok(is.read_bool()?)
  }
}

// =============================================================================
// Length-Delimited Types
// =============================================================================

/// Wire format for strings (protobuf `string`).
///
/// This type uses `String` as the value type for full serialization support.
/// For serializing string references, use `StringOwned` with `&str` coerced to `&String`.
pub struct String_;

impl WireFormat for String_ {
  type Value = String;
  const WIRE_TYPE: WireType = WireType::LengthDelimited;
  const CANONICAL: CanonicalType = CanonicalType::String;

  fn compute_size(value: &String, field_number: u32) -> u64 {
    protobuf::rt::string_size(field_number, value.as_str())
  }

  fn write(value: &String, field_number: u32, os: &mut CodedOutputStream<'_>) -> Result<()> {
    os.write_string(field_number, value.as_str())?;
    Ok(())
  }

  fn read(is: &mut CodedInputStream<'_>) -> Result<String> {
    Ok(is.read_string()?)
  }
}

/// Wire format for owned strings (protobuf `string`).
///
/// This is an alias for `String_` for clarity when the owned nature is important.
pub type StringOwned = String_;

/// Wire format for bytes (protobuf `bytes`).
///
/// This type uses `Vec<u8>` as the value type for full serialization support.
pub struct Bytes;

impl WireFormat for Bytes {
  type Value = Vec<u8>;
  const WIRE_TYPE: WireType = WireType::LengthDelimited;
  const CANONICAL: CanonicalType = CanonicalType::Bytes;

  fn compute_size(value: &Vec<u8>, field_number: u32) -> u64 {
    protobuf::rt::bytes_size(field_number, value.as_slice())
  }

  fn write(value: &Vec<u8>, field_number: u32, os: &mut CodedOutputStream<'_>) -> Result<()> {
    os.write_bytes(field_number, value.as_slice())?;
    Ok(())
  }

  fn read(is: &mut CodedInputStream<'_>) -> Result<Vec<u8>> {
    Ok(is.read_bytes()?)
  }
}

/// Wire format for owned bytes (protobuf `bytes`).
///
/// This is an alias for `Bytes` for clarity when the owned nature is important.
pub type BytesOwned = Bytes;

// =============================================================================
// Composite Type Markers
// =============================================================================

/// Marker for nested message types (protobuf `message`).
///
/// This doesn't fully implement `WireFormat` because message serialization requires the actual
/// message type. It's used as a marker for the wire type and canonical type.
pub struct Message;

impl Message {
  /// The wire type for messages.
  pub const WIRE_TYPE: WireType = WireType::LengthDelimited;
  /// The canonical type for messages.
  pub const CANONICAL: CanonicalType = CanonicalType::Message;
}

/// Marker for enum types (protobuf `enum`).
///
/// Enums use varint encoding like i32, but are semantically different for validation.
pub struct Enum;

impl WireFormat for Enum {
  type Value = i32;
  const WIRE_TYPE: WireType = WireType::Varint;
  const CANONICAL: CanonicalType = CanonicalType::Enum;

  fn compute_size(value: &i32, field_number: u32) -> u64 {
    protobuf::rt::int32_size(field_number, *value)
  }

  fn write(value: &i32, field_number: u32, os: &mut CodedOutputStream<'_>) -> Result<()> {
    os.write_int32(field_number, *value)?;
    Ok(())
  }

  fn read(is: &mut CodedInputStream<'_>) -> Result<i32> {
    Ok(is.read_int32()?)
  }
}
