// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

// Copyright (C) 2024 Bitdrift, Inc.
// SPDX-License-Identifier: Apache-2.0 OR PolyForm-Shield-1.0.0
// You may obtain a copy of the license at
// https://www.apache.org/licenses/LICENSE-2.0
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

//! Helper functions for implementing protobuf map serialization.
//!
//! These functions provide reusable implementations for map-like types (`HashMap`, `TinyMap`, etc.)
//! without requiring macro-based code generation.

use crate::serialization::{ProtoFieldDeserialize, ProtoFieldSerialize};
use anyhow::Result;
use protobuf::rt::WireType;
use protobuf::{CodedInputStream, CodedOutputStream};

/// Computes the protobuf wire size for a map field.
///
/// This helper can be used by any map-like type that implements `IntoIterator<Item = (&K, &V)>`.
///
/// # Protobuf encoding
///
/// Maps in protobuf are encoded as repeated messages, where each message contains:
/// - Field 1: key
/// - Field 2: value
///
/// # Example
///
/// ```ignore
/// impl<K, V> ProtoFieldSerialize for MyMap<K, V>
/// where
///   K: ProtoFieldSerialize,
///   V: ProtoFieldSerialize,
/// {
///   fn compute_size(&self, field_number: u32) -> u64 {
///     compute_map_size(self, field_number)
///   }
///   // ...
/// }
/// ```
pub fn compute_map_size<'a, K, V>(
  entries: impl IntoIterator<Item = (&'a K, &'a V)>,
  field_number: u32,
) -> u64
where
  K: ProtoFieldSerialize + 'a,
  V: ProtoFieldSerialize + 'a,
{
  let mut total_size = 0;
  let tag_size = protobuf::rt::tag_size(field_number);

  for (k, v) in entries {
    let key_size = k.compute_size(1);
    let value_size = v.compute_size(2);
    let entry_size = key_size + value_size;
    total_size += tag_size + protobuf::rt::compute_raw_varint64_size(entry_size) + entry_size;
  }
  total_size
}

/// Serializes a map field to a protobuf stream.
///
/// This helper can be used by any map-like type that implements `IntoIterator<Item = (&K, &V)>`.
///
/// # Protobuf encoding
///
/// Each map entry is written as a length-delimited message containing:
/// - Tag for the map field
/// - Length of the entry message
/// - Key (field 1)
/// - Value (field 2)
///
/// # Example
///
/// ```ignore
/// impl<K, V> ProtoFieldSerialize for MyMap<K, V>
/// where
///   K: ProtoFieldSerialize,
///   V: ProtoFieldSerialize,
/// {
///   fn serialize(&self, field_number: u32, os: &mut CodedOutputStream<'_>) -> Result<()> {
///     serialize_map(self, field_number, os)
///   }
///   // ...
/// }
/// ```
pub fn serialize_map<'a, K, V>(
  entries: impl IntoIterator<Item = (&'a K, &'a V)>,
  field_number: u32,
  os: &mut CodedOutputStream<'_>,
) -> Result<()>
where
  K: ProtoFieldSerialize + 'a,
  V: ProtoFieldSerialize + 'a,
{
  for (k, v) in entries {
    os.write_tag(field_number, WireType::LengthDelimited)?;

    let key_size = k.compute_size(1);
    let value_size = v.compute_size(2);
    let entry_size = key_size + value_size;

    #[allow(clippy::cast_possible_truncation)]
    let entry_size_u32 = entry_size as u32;
    os.write_raw_varint32(entry_size_u32)?;

    k.serialize(1, os)?;
    v.serialize(2, os)?;
  }
  Ok(())
}

/// Deserializes a single map entry from a protobuf stream.
///
/// Protobuf maps are encoded as repeated messages with key (field 1) and value (field 2).
/// This function reads one such entry and returns the key-value pair.
///
/// According to the protobuf specification, if a key or value is missing from a map entry,
/// the default value for that type should be used. This matches standard protobuf behavior.
///
/// # Usage
///
/// The `deserialize` implementation for map types should call this function and insert
/// the returned entry into the map. Since protobuf maps can have multiple entries, the
/// proc macro will call `merge_repeated` to merge entries together.
///
/// # Example
///
/// ```ignore
/// impl<K, V> ProtoFieldDeserialize for MyMap<K, V>
/// where
///   K: ProtoFieldDeserialize + Default,
///   V: ProtoFieldDeserialize + Default,
/// {
///   fn deserialize(is: &mut CodedInputStream<'_>) -> Result<Self> {
///     let (key, value) = deserialize_map_entry(is)?;
///     let mut map = Self::default();
///     map.insert(key, value);
///     Ok(map)
///   }
/// }
/// ```
pub fn deserialize_map_entry<K, V>(is: &mut CodedInputStream<'_>) -> Result<(K, V)>
where
  K: ProtoFieldDeserialize + Default,
  V: ProtoFieldDeserialize + Default,
{
  let len = is.read_raw_varint32()?;
  let old_limit = is.push_limit(u64::from(len))?;

  let mut key: Option<K> = None;
  let mut value: Option<V> = None;

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
          "Unknown wire type {wire_type_bits} in map entry (tag={tag}, field={field_number})"
        ));
      },
    };

    match field_number {
      1 => key = Some(K::deserialize(is)?),
      2 => value = Some(V::deserialize(is)?),
      _ => is.skip_field(wire_type)?,
    }
  }

  is.pop_limit(old_limit);

  // Per protobuf spec, if key or value is missing, use the type's default value
  let key = key.unwrap_or_default();
  let value = value.unwrap_or_default();

  Ok((key, value))
}
