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

//! Macros for implementing protobuf serialization traits.

/// Implements `ProtoFieldSerialize` and `ProtoFieldDeserialize` for repeated field collections.
///
/// This macro reduces boilerplate for collection types like `Vec<T>`, `BTreeSet<T>`, and
/// `TinySet<T>` by generating the standard repeated field serialization logic.
///
/// # Usage
///
/// ```ignore
/// // For Vec<T> (no additional trait bounds)
/// impl_proto_repeated!(Vec<T>, T);
///
/// // For BTreeSet<T> (requires Ord)
/// impl_proto_repeated!(std::collections::BTreeSet<T>, T, Ord);
///
/// // For TinySet<T> (requires PartialEq)
/// impl_proto_repeated!(TinySet<T>, T, PartialEq);
/// ```
///
/// # How it works
///
/// The macro generates implementations that:
/// - Serialize each element with the same field number (protobuf repeated fields)
/// - Deserialize returns a collection with a single element
/// - The proc macro uses `merge_repeated` to extend the target collection
///
/// # Variants
///
/// - `impl_proto_repeated!($collection:ty, $item:ident)` - No additional bounds (e.g., `Vec<T>`)
/// - `impl_proto_repeated!($collection:ty, $item:ident, $bounds:path)` - With bounds (e.g.,
///   `BTreeSet<T>` requires `Ord`)
#[macro_export]
macro_rules! impl_proto_repeated {
  // Version with additional trait bounds (e.g., Ord for BTreeSet)
  ($collection:ty, $item:ident, $bounds:path) => {
    impl<$item: $crate::serialization::ProtoType + $bounds> $crate::serialization::ProtoType
      for $collection
    {
      fn wire_type() -> protobuf::rt::WireType {
        $item::wire_type()
      }
    }

    impl<$item: $crate::serialization::ProtoFieldSerialize + $bounds>
      $crate::serialization::ProtoFieldSerialize for $collection
    {
      fn compute_size(&self, field_number: u32) -> u64 {
        self
          .iter()
          .map(|item| item.compute_size(field_number))
          .sum()
      }

      fn serialize(
        &self,
        field_number: u32,
        os: &mut protobuf::CodedOutputStream<'_>,
      ) -> anyhow::Result<()> {
        for item in self {
          item.serialize(field_number, os)?;
        }
        Ok(())
      }
    }

    impl<$item: $crate::serialization::ProtoFieldDeserialize + $bounds>
      $crate::serialization::ProtoFieldDeserialize for $collection
    where
      $collection: Default + Extend<$item>,
    {
      fn deserialize(is: &mut protobuf::CodedInputStream<'_>) -> anyhow::Result<Self> {
        // For repeated fields, deserialize returns a collection with a single element.
        // The macro will call merge_repeated to extend the target collection.
        let mut result = Self::default();
        result.extend(std::iter::once($item::deserialize(is)?));
        Ok(result)
      }
    }
  };

  // Version without additional trait bounds (for Vec)
  ($collection:ty, $item:ident) => {
    impl<$item: $crate::serialization::ProtoType> $crate::serialization::ProtoType for $collection {
      fn wire_type() -> protobuf::rt::WireType {
        $item::wire_type()
      }
    }

    impl<$item: $crate::serialization::ProtoFieldSerialize>
      $crate::serialization::ProtoFieldSerialize for $collection
    {
      fn compute_size(&self, field_number: u32) -> u64 {
        self
          .iter()
          .map(|item| item.compute_size(field_number))
          .sum()
      }

      fn serialize(
        &self,
        field_number: u32,
        os: &mut protobuf::CodedOutputStream<'_>,
      ) -> anyhow::Result<()> {
        for item in self {
          item.serialize(field_number, os)?;
        }
        Ok(())
      }
    }

    impl<$item: $crate::serialization::ProtoFieldDeserialize>
      $crate::serialization::ProtoFieldDeserialize for $collection
    where
      $collection: Default + Extend<$item>,
    {
      fn deserialize(is: &mut protobuf::CodedInputStream<'_>) -> anyhow::Result<Self> {
        // For repeated fields, deserialize returns a collection with a single element.
        // The macro will call merge_repeated to extend the target collection.
        let mut result = Self::default();
        result.extend(std::iter::once($item::deserialize(is)?));
        Ok(result)
      }
    }
  };
}

// Apply the macro to standard library types
impl_proto_repeated!(Vec<T>, T);
impl_proto_repeated!(std::collections::BTreeSet<T>, T, Ord);
